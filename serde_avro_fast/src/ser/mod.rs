//! Defines everything necessary for avro serialization
//!
//! # For advanced usage
//!
//! You typically want to use top-level functions such as
//! [`to_datum`](crate::to_datum) but access to this may be
//! necessary for more advanced usage.
//!
//! This gives manual access to the type that implements
//! [`serde::Serializer`]
//!
//! Such usage would go as follows:
//! ```
//! let schema: serde_avro_fast::Schema = r#"
//! {
//! 	"namespace": "test",
//! 	"type": "record",
//! 	"name": "Test",
//! 	"fields": [
//! 		{
//! 			"type": {
//! 				"type": "string"
//! 			},
//! 			"name": "field"
//! 		}
//! 	]
//! }
//! "#
//! .parse()
//! .expect("Failed to parse schema");
//!
//! #[derive(serde_derive::Serialize, Debug, PartialEq)]
//! struct Test<'a> {
//! 	field: &'a str,
//! }
//!
//! // Build the struct that will generally serve through serialization
//! let serializer_config = &mut serde_avro_fast::ser::SerializerConfig::new(&schema);
//! let mut serializer_state =
//! 	serde_avro_fast::ser::SerializerState::from_writer(Vec::new(), serializer_config);
//!
//! // It's not the `&mut SerializerState` that implements `serde::Serializer` directly, instead
//! // it is `DatumSerializer` (which is essentially an `&mut SerializerState` but not exactly
//! // because it also keeps track of the current schema node)
//! // We build it through `SerializerState::serializer`
//! serde::Serialize::serialize(&Test { field: "foo" }, serializer_state.serializer())
//! 	.expect("Failed to serialize");
//! let serialized = serializer_state.into_writer();
//!
//! assert_eq!(serialized, &[6, 102, 111, 111]);
//!
//! // reuse config & output buffer across serializations for ideal performance (~40% perf gain)
//! let mut serializer_state = serde_avro_fast::ser::SerializerState::from_writer(
//! 	{
//! 		let mut buf = serialized;
//! 		buf.clear();
//! 		buf
//! 	},
//! 	serializer_config,
//! );
//!
//! serde::Serialize::serialize(&Test { field: "bar" }, serializer_state.serializer())
//! 	.expect("Failed to serialize");
//! let serialized = serializer_state.into_writer();
//!
//! assert_eq!(serialized, &[6, b'b', b'a', b'r']);
//! ```

mod error;
mod serializer;

pub use {error::SerError, serializer::*};

use crate::schema::{self_referential::*, UnionVariantLookupKey};

use alloc::boxed::Box;
use alloc::vec::Vec;
use integer_encoding::VarInt;
use serde::ser::*;

/// All configuration and state necessary for the serialization to run
///
/// Notably holds the writer and a `&mut` [`SerializerConfig`].
///
/// Does not implement [`Serializer`] directly (use
/// [`.serializer`](Self::serializer) to obtain that).
pub struct SerializerState<'c, 's, W> {
	writer: W,
	/// Storing these here for reuse so that we can bypass the allocation,
	/// and statistically obtain buffers that are already the proper length
	/// (since we have used them for previous records)
	config: SerializerConfigRef<'c, 's>,
}

/// Schema + serialization buffers
///
/// It can be built from a schema:
/// ```
/// # use serde_avro_fast::{ser, Schema};
/// let schema: Schema = r#""int""#.parse().unwrap();
/// let serializer_config = &mut ser::SerializerConfig::new(&schema);
///
/// let mut serialized: Vec<u8> = serde_avro_fast::to_datum_vec(&3, serializer_config).unwrap();
/// assert_eq!(serialized, &[6]);
///
/// // reuse config & output buffer across serializations for ideal performance (~40% perf gain)
/// serialized.clear();
/// let serialized = serde_avro_fast::to_datum_vec(&4, serializer_config).unwrap();
/// assert_eq!(serialized, &[8]);
/// ```
pub struct SerializerConfig<'s> {
	buffers: Buffers,
	allow_slow_sequence_to_bytes: bool,
	/// This schema is the default when building a serializer (or otherwise
	/// calling `.schema()`). It can only be set to `None` within this crate.
	/// Allowing overriding of the SchemaNode when building the serializer is a
	/// ~hack to allow the object container file encoding writer to serialize
	/// the header without instantiating a full `Schema`. This is only possible
	/// within this crate.
	schema: Option<&'s Schema>,
}

impl<'s> SerializerConfig<'s> {
	/// Build a `SerializerConfig` with a given `schema`, default options
	/// and empty serialization buffers.
	///
	/// The `schema` will be used when instantiating a serializer from this
	/// `SerializerConfig`.
	///
	/// Reusing the same `SerializerConfig` across serializations is ideal for
	/// performance, as it allows the buffers to be reused to avoid
	/// allocations.
	pub fn new(schema: &'s Schema) -> Self {
		Self::new_with_optional_schema(Some(schema))
	}

	/// See doc of [Self::schema]
	pub(crate) fn new_with_optional_schema(schema: Option<&'s Schema>) -> Self {
		Self {
			schema,
			allow_slow_sequence_to_bytes: false,
			buffers: Buffers::default(),
		}
	}

	/// For when you can't use `serde_bytes` and really need to serialize a
	/// sequence as bytes.
	///
	/// If you need to serialize a `Vec<u8>` or `&[u8]` as `Bytes`/`Fixed`,
	/// [`serde_bytes`](https://docs.rs/serde_bytes/latest/serde_bytes/) is the way to go.
	/// If however you can't use it because e.g. you are transcoding... then you
	/// may enable this instead.
	///
	/// It will be slow, because the bytes will be processed one by one.
	pub fn allow_slow_sequence_to_bytes(&mut self) -> &mut Self {
		self.allow_slow_sequence_to_bytes = true;
		self
	}

	/// Get the schema that was used when creating this `SerializerConfig`.
	///
	/// That is the one that will be used when building a serializer from this
	/// `SerializerConfig`.
	pub fn schema(&self) -> &'s Schema {
		// A SerializerConfig with no schema can only be built within this
		// crate - in which case we don't call `.schema()`
		self.schema.expect("Unknown schema in SerializerConfig")
	}
}

impl<'c, 's, W: VecWriter> SerializerState<'c, 's, W> {
	/// Build a `SerializerState` from a writer and a `SerializerConfig`.
	///
	/// This contains all that's needed to perform serialization.
	///
	/// Note that the resulting `SerializerState` does not implement
	/// [`serde::Serializer`] directly. Instead, use
	/// [`SerializerState::serializer`] to obtain a `DatumSerializer` that
	/// does.
	pub fn from_writer(writer: W, serializer_config: &'c mut SerializerConfig<'s>) -> Self {
		Self {
			writer,
			config: SerializerConfigRef::Borrowed(serializer_config),
		}
	}

	/// Build a `SerializerState` from a writer and a `SerializerConfig`.
	///
	/// This behaves the same as [`SerializerState::from_writer`], but takes
	/// ownership of the `SerializerConfig`.
	///
	/// Note that the `SerializerConfig` contains the buffers that
	/// should be re-used for performance, so this function should only be used
	/// if the [`SerializerState`] is rarely instantiated.
	///
	/// For all other matters, please see [`SerializerState::from_writer`]'s
	/// documentation for more details.
	pub fn with_owned_config(writer: W, serializer_config: SerializerConfig<'s>) -> Self {
		Self {
			writer,
			config: SerializerConfigRef::Owned(Box::new(serializer_config)),
		}
	}

	pub(crate) fn with_opt_owned_config(
		writer: W,
		serializer_config: SerializerConfigRef<'c, 's>,
	) -> Self {
		Self {
			writer,
			config: serializer_config,
		}
	}

	/// Obtain the actual [`serde::Serializer`] for this `SerializerState`
	pub fn serializer<'r>(&'r mut self) -> DatumSerializer<'r, 'c, 's, W> {
		DatumSerializer {
			schema_node: self.config.schema().root().as_ref(),
			state: self,
		}
	}

	pub(crate) fn serializer_overriding_schema_root<'r>(
		&'r mut self,
		schema_root: &'s SchemaNode<'s>,
	) -> DatumSerializer<'r, 'c, 's, W> {
		DatumSerializer {
			schema_node: schema_root,
			state: self,
		}
	}
}

impl<W> SerializerState<'_, '_, W> {
	/// Get writer back
	pub fn into_writer(self) -> W {
		self.writer
	}

	/// Get writer by reference
	///
	/// This may be useful to observe the state of the inner buffer,
	/// notably when re-using a `SerializerState` to write multiple objects.
	pub fn writer(&self) -> &W {
		&self.writer
	}

	/// Get writer by mutable reference
	///
	/// This may be useful to clear the inner buffer, when re-using a
	/// `SerializerState`.
	pub fn writer_mut(&mut self) -> &mut W {
		&mut self.writer
	}
}

/// Buffers used during serialization, for reuse across serializations
///
/// In order to avoid allocating even when field reordering is necessary we can
/// preserve the necessary allocations from one record to another (even across
/// deserializations).
///
/// This brings ~40% perf improvement
#[derive(Default)]
struct Buffers {
	field_reordering_buffers: Vec<Vec<u8>>,
	field_reordering_super_buffers: Vec<Vec<Option<Vec<u8>>>>,
}

pub(crate) enum SerializerConfigRef<'c, 's> {
	Borrowed(&'c mut SerializerConfig<'s>),
	Owned(Box<SerializerConfig<'s>>),
}
impl<'c, 's> core::ops::Deref for SerializerConfigRef<'c, 's> {
	type Target = SerializerConfig<'s>;

	fn deref(&self) -> &Self::Target {
		match self {
			Self::Borrowed(config) => &**config,
			Self::Owned(config) => &**config,
		}
	}
}
impl core::ops::DerefMut for SerializerConfigRef<'_, '_> {
	fn deref_mut(&mut self) -> &mut Self::Target {
		match &mut *self {
			Self::Borrowed(config) => &mut **config,
			Self::Owned(config) => &mut **config,
		}
	}
}

/// Trait for types that can be written to (used for serialization).
///
/// This is implemented for `Vec<u8>` and when the `std` feature is enabled,
/// for any type implementing `std::io::Write`.
pub trait VecWriter {
	/// Write all bytes from the buffer.
	fn write_all(&mut self, buf: &[u8]) -> Result<(), SerError>;
	/// Write a varint-encoded integer.
	fn write_varint<I: VarInt>(&mut self, n: I) -> Result<(), SerError>;
}

impl VecWriter for Vec<u8> {
	fn write_all(&mut self, buf: &[u8]) -> Result<(), SerError> {
		self.extend_from_slice(buf);
		Ok(())
	}
	fn write_varint<I: VarInt>(&mut self, n: I) -> Result<(), SerError> {
		let mut buf = [0u8; 10];
		let len = n.encode_var(&mut buf);
		self.extend_from_slice(&buf[..len]);
		Ok(())
	}
}

impl VecWriter for &mut Vec<u8> {
	fn write_all(&mut self, buf: &[u8]) -> Result<(), SerError> {
		self.extend_from_slice(buf);
		Ok(())
	}
	fn write_varint<I: VarInt>(&mut self, n: I) -> Result<(), SerError> {
		let mut buf = [0u8; 10];
		let len = n.encode_var(&mut buf);
		self.extend_from_slice(&buf[..len]);
		Ok(())
	}
}

#[cfg(feature = "std")]
impl<W: std::io::Write> VecWriter for StdWriter<W> {
	fn write_all(&mut self, buf: &[u8]) -> Result<(), SerError> {
		std::io::Write::write_all(&mut self.0, buf).map_err(SerError::io)
	}
	fn write_varint<I: VarInt>(&mut self, n: I) -> Result<(), SerError> {
		<Self as integer_encoding::VarIntWriter>::write_varint(&mut *self, n)
			.map(|_| ())
			.map_err(SerError::io)
	}
}

/// Wrapper for std::io::Write to implement VecWriter
#[cfg(feature = "std")]
pub struct StdWriter<W>(pub W);

#[cfg(feature = "std")]
impl<W: std::io::Write> std::io::Write for StdWriter<W> {
	fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
		self.0.write(buf)
	}
	fn flush(&mut self) -> std::io::Result<()> {
		self.0.flush()
	}
}

/// Serialize an avro "datum" (raw data, no headers...)
///
/// to a newly allocated Vec
///
/// See [`to_datum`](crate::to_datum) for more details when `std` feature is enabled.
pub fn to_datum_vec<T>(
	value: &T,
	serializer_config: &mut SerializerConfig<'_>,
) -> Result<Vec<u8>, SerError>
where
	T: serde::Serialize + ?Sized,
{
	let mut serializer_state = SerializerState::from_writer(Vec::new(), serializer_config);
	serde::Serialize::serialize(value, serializer_state.serializer())?;
	Ok(serializer_state.into_writer())
}
