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

use {integer_encoding::VarIntWriter, serde::ser::*, std::io::Write};

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
	config: &'c mut SerializerConfig<'s>,
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
/// let serialized = serde_avro_fast::to_datum(&4, serialized, serializer_config).unwrap();
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
	/// Build a new `SerializerConfig` with a given `schema`, default options
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

impl<'c, 's, W: std::io::Write> SerializerState<'c, 's, W> {
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
