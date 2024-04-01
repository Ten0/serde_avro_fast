//! Defines everything necessary for avro deserialization
//!
//! # For advanced usage
//!
//! You typically want to use top-level functions such as
//! [`from_datum_slice`](crate::from_datum_slice) but access to this may be
//! necessary for more advanced usage.
//!
//! This gives manual access to the type that implements
//! [`serde::Deserializer`], as well as its building blocks in order to set
//! configuration parameters meant to prevent DOS:
//! - [`DeserializerConfig::max_seq_size`]
//! - [`read::ReaderRead::max_alloc_size`]
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
//! #[derive(serde_derive::Deserialize, Debug, PartialEq)]
//! struct Test {
//! 	field: String,
//! }
//!
//! let avro_datum: &[u8] = &[6, 102, 111, 111]; // Any `impl BufRead`
//!
//! // Of course, don't actually use `ReaderRead` if you have a slice
//! let mut avro_reader = serde_avro_fast::de::read::ReaderRead::new(avro_datum);
//!
//! // Now we can set some custom parameters
//! avro_reader.max_alloc_size = 32 * 1024;
//!
//! // We can also set parameters that are common to the slice version and the reader version
//! let mut deserializer_config = serde_avro_fast::de::DeserializerConfig::new(&schema);
//! deserializer_config.max_seq_size = 1_000_000;
//!
//! // Now we can build the struct that will generally serve through deserialization
//! let mut deserializer_state =
//! 	serde_avro_fast::de::DeserializerState::with_config(avro_reader, deserializer_config);
//!
//! // It's not the `&mut DeserializerState` that implements `serde::Deserializer` directly, instead
//! // it is `DatumDeserializer` (which is essentially an `&mut DeserializerState` but not exactly
//! // because it also keeps track of the current schema node)
//! // We build it through `DeserializerState::deserializer`
//! let result: Test = serde::Deserialize::deserialize(deserializer_state.deserializer())
//! 	.expect("Failed to deserialize");
//! assert_eq!(
//! 	result,
//! 	Test {
//! 		field: "foo".to_owned()
//! 	}
//! );
//! ```

mod deserializer;
mod error;
pub mod read;

use read::*;
pub use {deserializer::*, error::DeError};

use crate::schema::self_referential::*;

use serde::de::*;

/// All configuration and state necessary for the deserialization to run
///
/// Notably holds the reader and a [`DeserializerConfig`].
///
/// Does not implement [`Deserializer`] directly (use
/// [`.deserializer`](Self::deserializer) to obtain that).
pub struct DeserializerState<'s, R> {
	pub(crate) reader: R,
	config: DeserializerConfig<'s>,
}
/// Schema + other configs for deserialization
#[derive(Clone)]
pub struct DeserializerConfig<'s> {
	schema_root: NodeRef<'s>,
	/// If a sequence turns out to be longer than this during deserialization,
	/// we will throw an error instead.
	///
	/// This is to avoid running into an infinite loop at deserialization time.
	/// Default for this is `1 000 000 000` (~1s CPU time)
	///
	/// Note that if you're deserializing from an `impl BufRead` instead of a
	/// slice (consequently using [`ReaderRead`]), there's an additional similar
	/// parameter [there](ReaderRead::max_alloc_size) that you may want to
	/// configure.
	pub max_seq_size: usize,
	/// If a datum turns out to be deeper than this during deserialization, we
	/// will throw an error instead.
	///
	/// This is to avoid running into a stack overflow at deserialization time.
	/// Default for this is `64`.
	pub allowed_depth: usize,
}

impl<'s> DeserializerConfig<'s> {
	/// Construct a `DeserializerConfig` from a schema, otherwise initializing
	/// all other parameters to their default values
	pub fn new(schema: &'s Schema) -> Self {
		Self::from_schema_node(schema.root())
	}
	pub(crate) fn from_schema_node(schema_root: NodeRef<'s>) -> Self {
		Self {
			schema_root,
			max_seq_size: 1_000_000_000,
			allowed_depth: 64,
		}
	}
}

impl<'s, 'de, R: ReadSlice<'de>> DeserializerState<'s, R> {
	/// Construct a `DeserializerState` from a reader and a schema, internally
	/// initializing a `DeserializerConfig` from the schema with all other
	/// parameters set to their default values
	pub fn new(r: R, schema: &'s Schema) -> Self {
		Self::from_schema_node(r, schema.root())
	}

	pub(crate) fn from_schema_node(r: R, schema_root: NodeRef<'s>) -> Self {
		Self::with_config(r, DeserializerConfig::from_schema_node(schema_root))
	}

	/// Construct a `DeserializerState` from a `ReadSlice` (either a
	/// [`SliceRead`] or a [`ReaderRead`]) and a [`DeserializerConfig`]
	///
	/// This is only useful if you want to set custom parameters on the
	/// `DeserializerConfig` for the deserialization, otherwise you may simply
	/// use [`DeserializerState::from_slice`] or
	/// [`DeserializerState::from_reader`].
	pub fn with_config(r: R, config: DeserializerConfig<'s>) -> Self {
		DeserializerState { reader: r, config }
	}

	/// Obtain the actual [`serde::Deserializer`] for this `DeserializerState`
	pub fn deserializer<'r>(&'r mut self) -> DatumDeserializer<'r, 's, R> {
		DatumDeserializer {
			schema_node: self.config.schema_root.as_ref(),
			allowed_depth: AllowedDepth::new(self.config.allowed_depth),
			state: self,
		}
	}
}
impl<'s, R> DeserializerState<'s, R> {
	/// Turn the `DeserializerState` into the reader it was built from
	pub fn into_reader(self) -> R {
		self.reader
	}

	/// Turn the `DeserializerState` into the reader it was built from, also
	/// extracting the original configuration (in case that needs to be re-used)
	pub fn into_inner(self) -> (R, DeserializerConfig<'s>) {
		(self.reader, self.config)
	}
}
impl<'s, R> DeserializerState<'s, R> {
	/// Get the configuration that this `DeserializerState` uses (that it was
	/// built with)
	pub fn config(&self) -> &DeserializerConfig<'s> {
		&self.config
	}
}

impl<'s, 'a> DeserializerState<'s, read::SliceRead<'a>> {
	/// Construct a `DeserializerState` from an `&[u8]` and a schema, otherwise
	/// initializing all other parameters to their default values
	pub fn from_slice(slice: &'a [u8], schema: &'s Schema) -> Self {
		Self::new(read::SliceRead::new(slice), schema)
	}
}

impl<'s, R: std::io::BufRead> DeserializerState<'s, read::ReaderRead<R>> {
	/// Construct a `DeserializerState` from an
	/// [`impl BufRead`](std::io::BufRead) and a schema, otherwise initializing
	/// all other parameters to their default values
	///
	/// Prefer using [`DeserializerState::from_slice`] if you have a slice, as
	/// that will be more performant and enable you to borrow `&str`s from the
	/// original slice.
	pub fn from_reader(reader: R, schema: &'s Schema) -> Self {
		Self::new(read::ReaderRead::new(reader), schema)
	}
}

impl<R> std::ops::Deref for DeserializerState<'_, R> {
	type Target = R;
	fn deref(&self) -> &Self::Target {
		&self.reader
	}
}

impl<R> std::ops::DerefMut for DeserializerState<'_, R> {
	fn deref_mut(&mut self) -> &mut Self::Target {
		&mut self.reader
	}
}
