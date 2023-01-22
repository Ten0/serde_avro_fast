//! Defines everything necessary for avro deserialization
//!
//! You typically want to use top-level functions such as [`from_datum_slice`](crate::from_datum_slice)
//! but access to this may be necessary for more advanced usage.

mod deserializer;
mod error;
pub mod read;
mod types;

pub use {deserializer::*, error::DeError};
use {read::*, types::*};

use crate::schema::{RecordField, Schema, SchemaNode, Union};

use serde::de::*;

/// All configuration and state necessary for the deserialization to run
///
/// Does not implement [`Deserializer`] directly (use [`.deserializer`](Self::deserializer) to obtain that).
pub struct DeserializerState<'s, R> {
	pub(crate) reader: R,
	config: DeserializerConfig<'s>,
}
#[derive(Clone)]
pub struct DeserializerConfig<'s> {
	schema_root: &'s SchemaNode<'s>,
	pub max_seq_size: usize,
}

impl<'s> DeserializerConfig<'s> {
	pub fn new(schema: &'s Schema) -> Self {
		Self::from_schema_node(schema.root())
	}
	pub fn from_schema_node(schema_root: &'s SchemaNode<'s>) -> Self {
		Self {
			schema_root,
			max_seq_size: 1_000_000_000,
		}
	}
}

impl<'s, 'de, R: ReadSlice<'de>> DeserializerState<'s, R> {
	pub fn new(r: R, schema: &'s Schema) -> Self {
		Self::from_schema_node(r, schema.root())
	}

	pub fn from_schema_node(r: R, schema_root: &'s SchemaNode<'s>) -> Self {
		Self::with_config(r, DeserializerConfig::from_schema_node(schema_root))
	}

	pub fn with_config(r: R, config: DeserializerConfig<'s>) -> Self {
		DeserializerState { reader: r, config }
	}

	pub fn deserializer<'r>(&'r mut self) -> DatumDeserializer<'r, 's, R> {
		DatumDeserializer {
			schema_node: self.config.schema_root,
			state: self,
		}
	}
}
impl<'s, R> DeserializerState<'s, R> {
	pub fn into_reader(self) -> R {
		self.reader
	}

	pub fn into_inner(self) -> (R, DeserializerConfig<'s>) {
		(self.reader, self.config)
	}
}
impl<'s, R> DeserializerState<'s, R> {
	pub fn config(&self) -> &DeserializerConfig<'s> {
		&self.config
	}
}

impl<'s, 'a> DeserializerState<'s, read::SliceRead<'a>> {
	pub fn from_slice(slice: &'a [u8], schema: &'s Schema) -> Self {
		Self::new(read::SliceRead::new(slice), schema)
	}
}

impl<'s, R: std::io::Read> DeserializerState<'s, read::ReaderRead<R>> {
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
