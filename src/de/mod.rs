mod deserializer;
mod error;
pub mod read;
mod types;

pub use {deserializer::*, error::DeError};
use {read::*, types::*};

use crate::{
	schema::{RecordField, SchemaNode, SchemaStorage, UnionSchema},
	Schema,
};

use serde::de::*;

pub struct DeserializerState<'s, R> {
	reader: R,
	schema: &'s SchemaStorage,
	max_seq_size: usize,
}
impl<'s, 'de, R: Read<'de>> DeserializerState<'s, R> {
	pub fn new(r: R, schema: &'s Schema) -> Self {
		DeserializerState {
			reader: r,
			schema: schema.storage(),
			max_seq_size: 1_000_000_000,
		}
	}

	pub fn deserializer<'r>(&'r mut self) -> DatumDeserializer<'r, 's, R> {
		DatumDeserializer {
			schema_node: self.schema.root(),
			state: self,
		}
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
