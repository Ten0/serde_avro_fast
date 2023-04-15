mod error;
mod serializer;

pub use {error::SerError, serializer::*};

use crate::schema::{
	DecimalRepr, Enum, Fixed, RecordField, Schema, SchemaNode, Union, UnionVariantLookupKey,
};

use {integer_encoding::VarIntWriter, serde::ser::*, std::io::Write};

pub struct SerializerState<'s, W> {
	pub(crate) writer: W,
	config: SerializerConfig<'s>,
}
/// Schema + other configs for deserialization
#[derive(Clone)]
pub struct SerializerConfig<'s> {
	schema_root: &'s SchemaNode<'s>,
}

impl<'s, W: std::io::Write> SerializerState<'s, W> {
	pub fn from_writer(writer: W, schema: &'s Schema) -> Self {
		Self {
			writer,
			config: SerializerConfig {
				schema_root: schema.root(),
			},
		}
	}

	pub fn serializer<'r>(&'r mut self) -> DatumSerializer<'r, 's, W> {
		DatumSerializer {
			schema_node: self.config.schema_root,
			state: self,
		}
	}
}
