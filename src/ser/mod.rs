//! Defines everything necessary for avro serialization

mod error;
mod serializer;

pub use {error::SerError, serializer::*};

use crate::schema::{
	DecimalRepr, Enum, Fixed, RecordField, Schema, SchemaNode, Union, UnionVariantLookupKey,
};

use {integer_encoding::VarIntWriter, serde::ser::*, std::io::Write};

/// All configuration and state necessary for the serialization to run
///
/// Notably holds the writer and a [`SerializerConfig`].
///
/// Does not implement [`Serializer`] directly (use
/// [`.serializer`](Self::serializer) to obtain that).
pub struct SerializerState<'s, W> {
	pub(crate) writer: W,
	config: SerializerConfig<'s>,
}
/// Schema + other configs for serialization
#[derive(Clone)]
pub struct SerializerConfig<'s> {
	schema_root: &'s SchemaNode<'s>,
}

impl<'s> SerializerConfig<'s> {
	pub fn new(schema: &'s Schema) -> Self {
		Self::from_schema_node(schema.root())
	}
	pub fn from_schema_node(schema_root: &'s SchemaNode<'s>) -> Self {
		Self { schema_root }
	}
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

	pub fn with_config(writer: W, config: SerializerConfig<'s>) -> Self {
		SerializerState { writer, config }
	}

	pub fn serializer<'r>(&'r mut self) -> DatumSerializer<'r, 's, W> {
		DatumSerializer {
			schema_node: self.config.schema_root,
			state: self,
		}
	}
}
