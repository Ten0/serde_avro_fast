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
