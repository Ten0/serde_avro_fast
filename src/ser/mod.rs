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
	/// Storing these here for reuse so that we can bypass the allocation,
	/// and statistically obtain buffers that are already the proper length
	/// (since we have used them for previous records)
	buffers: Buffers,
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
			buffers: Buffers::default(),
		}
	}

	pub fn with_config(writer: W, config: SerializerConfig<'s>) -> Self {
		SerializerState {
			writer,
			config,
			buffers: Buffers::default(),
		}
	}

	pub fn serializer<'r>(&'r mut self) -> DatumSerializer<'r, 's, W> {
		DatumSerializer {
			schema_node: self.config.schema_root,
			state: self,
		}
	}

	/// Reuse buffers from a previous serializer
	///
	/// In order to avoid allocating even when field reordering is necessary we
	/// can preserve the necessary allocations from one record to another (even
	/// across deserializations).
	///
	/// This brings ~40% perf improvement
	pub fn add_buffers(&mut self, buffers: Buffers) {
		if self.buffers.field_reordering_buffers.is_empty() {
			self.buffers.field_reordering_buffers = buffers.field_reordering_buffers;
		} else {
			self.buffers
				.field_reordering_buffers
				.extend(buffers.field_reordering_buffers);
		}
		if self.buffers.field_reordering_super_buffers.is_empty() {
			self.buffers.field_reordering_super_buffers = buffers.field_reordering_super_buffers;
		} else {
			self.buffers
				.field_reordering_super_buffers
				.extend(buffers.field_reordering_super_buffers);
		}
	}
}

impl<W> SerializerState<'_, W> {
	/// Get writer back
	pub fn into_writer(self) -> W {
		self.writer
	}

	/// Get writer back, as well as buffers
	///
	/// You may reuse these buffers with another serializer for increased
	/// performance
	///
	/// These are used when it is required to buffer for field reordering
	/// (when the fields of a record are provided by serde not in the same order
	/// as they have to be serialized according to the schema)
	///
	/// If you are in a such scenario, reusing those should lead to about ~40%
	/// perf improvement.
	pub fn into_writer_and_buffers(self) -> (W, Buffers) {
		(self.writer, self.buffers)
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
pub struct Buffers {
	field_reordering_buffers: Vec<Vec<u8>>,
	field_reordering_super_buffers: Vec<Vec<Option<Vec<u8>>>>,
}
