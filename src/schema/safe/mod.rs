//! Defines a fully-safe counterpart of the [`Schema`](crate::Schema) that is
//! used for its initialization

mod canonical_form;
mod check_for_cycles;
mod parsing;

use super::{Decimal, Fixed, Name};

pub use {check_for_cycles::UnconditionalCycle, parsing::*};

/// A fully-safe counterpart of the [`Schema`](crate::Schema) that is used for
/// its initialization
///
/// In there, references to other nodes are represented as [`SchemaKey`], which
/// allow to index into [`Schema`].
///
/// For details about the meaning of the fields, see the
/// [`SchemaNode`](crate::schema::SchemaNode) documentation.
#[derive(Clone, Debug)]
pub struct Schema {
	// First node in the array is considered to be the root
	pub(super) nodes: Vec<SchemaNode>,
	pub(super) schema_json: Option<String>,
}

impl Schema {
	/// Obtain the underlying graph storage
	///
	/// [`SchemaKey`]s can be converted to indexes of this `Vec`.
	pub fn into_nodes(self) -> Vec<SchemaNode> {
		self.nodes
	}
}

/// The location of a node in the [`Schema`]
///
/// This can be used to [`Index`](std::ops::Index) into the [`Schema`].
#[derive(Copy, Clone, Debug)]
pub struct SchemaKey {
	pub(super) idx: usize,
	pub(super) is_ref: bool,
}

impl SchemaKey {
	// Construct a new SchemaKey
	//
	// This will not be serialized as a reference, instead the full type will be
	// serialized.
	pub fn from_idx(idx: usize) -> Self {
		Self { idx, is_ref: false }
	}
	pub fn reference(idx: usize) -> Self {
		Self { idx, is_ref: true }
	}
	pub fn idx(self) -> usize {
		self.idx
	}
	pub fn is_ref(self) -> bool {
		self.is_ref
	}
}
impl std::ops::Index<SchemaKey> for Schema {
	type Output = SchemaNode;
	fn index(&self, key: SchemaKey) -> &Self::Output {
		&self.nodes[key.idx]
	}
}

/// The safe (non self-referential) counterpart of
/// [`SchemaNode`](crate::schema::SchemaNode)
///
/// In there, references to other nodes are represented as [`SchemaKey`], which
/// allow to index into [`Schema`].
///
/// For details about the meaning of the fields, see the
/// [`SchemaNode`](crate::schema::SchemaNode) documentation.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub enum SchemaNode {
	Null,
	Boolean,
	Int,
	Long,
	Float,
	Double,
	Bytes,
	String,
	Array(SchemaKey),
	Map(SchemaKey),
	Union(Union),
	Record(Record),
	Enum(Enum),
	Fixed(Fixed),
	Decimal(Decimal),
	Uuid,
	Date,
	TimeMillis,
	TimeMicros,
	TimestampMillis,
	TimestampMicros,
	Duration,
}

/// Component of a [`SchemaNode`]
#[derive(Clone, Debug)]
pub struct Union {
	pub variants: Vec<SchemaKey>,
}

/// Component of a [`SchemaNode`]
#[derive(Clone, Debug)]
pub struct Record {
	pub fields: Vec<RecordField>,
	pub name: Name,
}

/// Component of a [`SchemaNode`]
#[derive(Clone, Debug)]
pub struct RecordField {
	pub name: String,
	pub schema: SchemaKey,
}

/// Component of a [`SchemaNode`]
#[derive(Clone, Debug)]
pub struct Enum {
	pub symbols: Vec<String>,
	pub name: Name,
}
