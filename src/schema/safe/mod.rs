//! Defines a fully-safe counterpart of the [`Schema`](crate::Schema) that is
//! used for its initialization

mod canonical_form;
mod check_for_cycles;
mod parsing;
mod serialize;

use super::{Fixed, Name, SchemaError};

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
pub struct EditableSchema {
	// First node in the array is considered to be the root
	pub(super) nodes: Vec<SchemaNode>,
	pub(super) schema_json: Option<String>,
}

impl EditableSchema {
	/// Obtain the underlying graph storage
	///
	/// [`SchemaKey`]s can be converted to indexes of this `Vec`.
	pub fn nodes(&self) -> &[SchemaNode] {
		&self.nodes
	}

	/// Obtain the underlying graph storage mutably
	///
	/// This loses the original JSON. If obtaining it again (for e.g. object
	/// container file encoding) it will be re-generated and will lose all
	/// non-stored schema fields (`doc`, `aliases`, `default`, ...).
	///
	/// [`SchemaKey`]s can be converted to indexes of this `Vec`.
	pub fn nodes_mut(&mut self) -> &mut Vec<SchemaNode> {
		self.schema_json = None;
		&mut self.nodes
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
impl std::ops::Index<SchemaKey> for EditableSchema {
	type Output = SchemaNode;
	fn index(&self, key: SchemaKey) -> &Self::Output {
		&self.nodes[key.idx]
	}
}

/// A node of an avro schema, stored in an [`EditableSchema`].
///
/// More information about Avro schemas can be found in the
/// [Avro Specification](https://avro.apache.org/docs/current/specification/).
///
/// In there, references to other nodes are represented as [`SchemaKey`], which
/// allow to index into [`EditableSchema`].
#[derive(Clone, Debug)]
pub struct SchemaNode {
	pub type_: SchemaType,
	pub logical_type: Option<LogicalType>,
}

/// A primitive or complex type of an avro schema, stored in a [`SchemaNode`].
///
/// More information about Avro schemas can be found in the
/// [Avro Specification](https://avro.apache.org/docs/current/specification/).
///
/// In there, references to other nodes are represented as [`SchemaKey`], which
/// allow to index into [`EditableSchema`].
#[derive(Clone, Debug)]
pub enum SchemaType {
	/// A `null` Avro schema.
	Null,
	/// A `boolean` Avro schema.
	Boolean,
	/// An `int` Avro schema.
	Int,
	/// A `long` Avro schema.
	Long,
	/// A `float` Avro schema.
	Float,
	/// A `double` Avro schema.
	Double,
	/// A `bytes` Avro schema.
	/// `Bytes` represents a sequence of 8-bit unsigned bytes.
	Bytes,
	/// A `string` Avro schema.
	/// `String` represents a unicode character sequence.
	String,
	/// A `array` Avro schema. Avro arrays are required to have the same type
	/// for each element. This variant holds the `Schema` for the array element
	/// type.
	Array(SchemaKey),
	/// A `map` Avro schema.
	/// `Map` holds a pointer to the `Schema` of its values, which must all be
	/// the same schema. `Map` keys are assumed to be `string`.
	Map(SchemaKey),
	/// A `union` Avro schema.
	///
	/// These can be deserialized into rust enums, where the variant name
	/// should match:
	/// - If it's not a named type, the PascalCase of the type (e.g. `String`,
	///   `Uuid`...)
	/// - If it's a named type, the fully qualified name of the type (e.g for a
	///   record `{"namespace": "foo", "name": "bar"}`, `foo.bar`)
	///
	/// See the `tests/unions.rs` file for more examples.
	Union(Union),
	/// A `record` Avro schema.
	Record(Record),
	/// An `enum` Avro schema.
	///
	/// These can be deserialized into rust enums, matching on the name
	/// as defined in the schema.
	Enum(Enum),
	/// A `fixed` Avro schema.
	Fixed(Fixed),
}

/// Logical type
///
/// <https://avro.apache.org/docs/current/specification/#logical-types>
#[derive(Clone, Debug)]
#[non_exhaustive]
pub enum LogicalType {
	/// Logical type which represents `Decimal` values. The underlying type is
	/// serialized and deserialized as `Schema::Bytes` or `Schema::Fixed`.
	///
	/// `scale` defaults to 0 and is an integer greater than or equal to 0 and
	/// `precision` is an integer greater than 0.
	///
	/// <https://avro.apache.org/docs/current/specification/#decimal>
	Decimal(Decimal),
	/// A universally unique identifier, annotating a string.
	Uuid,
	/// Logical type which represents the number of days since the unix epoch.
	/// Serialization format is `Schema::Int`.
	Date,
	/// The time of day in number of milliseconds after midnight with no
	/// reference any calendar, time zone or date in particular.
	TimeMillis,
	/// The time of day in number of microseconds after midnight with no
	/// reference any calendar, time zone or date in particular.
	TimeMicros,
	/// An instant in time represented as the number of milliseconds after the
	/// UNIX epoch.
	///
	/// You probably want to use
	/// [`TimestampMilliSeconds`](https://docs.rs/serde_with/latest/serde_with/struct.TimestampMilliSeconds.html)
	/// from [`serde_with`](https://docs.rs/serde_with/latest/serde_with/index.html#examples) when deserializing this.
	TimestampMillis,
	/// An instant in time represented as the number of microseconds after the
	/// UNIX epoch.
	///
	/// You probably want to use
	/// [`TimestampMicroSeconds`](https://docs.rs/serde_with/latest/serde_with/struct.TimestampMicroSeconds.html)
	/// from [`serde_with`](https://docs.rs/serde_with/latest/serde_with/index.html#examples) when deserializing this.
	TimestampMicros,
	/// An amount of time defined by a number of months, days and milliseconds.
	///
	/// This deserializes to a struct that has the `months`, `days`, and
	/// `milliseconds` fields declared as `u32`s, or to a `(u32, u32, u32)`
	/// tuple, or to its raw representation [as defined by the specification](https://avro.apache.org/docs/current/specification/#duration)
	/// if the deserializer is hinted this way ([`serde_bytes`](https://docs.rs/serde_bytes/latest/serde_bytes/)).
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

/// Component of a [`SchemaNode`]
#[derive(Clone, Debug)]
pub struct Decimal {
	pub precision: usize,
	pub scale: u32,
}