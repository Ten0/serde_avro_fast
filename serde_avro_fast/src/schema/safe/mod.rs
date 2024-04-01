//! Defines a fully-safe counterpart of the [`Schema`](crate::Schema) that is
//! used for its initialization

mod canonical_form;
mod check_for_cycles;
mod parsing;
mod rabin;
mod serialize;

use super::{Fixed, Name, SchemaError};

pub use check_for_cycles::UnconditionalCycle;

/// An editable representation of an Avro schema
///
/// In there, references to other nodes are represented as [`SchemaKey`], which
/// allow to index into [`SchemaMut`].
///
/// It is useful to implement it this way because, due to how referencing via
/// [Names](https://avro.apache.org/docs/current/specification/#names) works in Avro,
/// the most performant representation of an Avro schema is not a tree but a
/// possibly-cyclic general directed graph.
///
/// For details about the meaning of the fields, see the
/// [`SchemaNode`](crate::schema::SchemaNode) documentation.
#[derive(Clone, Debug)]
pub struct SchemaMut {
	// First node in the array is considered to be the root
	pub(super) nodes: Vec<SchemaNode>,
	pub(super) schema_json: Option<String>,
}

impl SchemaMut {
	/// Obtain the underlying graph storage
	///
	/// The first node (index `0`) is the root of the schema.
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
	/// The first node (index `0`) is the root of the schema.
	///
	/// [`SchemaKey`]s can be converted to/from indexes of this `Vec`.
	pub fn nodes_mut(&mut self) -> &mut Vec<SchemaNode> {
		self.schema_json = None;
		&mut self.nodes
	}

	/// Obtain the root of the Schema
	///
	/// It is the first node of the `nodes` `Vec`.
	///
	/// # Panics
	/// If the `nodes` `Vec` is empty.
	/// This can only happen if you have updated it through
	/// [`nodes_mut`](Self::nodes_mut), as parsing otherwise guarantees that
	/// this cannot happen.
	pub fn root(&self) -> &SchemaNode {
		self.nodes.first().expect(
			"Schema should have nodes - have you updated it \
				in such a way that all of its nodes were removed?",
		)
	}

	/// Initialize a [`SchemaMut`] from a set of nodes.
	///
	/// The first node (index `0`) is the root of the schema.
	pub fn from_nodes(nodes: Vec<SchemaNode>) -> Self {
		Self {
			nodes,
			schema_json: None,
		}
	}

	/// Turn this [`SchemaMut`] into a [`Schema`](crate::Schema)
	///
	/// [`Schema`](crate::Schema) is necessary for use with the serializer and
	/// deserializer.
	///
	/// This will fail if the schema is invalid (e.g. incorrect [`SchemaKey`]`).
	pub fn freeze(self) -> Result<super::Schema, SchemaError> {
		self.try_into()
	}

	/// Try to get the node at the given [`SchemaKey`]
	///
	/// (or return `None` if the key is invalid)
	///
	/// If you want to panic on invalid keys, use `schema[key]`
	/// instead.
	pub fn get(&self, key: SchemaKey) -> Option<&SchemaNode> {
		self.nodes.get(key.idx)
	}
}

/// The location of a node in a [`SchemaMut`]
///
/// This can be used to [`Index`](std::ops::Index) into the [`SchemaMut`].
///
/// (Note that `Index`ing into a `SchemaMut` with an invalid index would cause a
/// panic.)
#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SchemaKey {
	pub(super) idx: usize,
}

impl SchemaKey {
	/// Construct a new SchemaKey
	///
	/// This is expected to be an index in the [`nodes`](SchemaMut::nodes_mut)
	/// `Vec` of a [`SchemaMut`].
	///
	///
	/// (Note that [`Index`](std::ops::Index)ing into a `SchemaMut` with an
	/// invalid index would cause a panic.)
	pub const fn from_idx(idx: usize) -> Self {
		Self { idx }
	}
	/// Obtain the index in the [`nodes`](SchemaMut::nodes) `Vec` of a
	/// [`SchemaMut`] that this [`SchemaKey`] points to.
	pub const fn idx(self) -> usize {
		self.idx
	}
	/// Construct a new SchemaKey representing the root of the schema
	///
	/// This is equivalent to `SchemaKey::from_idx(0)`, since the root of the
	/// schema is always simply the first element of the `nodes` array.
	pub const fn root() -> Self {
		Self { idx: 0 }
	}
}
impl std::ops::Index<SchemaKey> for SchemaMut {
	type Output = SchemaNode;
	fn index(&self, key: SchemaKey) -> &Self::Output {
		&self.nodes[key.idx]
	}
}
impl std::fmt::Debug for SchemaKey {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		std::fmt::Debug::fmt(&self.idx, f)
	}
}

/// A node of an avro schema, stored in a [`SchemaMut`].
///
/// More information about Avro schemas can be found in the
/// [Avro Specification](https://avro.apache.org/docs/current/specification/).
///
/// In there, references to other nodes are represented as [`SchemaKey`], which
/// allow to index into [`SchemaMut`].
#[derive(Clone, Debug)]
pub enum SchemaNode {
	/// An Avro type that's not annotated with a logical type
	RegularType(RegularType),
	/// An Avro type that is annotated with a logical type
	LogicalType {
		/// The key of the [`RegularType`] (in the [`SchemaMut`]) that is
		/// annotated with this logical type
		inner: SchemaKey,
		/// The LogicalType this node is annotated with
		logical_type: LogicalType,
	},
}

/// A primitive or complex type of an avro schema, stored in a [`SchemaNode`].
///
/// More information about Avro schemas can be found in the
/// [Avro Specification](https://avro.apache.org/docs/current/specification/).
///
/// In there, references to other nodes are represented as [`SchemaKey`], which
/// allow to index into [`SchemaMut`].
#[derive(Clone, Debug)]
pub enum RegularType {
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
	Array(Array),
	/// A `map` Avro schema.
	/// `Map` holds a pointer to the `Schema` of its values, which must all be
	/// the same schema. `Map` keys are assumed to be `string`.
	Map(Map),
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

/// Component of a [`SchemaMut`]
#[derive(Clone, Debug)]
pub struct Array {
	/// The key (in the [`SchemaMut`]) of the schema of each item that will be
	/// in the array
	pub items: SchemaKey,
	pub(crate) _private: (),
}
impl Array {
	/// `items` is the key (in the [`SchemaMut`]) of the schema of each item
	/// that will be in the array
	pub fn new(items: SchemaKey) -> Self {
		Self {
			items,
			_private: (),
		}
	}
}

/// Component of a [`SchemaMut`]
///
/// An Avro map is a collection of key-value pairs, where the keys are assumed
/// to be strings.
#[derive(Clone, Debug)]
pub struct Map {
	/// The key (in the [`SchemaMut`]) of the schema of each value that will be
	/// in the map
	///
	/// In an Avro map, all keys are assumed to be strings.
	pub values: SchemaKey,
	pub(crate) _private: (),
}
impl Map {
	/// `values` is the key (in the [`SchemaMut`]) of the schema of each value
	/// that will be in the map
	///
	/// In an Avro map, all keys are assumed to be strings.
	pub fn new(values: SchemaKey) -> Self {
		Self {
			values,
			_private: (),
		}
	}
}

/// Component of a [`SchemaMut`]
#[derive(Clone, Debug)]
pub struct Union {
	/// The keys (in the [`SchemaMut`]) of the schemas of each variant that
	/// this Avro *union* supports.
	pub variants: Vec<SchemaKey>,
	pub(crate) _private: (),
}
impl Union {
	/// `variants` is the keys (in the [`SchemaMut`]) of the schemas of each
	/// variant that this Avro *union* supports.
	pub fn new(variants: Vec<SchemaKey>) -> Self {
		Self {
			variants,
			_private: (),
		}
	}
}

/// Component of a [`SchemaMut`]
///
/// An avro `record` is ~equivalent to a Rust struct.
#[derive(Clone, Debug)]
pub struct Record {
	/// The list of fields in this *record* (~= `struct`)
	pub fields: Vec<RecordField>,
	/// The name of the record (including namespace)
	pub name: Name,
	pub(crate) _private: (),
}
impl Record {
	/// `name` is the name of the record (including namespace), and `fields` is
	/// the list of fields in this record.
	pub fn new(name: Name, fields: Vec<RecordField>) -> Self {
		Self {
			fields,
			name,
			_private: (),
		}
	}
}

/// Component of a [`SchemaMut`]
#[derive(Clone, Debug)]
pub struct RecordField {
	/// Name of the field
	pub name: String,
	/// The key (in the [`SchemaMut`]) of the schema of the type of this field
	pub type_: SchemaKey,
	pub(crate) _private: (),
}
impl RecordField {
	/// `schema` is the key (in the [`SchemaMut`]) of the schema of the type of
	/// this field.
	pub fn new(name: impl Into<String>, schema: SchemaKey) -> Self {
		Self {
			name: name.into(),
			type_: schema,
			_private: (),
		}
	}
}

/// Component of a [`SchemaMut`]
///
/// This is the ~equivalent of a Rust `enum` where none of the variants would
/// hold any inner value. (e.g. `enum Foo { Bar, Baz }`)
#[derive(Clone, Debug)]
pub struct Enum {
	/// All the variants of the enum (e.g. `["Bar", "Baz"]`)
	pub symbols: Vec<String>,
	/// The name of the enum (including namespace)
	pub name: Name,
	pub(crate) _private: (),
}
impl Enum {
	/// `name` is the name of the enum (including namespace), and `symbols` is
	/// the list of variants of the enum.
	pub fn new(name: Name, symbols: Vec<String>) -> Self {
		Self {
			symbols,
			name,
			_private: (),
		}
	}
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
	///
	/// Annotates an [`Int`](RegularType::Int).
	TimeMillis,
	/// The time of day in number of microseconds after midnight with no
	/// reference any calendar, time zone or date in particular.
	///
	/// Annotates a [`Long`](RegularType::Long).
	TimeMicros,
	/// An instant in time represented as the number of milliseconds after the
	/// UNIX epoch.
	///
	/// Annotates a [`Long`](RegularType::Long).
	///
	/// You probably want to use
	/// [`TimestampMilliSeconds`](https://docs.rs/serde_with/latest/serde_with/struct.TimestampMilliSeconds.html)
	/// from [`serde_with`](https://docs.rs/serde_with/latest/serde_with/index.html#examples) when deserializing this.
	TimestampMillis,
	/// An instant in time represented as the number of microseconds after the
	/// UNIX epoch.
	///
	/// Annotates a [`Long`](RegularType::Long).
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
	/// A logical type that is not known or not handled in any particular way
	/// by this library.
	///
	/// **You should not match on this variant.** (See below.)
	///
	/// This is the string that is used in the schema JSON to refer to this
	/// logical type.
	///
	/// Logical types of this variant may turn into known logical types from one
	/// release to another, as new logical types get added, so you should not
	/// match on this variant, and if you need to check for a specific unknown
	/// logical type, you should use [`as_str`](Self::as_str) instead, as this
	/// is guaranteed to keep working from one release to another:
	///
	/// ```rust
	/// # use serde_avro_fast::schema::LogicalType;
	/// # let logical_type = LogicalType::Unknown(serde_avro_fast::schema::UnknownLogicalType::new("foo"));
	/// match logical_type {
	/// 	LogicalType::Uuid => { /* ... */ }
	/// 	LogicalType::TimestampMillis => { /* ... */ }
	/// 	_ => match logical_type.as_str() {
	/// 		"some-unknown-logical-type" => { /* ... */ }
	/// 		"some-other-unknown-logical-type" => { /* ... */ }
	/// 		_ => { /* ... */ }
	/// 	},
	/// }
	/// ```
	///
	/// However, you may construct an instance of this variant if you need to
	/// build a [`SchemaMut`] with a logical type that is not known to this
	/// library.
	Unknown(UnknownLogicalType),
}

/// Component of a [`SchemaMut`]
#[derive(Clone, Debug)]
pub struct Decimal {
	/// The scale of the decimal number, which is the number of digits to the
	/// right of the decimal point.
	pub scale: u32,
	/// The precision of the decimal number, which is the number of significant
	/// digits in the number.
	pub precision: usize,
	pub(crate) _private: (),
}
impl Decimal {
	/// `scale` is the number of digits to the right of the decimal point, and
	/// `precision` is the number of significant digits in the number.
	pub fn new(scale: u32, precision: usize) -> Self {
		Self {
			precision,
			scale,
			_private: (),
		}
	}
}

/// Component of a [`SchemaMut`]
///
/// Represents a logical type that is not known or not handled in any particular
/// way by this library.
#[derive(Clone, Debug)]
pub struct UnknownLogicalType {
	/// The name of the logical type, as it appears in the schema JSON
	pub logical_type_name: String,
	_private: (),
}
impl UnknownLogicalType {
	/// `logical_type_name` is the name of the logical type, as it appears in
	/// the schema JSON
	pub fn new(logical_type_name: impl Into<String>) -> Self {
		Self {
			logical_type_name: logical_type_name.into(),
			_private: (),
		}
	}

	/// Gives the name of the logical type, as it appears in the schema JSON
	pub fn as_str(&self) -> &str {
		&self.logical_type_name
	}
}

impl LogicalType {
	/// The name of the logical type
	///
	/// This is the string that is used in the schema JSON to refer to this
	/// logical type.
	///
	/// For example, the `Decimal` logical type is named `decimal`.
	pub fn as_str(&self) -> &str {
		match self {
			LogicalType::Decimal(_) => "decimal",
			LogicalType::Uuid => "uuid",
			LogicalType::Date => "date",
			LogicalType::TimeMillis => "time-millis",
			LogicalType::TimeMicros => "time-micros",
			LogicalType::TimestampMillis => "timestamp-millis",
			LogicalType::TimestampMicros => "timestamp-micros",
			LogicalType::Duration => "duration",
			LogicalType::Unknown(unknown_logical_type) => &unknown_logical_type.logical_type_name,
		}
	}
}

impl From<RegularType> for SchemaNode {
	fn from(regular_type: RegularType) -> Self {
		Self::RegularType(regular_type)
	}
}

macro_rules! impl_froms_for_regular_type {
	($($variant: ident)*) => {
		$(
			impl From<$variant> for RegularType {
				fn from(variant: $variant) -> Self {
					Self::$variant(variant)
				}
			}
			impl From<$variant> for SchemaNode {
				fn from(variant: $variant) -> Self {
					SchemaNode::RegularType(RegularType::$variant(variant))
				}
			}
		)*
	};
}
impl_froms_for_regular_type! { Array Map Union Record Enum Fixed }
