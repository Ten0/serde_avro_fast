use super::{
	safe::SchemaNode as SafeSchemaNode,
	union_variants_per_type_lookup::PerTypeLookup as UnionVariantsPerTypeLookup, Decimal, Fixed,
	Name,
};

use std::collections::HashMap;

/// The most performant and easiest to navigate version of an Avro schema
///
/// Navigated through [`SchemaNode`] via [`.root`](Schema::root).
///
/// To achieve the ideal performance and ease of use via self-referencing
/// [`SchemaNode`]s all held in the [`Schema`], it is built using `unsafe`, so
/// it can only be built through
/// [its safe counterpart](crate::schema::safe::Schema) (via [`From`]) because
/// it makes the conversion code simple enough that we can reasonably guarantee
/// its correctness despite the usage of `unsafe`.
///
/// It is useful to implement it this way because, due to how referencing via
/// [Names](https://avro.apache.org/docs/current/specification/#names) work in Avro,
/// the most performant representation of an Avro schema is not a tree but a
/// possibly-cyclic general directed graph.
pub struct Schema {
	// First node in the array is considered to be the root
	//
	// This lifetime is fake, but since all elements have to be accessed by the `root` function
	// which will downcast it and we never push anything more in there (which would cause
	// reallocation and invalidate all nodes) this is correct.
	nodes: Vec<SchemaNode<'static>>,
	parsing_canonical_form: String,
	fingerprint: [u8; 8],
}

impl Schema {
	/// The Avro schema is represented internally as a directed graph of nodes,
	/// all stored in [`Schema`].
	///
	/// The root node represents the whole schema.
	pub fn root<'a>(&'a self) -> &'a SchemaNode<'a> {
		// the signature of this function downgrades the fake 'static lifetime in a way
		// that makes it correct
		&self.nodes[0]
	}

	/// Obtain the
	/// [Parsing Canonical Form](https://avro.apache.org/docs/current/specification/#parsing-canonical-form-for-schemas)
	/// of the schema
	pub fn parsing_canonical_form(&self) -> &str {
		self.parsing_canonical_form.as_str()
	}

	/// Obtain the Rabin fingerprint of the schema
	pub fn rabin_fingerprint(&self) -> &[u8; 8] {
		&self.fingerprint
	}
}

/// A node of an avro schema, borrowed from a [`Schema`].
///
/// More information about Avro schemas can be found in the
/// [Avro Specification](https://avro.apache.org/docs/current/specification/).
///
/// This enum is borrowed from a [`Schema`] and is used to navigate it.
pub enum SchemaNode<'a> {
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
	Array(&'a SchemaNode<'a>),
	/// A `map` Avro schema.
	/// `Map` holds a pointer to the `Schema` of its values, which must all be
	/// the same schema. `Map` keys are assumed to be `string`.
	Map(&'a SchemaNode<'a>),
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
	Union(Union<'a>),
	/// A `record` Avro schema.
	Record(Record<'a>),
	/// An `enum` Avro schema.
	///
	/// These can be deserialized into rust enums, matching on the name
	/// as defined in the schema.
	Enum(Enum),
	/// A `fixed` Avro schema.
	Fixed(Fixed),
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
pub struct Union<'a> {
	pub variants: Vec<&'a SchemaNode<'a>>,
	pub(crate) per_type_lookup: UnionVariantsPerTypeLookup<'a>,
}

impl std::fmt::Debug for Union<'_> {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		// Skip per_type_lookup for readability
		f.debug_struct("Union")
			.field("variants", &self.variants)
			.finish()
	}
}

/// Component of a [`SchemaNode`]
#[derive(Debug)]
pub struct Record<'a> {
	pub fields: Vec<RecordField<'a>>,
	pub name: Name,
	pub per_name_lookup: HashMap<String, usize>,
}

/// Component of a [`SchemaNode`]
#[derive(Debug)]
pub struct RecordField<'a> {
	pub name: String,
	pub schema: &'a SchemaNode<'a>,
}

/// Component of a [`SchemaNode`]
#[derive(Clone)]
pub struct Enum {
	pub symbols: Vec<String>,
	pub name: Name,
	pub per_name_lookup: HashMap<String, usize>,
}

impl std::fmt::Debug for Enum {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		// Skip per_type_lookup for readability
		f.debug_struct("Enum")
			.field("name", &self.name)
			.field("symbols", &self.symbols)
			.finish()
	}
}

impl From<super::safe::Schema> for Schema {
	fn from(safe: super::safe::Schema) -> Self {
		// This allocation should never be moved otherwise references will become
		// invalid
		let mut ret = Self {
			nodes: (0..safe.nodes.len()).map(|_| SchemaNode::Null).collect(),
			parsing_canonical_form: safe.parsing_canonical_form,
			fingerprint: safe.fingerprint,
		};
		let len = ret.nodes.len();
		// Let's be extra-sure (second condition is for calls to add)
		assert!(len > 0 && len == safe.nodes.len() && len <= (isize::MAX as usize));
		let storage_start_ptr = ret.nodes.as_mut_ptr();
		// unsafe closure used below in unsafe block
		let key_to_node = |schema_key: super::safe::SchemaKey| -> &'static SchemaNode {
			let idx = schema_key.idx;
			assert!(idx < len);
			unsafe { &*(storage_start_ptr.add(schema_key.idx)) }
		};
		let mut curr_storage_node_ptr = storage_start_ptr;
		for safe_node in safe.nodes {
			// Safety:
			// - The nodes we create here are never moving in memory since the entire vec is
			//   preallocated, and even when moving a vec, the pointed space doesn't move.
			// - The fake `'static` lifetimes are always downgraded before being made
			//   available.
			// - We only use pointers from the point at which we call `as_mut_ptr` so the
			//   compiler will not have aliasing constraints.
			// - We don't dereference the references we create in key_to_node until the
			//   original &mut is released and well out of scope (we don't dereference them
			//   at all in this function).
			unsafe {
				*curr_storage_node_ptr = match safe_node {
					SafeSchemaNode::Null => SchemaNode::Null,
					SafeSchemaNode::Boolean => SchemaNode::Boolean,
					SafeSchemaNode::Int => SchemaNode::Int,
					SafeSchemaNode::Long => SchemaNode::Long,
					SafeSchemaNode::Float => SchemaNode::Float,
					SafeSchemaNode::Double => SchemaNode::Double,
					SafeSchemaNode::Bytes => SchemaNode::Bytes,
					SafeSchemaNode::String => SchemaNode::String,
					SafeSchemaNode::Array(schema_key) => SchemaNode::Array(key_to_node(schema_key)),
					SafeSchemaNode::Map(schema_key) => SchemaNode::Map(key_to_node(schema_key)),
					SafeSchemaNode::Union(union) => SchemaNode::Union({
						let variants: Vec<&SchemaNode> = union
							.variants
							.into_iter()
							.map(|schema_key| key_to_node(schema_key))
							.collect();
						Union {
							per_type_lookup: UnionVariantsPerTypeLookup::new(&variants),
							variants,
						}
					}),
					SafeSchemaNode::Record(record) => SchemaNode::Record(Record {
						per_name_lookup: record
							.fields
							.iter()
							.enumerate()
							.map(|(i, v)| (v.name.clone(), i))
							.collect(),
						fields: record
							.fields
							.into_iter()
							.map(|f| RecordField {
								name: f.name,
								schema: key_to_node(f.schema),
							})
							.collect(),
						name: record.name,
					}),
					SafeSchemaNode::Enum(enum_) => SchemaNode::Enum(Enum {
						per_name_lookup: enum_
							.symbols
							.iter()
							.enumerate()
							.map(|(i, v)| (v.clone(), i))
							.collect(),
						symbols: enum_.symbols,
						name: enum_.name,
					}),
					SafeSchemaNode::Fixed(fixed) => SchemaNode::Fixed(fixed),
					SafeSchemaNode::Decimal(decimal) => SchemaNode::Decimal(decimal),
					SafeSchemaNode::Uuid => SchemaNode::Uuid,
					SafeSchemaNode::Date => SchemaNode::Date,
					SafeSchemaNode::TimeMillis => SchemaNode::TimeMillis,
					SafeSchemaNode::TimeMicros => SchemaNode::TimeMicros,
					SafeSchemaNode::TimestampMillis => SchemaNode::TimestampMillis,
					SafeSchemaNode::TimestampMicros => SchemaNode::TimestampMicros,
					SafeSchemaNode::Duration => SchemaNode::Duration,
				};
				curr_storage_node_ptr = curr_storage_node_ptr.add(1);
			};
		}
		ret
	}
}

impl std::fmt::Debug for Schema {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		<SchemaNode<'_> as std::fmt::Debug>::fmt(self.root(), f)
	}
}

impl<'a> std::fmt::Debug for SchemaNode<'a> {
	fn fmt(&self, f: &mut std::fmt::Formatter) -> ::std::fmt::Result {
		// Avoid going into stack overflow when rendering SchemaNode's debug impl, in
		// case there are loops

		use std::cell::Cell;
		struct SchemaNodeRenderingDepthGuard;
		thread_local! {
			static DEPTH: Cell<u32> = Cell::new(0);
		}
		impl Drop for SchemaNodeRenderingDepthGuard {
			fn drop(&mut self) {
				DEPTH.with(|cell| cell.set(cell.get().checked_sub(1).unwrap()));
			}
		}
		const MAX_DEPTH: u32 = 2;
		let depth = DEPTH.with(|cell| {
			let val = cell.get();
			cell.set(val + 1);
			val
		});
		let _decrement_depth_guard = SchemaNodeRenderingDepthGuard;

		match *self {
			SchemaNode::Null => f.debug_tuple("Null").finish(),
			SchemaNode::Boolean => f.debug_tuple("Boolean").finish(),
			SchemaNode::Int => f.debug_tuple("Int").finish(),
			SchemaNode::Long => f.debug_tuple("Long").finish(),
			SchemaNode::Float => f.debug_tuple("Float").finish(),
			SchemaNode::Double => f.debug_tuple("Double").finish(),
			SchemaNode::Bytes => f.debug_tuple("Bytes").finish(),
			SchemaNode::String => f.debug_tuple("String").finish(),
			SchemaNode::Array(inner) => {
				let mut d = f.debug_tuple("Array");
				if depth < MAX_DEPTH {
					d.field(inner);
				}
				d.finish()
			}
			SchemaNode::Map(inner) => {
				let mut d = f.debug_tuple("Map");
				if depth < MAX_DEPTH {
					d.field(inner);
				}
				d.finish()
			}
			SchemaNode::Union(ref inner) => {
				let mut d = f.debug_tuple("Union");
				if depth < MAX_DEPTH {
					d.field(inner);
				}
				d.finish()
			}
			SchemaNode::Record(ref inner) => {
				let mut d = f.debug_tuple("Record");
				if depth < MAX_DEPTH {
					d.field(inner);
				}
				d.finish()
			}
			SchemaNode::Enum(ref inner) => {
				let mut d = f.debug_tuple("Enum");
				if depth < MAX_DEPTH {
					d.field(inner);
				}
				d.finish()
			}
			SchemaNode::Fixed(ref inner) => {
				let mut d = f.debug_tuple("Fixed");
				if depth < MAX_DEPTH {
					d.field(inner);
				}
				d.finish()
			}
			SchemaNode::Decimal(ref inner) => {
				let mut d = f.debug_tuple("Decimal");
				if depth < MAX_DEPTH {
					d.field(inner);
				}
				d.finish()
			}
			SchemaNode::Uuid => f.debug_tuple("Uuid").finish(),
			SchemaNode::Date => f.debug_tuple("Date").finish(),
			SchemaNode::TimeMillis => f.debug_tuple("TimeMillis").finish(),
			SchemaNode::TimeMicros => f.debug_tuple("TimeMicros").finish(),
			SchemaNode::TimestampMillis => f.debug_tuple("TimestampMillis").finish(),
			SchemaNode::TimestampMicros => f.debug_tuple("TimestampMicros").finish(),
			SchemaNode::Duration => f.debug_tuple("Duration").finish(),
		}
	}
}
