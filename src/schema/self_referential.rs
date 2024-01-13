use super::{
	safe::{LogicalType, SchemaNode as SafeSchemaNode, SchemaType as SafeSchemaType},
	union_variants_per_type_lookup::PerTypeLookup as UnionVariantsPerTypeLookup,
	SchemaError,
};

use std::collections::HashMap;

pub(crate) use super::{Fixed, Name};

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
/// [Names](https://avro.apache.org/docs/current/specification/#names) works in Avro,
/// the most performant representation of an Avro schema is not a tree but a
/// possibly-cyclic general directed graph.
pub struct Schema {
	// First node in the array is considered to be the root
	//
	// This lifetime is fake, but since all elements have to be accessed by the `root` function
	// which will downcast it and we never push anything more in there (which would cause
	// reallocation and invalidate all nodes) this is correct.
	nodes: Vec<SchemaNode<'static>>,
	fingerprint: [u8; 8],
	schema_json: String,
}

impl Schema {
	/// The Avro schema is represented internally as a directed graph of nodes,
	/// all stored in [`Schema`].
	///
	/// The root node represents the whole schema.
	pub(crate) fn root<'a>(&'a self) -> &'a SchemaNode<'a> {
		// the signature of this function downgrades the fake 'static lifetime in a way
		// that makes it correct
		&self.nodes[0]
	}

	/// Obtain the JSON for this schema
	pub fn json(&self) -> &str {
		&self.schema_json
	}

	/// Obtain the Rabin fingerprint of the schema
	pub fn rabin_fingerprint(&self) -> &[u8; 8] {
		&self.fingerprint
	}
}

/// A node of an avro schema, borrowed from a [`Schema`].
///
/// This enum is borrowed from a [`Schema`] and is used to navigate it.
///
/// For details about the meaning of the variants, see the
/// [`SchemaNode`](crate::schema::SchemaNode) documentation.
#[non_exhaustive]
pub(crate) enum SchemaNode<'a> {
	Null,
	Boolean,
	Int,
	Long,
	Float,
	Double,
	Bytes,
	String,
	Array(&'a SchemaNode<'a>),
	Map(&'a SchemaNode<'a>),
	Union(Union<'a>),
	Record(Record<'a>),
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
pub(crate) struct Union<'a> {
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
pub(crate) struct Record<'a> {
	pub fields: Vec<RecordField<'a>>,
	pub name: Name,
	pub per_name_lookup: HashMap<String, usize>,
}

impl<'a> std::fmt::Debug for Record<'a> {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		// Skip per_type_lookup for readability
		f.debug_struct("Record")
			.field("fields", &self.fields)
			.field("name", &self.name)
			.finish()
	}
}

/// Component of a [`SchemaNode`]
#[derive(Debug)]
pub(crate) struct RecordField<'a> {
	pub name: String,
	pub schema: &'a SchemaNode<'a>,
}

/// Component of a [`SchemaNode`]
#[derive(Clone)]
pub(crate) struct Enum {
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

/// Component of a [`SchemaNode`]
#[derive(Clone, Debug)]
pub struct Decimal {
	pub precision: usize,
	pub scale: u32,
	pub repr: DecimalRepr,
}
#[derive(Clone, Debug)]
pub enum DecimalRepr {
	Bytes,
	Fixed(Fixed),
}

impl TryFrom<super::safe::EditableSchema> for Schema {
	type Error = SchemaError;
	fn try_from(safe: super::safe::EditableSchema) -> Result<Self, SchemaError> {
		if safe.nodes().is_empty() {
			return Err(SchemaError::msg(
				"Schema must have at least one node (the root)",
			));
		}
		// The `nodes` allocation should never be moved otherwise references will become
		// invalid
		let mut ret = Self {
			nodes: (0..safe.nodes.len()).map(|_| SchemaNode::Null).collect(),
			fingerprint: safe.canonical_form_rabin_fingerprint()?,
			schema_json: match safe.schema_json {
				None => safe.serialize_to_json()?,
				Some(json) => json,
			},
		};
		let len = ret.nodes.len();
		// Let's be extra-sure (second condition is for calls to add)
		assert!(len > 0 && len == safe.nodes.len() && len <= (isize::MAX as usize));
		let storage_start_ptr = ret.nodes.as_mut_ptr();
		// unsafe closure used below in unsafe block
		let key_to_node =
			|schema_key: super::safe::SchemaKey| -> Result<&'static SchemaNode, SchemaError> {
				let idx = schema_key.idx;
				if idx >= len {
					return Err(SchemaError::msg(format_args!(
						"SchemaKey index {} is out of bounds (len: {})",
						idx, len
					)));
				}
				Ok(unsafe { &*(storage_start_ptr.add(schema_key.idx)) })
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
			// - We don't dereference the references we create in key_to_node until they
			//   they are all initialized.

			unsafe {
				*curr_storage_node_ptr = match safe_node {
					SafeSchemaNode {
						logical_type: Some(LogicalType::Decimal(decimal)),
						type_: SafeSchemaType::Bytes,
					} => SchemaNode::Decimal(Decimal {
						precision: decimal.precision,
						scale: decimal.scale,
						repr: DecimalRepr::Bytes,
					}),
					SafeSchemaNode {
						logical_type: Some(LogicalType::Decimal(decimal)),
						type_: SafeSchemaType::Fixed(fixed),
					} => SchemaNode::Decimal(Decimal {
						precision: decimal.precision,
						scale: decimal.scale,
						repr: DecimalRepr::Fixed(fixed),
					}),
					SafeSchemaNode {
						logical_type: Some(LogicalType::Uuid),
						type_: SafeSchemaType::String,
					} => SchemaNode::Uuid,
					SafeSchemaNode {
						logical_type: Some(LogicalType::Date),
						type_: SafeSchemaType::Int,
					} => SchemaNode::Date,
					SafeSchemaNode {
						logical_type: Some(LogicalType::TimeMillis),
						type_: SafeSchemaType::Int,
					} => SchemaNode::TimeMillis,
					SafeSchemaNode {
						logical_type: Some(LogicalType::TimeMicros),
						type_: SafeSchemaType::Long,
					} => SchemaNode::TimeMicros,
					SafeSchemaNode {
						logical_type: Some(LogicalType::TimestampMillis),
						type_: SafeSchemaType::Long,
					} => SchemaNode::TimestampMillis,
					SafeSchemaNode {
						logical_type: Some(LogicalType::TimestampMicros),
						type_: SafeSchemaType::Long,
					} => SchemaNode::TimestampMicros,
					SafeSchemaNode {
						logical_type: Some(LogicalType::Duration),
						type_: SafeSchemaType::Fixed(fixed),
					} if fixed.size == 12 => SchemaNode::Duration,
					_ => match safe_node.type_ {
						SafeSchemaType::Null => SchemaNode::Null,
						SafeSchemaType::Boolean => SchemaNode::Boolean,
						SafeSchemaType::Int => SchemaNode::Int,
						SafeSchemaType::Long => SchemaNode::Long,
						SafeSchemaType::Float => SchemaNode::Float,
						SafeSchemaType::Double => SchemaNode::Double,
						SafeSchemaType::Bytes => SchemaNode::Bytes,
						SafeSchemaType::String => SchemaNode::String,
						SafeSchemaType::Array(schema_key) => {
							SchemaNode::Array(key_to_node(schema_key)?)
						}
						SafeSchemaType::Map(schema_key) => {
							SchemaNode::Map(key_to_node(schema_key)?)
						}
						SafeSchemaType::Union(union) => SchemaNode::Union({
							Union {
								variants: {
									let mut variants = Vec::with_capacity(union.variants.len());
									for schema_key in union.variants {
										variants.push(key_to_node(schema_key)?);
									}
									variants
								},
								per_type_lookup: {
									// Can't be initialized just yet because other nodes
									// may not have been initialized
									UnionVariantsPerTypeLookup::placeholder()
								},
							}
						}),
						SafeSchemaType::Record(record) => SchemaNode::Record(Record {
							per_name_lookup: record
								.fields
								.iter()
								.enumerate()
								.map(|(i, v)| (v.name.clone(), i))
								.collect(),
							fields: {
								let mut fields = Vec::with_capacity(record.fields.len());
								for field in record.fields {
									fields.push(RecordField {
										name: field.name,
										schema: key_to_node(field.schema)?,
									});
								}
								fields
							},
							name: record.name,
						}),
						SafeSchemaType::Enum(enum_) => SchemaNode::Enum(Enum {
							per_name_lookup: enum_
								.symbols
								.iter()
								.enumerate()
								.map(|(i, v)| (v.clone(), i))
								.collect(),
							symbols: enum_.symbols,
							name: enum_.name,
						}),
						SafeSchemaType::Fixed(fixed) => SchemaNode::Fixed(fixed),
					},
				};
				curr_storage_node_ptr = curr_storage_node_ptr.add(1);
			};
		}
		// Now that all the nodes have been initialized (except their `per_type_lookup`
		// tables) we can initialize the `per_type_lookup` tables
		curr_storage_node_ptr = storage_start_ptr;
		for _ in 0..len {
			// Safety:
			// - UnionVariantsPerTypeLookup won't ever read `per_type_lookup` of the other
			//   nodes, so there are no aliasing issues. (Tbh I'm not even sure that would
			//   really be an issue because we'd have `& &mut` anyway but with that I'm sure
			//   there isn't an issue)
			unsafe {
				match *curr_storage_node_ptr {
					SchemaNode::Union(Union {
						ref variants,
						ref mut per_type_lookup,
					}) => {
						*per_type_lookup = UnionVariantsPerTypeLookup::new(variants);
					}
					_ => {}
				}
				curr_storage_node_ptr = curr_storage_node_ptr.add(1);
			}
		}
		Ok(ret)
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
