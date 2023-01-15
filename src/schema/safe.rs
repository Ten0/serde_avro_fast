//! Defines a fully-safe counterpart of the [`Schema`](crate::Schema) that is used for its initialization
use std::collections::{hash_map, HashMap};

/// A fully-safe counterpart of the [`Schema`](crate::Schema) that is used for its initialization
#[derive(Clone, Debug)]
pub struct Schema {
	// First node in the array is considered to be the root
	pub(super) nodes: Vec<SchemaNode>,
}

impl Schema {
	pub fn nodes(&self) -> &[SchemaNode] {
		&self.nodes
	}
}

#[derive(Copy, Clone, Debug)]
pub struct SchemaKey {
	pub(super) idx: usize,
}

/// Represents any valid Avro schema
/// More information about Avro schemas can be found in the
/// [Avro Specification](https://avro.apache.org/docs/current/spec.html#schemas)
#[derive(Clone, Debug)]
pub enum SchemaNode {
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
	/// A `array` Avro schema. Avro arrays are required to have the same type for each element.
	/// This variant holds the `Schema` for the array element type.
	Array(SchemaKey),
	/// A `map` Avro schema.
	/// `Map` holds a pointer to the `Schema` of its values, which must all be the same schema.
	/// `Map` keys are assumed to be `string`.
	Map(SchemaKey),
	/// A `union` Avro schema.
	Union(UnionSchema),
	/// A `record` Avro schema.
	///
	/// The `lookup` table maps field names to their position in the `Vec`
	/// of `fields`.
	Record(RecordSchema),
	/// An `enum` Avro schema.
	Enum { symbols: Vec<String> },
	/// A `fixed` Avro schema.
	Fixed { size: usize },
	/// Logical type which represents `Decimal` values. The underlying type is serialized and
	/// deserialized as `Schema::Bytes` or `Schema::Fixed`.
	///
	/// `scale` defaults to 0 and is an integer greater than or equal to 0 and `precision` is an
	/// integer greater than 0.
	Decimal {
		precision: usize,
		scale: usize,
		inner: SchemaKey,
	},
	/// A universally unique identifier, annotating a string.
	Uuid,
	/// Logical type which represents the number of days since the unix epoch.
	/// Serialization format is `Schema::Int`.
	Date,
	/// The time of day in number of milliseconds after midnight with no reference any calendar,
	/// time zone or date in particular.
	TimeMillis,
	/// The time of day in number of microseconds after midnight with no reference any calendar,
	/// time zone or date in particular.
	TimeMicros,
	/// An instant in time represented as the number of milliseconds after the UNIX epoch.
	TimestampMillis,
	/// An instant in time represented as the number of microseconds after the UNIX epoch.
	TimestampMicros,
	/// An amount of time defined by a number of months, days and milliseconds.
	Duration,
}

#[derive(Clone, Debug)]
pub struct UnionSchema {
	pub variants: Vec<SchemaKey>,
}

#[derive(Clone, Debug)]
pub struct RecordSchema {
	pub fields: Vec<RecordField>,
}

#[derive(Clone, Debug)]
pub struct RecordField {
	pub name: String,
	pub schema: SchemaKey,
}

impl std::str::FromStr for Schema {
	type Err = ParseSchemaError;
	fn from_str(s: &str) -> Result<Self, Self::Err> {
		let apache_schema = apache::Schema::parse_str(s)?;
		Ok(Schema::from_apache_schema(&apache_schema)?)
	}
}
#[derive(thiserror::Error, Debug)]
pub enum ParseSchemaError {
	#[error("Could not parse Schema using apache-avro lib: {0}")]
	ApacheAvro(#[from] apache::Error),
	#[error("Could not turn apache-avro schema into fast schema: {0}")]
	ApacheToFast(#[from] BuildSchemaFromApacheSchemaError),
}

impl std::ops::Index<SchemaKey> for Schema {
	type Output = SchemaNode;
	fn index(&self, key: SchemaKey) -> &Self::Output {
		&self.nodes[key.idx]
	}
}

pub(crate) mod apache {
	pub(crate) use apache_avro::{schema::Name, Error, Schema};
}

#[derive(Debug, thiserror::Error)]
pub enum BuildSchemaFromApacheSchemaError {
	#[error("The apache_avro::Schema contained an unknown reference: {0}")]
	InvalidReference(apache::Name),
	#[error("The apache_avro::Schema contains duplicate definitions for {0}")]
	DuplicateName(apache::Name),
}
impl Schema {
	pub fn from_apache_schema(apache_schema: &apache::Schema) -> Result<Self, BuildSchemaFromApacheSchemaError> {
		let mut names: HashMap<apache::Name, usize> = HashMap::new();
		let mut schema = Self { nodes: Vec::new() };
		let mut unresolved_names: Vec<apache::Name> = Vec::new();
		const REMAP_BIT: usize = 1usize << (usize::BITS - 1);
		apache_schema_to_node(&mut schema, &mut names, &mut unresolved_names, apache_schema, &None)?;
		fn apache_schema_to_node<'a>(
			schema: &mut Schema,
			names: &mut HashMap<apache::Name, usize>,
			unresolved_names: &mut Vec<apache::Name>,
			apache_schema: &'a apache::Schema,
			enclosing_namespace: &Option<String>,
		) -> Result<SchemaKey, BuildSchemaFromApacheSchemaError> {
			let idx = schema.nodes.len();
			schema.nodes.push(SchemaNode::Null);
			let new_node = match apache_schema {
				apache::Schema::Ref { name } => {
					schema.nodes.pop().unwrap();
					let idx = unresolved_names.len();
					unresolved_names.push(name.fully_qualified_name(enclosing_namespace));
					return Ok(SchemaKey { idx: REMAP_BIT | idx });
				}
				apache::Schema::Array(apache_schema) => SchemaNode::Array(apache_schema_to_node(
					schema,
					names,
					unresolved_names,
					apache_schema,
					enclosing_namespace,
				)?),
				apache::Schema::Map(apache_schema) => SchemaNode::Map(apache_schema_to_node(
					schema,
					names,
					unresolved_names,
					apache_schema,
					enclosing_namespace,
				)?),
				apache::Schema::Union(union_schemas) => SchemaNode::Union(UnionSchema {
					variants: union_schemas
						.variants()
						.iter()
						.map(|s| apache_schema_to_node(schema, names, unresolved_names, s, enclosing_namespace))
						.collect::<Result<_, _>>()?,
				}),
				apache::Schema::Enum { name, symbols, .. } => {
					match names.entry(name.fully_qualified_name(enclosing_namespace)) {
						hash_map::Entry::Occupied(occ) => {
							return Err(BuildSchemaFromApacheSchemaError::DuplicateName(occ.remove_entry().0))
						}
						hash_map::Entry::Vacant(vacant) => {
							vacant.insert(idx);
							SchemaNode::Enum {
								symbols: symbols.clone(),
							}
						}
					}
				}
				apache::Schema::Fixed { name, size, .. } => {
					match names.entry(name.fully_qualified_name(enclosing_namespace)) {
						hash_map::Entry::Occupied(occ) => {
							return Err(BuildSchemaFromApacheSchemaError::DuplicateName(occ.remove_entry().0))
						}
						hash_map::Entry::Vacant(vacant) => {
							vacant.insert(idx);
							SchemaNode::Fixed { size: *size }
						}
					}
				}
				apache::Schema::Record { name, fields, .. } => {
					let fully_qualified_name = name.fully_qualified_name(enclosing_namespace);
					let record_schema = RecordSchema {
						fields: fields
							.iter()
							.map(|field| {
								Ok(RecordField {
									name: field.name.clone(),
									schema: apache_schema_to_node(
										schema,
										names,
										unresolved_names,
										&field.schema,
										&fully_qualified_name.namespace,
									)?,
								})
							})
							.collect::<Result<_, BuildSchemaFromApacheSchemaError>>()?,
					};
					match names.entry(fully_qualified_name) {
						hash_map::Entry::Occupied(occ) => {
							return Err(BuildSchemaFromApacheSchemaError::DuplicateName(occ.remove_entry().0))
						}
						hash_map::Entry::Vacant(vacant) => {
							vacant.insert(idx);
							SchemaNode::Record(record_schema)
						}
					}
				}
				apache_avro::Schema::Decimal {
					precision,
					scale,
					inner,
				} => SchemaNode::Decimal {
					precision: *precision,
					scale: *scale,
					inner: apache_schema_to_node(schema, names, unresolved_names, inner, enclosing_namespace)?,
				},
				apache_avro::Schema::Null => SchemaNode::Null,
				apache_avro::Schema::Boolean => SchemaNode::Boolean,
				apache_avro::Schema::Int => SchemaNode::Int,
				apache_avro::Schema::Long => SchemaNode::Long,
				apache_avro::Schema::Float => SchemaNode::Float,
				apache_avro::Schema::Double => SchemaNode::Double,
				apache_avro::Schema::Bytes => SchemaNode::Bytes,
				apache_avro::Schema::String => SchemaNode::String,
				apache_avro::Schema::Uuid => SchemaNode::Uuid,
				apache_avro::Schema::Date => SchemaNode::Date,
				apache_avro::Schema::TimeMillis => SchemaNode::TimeMillis,
				apache_avro::Schema::TimeMicros => SchemaNode::TimeMicros,
				apache_avro::Schema::TimestampMillis => SchemaNode::TimestampMillis,
				apache_avro::Schema::TimestampMicros => SchemaNode::TimestampMicros,
				apache_avro::Schema::Duration => SchemaNode::Duration,
			};
			schema.nodes[idx] = new_node;
			Ok(SchemaKey { idx })
		}

		let resolved_names: Vec<SchemaKey> = unresolved_names
			.into_iter()
			.map(|name| {
				names
					.get(&name)
					.ok_or(BuildSchemaFromApacheSchemaError::InvalidReference(name))
					.map(|&idx| SchemaKey { idx })
			})
			.collect::<Result<_, _>>()?;
		let fix_key = |key: &mut SchemaKey| {
			if key.idx & REMAP_BIT != 0 {
				*key = resolved_names[key.idx ^ REMAP_BIT];
			}
		};
		for schema_node in &mut schema.nodes {
			match schema_node {
				SchemaNode::Array(key)
				| SchemaNode::Map(key)
				| SchemaNode::Decimal {
					inner: key,
					precision: _,
					scale: _,
				} => fix_key(key),
				SchemaNode::Union(union) => union.variants.iter_mut().for_each(fix_key),
				SchemaNode::Record(record) => record.fields.iter_mut().for_each(|f| fix_key(&mut f.schema)),
				SchemaNode::Null
				| SchemaNode::Boolean
				| SchemaNode::Int
				| SchemaNode::Long
				| SchemaNode::Float
				| SchemaNode::Double
				| SchemaNode::Bytes
				| SchemaNode::String
				| SchemaNode::Enum { symbols: _ }
				| SchemaNode::Fixed { size: _ }
				| SchemaNode::Uuid
				| SchemaNode::Date
				| SchemaNode::TimeMillis
				| SchemaNode::TimeMicros
				| SchemaNode::TimestampMillis
				| SchemaNode::TimestampMicros
				| SchemaNode::Duration => {}
			}
		}

		Ok(schema)
	}
}
