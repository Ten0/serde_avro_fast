//! Defines a fully-safe counterpart of the [`Schema`](crate::Schema) that is
//! used for its initialization

use super::{Decimal, DecimalRepr, Fixed, Name};

use std::collections::{hash_map, HashMap};

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
	pub(super) parsing_canonical_form: String,
	pub(super) fingerprint: [u8; 8],
}

impl Schema {
	/// Obtain the underlying graph storage
	///
	/// [`SchemaKey`]s can be converted to indexes of this `Vec`.
	pub fn into_nodes(self) -> Vec<SchemaNode> {
		self.nodes
	}

	/// Obtain the
	/// [Parsing Canonical Form](https://avro.apache.org/docs/current/specification/#parsing-canonical-form-for-schemas)
	/// of the schema
	pub fn parsing_canonical_form(&self) -> &str {
		&&self.parsing_canonical_form
	}

	/// Obtain the Rabin fingerprint of the schema
	pub fn rabin_fingerprint(&self) -> &[u8; 8] {
		&self.fingerprint
	}
}

/// The location of a node in the [`Schema`]
///
/// This can be used to [`Index`](std::ops::Index) into the [`Schema`].
#[derive(Copy, Clone, Debug)]
pub struct SchemaKey {
	pub(super) idx: usize,
}

impl SchemaKey {
	pub fn from_idx(idx: usize) -> Self {
		Self { idx }
	}
	pub fn idx(self) -> usize {
		self.idx
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

impl std::str::FromStr for Schema {
	type Err = ParseSchemaError;
	fn from_str(s: &str) -> Result<Self, Self::Err> {
		let apache_schema = apache_avro::Schema::parse_str(s)?;
		Ok(Schema::from_apache_schema(&apache_schema)?)
	}
}
/// Any error that may happen when [`parse`](str::parse)ing a schema from a JSON
/// `&str`
#[derive(thiserror::Error, Debug)]
pub enum ParseSchemaError {
	#[error("Could not parse Schema using apache-avro lib: {0}")]
	ApacheAvro(#[from] apache_avro::Error),
	#[error("Could not turn apache-avro schema into fast schema: {0}")]
	ApacheToFast(#[from] BuildSchemaFromApacheSchemaError),
}

impl std::ops::Index<SchemaKey> for Schema {
	type Output = SchemaNode;
	fn index(&self, key: SchemaKey) -> &Self::Output {
		&self.nodes[key.idx]
	}
}

/// Any error that may happen when converting a [`Schema`](apache_avro::Schema)
/// from the `apache-avro` crate into a [`Schema`]
#[derive(Debug, thiserror::Error)]
pub enum BuildSchemaFromApacheSchemaError {
	#[error("The apache_avro::Schema contained an unknown reference: {0}")]
	InvalidReference(apache_avro::schema::Name),
	#[error("The apache_avro::Schema contains duplicate definitions for {0}")]
	DuplicateName(apache_avro::schema::Name),
	#[error("The apache_avro::Schema contains a Decimal whose representation is neither Bytes nor Fixed")]
	IncorrectDecimalRepr,
	#[error("The apache_avro::Schema contains an unreasonably large `scale` for a Decimal")]
	DecimalScaleTooLarge { scale_value: usize },
}
impl Schema {
	/// Attempt to convert a [`Schema`](apache_avro::Schema) from the
	/// `apache-avro` crate into a [`Schema`]
	pub fn from_apache_schema(
		apache_schema: &apache_avro::Schema,
	) -> Result<Self, BuildSchemaFromApacheSchemaError> {
		let mut names: HashMap<apache_avro::schema::Name, usize> = HashMap::new();
		let parsing_canonical_form = apache_schema.canonical_form();
		let mut schema = Self {
			nodes: Vec::new(),
			fingerprint: <apache_avro::rabin::Rabin as digest::Digest>::digest(
				&parsing_canonical_form,
			)
			.into(),
			parsing_canonical_form,
		};
		let mut unresolved_names: Vec<apache_avro::schema::Name> = Vec::new();
		const REMAP_BIT: usize = 1usize << (usize::BITS - 1);
		apache_schema_to_node(
			&mut schema,
			&mut names,
			&mut unresolved_names,
			apache_schema,
			&None,
		)?;
		fn apache_schema_to_node<'a>(
			schema: &mut Schema,
			names: &mut HashMap<apache_avro::schema::Name, usize>,
			unresolved_names: &mut Vec<apache_avro::schema::Name>,
			apache_schema: &'a apache_avro::Schema,
			enclosing_namespace: &Option<String>,
		) -> Result<SchemaKey, BuildSchemaFromApacheSchemaError> {
			let idx = schema.nodes.len();
			schema.nodes.push(SchemaNode::Null);
			let mut register_name = |name: &apache_avro::schema::Name|
			 -> Result<Name, BuildSchemaFromApacheSchemaError> {
				match names.entry(name.fully_qualified_name(enclosing_namespace)) {
					hash_map::Entry::Occupied(occ) => Err(
						BuildSchemaFromApacheSchemaError::DuplicateName(occ.remove_entry().0),
					),
					hash_map::Entry::Vacant(vacant) => {
						let fully_qualified_name = vacant.key();
						let name = match fully_qualified_name.namespace {
							None => Name {
								fully_qualified_name: fully_qualified_name.name.clone(),
								namespace_delimiter_idx: None,
							},
							Some(ref namespace) => Name {
								fully_qualified_name: format!(
									"{namespace}.{}",
									fully_qualified_name.name
								),
								namespace_delimiter_idx: Some(namespace.len()),
							},
						};
						vacant.insert(idx);
						Ok(name)
					}
				}
			};
			let new_node = match apache_schema {
				apache_avro::Schema::Ref { name } => {
					schema.nodes.pop().unwrap();
					let idx = unresolved_names.len();
					unresolved_names.push(name.fully_qualified_name(enclosing_namespace));
					return Ok(SchemaKey {
						idx: REMAP_BIT | idx,
					});
				}
				apache_avro::Schema::Array(apache_schema) => {
					SchemaNode::Array(apache_schema_to_node(
						schema,
						names,
						unresolved_names,
						apache_schema,
						enclosing_namespace,
					)?)
				}
				apache_avro::Schema::Map(apache_schema) => SchemaNode::Map(apache_schema_to_node(
					schema,
					names,
					unresolved_names,
					apache_schema,
					enclosing_namespace,
				)?),
				apache_avro::Schema::Union(union_schemas) => SchemaNode::Union(Union {
					variants: union_schemas
						.variants()
						.iter()
						.map(|s| {
							apache_schema_to_node(
								schema,
								names,
								unresolved_names,
								s,
								enclosing_namespace,
							)
						})
						.collect::<Result<_, _>>()?,
				}),
				apache_avro::Schema::Enum { name, symbols, .. } => SchemaNode::Enum(Enum {
					name: register_name(name)?,
					symbols: symbols.clone(),
				}),
				apache_avro::Schema::Fixed { name, size, .. } => SchemaNode::Fixed(Fixed {
					name: register_name(name)?,
					size: *size,
				}),
				apache_avro::Schema::Record { name, fields, .. } => {
					let namespace = match &name.namespace {
						namespace @ Some(_) => namespace,
						None => enclosing_namespace,
					};
					let name = register_name(name)?;
					SchemaNode::Record(Record {
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
										namespace,
									)?,
								})
							})
							.collect::<Result<_, BuildSchemaFromApacheSchemaError>>()?,
						name,
					})
				}
				apache_avro::Schema::Decimal {
					precision,
					scale,
					inner,
				} => SchemaNode::Decimal(Decimal {
					precision: *precision,
					scale: {
						let scale_value = *scale;
						scale_value.try_into().map_err(|_| {
							BuildSchemaFromApacheSchemaError::DecimalScaleTooLarge { scale_value }
						})?
					},
					repr: match &**inner {
						apache_avro::Schema::Bytes => DecimalRepr::Bytes,
						apache_avro::Schema::Fixed {
							name,
							aliases: _,
							doc: _,
							size,
							attributes: _,
						} => DecimalRepr::Fixed(Fixed {
							name: register_name(name)?,
							size: *size,
						}),
						_ => return Err(BuildSchemaFromApacheSchemaError::IncorrectDecimalRepr),
					},
				}),
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
				SchemaNode::Array(key) | SchemaNode::Map(key) => fix_key(key),
				SchemaNode::Union(union) => union.variants.iter_mut().for_each(fix_key),
				SchemaNode::Record(record) => record
					.fields
					.iter_mut()
					.for_each(|f| fix_key(&mut f.schema)),
				SchemaNode::Decimal(Decimal {
					repr: DecimalRepr::Bytes | DecimalRepr::Fixed(Fixed { size: _, name: _ }),
					precision: _,
					scale: _,
				})
				| SchemaNode::Null
				| SchemaNode::Boolean
				| SchemaNode::Int
				| SchemaNode::Long
				| SchemaNode::Float
				| SchemaNode::Double
				| SchemaNode::Bytes
				| SchemaNode::String
				| SchemaNode::Enum(Enum {
					symbols: _,
					name: _,
				})
				| SchemaNode::Fixed(Fixed { size: _, name: _ })
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
