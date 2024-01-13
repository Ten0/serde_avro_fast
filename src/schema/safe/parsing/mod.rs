mod canonical_form;
mod raw;

use crate::schema::{
	safe::{Enum, Record, RecordField, Schema, SchemaKey, SchemaNode, UnconditionalCycle, Union},
	Decimal, DecimalRepr, Fixed, Name,
};

use {
	serde::de,
	std::collections::{hash_map, HashMap},
};

/// Any error that may happen when [`parse`](str::parse)ing a schema from a JSON
/// `&str`
#[derive(thiserror::Error, Debug)]
pub enum ParseSchemaError {
	#[error("Invalid Schema JSON: {0}")]
	Json(#[from] serde_json::Error),
	#[error("The Schema contains an unknown reference: {}", .0.fully_qualified_name())]
	InvalidReference(Name),
	#[error("The Schema contains duplicate definitions for {}", .0.fully_qualified_name())]
	DuplicateName(Name),
	#[error("The Schema contains a Decimal whose representation is neither Bytes nor Fixed")]
	IncorrectDecimalRepr,
	#[error("The Schema contains an unreasonably large `scale` for a Decimal")]
	DecimalScaleTooLarge { scale_value: usize },
	#[error("The schema contains a record that ends up always containing itself")]
	UnconditionalCycle,
}

struct SchemaDeserializerState<'a> {
	nodes: Vec<SchemaNode>,
	names: HashMap<NameKey<'a>, usize>,
	unresolved_names: Vec<NameKey<'a>>,
}

struct SchemaDeserializerSeed<'a, 's> {
	state: &'s mut SchemaDeserializerState<'a>,
	enclosing_namespace: Option<&'a str>,
}

impl<'de, 's> de::DeserializeSeed<'de> for SchemaDeserializerSeed<'de, 's> {
	type Value = SchemaKey;

	fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
	where
		D: serde::Deserializer<'de>,
	{
		todo!()
	}
}
const REMAP_BIT: usize = 1usize << (usize::BITS - 1);

impl std::str::FromStr for Schema {
	type Err = ParseSchemaError;
	fn from_str(s: &str) -> Result<Self, Self::Err> {
		let state = SchemaDeserializerState {
			nodes: Vec::new(),
			names: HashMap::new(),
			unresolved_names: Vec::new(),
		};

		let raw_schema: raw::SchemaNode = serde_json::from_str(s)?;

		let mut names: HashMap<Name, usize> = HashMap::new();
		let mut nodes = Vec::new();

		let mut unresolved_names: Vec<Name> = Vec::new();
		raw_schema_node_to_node(
			&mut nodes,
			&mut names,
			&mut unresolved_names,
			&raw_schema,
			None,
		)?;
		fn raw_schema_node_to_node<'a>(
			nodes: &mut Vec<SchemaNode>,
			names: &mut HashMap<NameKey, usize>,
			unresolved_names: &mut Vec<NameKey>,
			raw_schema: &'a raw::SchemaNode,
			enclosing_namespace: Option<&str>,
		) -> Result<SchemaKey, ParseSchemaError> {
			let idx = nodes.len();
			nodes.push(SchemaNode::Null); // Reserve the spot for us

			// Register name->node idx to the name HashMap
			let name = if let Some(name) = raw_schema.name.as_deref() {
				let name_key = if let Some((namespace, name)) = name.rsplit_once('.') {
					NameKey {
						namespace: Some(namespace),
						name,
					}
				} else {
					NameKey {
						namespace: raw_schema.namespace.as_deref().or(enclosing_namespace),
						name: &name,
					}
				};
				let name = match name_key.namespace {
					None => Name {
						fully_qualified_name: name_key.name.to_owned(),
						namespace_delimiter_idx: None,
					},
					Some(namespace) => Name {
						fully_qualified_name: format!("{}.{}", namespace, name_key.name),
						namespace_delimiter_idx: Some(namespace.len()),
					},
				};
				if let Some(_) = names.insert(name_key, idx) {
					return Err(ParseSchemaError::DuplicateName(name));
				}
				Some(name)
			} else {
				None
			};

			let new_node = match raw_schema.type_ {
				apache_avro::Schema::Ref { name } => {
					nodes.pop().unwrap();
					let idx = unresolved_names.len();
					unresolved_names.push(name.fully_qualified_name(enclosing_namespace));
					return Ok(SchemaKey {
						idx: REMAP_BIT | idx,
					});
				}
				apache_avro::Schema::Array(apache_schema) => {
					SchemaNode::Array(raw_schema_node_to_node(
						nodes,
						names,
						unresolved_names,
						apache_schema,
						enclosing_namespace,
					)?)
				}
				apache_avro::Schema::Map(apache_schema) => {
					SchemaNode::Map(raw_schema_node_to_node(
						nodes,
						names,
						unresolved_names,
						apache_schema,
						enclosing_namespace,
					)?)
				}
				apache_avro::Schema::Union(union_schemas) => SchemaNode::Union(Union {
					variants: union_schemas
						.variants()
						.iter()
						.map(|s| {
							raw_schema_node_to_node(
								nodes,
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
									schema: raw_schema_node_to_node(
										nodes,
										names,
										unresolved_names,
										&field.schema,
										namespace,
									)?,
								})
							})
							.collect::<Result<_, ParseSchemaError>>()?,
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
						scale_value
							.try_into()
							.map_err(|_| ParseSchemaError::DecimalScaleTooLarge { scale_value })?
					},
					repr: match &**inner {
						apache_avro::Schema::Bytes => DecimalRepr::Bytes,
						apache_avro::Schema::Fixed {
							name,
							aliases: _,
							doc: _,
							size,
						} => DecimalRepr::Fixed(Fixed {
							name: register_name(name)?,
							size: *size,
						}),
						_ => return Err(ParseSchemaError::IncorrectDecimalRepr),
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
				reference => {
					// Any other type is supposed to be the fullname of a
					// previous named type. According to the spec the type
					// definition should always be parsed before, but we support
					// even if it's unordered because we're not in 1980 anymore.
					let name_key = if let Some((namespace, name)) = reference.rsplit_once('.') {
						NameKey {
							namespace: Some(namespace),
							name,
						}
					} else {
						NameKey {
							namespace: raw_schema.namespace.as_deref().or(enclosing_namespace),
							name: &name,
						}
					};
				}
			};
			nodes[idx] = new_node; // Fill the spot we have previously reserved
			Ok(SchemaKey { idx })
		}

		let resolved_names: Vec<SchemaKey> = unresolved_names
			.into_iter()
			.map(|name| {
				names
					.get(&name)
					.ok_or(ParseSchemaError::InvalidReference(name))
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

		let mut schema = Self {
			nodes,
			fingerprint: raw_schema.rabin_fingerprint(),
			schema_json: String::from_utf8({
				// Sanitize & minify json, preserving all keys.
				let mut serializer = serde_json::Serializer::new(Vec::new());
				serde_transcode::transcode(
					&mut serde_json::Deserializer::from_str(s),
					&mut serializer,
				)?;
				serializer.into_inner()
			})
			.expect("serde_json should not emit invalid UTF-8"),
		};

		schema
			.check_for_cycles()
			.map_err(|_: UnconditionalCycle| ParseSchemaError::UnconditionalCycle)?;

		Ok(schema)
	}
}

#[derive(PartialEq, Eq, Hash)]
struct NameKey<'a> {
	namespace: Option<&'a str>,
	name: &'a str,
}
