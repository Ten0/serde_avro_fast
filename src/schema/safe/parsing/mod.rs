mod raw;

use crate::schema::{safe::*, SchemaError};

use std::collections::HashMap;

const REMAP_BIT: usize = 1usize << (usize::BITS - 1);

struct SchemaConstructionState<'a> {
	nodes: Vec<SchemaNode>,
	names: HashMap<NameKey<'a>, usize>,
	unresolved_names: Vec<NameKey<'a>>,
}

impl std::str::FromStr for EditableSchema {
	type Err = SchemaError;
	fn from_str(s: &str) -> Result<Self, Self::Err> {
		let mut state = SchemaConstructionState {
			nodes: Vec::new(),
			names: HashMap::new(),
			unresolved_names: Vec::new(),
		};

		let raw_schema: raw::SchemaNode =
			serde_json::from_str(s).map_err(SchemaError::serde_json)?;

		state.register_node(&raw_schema, None)?;

		// Support for unordered definitions
		if !state.unresolved_names.is_empty() {
			let resolved_names: Vec<SchemaKey> = state
				.unresolved_names
				.into_iter()
				.map(|name| {
					state
						.names
						.get(&name)
						.ok_or(SchemaError::msg(format_args!(
							"The Schema contains an unknown reference: {}",
							name,
						)))
						.map(|&idx| SchemaKey { idx })
				})
				.collect::<Result<_, _>>()?;
			let fix_key = |key: &mut SchemaKey| {
				if key.idx & REMAP_BIT != 0 {
					*key = resolved_names[key.idx ^ REMAP_BIT];
				}
			};
			for schema_node in &mut state.nodes {
				match &mut schema_node.type_ {
					SchemaType::Array(key) | SchemaType::Map(key) => fix_key(key),
					SchemaType::Union(union) => union.variants.iter_mut().for_each(fix_key),
					SchemaType::Record(record) => record
						.fields
						.iter_mut()
						.for_each(|f| fix_key(&mut f.schema)),
					SchemaType::Null
					| SchemaType::Boolean
					| SchemaType::Int
					| SchemaType::Long
					| SchemaType::Float
					| SchemaType::Double
					| SchemaType::Bytes
					| SchemaType::String
					| SchemaType::Enum(Enum {
						symbols: _,
						name: _,
					})
					| SchemaType::Fixed(Fixed { size: _, name: _ }) => {}
				}
			}
		}

		let schema = Self {
			nodes: state.nodes,
			schema_json: Some(
				String::from_utf8({
					// Sanitize & minify json, preserving all keys.
					let mut serializer = serde_json::Serializer::new(Vec::new());
					serde_transcode::transcode(
						&mut serde_json::Deserializer::from_str(s),
						&mut serializer,
					)
					.map_err(SchemaError::serde_json)?;
					serializer.into_inner()
				})
				.map_err(|e| {
					SchemaError::msg(format_args!(
						"serde_json should not emit invalid UTF-8 but got {e}"
					))
				})?,
			),
		};

		schema.check_for_cycles().map_err(|_: UnconditionalCycle| {
			SchemaError::msg("The schema contains a record that ends up always containing itself")
		})?;

		Ok(schema)
	}
}

impl<'a> SchemaConstructionState<'a> {
	fn register_node(
		&mut self,
		raw_schema: &'a raw::SchemaNode<'a>,
		enclosing_namespace: Option<&'a str>,
	) -> Result<SchemaKey, SchemaError> {
		enum TypeOrUnion<'r, 'a> {
			Type(raw::Type),
			Union(&'r Vec<raw::SchemaNode<'a>>),
		}
		let (type_, object) = match *raw_schema {
			raw::SchemaNode::TypeOnly(type_) => (TypeOrUnion::Type(type_), None),
			raw::SchemaNode::Object(ref object) => (TypeOrUnion::Type(object.type_), Some(object)),
			raw::SchemaNode::Union(ref union_schemas) => (TypeOrUnion::Union(union_schemas), None),
			raw::SchemaNode::Ref(ref reference) => {
				// This is supposed to be the fullname of a
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
						namespace: None,
						name: &reference,
					}
				};
				return Ok(match self.names.get(&name_key) {
					Some(&idx) => SchemaKey { idx },
					None => {
						let idx = self.unresolved_names.len();
						self.unresolved_names.push(name_key);
						SchemaKey {
							idx: idx | REMAP_BIT,
						}
					}
				});
			}
		};
		let idx = self.nodes.len();
		self.nodes.push(SchemaNode {
			type_: SchemaType::Null,
			logical_type: None,
		}); // Reserve the spot for us

		// Register name->node idx to the name HashMap
		let name_key = if let Some(
			raw_schema @ raw::SchemaNodeObject {
				name: Some(name), ..
			},
		) = object
		{
			let name: &str = &*name.0;
			let name_key = if let Some((namespace, name)) = name.rsplit_once('.') {
				NameKey {
					namespace: Some(namespace),
					name,
				}
			} else {
				NameKey {
					namespace: raw_schema
						.namespace
						.as_ref()
						.map(|c| &*c.0)
						.or(enclosing_namespace),
					name: &name,
				}
			};
			if let Some(_) = self.names.insert(name_key, idx) {
				return Err(SchemaError::msg(format_args!(
					"The Schema contains duplicate definitions for {}",
					name_key
				)));
			}
			Some(name_key)
		} else {
			None
		};
		let name = || match name_key {
			None => Err(SchemaError::msg("Missing name")),
			Some(name_key) => Ok((name_key.name(), name_key)),
		};

		let new_node = match type_ {
			TypeOrUnion::Union(union_schemas) => SchemaType::Union(Union {
				variants: union_schemas
					.iter()
					.map(|schema| self.register_node(schema, enclosing_namespace))
					.collect::<Result<_, _>>()?,
			}),
			TypeOrUnion::Type(type_) => {
				macro_rules! field {
					($name: ident) => {
						match object {
							Some(raw::SchemaNodeObject { $name: Some(v), .. }) => v,
							_ => {
								return Err(SchemaError::msg(format_args!(
									concat!("Missing field `", stringify!($name), "` on type {:?}",),
									type_
								)));
							}
						}
					};
				}
				match type_ {
					raw::Type::Array => {
						SchemaType::Array(self.register_node(field!(items), enclosing_namespace)?)
					}
					raw::Type::Map => {
						SchemaType::Map(self.register_node(field!(values), enclosing_namespace)?)
					}

					raw::Type::Enum => SchemaType::Enum(Enum {
						name: name()?.0,
						symbols: field!(symbols).iter().map(|e| (*e.0).to_owned()).collect(),
					}),
					raw::Type::Fixed => SchemaType::Fixed(Fixed {
						name: name()?.0,
						size: *field!(size),
					}),
					raw::Type::Record => {
						let (name, name_key) = name()?;
						SchemaType::Record(Record {
							fields: field!(fields)
								.iter()
								.map(|field| {
									Ok(RecordField {
										name: (*field.name.0).to_owned(),
										schema: self
											.register_node(&field.type_, name_key.namespace)?,
									})
								})
								.collect::<Result<_, SchemaError>>()?,
							name,
						})
					}
					raw::Type::Null => SchemaType::Null,
					raw::Type::Boolean => SchemaType::Boolean,
					raw::Type::Int => SchemaType::Int,
					raw::Type::Long => SchemaType::Long,
					raw::Type::Float => SchemaType::Float,
					raw::Type::Double => SchemaType::Double,
					raw::Type::Bytes => SchemaType::Bytes,
					raw::Type::String => SchemaType::String,
				}
			}
		};

		self.nodes[idx] = SchemaNode {
			type_: new_node,
			logical_type: match object {
				Some(raw::SchemaNodeObject {
					logical_type: Some(logical_type),
					..
				}) => {
					macro_rules! field {
						($name: ident) => {
							match object {
								Some(raw::SchemaNodeObject { $name: Some(v), .. }) => *v,
								_ => {
									return Err(SchemaError::msg(format_args!(
										concat!(
											"Missing field `",
											stringify!($name),
											"` on logical type {:?}",
										),
										logical_type
									)));
								}
							}
						};
					}
					match *logical_type {
						"decimal" => Some(LogicalType::Decimal(Decimal {
							precision: field!(precision),
							scale: field!(scale),
						})),
						"uuid" => Some(LogicalType::Uuid),
						"date" => Some(LogicalType::Date),
						"time-millis" => Some(LogicalType::TimeMillis),
						"time-micros" => Some(LogicalType::TimeMicros),
						"timestamp-millis" => Some(LogicalType::TimestampMillis),
						"timestamp-micros" => Some(LogicalType::TimestampMicros),
						"duration" => Some(LogicalType::Duration),
						_ => None,
					}
				}
				_ => None,
			},
		}; // Fill the spot we have previously reserved
		Ok(SchemaKey { idx })
	}
}

#[derive(PartialEq, Eq, Hash, Clone, Copy)]
struct NameKey<'a> {
	namespace: Option<&'a str>,
	name: &'a str,
}
impl NameKey<'_> {
	fn name(&self) -> Name {
		match self.namespace {
			None => Name {
				fully_qualified_name: self.name.to_owned(),
				namespace_delimiter_idx: None,
			},
			Some(namespace) => Name {
				fully_qualified_name: format!("{}.{}", namespace, self.name),
				namespace_delimiter_idx: Some(namespace.len()),
			},
		}
	}
}
impl std::fmt::Display for NameKey<'_> {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match self.namespace {
			None => self.name.fmt(f),
			Some(namespace) => write!(f, "{}.{}", namespace, self.name),
		}
	}
}
