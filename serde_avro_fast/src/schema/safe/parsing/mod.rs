mod raw;

use crate::schema::safe::*;

use std::collections::HashMap;

const LATE_NAME_LOOKUP_REMAP_BIT: usize = 1usize << (usize::BITS - 1);

struct SchemaConstructionState<'a> {
	nodes: Vec<SchemaNode>,
	names: HashMap<NameKey<'a>, usize>,
	unresolved_names: Vec<NameKey<'a>>,
}

impl std::str::FromStr for SchemaMut {
	type Err = SchemaError;
	fn from_str(s: &str) -> Result<Self, Self::Err> {
		let mut state = SchemaConstructionState {
			nodes: Vec::new(),
			names: HashMap::new(),
			unresolved_names: Vec::new(),
		};

		let raw_schema: raw::SchemaNode<'_> =
			serde_json::from_str(s).map_err(SchemaError::serde_json)?;

		state.register_node(&raw_schema, None)?;

		// Support for unordered name definitions
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
				if key.idx & LATE_NAME_LOOKUP_REMAP_BIT != 0 {
					*key = resolved_names[key.idx ^ LATE_NAME_LOOKUP_REMAP_BIT];
				}
			};
			for schema_node in &mut state.nodes {
				match &mut schema_node.type_ {
					RegularType::Array(Array { items: key })
					| RegularType::Map(Map { values: key }) => fix_key(key),
					RegularType::Union(union) => union.variants.iter_mut().for_each(fix_key),
					RegularType::Record(record) => {
						record.fields.iter_mut().for_each(|f| fix_key(&mut f.type_))
					}
					RegularType::Null
					| RegularType::Boolean
					| RegularType::Int
					| RegularType::Long
					| RegularType::Float
					| RegularType::Double
					| RegularType::Bytes
					| RegularType::String
					| RegularType::Enum(Enum {
						symbols: _,
						name: _,
					})
					| RegularType::Fixed(Fixed { size: _, name: _ }) => {}
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

		schema
			.check_for_cycles()
			.map_err(|e: UnconditionalCycle| SchemaError::display(e))?;

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
			raw::SchemaNode::Type(type_) => (TypeOrUnion::Type(type_), None),
			raw::SchemaNode::Object(ref object) => {
				(TypeOrUnion::Type(object.type_), Some(&**object))
			}
			raw::SchemaNode::Union(ref union_schemas) => (TypeOrUnion::Union(union_schemas), None),
			raw::SchemaNode::Ref(ref reference) => {
				// This is supposed to be the fullname of a
				// previous named type. According to the spec the type
				// definition should always be parsed before, but we support
				// even if it's unordered because we're not in 1980 anymore.
				let name_key = if let Some((namespace, name)) = reference.rsplit_once('.') {
					NameKey {
						namespace: Some(namespace).filter(|&s| !s.is_empty()),
						name,
					}
				} else {
					NameKey {
						namespace: enclosing_namespace,
						name: reference,
					}
				};
				return Ok(match self.names.get(&name_key) {
					Some(&idx) => SchemaKey { idx },
					None => {
						let idx = self.unresolved_names.len();
						self.unresolved_names.push(name_key);
						SchemaKey {
							idx: idx | LATE_NAME_LOOKUP_REMAP_BIT,
						}
					}
				});
			}
		};
		let idx = self.nodes.len();
		self.nodes.push(RegularType::Null.into()); // Reserve the spot for us

		// Register name->node idx to the name HashMap
		let name_key = if let Some(
			object @ raw::SchemaNodeObject {
				name: Some(name), ..
			},
		) = object
		{
			let name: &str = &*name.0;
			let name_key = if let Some((namespace, name)) = name.rsplit_once('.') {
				NameKey {
					namespace: Some(namespace).filter(|&s| !s.is_empty()),
					name,
				}
			} else {
				NameKey {
					namespace: match object.namespace {
						Some(ref namespace) => {
							// If the object explicitly specifies an empty string
							// as namespace, "this indicates the null namespace"
							// (aka no namespace)
							Some(&*namespace.0).filter(|&s| !s.is_empty())
						}
						None => enclosing_namespace,
					},
					name,
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
		let name = |type_: raw::Type| match name_key {
			None => Err(SchemaError::msg(format_args!(
				"Missing name for type {:?}",
				type_
			))),
			Some(name_key) => Ok((name_key.name(), name_key)),
		};

		let new_node = match type_ {
			TypeOrUnion::Union(union_schemas) => RegularType::Union(Union {
				variants: union_schemas
					.iter()
					.map(|schema| self.register_node(schema, enclosing_namespace))
					.collect::<Result<_, _>>()?,
			}),
			TypeOrUnion::Type(type_) => {
				let name = || name(type_);
				macro_rules! field {
					($name: ident) => {
						match object {
							Some(raw::SchemaNodeObject { $name: Some(v), .. }) => v,
							None => {
								return Err(SchemaError::msg(format_args!(
									"Expected primitive type name, but got {:?} as type which is \
										a complex type, so should be in an object.",
									type_
								)))
							}
							Some(_) => {
								return Err(SchemaError::msg(format_args!(
									concat!("Missing field `", stringify!($name), "` on type {:?}"),
									type_
								)));
							}
						}
					};
				}
				match type_ {
					raw::Type::Array => RegularType::Array(Array {
						items: self.register_node(field!(items), enclosing_namespace)?,
					}),
					raw::Type::Map => RegularType::Map(Map {
						values: self.register_node(field!(values), enclosing_namespace)?,
					}),
					raw::Type::Enum => RegularType::Enum(Enum {
						name: name()?.0,
						symbols: field!(symbols).iter().map(|e| (*e.0).to_owned()).collect(),
					}),
					raw::Type::Fixed => RegularType::Fixed(Fixed {
						name: name()?.0,
						size: *field!(size),
					}),
					raw::Type::Record => {
						let (name, name_key) = name()?;
						RegularType::Record(Record {
							fields: field!(fields)
								.iter()
								.map(|field| {
									Ok(RecordField {
										name: (*field.name.0).to_owned(),
										type_: self
											.register_node(&field.type_, name_key.namespace)?,
									})
								})
								.collect::<Result<_, SchemaError>>()?,
							name,
						})
					}
					raw::Type::Null => RegularType::Null,
					raw::Type::Boolean => RegularType::Boolean,
					raw::Type::Int => RegularType::Int,
					raw::Type::Long => RegularType::Long,
					raw::Type::Float => RegularType::Float,
					raw::Type::Double => RegularType::Double,
					raw::Type::Bytes => RegularType::Bytes,
					raw::Type::String => RegularType::String,
				}
			}
		};

		// Fill the spot we have previously reserved
		self.nodes[idx] = SchemaNode {
			type_: new_node,
			logical_type: match object {
				Some(
					object @ raw::SchemaNodeObject {
						logical_type: Some(logical_type),
						..
					},
				) => Some({
					let logical_type = &*logical_type.0;
					macro_rules! field {
						($name: ident) => {
							match object {
								raw::SchemaNodeObject { $name: Some(v), .. } => *v,
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
					match logical_type {
						"decimal" => LogicalType::Decimal(Decimal {
							precision: field!(precision),
							scale: field!(scale),
						}),
						"uuid" => LogicalType::Uuid,
						"date" => LogicalType::Date,
						"time-millis" => LogicalType::TimeMillis,
						"time-micros" => LogicalType::TimeMicros,
						"timestamp-millis" => LogicalType::TimestampMillis,
						"timestamp-micros" => LogicalType::TimestampMicros,
						"duration" => LogicalType::Duration,
						"big-decimal" => LogicalType::BigDecimal,
						unknown => LogicalType::Unknown(UnknownLogicalType::new(unknown)),
					}
				}),
				_ => None,
			},
		};

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
