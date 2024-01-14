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

		state.register_node(&raw_schema, None, None)?;

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
				match schema_node {
					SchemaNode::RegularType(schema_type) => match schema_type {
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
					},
					SchemaNode::LogicalType {
						inner: _,
						logical_type: _,
					} => {}
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
		will_have_logical_type: Option<&str>,
	) -> Result<SchemaKey, SchemaError> {
		Ok(match *raw_schema {
			raw::SchemaNode::Type(type_) => {
				let idx = self.nodes.len();
				self.nodes.push(SchemaNode::RegularType(match type_ {
					raw::Type::Null => SchemaType::Null,
					raw::Type::Boolean => SchemaType::Boolean,
					raw::Type::Int => SchemaType::Int,
					raw::Type::Long => SchemaType::Long,
					raw::Type::Float => SchemaType::Float,
					raw::Type::Double => SchemaType::Double,
					raw::Type::Bytes => SchemaType::Bytes,
					raw::Type::String => SchemaType::String,
					complex_type @ (raw::Type::Array
					| raw::Type::Map
					| raw::Type::Record
					| raw::Type::Enum
					| raw::Type::Fixed) => {
						return Err(SchemaError::msg(format_args!(
							"Expected primitive type name, but got {:?} as type which is a complex \
								type, so should be in an object.",
							complex_type
						)))
					}
				}));
				SchemaKey { idx }
			}
			raw::SchemaNode::Object(ref object) => {
				let idx = self.nodes.len();
				let object = &**object;
				// Register name->node idx to the name HashMap
				let name_key = if let Some(ref name) = object.name {
					let name: &str = &*name.0;
					let name_key = if let Some((namespace, name)) = name.rsplit_once('.') {
						NameKey {
							namespace: Some(namespace),
							name,
						}
					} else {
						NameKey {
							namespace: object
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
				let name = |type_: raw::Type| match name_key {
					None => Err(SchemaError::msg(format_args!(
						"Missing name for type {:?}",
						type_
					))),
					Some(name_key) => Ok((name_key.name(), name_key)),
				};

				self.nodes.push(SchemaNode::RegularType(SchemaType::Null)); // Reserve the spot for us
				let new_node: SchemaNode = match object.logical_type {
					None => SchemaNode::RegularType({
						macro_rules! field {
							($type_: ident $name: ident) => {
								match &object.$name {
									Some(v) => v,
									_ => {
										return Err(SchemaError::msg(format_args!(
											concat!(
												"Missing field `",
												stringify!($name),
												"` on type {:?}",
											),
											$type_
										)));
									}
								}
							};
						}
						match object.type_ {
							raw::SchemaNode::Type(t @ raw::Type::Array) => SchemaType::Array(
								self.register_node(field!(t items), enclosing_namespace, None)?,
							),
							raw::SchemaNode::Type(t @ raw::Type::Map) => SchemaType::Map(
								self.register_node(field!(t values), enclosing_namespace, None)?,
							),
							raw::SchemaNode::Type(t @ raw::Type::Enum) => SchemaType::Enum(Enum {
								name: name(t)?.0,
								symbols: field!(t symbols)
									.iter()
									.map(|e| (*e.0).to_owned())
									.collect(),
							}),
							raw::SchemaNode::Type(t @ raw::Type::Fixed) => {
								SchemaType::Fixed(Fixed {
									name: name(t)?.0,
									size: *field!(t size),
								})
							}
							raw::SchemaNode::Type(t @ raw::Type::Record) => {
								let (name, name_key) = name(t)?;
								SchemaType::Record(Record {
									fields: field!(t fields)
										.iter()
										.map(|field| {
											Ok(RecordField {
												name: (*field.name.0).to_owned(),
												schema: self.register_node(
													&field.type_,
													name_key.namespace,
													None,
												)?,
											})
										})
										.collect::<Result<_, SchemaError>>()?,
									name,
								})
							}
							raw::SchemaNode::Type(
								t @ (raw::Type::Null
								| raw::Type::Boolean
								| raw::Type::Int
								| raw::Type::Long
								| raw::Type::Float
								| raw::Type::Double
								| raw::Type::Bytes
								| raw::Type::String),
							) => {
								return Err(SchemaError::msg(format_args!(
								"Expected complex type name, but got {t:?} as type in a type object \
									and there is no logical type in that type object.",
							)));
							}
							raw::SchemaNode::Ref(ref t) => {
								return Err(SchemaError::msg(format_args!(
									"Expected complex type name, but got another type object \
									(ref to {t}) as type in a type object \
									and there is no logical type in that type object.",
								)));
							}
							raw::SchemaNode::Object(_) => {
								return Err(SchemaError::new(
									"Expected complex type name, but got another type object \
									({{ ... }}) as type in a type object \
									and there is no logical type in that type object.",
								));
							}
							raw::SchemaNode::Union(_) => {
								return Err(SchemaError::new(
								"Expected complex type name, but got a union as type in a type \
									object and there is no logical type in that type object.",
							));
							}
						}
					}),
					Some(ref logical_type) => {
						let logical_type = &*logical_type.0;
						if let Some(will_have_logical_type) = will_have_logical_type {
							return Err(SchemaError::msg(format_args!(
								"Immediately-nested logical types: {:?} in {:?}",
								logical_type, will_have_logical_type
							)));
						} else {
							SchemaNode::LogicalType {
								logical_type: {
									macro_rules! field {
										($name: ident) => {
											match object.$name {
												Some(v) => v,
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
										unknown => LogicalType::Unknown(unknown.to_owned()),
									}
								},
								inner: self.register_node(
									&object.type_,
									enclosing_namespace,
									Some(logical_type),
								)?,
							}
						}
					}
				};
				self.nodes[idx] = new_node;
				SchemaKey { idx }
			}
			raw::SchemaNode::Union(ref union_schemas) => {
				let idx = self.nodes.len();
				self.nodes.push(SchemaNode::RegularType(SchemaType::Null)); // Reserve the spot for us
				let new_node = SchemaNode::RegularType(SchemaType::Union(Union {
					variants: union_schemas
						.iter()
						.map(|schema| self.register_node(schema, enclosing_namespace, None))
						.collect::<Result<_, _>>()?,
				}));
				self.nodes[idx] = new_node;
				SchemaKey { idx }
			}
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
		})
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
