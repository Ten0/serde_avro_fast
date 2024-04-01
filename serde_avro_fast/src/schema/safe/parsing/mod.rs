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

		let raw_schema: raw::SchemaNode =
			serde_json::from_str(s).map_err(SchemaError::serde_json)?;

		state.register_node(&raw_schema, None, None)?;

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
				match schema_node {
					SchemaNode::RegularType(schema_type) => match schema_type {
						RegularType::Array(Array {
							items: key,
							_private,
						})
						| RegularType::Map(Map {
							values: key,
							_private,
						}) => fix_key(key),
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
							_private: (),
						})
						| RegularType::Fixed(Fixed {
							size: _,
							name: _,
							_private: (),
						}) => {}
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
					raw::Type::Null => RegularType::Null,
					raw::Type::Boolean => RegularType::Boolean,
					raw::Type::Int => RegularType::Int,
					raw::Type::Long => RegularType::Long,
					raw::Type::Float => RegularType::Float,
					raw::Type::Double => RegularType::Double,
					raw::Type::Bytes => RegularType::Bytes,
					raw::Type::String => RegularType::String,
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

				self.nodes.push(SchemaNode::RegularType(RegularType::Null)); // Reserve the spot for us
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
							raw::SchemaNode::Type(t @ raw::Type::Array) => {
								RegularType::Array(Array {
									items: self.register_node(
										field!(t items),
										enclosing_namespace,
										None,
									)?,
									_private: (),
								})
							}
							raw::SchemaNode::Type(t @ raw::Type::Map) => RegularType::Map(Map {
								values: self.register_node(
									field!(t values),
									enclosing_namespace,
									None,
								)?,
								_private: (),
							}),
							raw::SchemaNode::Type(t @ raw::Type::Enum) => RegularType::Enum(Enum {
								name: name(t)?.0,
								symbols: field!(t symbols)
									.iter()
									.map(|e| (*e.0).to_owned())
									.collect(),
								_private: (),
							}),
							raw::SchemaNode::Type(t @ raw::Type::Fixed) => {
								RegularType::Fixed(Fixed {
									name: name(t)?.0,
									size: *field!(t size),
									_private: (),
								})
							}
							raw::SchemaNode::Type(t @ raw::Type::Record) => {
								let (name, name_key) = name(t)?;
								RegularType::Record(Record {
									fields: field!(t fields)
										.iter()
										.map(|field| {
											Ok(RecordField {
												name: (*field.name.0).to_owned(),
												type_: self.register_node(
													&field.type_,
													name_key.namespace,
													None,
												)?,
												_private: (),
											})
										})
										.collect::<Result<_, SchemaError>>()?,
									name,
									_private: (),
								})
							}
							ref inner_type @ (raw::SchemaNode::Type(
								raw::Type::Null
								| raw::Type::Boolean
								| raw::Type::Int
								| raw::Type::Long
								| raw::Type::Float
								| raw::Type::Double
								| raw::Type::Bytes
								| raw::Type::String,
							)
							| raw::SchemaNode::Ref(_)
							| raw::SchemaNode::Object(_)
							| raw::SchemaNode::Union(_)) => {
								// We have to allow {"type": {"type": "string"}}
								// (an object with an inner type and nothing
								// else is a valid representation)
								// However in that case we would ignore all keys
								// that are set at our current level, so we check for this
								// Let's just pass the namespace if overridden,
								// that seems reasonable...
								match object {
									&raw::SchemaNodeObject {
										type_: _,
										logical_type: _,
										name: _,
										namespace: _,
										fields: None,
										symbols: None,
										items: None,
										values: None,
										size: None,
										precision: None,
										scale: None,
									} => {
										self.nodes.pop().expect("We have just pushed");
										return self.register_node(
											inner_type,
											name_key
												.as_ref()
												.and_then(|n| n.namespace)
												.or(enclosing_namespace),
											will_have_logical_type,
										);
									}
									_ => {
										return Err(SchemaError::new(
											"Got unnecessarily-nested type, but \
												local object properties are set \
												- those would be ignored",
										))
									}
								}
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
											_private: (),
										}),
										"uuid" => LogicalType::Uuid,
										"date" => LogicalType::Date,
										"time-millis" => LogicalType::TimeMillis,
										"time-micros" => LogicalType::TimeMicros,
										"timestamp-millis" => LogicalType::TimestampMillis,
										"timestamp-micros" => LogicalType::TimestampMicros,
										"duration" => LogicalType::Duration,
										unknown => {
											LogicalType::Unknown(UnknownLogicalType::new(unknown))
										}
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
				self.nodes.push(SchemaNode::RegularType(RegularType::Null)); // Reserve the spot for us
				let new_node = SchemaNode::RegularType(RegularType::Union(Union {
					variants: union_schemas
						.iter()
						.map(|schema| self.register_node(schema, enclosing_namespace, None))
						.collect::<Result<_, _>>()?,
					_private: (),
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
						namespace: Some(namespace).filter(|&s| !s.is_empty()),
						name,
					}
				} else {
					NameKey {
						namespace: enclosing_namespace,
						name: reference,
					}
				};
				match self.names.get(&name_key) {
					Some(&idx) => SchemaKey { idx },
					None => {
						let idx = self.unresolved_names.len();
						self.unresolved_names.push(name_key);
						SchemaKey {
							idx: idx | LATE_NAME_LOOKUP_REMAP_BIT,
						}
					}
				}
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
