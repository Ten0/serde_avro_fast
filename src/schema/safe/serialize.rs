use super::*;

use {
	serde::ser::*,
	std::{borrow::Cow, cell::Cell},
};

impl SchemaMut {
	pub(crate) fn serialize_to_json(&self) -> Result<String, SchemaError> {
		serde_json::to_string(self).map_err(SchemaError::serde_json)
	}
}

impl Serialize for SchemaMut {
	fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
		// `written` serves both to avoid infinite recursion and to avoid writing the
		// same node twice in an unnamed manner.
		let written = vec![Cell::new(false); self.nodes.len()];
		SerializeSchema {
			schema_nodes: self.nodes(),
			key: SchemaKey::from_idx(0),
			namespace: None,
			written: written.as_slice(),
		}
		.serialize(serializer)
	}
}

struct SerializeSchema<'a, K> {
	schema_nodes: &'a [SchemaNode],
	written: &'a [Cell<bool>],
	key: K,
	namespace: Option<&'a str>,
}

impl<'a, K> SerializeSchema<'a, K> {
	fn serializable<NK>(&self, key: NK) -> SerializeSchema<'a, NK> {
		SerializeSchema {
			schema_nodes: self.schema_nodes,
			written: self.written,
			key,
			namespace: self.namespace,
		}
	}
	fn serializable_with_namespace<NK>(
		&self,
		key: NK,
		namespace: Option<&'a str>,
	) -> SerializeSchema<'a, NK> {
		SerializeSchema {
			schema_nodes: self.schema_nodes,
			written: self.written,
			key,
			namespace,
		}
	}
}
impl<'a> SerializeSchema<'a, SchemaKey> {
	/// Make sure we aren't cycling and every node is written at most once
	fn check_not_written<E: serde::ser::Error>(&self) -> Result<(), E> {
		if self.should_write_as_ref() {
			Err(E::custom(
				"Schema contains a cycle that can't be avoided using named references",
			))
		} else {
			Ok(())
		}
	}
	/// If this node was already written, return true (don't write it again).
	/// Either way, mark it as written now.
	fn should_write_as_ref(&self) -> bool {
		self.written[self.key.idx].replace(true)
	}
	fn str_for_ref(&self, name: &'a Name) -> Cow<'a, str> {
		if self.namespace == name.namespace() {
			Cow::Borrowed(name.name())
		} else if name.namespace().is_none() {
			// This syntax with the leading dot is unspecified, and it's probably impossible
			// to initially parse a such schema from a json (unless it's also specified this
			// way), but if building a schema programatically it seems possible, and that
			// would parse correctly in the Java implementation.
			Cow::Owned(format!(".{}", name.fully_qualified_name()))
		} else {
			Cow::Borrowed(name.fully_qualified_name())
		}
	}
	fn serialize_name<M: SerializeMap>(&self, map: &mut M, name: &'a Name) -> Result<(), M::Error> {
		if self.namespace == name.namespace() {
			map.serialize_entry("name", name.name())?;
		} else if name.namespace().is_none() {
			// To get the "null namespace" back, it's specified that one should specify
			// "namespace": "" in the json.
			map.serialize_entry("namespace", "")?;
			map.serialize_entry("name", name.name())?;
		} else {
			// We need to update the namespace, might as well put it in the name
			map.serialize_entry("name", name.fully_qualified_name())?;
		}
		Ok(())
	}
}

impl Serialize for SerializeSchema<'_, SchemaKey> {
	fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
		let node = self
			.schema_nodes
			.get(self.key.idx)
			.ok_or_else(|| S::Error::custom("SchemaKey refers to non-existing node"))?;
		match node {
			SchemaNode::LogicalType {
				inner,
				logical_type,
			} => {
				self.check_not_written()?;
				let mut map = serializer.serialize_map(Some(2))?;
				map.serialize_entry("logical_type", logical_type.as_str())?;
				map.serialize_entry("type", &self.serializable(*inner))?;
				map.end()
			}
			SchemaNode::RegularType(schema_type) => match *schema_type {
				RegularType::Null => {
					self.check_not_written()?;
					serializer.serialize_str("null")
				}
				RegularType::Boolean => {
					self.check_not_written()?;
					serializer.serialize_str("boolean")
				}
				RegularType::Int => {
					self.check_not_written()?;
					serializer.serialize_str("int")
				}
				RegularType::Long => {
					self.check_not_written()?;
					serializer.serialize_str("long")
				}
				RegularType::Float => {
					self.check_not_written()?;
					serializer.serialize_str("float")
				}
				RegularType::Double => {
					self.check_not_written()?;
					serializer.serialize_str("double")
				}
				RegularType::Bytes => {
					self.check_not_written()?;
					serializer.serialize_str("bytes")
				}
				RegularType::String => {
					self.check_not_written()?;
					serializer.serialize_str("string")
				}
				RegularType::Array(Array { items, _private }) => {
					self.check_not_written()?;
					let mut map = serializer.serialize_map(Some(2))?;
					map.serialize_entry("type", "array")?;
					map.serialize_entry("items", &self.serializable(items))?;
					map.end()
				}
				RegularType::Map(Map { values, _private }) => {
					self.check_not_written()?;
					let mut map = serializer.serialize_map(Some(2))?;
					map.serialize_entry("type", "map")?;
					map.serialize_entry("values", &self.serializable(values))?;
					map.end()
				}
				RegularType::Union(Union {
					ref variants,
					_private,
				}) => {
					self.check_not_written()?;
					let mut seq = serializer.serialize_seq(Some(variants.len()))?;
					for &union_variant_key in variants {
						seq.serialize_element(&self.serializable(union_variant_key))?;
					}
					seq.end()
				}
				RegularType::Record(Record {
					ref name,
					ref fields,
					_private,
				}) => {
					if self.should_write_as_ref() {
						serializer.serialize_str(&self.str_for_ref(name))
					} else {
						let mut map = serializer.serialize_map(None)?;
						map.serialize_entry("type", "record")?;
						self.serialize_name(&mut map, name)?;
						map.serialize_entry(
							"fields",
							&self.serializable_with_namespace(fields.as_slice(), name.namespace()),
						)?;
						map.end()
					}
				}
				RegularType::Enum(Enum {
					ref name,
					ref symbols,
					_private,
				}) => {
					if self.should_write_as_ref() {
						serializer.serialize_str(&self.str_for_ref(name))
					} else {
						let mut map = serializer.serialize_map(None)?;
						map.serialize_entry("type", "enum")?;
						self.serialize_name(&mut map, name)?;
						map.serialize_entry("symbols", symbols)?;
						map.end()
					}
				}
				RegularType::Fixed(Fixed {
					ref name,
					ref size,
					_private,
				}) => {
					if self.should_write_as_ref() {
						serializer.serialize_str(&self.str_for_ref(name))
					} else {
						let mut map = serializer.serialize_map(None)?;
						map.serialize_entry("type", "fixed")?;
						self.serialize_name(&mut map, name)?;
						map.serialize_entry("size", size)?;
						map.end()
					}
				}
			},
		}
	}
}

impl Serialize for SerializeSchema<'_, &[RecordField]> {
	fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
		let mut seq = serializer.serialize_seq(Some(self.key.len()))?;
		for field in self.key {
			seq.serialize_element(&self.serializable(field))?;
		}
		seq.end()
	}
}

impl Serialize for SerializeSchema<'_, &RecordField> {
	fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
		let mut map = serializer.serialize_map(Some(2))?;
		map.serialize_entry("name", &self.key.name)?;
		map.serialize_entry("type", &self.serializable(self.key.type_))?;
		map.end()
	}
}
