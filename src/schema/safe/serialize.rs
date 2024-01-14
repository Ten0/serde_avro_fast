use super::*;

use serde::ser::*;

impl SchemaMut {
	pub(crate) fn serialize_to_json(&self) -> Result<String, SchemaError> {
		serde_json::to_string(self).map_err(SchemaError::serde_json)
	}
}

impl Serialize for SchemaMut {
	fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
		SerializeSchema {
			schema: self,
			key: SchemaKey::from_idx(0),
		}
		.serialize(serializer)
	}
}

struct SerializeSchema<'a, K> {
	schema: &'a SchemaMut,
	key: K,
}

impl<'a, K> SerializeSchema<'a, K> {
	fn serializable<NK>(&self, key: NK) -> SerializeSchema<'a, NK> {
		SerializeSchema {
			schema: self.schema,
			key,
		}
	}
}

impl Serialize for SerializeSchema<'_, SchemaKey> {
	fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
		let node = self
			.schema
			.nodes
			.get(self.key.idx)
			.ok_or_else(|| S::Error::custom("SchemaKey refers to non-existing node"))?;
		match node {
			SchemaNode::LogicalType {
				inner,
				logical_type,
			} => {
				let mut map = serializer.serialize_map(Some(2))?;
				map.serialize_entry("logical_type", logical_type.as_str())?;
				map.serialize_entry("type", &self.serializable(*inner))?;
				map.end()
			}
			SchemaNode::RegularType(schema_type) => match *schema_type {
				SchemaType::Null => serializer.serialize_str("null"),
				SchemaType::Boolean => serializer.serialize_str("boolean"),
				SchemaType::Int => serializer.serialize_str("int"),
				SchemaType::Long => serializer.serialize_str("long"),
				SchemaType::Float => serializer.serialize_str("float"),
				SchemaType::Double => serializer.serialize_str("double"),
				SchemaType::Bytes => serializer.serialize_str("bytes"),
				SchemaType::String => serializer.serialize_str("string"),
				SchemaType::Array(array_elements) => {
					let mut map = serializer.serialize_map(Some(2))?;
					map.serialize_entry("type", "array")?;
					map.serialize_entry("items", &self.serializable(array_elements))?;
					map.end()
				}
				SchemaType::Map(map_elements) => {
					let mut map = serializer.serialize_map(Some(2))?;
					map.serialize_entry("type", "map")?;
					map.serialize_entry("values", &self.serializable(map_elements))?;
					map.end()
				}
				SchemaType::Union(Union {
					ref variants,
					_private,
				}) => {
					let mut seq = serializer.serialize_seq(Some(variants.len()))?;
					for &union_variant_key in variants {
						seq.serialize_element(&self.serializable(union_variant_key))?;
					}
					seq.end()
				}
				SchemaType::Record(Record {
					ref name,
					ref fields,
					_private,
				}) => {
					let mut map = serializer.serialize_map(Some(3))?;
					map.serialize_entry("type", "record")?;
					map.serialize_entry("name", name.fully_qualified_name())?;
					map.serialize_entry("fields", &self.serializable(fields.as_slice()))?;
					map.end()
				}
				SchemaType::Enum(Enum {
					ref name,
					ref symbols,
					_private,
				}) => {
					let mut map = serializer.serialize_map(Some(3))?;
					map.serialize_entry("type", "enum")?;
					map.serialize_entry("name", name.fully_qualified_name())?;
					map.serialize_entry("symbols", symbols)?;
					map.end()
				}
				SchemaType::Fixed(Fixed {
					ref name,
					ref size,
					_private,
				}) => {
					let mut map = serializer.serialize_map(Some(3))?;
					map.serialize_entry("type", "fixed")?;
					map.serialize_entry("name", name.fully_qualified_name())?;
					map.serialize_entry("size", size)?;
					map.end()
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
