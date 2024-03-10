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
		// This serves both to avoid infinite recursion and to avoid writing the
		// same node twice in an unnamed manner.
		let node_traversal_state = vec![Cell::new(0); self.nodes.len()];

		let n_written_names = Cell::new(1);
		SerializeSchema {
			schema_nodes: self.nodes(),
			key: SchemaKey::from_idx(0),
			parent_namespace: None,
			n_written_names: &n_written_names,
			node_traversal_state: node_traversal_state.as_slice(),
		}
		.serialize(serializer)
	}
}

struct SerializeSchema<'a, K> {
	schema_nodes: &'a [SchemaNode],
	key: K,
	n_written_names: &'a Cell<u64>,
	node_traversal_state: &'a [Cell<u64>],
	parent_namespace: Option<&'a str>,
}

impl<'a, K> SerializeSchema<'a, K> {
	fn serializable<NK>(&self, key: NK) -> SerializeSchema<'a, NK> {
		SerializeSchema {
			key,
			schema_nodes: self.schema_nodes,
			n_written_names: self.n_written_names,
			node_traversal_state: self.node_traversal_state,
			parent_namespace: self.parent_namespace,
		}
	}
	/// Current field overrides the namespace, so we need to propagate that to
	/// children
	fn serializable_with_namespace<NK>(
		&self,
		key: NK,
		namespace: Option<&'a str>,
	) -> SerializeSchema<'a, NK> {
		SerializeSchema {
			key,
			schema_nodes: self.schema_nodes,
			n_written_names: self.n_written_names,
			node_traversal_state: self.node_traversal_state,
			parent_namespace: namespace,
		}
	}
}
impl<'a> SerializeSchema<'a, SchemaKey> {
	/// Make sure we aren't cycling
	fn no_cycle_guard<E: serde::ser::Error>(&self) -> Result<NoCycleGuard, E> {
		let cell = &self.node_traversal_state[self.key.idx];
		let n_written_names = self.n_written_names.get();
		let prev_n_written_names = cell.replace(n_written_names);
		// If we encounter the same node without having written a new name, this means
		// that we are in a cycle. If however we have written a new name, then next loop
		// we will likely just reference that name (if it is indeed in that particular
		// loop), so the schema serialization will not loop indefinitely. -> Let's allow
		// another round. (If it was in another loop, since we'll redo that check on our
		// next loop we'll also catch it.)
		// Complexity is at most O(n²) since we'll at most go through every node for
		// every named node. In practice such degenerate schemas are very unlikely to
		// exist so it will run in a reasonable amount of time.

		// Should never be greater, only at most equal, but this is a safer writing
		if prev_n_written_names >= n_written_names {
			Err(E::custom(
				"Schema contains a cycle that can't be avoided using named references",
			))
		} else {
			// We will use this to reset
			Ok(NoCycleGuard {
				node_traversal_state: cell,
			})
		}
	}
	/// If this node was already written, return true (don't write it again).
	/// Otherwise, return false, assume we'll write it entirely and increment
	/// the counter of written names.
	fn should_write_as_ref(&self) -> bool {
		let key_generation = &self.node_traversal_state[self.key.idx];
		if key_generation.get() > 0 {
			// We have already written that one, just write it as a ref
			true
		} else {
			let generation = self.n_written_names.get();
			key_generation.set(generation);
			// Mark that we are going to write one additional named node by increasing the
			// generation, so that resets the cycle checker for all parent nodes
			// We can't count to u64::MAX so this can't overflow
			self.n_written_names.set(generation + 1);
			false
		}
	}
	fn str_for_ref(&self, name: &'a Name) -> Cow<'a, str> {
		if self.parent_namespace == name.namespace() {
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
		if self.parent_namespace == name.namespace() {
			map.serialize_entry("name", name.name())?;
		} else if name.namespace().is_none() {
			// To get the "null namespace" back, it's specified that one should write
			// "namespace": "" in the json.
			map.serialize_entry("namespace", "")?;
			map.serialize_entry("name", name.name())?;
		} else {
			// We need to update the namespace (and we know it's not None), might as well
			// put it in the name
			map.serialize_entry("name", name.fully_qualified_name())?;
		}
		Ok(())
	}
}

#[must_use]
struct NoCycleGuard<'a> {
	node_traversal_state: &'a Cell<u64>,
}
impl NoCycleGuard<'_> {
	fn release(self) {
		self.node_traversal_state.set(0);
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
				let no_cycle_guard = self.no_cycle_guard()?;
				let mut map = serializer.serialize_map(None)?;
				map.serialize_entry("logicalType", logical_type.as_str())?;
				map.serialize_entry("type", &self.serializable(*inner))?;
				match logical_type {
					LogicalType::Decimal(decimal) => {
						map.serialize_entry("scale", &decimal.scale)?;
						map.serialize_entry("precision", &decimal.precision)?;
					}
					LogicalType::Uuid
					| LogicalType::Date
					| LogicalType::TimeMillis
					| LogicalType::TimeMicros
					| LogicalType::TimestampMillis
					| LogicalType::TimestampMicros
					| LogicalType::Duration => {}
					LogicalType::Unknown(_) => {}
				}
				let res = map.end();
				no_cycle_guard.release();
				res
			}
			SchemaNode::RegularType(schema_type) => match *schema_type {
				RegularType::Null => serializer.serialize_str("null"),
				RegularType::Boolean => serializer.serialize_str("boolean"),
				RegularType::Int => serializer.serialize_str("int"),
				RegularType::Long => serializer.serialize_str("long"),
				RegularType::Float => serializer.serialize_str("float"),
				RegularType::Double => serializer.serialize_str("double"),
				RegularType::Bytes => serializer.serialize_str("bytes"),
				RegularType::String => serializer.serialize_str("string"),
				RegularType::Array(Array { items, _private }) => {
					let no_cycle_guard = self.no_cycle_guard()?;
					let mut map = serializer.serialize_map(Some(2))?;
					map.serialize_entry("type", "array")?;
					map.serialize_entry("items", &self.serializable(items))?;
					let res = map.end();
					no_cycle_guard.release();
					res
				}
				RegularType::Map(Map { values, _private }) => {
					let no_cycle_guard = self.no_cycle_guard()?;
					let mut map = serializer.serialize_map(Some(2))?;
					map.serialize_entry("type", "map")?;
					map.serialize_entry("values", &self.serializable(values))?;
					let res = map.end();
					no_cycle_guard.release();
					res
				}
				RegularType::Union(Union {
					ref variants,
					_private,
				}) => {
					let no_cycle_guard = self.no_cycle_guard()?;
					let mut seq = serializer.serialize_seq(Some(variants.len()))?;
					for &union_variant_key in variants {
						seq.serialize_element(&self.serializable(union_variant_key))?;
					}
					let res = seq.end();
					no_cycle_guard.release();
					res
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
