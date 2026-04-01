use super::{Array, Map, Record, RegularType, SchemaError, SchemaKey, SchemaMut, Union};

impl SchemaMut {
	pub(crate) fn remove_unreferenced_nodes(&mut self) -> Result<(), SchemaError> {
		if self.nodes.is_empty() {
			return Ok(());
		}
		let mut reachable_nodes = vec![false; self.nodes.len()];
		mark_reachable(self, SchemaKey::root(), &mut reachable_nodes)?;
		let key_remap = build_remap(&reachable_nodes);
		remap_nodes(self, &reachable_nodes, &key_remap)?;
		remove_unreachable_nodes(self, &reachable_nodes);
		self.schema_json = None;
		Ok(())
	}
}

fn remove_unreachable_nodes(schema: &mut SchemaMut, reachable_nodes: &[bool]) {
	let mut i = 0;
	schema.nodes.retain(|_| {
		let keep = reachable_nodes[i];
		i += 1;
		keep
	});
}

fn build_remap(reachable_nodes: &[bool]) -> Vec<Option<SchemaKey>> {
	let mut new_idx = 0;
	reachable_nodes
		.iter()
		.map(|&reachable| {
			if reachable {
				let mapped_key = SchemaKey::from_idx(new_idx);
				new_idx += 1;
				Some(mapped_key)
			} else {
				None
			}
		})
		.collect()
}

fn remap_nodes(
	schema: &mut SchemaMut,
	reachable_nodes: &[bool],
	key_remap: &[Option<SchemaKey>],
) -> Result<(), SchemaError> {
	for (idx, node) in schema.nodes.iter_mut().enumerate() {
		if !reachable_nodes[idx] {
			continue;
		}
		match &mut node.type_ {
			RegularType::Array(Array { items, .. }) => {
				*items = *key_remap
					.get(items.idx())
					.ok_or_else(|| SchemaError::new("SchemaKey refers to non-existing node"))?
					.as_ref()
					.ok_or_else(|| {
						SchemaError::new("Remapped SchemaKey refers to an unreachable node")
					})?;
			}
			RegularType::Map(Map { values, .. }) => {
				*values = *key_remap
					.get(values.idx())
					.ok_or_else(|| SchemaError::new("SchemaKey refers to non-existing node"))?
					.as_ref()
					.ok_or_else(|| {
						SchemaError::new("Remapped SchemaKey refers to an unreachable node")
					})?;
			}
			RegularType::Union(Union { variants, .. }) => {
				for variant in variants {
					*variant = *key_remap
						.get(variant.idx())
						.ok_or_else(|| SchemaError::new("SchemaKey refers to non-existing node"))?
						.as_ref()
						.ok_or_else(|| {
							SchemaError::new("Remapped SchemaKey refers to an unreachable node")
						})?;
				}
			}
			RegularType::Record(Record { fields, .. }) => {
				for field in fields {
					field.type_ = *key_remap
						.get(field.type_.idx())
						.ok_or_else(|| SchemaError::new("SchemaKey refers to non-existing node"))?
						.as_ref()
						.ok_or_else(|| {
							SchemaError::new("Remapped SchemaKey refers to an unreachable node")
						})?;
				}
			}
			RegularType::Null
			| RegularType::Boolean
			| RegularType::Int
			| RegularType::Long
			| RegularType::Float
			| RegularType::Double
			| RegularType::Bytes
			| RegularType::String
			| RegularType::Enum(_)
			| RegularType::Fixed(_) => {}
		}
	}
	Ok(())
}

fn mark_reachable(
	schema: &SchemaMut,
	key: SchemaKey,
	reachable_nodes: &mut [bool],
) -> Result<(), SchemaError> {
	let reachable = reachable_nodes
		.get_mut(key.idx())
		.ok_or_else(|| SchemaError::new("SchemaKey refers to non-existing node"))?;

	if *reachable {
		return Ok(());
	}
	*reachable = true;
	let node = schema
		.nodes
		.get(key.idx())
		.ok_or_else(|| SchemaError::new("SchemaKey refers to non-existing node"))?;
	match &node.type_ {
		RegularType::Array(Array { items, .. }) => {
			mark_reachable(schema, *items, reachable_nodes)?;
		}
		RegularType::Map(Map { values, .. }) => {
			mark_reachable(schema, *values, reachable_nodes)?;
		}
		RegularType::Union(Union { variants, .. }) => {
			for variant in variants {
				mark_reachable(schema, *variant, reachable_nodes)?;
			}
		}
		RegularType::Record(Record { fields, .. }) => {
			for field in fields {
				mark_reachable(schema, field.type_, reachable_nodes)?;
			}
		}
		RegularType::Null
		| RegularType::Boolean
		| RegularType::Int
		| RegularType::Long
		| RegularType::Float
		| RegularType::Double
		| RegularType::Bytes
		| RegularType::String
		| RegularType::Enum(_)
		| RegularType::Fixed(_) => {}
	}
	Ok(())
}

#[cfg(test)]
mod tests {
	use {
		super::*,
		crate::schema::{Name, RecordField, SchemaNode},
	};

	#[test]
	fn remove_unreferenced_nodes_prunes_unreachable_nodes() {
		let mut schema = SchemaMut::from_nodes(vec![
			SchemaNode::new(RegularType::Record(Record::new(
				Name::from_fully_qualified_name("Root"),
				vec![RecordField::new("f", SchemaKey::from_idx(1))],
			))),
			SchemaNode::new(RegularType::Int),
			SchemaNode::new(RegularType::Record(Record::new(
				Name::from_fully_qualified_name("Orphan"),
				vec![RecordField::new("x", SchemaKey::from_idx(1))],
			))),
		]);

		schema.remove_unreferenced_nodes().unwrap();

		assert_eq!(
			schema,
			SchemaMut::from_nodes(vec![
				SchemaNode::new(RegularType::Record(Record::new(
					Name::from_fully_qualified_name("Root"),
					vec![RecordField::new("f", SchemaKey::from_idx(1))],
				))),
				SchemaNode::new(RegularType::Int),
			])
		);
	}

	#[test]
	fn remove_unreferenced_nodes_returns_error_on_invalid_schema_key() {
		let mut schema =
			SchemaMut::from_nodes(vec![SchemaNode::new(RegularType::Record(Record::new(
				Name::from_fully_qualified_name("Bad"),
				vec![RecordField::new("f", SchemaKey::from_idx(999))],
			)))]);

		assert_eq!(
			schema.remove_unreferenced_nodes().unwrap_err().to_string(),
			"SchemaKey refers to non-existing node"
		);
	}
}
