use super::{Array, Map, Record, RegularType, SchemaError, SchemaKey, SchemaMut, Union};

impl SchemaMut {
	pub(crate) fn remove_unreferenced_nodes(&mut self) -> Result<(), SchemaError> {
		if self.nodes.is_empty() {
			return Ok(());
		}
		let mut reachable_nodes = vec![false; self.nodes.len()];
		mark_reachable(self, SchemaKey::root(), &mut reachable_nodes)?;
		let key_remap = build_remap(&reachable_nodes);
		remap_nodes(self, &reachable_nodes, &key_remap);
		remove_unreachable_nodes(self, &reachable_nodes);
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

fn remap_nodes(schema: &mut SchemaMut, reachable_nodes: &[bool], key_remap: &[Option<SchemaKey>]) {
	let get_remapped_key = |key: SchemaKey| {
		key_remap
			.get(key.idx())
			.expect("SchemaKey referring to a non-existing node should have been caught by mark_reachable")
			.expect("An unreachable node should not be able to be referred to by a reachable node")
	};
	for (idx, node) in schema.nodes.iter_mut().enumerate() {
		if !reachable_nodes[idx] {
			continue;
		}
		match &mut node.type_ {
			RegularType::Array(Array { items, .. }) => {
				*items = get_remapped_key(*items);
			}
			RegularType::Map(Map { values, .. }) => {
				*values = get_remapped_key(*values);
			}
			RegularType::Union(Union { variants, .. }) => {
				for variant in variants {
					*variant = get_remapped_key(*variant);
				}
			}
			RegularType::Record(Record { fields, .. }) => {
				for field in fields {
					field.type_ = get_remapped_key(field.type_);
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
}

fn mark_reachable(
	schema: &SchemaMut,
	key: SchemaKey,
	reachable_nodes: &mut [bool],
) -> Result<(), SchemaError> {
	let out_of_bounds_error = || {
		SchemaError::msg(format_args!(
			"SchemaKey index {} is out of bounds (len: {})",
			key.idx(),
			schema.nodes.len()
		))
	};

	let reachable = reachable_nodes
		.get_mut(key.idx())
		.ok_or_else(out_of_bounds_error)?;

	if *reachable {
		return Ok(());
	}
	*reachable = true;
	let node = schema
		.nodes
		.get(key.idx())
		.ok_or_else(out_of_bounds_error)?;
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

// Mainly to verify correctness of `remove_unreferenced_nodes` implementation,
// which is an internal method that we don't want to expose publicly, which is
// why it lives here instead of in the tests module.
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

		assert_eq!(schema.nodes().len(), 3);
		schema.remove_unreferenced_nodes().unwrap();
		assert_eq!(schema.nodes().len(), 2);
		assert_eq!(
			serde_json::to_string(&schema).unwrap(),
			r#"{"type":"record","name":"Root","fields":[{"name":"f","type":"int"}]}"#
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
			"SchemaKey index 999 is out of bounds (len: 1)"
		);
	}
}
