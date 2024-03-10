use super::{RegularType, SchemaMut, SchemaNode};

impl SchemaMut {
	/// Check that the schema does not contain zero-sized unconditional cycles.
	///
	/// This is called by the parsing functions already, so this may only be
	/// useful if you've manally edited the [`SchemaMut`] graph.
	///
	/// Note that deserialization functions otherwise already prevent stack
	/// overflows by limiting the recursion depth.
	pub(crate) fn check_for_cycles(&self) -> Result<(), UnconditionalCycle> {
		// Zero-size cycles (that would trigger infinite recursion when parsing, without
		// consuming any input) can only happen with records that end up containing
		// themselves ~immediately (that is, only through record paths).
		// Any other path would consume at least one byte (e.g union discriminant...)

		// Since we shouldn't forbid conditional self-referential records (e.g. `Self {
		// next: union { null, Self } }`), we can't really prevent non zero-sized
		// stack overflows anyway (besides limiting depth in the deserializer), so best
		// we can reliably do at this step is only to prevent zero-sized cycles.
		let mut visited_nodes = vec![false; self.nodes.len()];
		let mut checked_nodes = vec![false; self.nodes.len()];
		for (idx, node) in self.nodes.iter().enumerate() {
			if matches!(node, SchemaNode::RegularType(RegularType::Record(_)))
				&& !checked_nodes[idx]
			{
				check_no_zero_sized_cycle_inner(self, idx, &mut visited_nodes, &mut checked_nodes)?;
			}
		}
		Ok(())
	}
}

#[derive(Debug, thiserror::Error)]
#[error("The schema contains a record that ends up always containing itself")]
/// Error: Detected unconditional cycle in provided schema
///
/// It was detected that the schema contains a record that ends up always
/// containing itself
pub struct UnconditionalCycle {
	_private: (),
}
fn check_no_zero_sized_cycle_inner(
	schema: &SchemaMut,
	node_idx: usize,
	visited_nodes: &mut Vec<bool>,
	checked_nodes: &mut Vec<bool>,
) -> Result<(), UnconditionalCycle> {
	visited_nodes[node_idx] = true;
	for field in match &schema.nodes[node_idx] {
		SchemaNode::RegularType(RegularType::Record(record)) => &record.fields,
		_ => unreachable!(),
	} {
		if let SchemaNode::RegularType(RegularType::Record(_)) = &schema.nodes[field.type_.idx] {
			if visited_nodes[field.type_.idx] {
				return Err(UnconditionalCycle { _private: () });
			} else {
				check_no_zero_sized_cycle_inner(
					schema,
					field.type_.idx,
					visited_nodes,
					checked_nodes,
				)?;
			}
		}
	}
	visited_nodes[node_idx] = false;
	// If we have visited a node and it was ok as part of another record, no need to
	// re-visit it individually.
	checked_nodes[node_idx] = true;
	Ok(())
}
