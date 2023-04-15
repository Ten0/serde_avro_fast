//! Fast lookup into unions
//!
//! We build a structure which can lookup in a pre-computed array for each type
//! of query the serializer may make.
//!
//! The contents of this module are very tied to the serializer's behavior

use std::{cmp::Ordering, collections::HashMap};

use super::*;

/// Indexes for union variants that can be obtained directly from the type
///
/// When the variant is such that there may be several of the same variant in
/// the union based on the name, it's a different enum
#[derive(Debug, Clone, Copy)]
pub(crate) enum UnionVariantLookupKey {
	Null,
	UnitStruct,
	Boolean,
	Integer,
	Integer4,
	Integer8,
	Float4,
	Float8,
	Str,
	SliceU8,
	UnitVariant,
	StructOrMap,
}
const N_VARIANTS: usize = 20;

pub(crate) struct PerTypeLookup<'a> {
	per_name: HashMap<String, &'a SchemaNode<'a>>,
	per_direct_union_variant: [Option<(i64, &'a SchemaNode<'a>)>; N_VARIANTS],
}
impl<'a> PerTypeLookup<'a> {
	pub(crate) fn unnamed(
		&self,
		variant: UnionVariantLookupKey,
	) -> Option<(i64, &'a SchemaNode<'a>)> {
		self.per_direct_union_variant[variant as usize]
	}
	pub(crate) fn named(&self, name: &str) -> Option<&'a SchemaNode<'a>> {
		self.per_name.get(name).copied()
	}

	pub(crate) fn new(variants: &[&'a SchemaNode<'a>]) -> PerTypeLookup<'a> {
		#[derive(Clone, Copy)]
		enum NoneSomeOrConflict<'a> {
			None,
			Some {
				priority: usize,
				discriminant_and_schema_node: (i64, &'a SchemaNode<'a>),
			},
			Conflict {
				priority: usize,
			},
		}
		let mut per_direct_union_variant = [NoneSomeOrConflict::None; N_VARIANTS];
		let mut per_name = HashMap::new();
		for (discriminant, &schema_node) in variants.iter().enumerate() {
			let discriminant: i64 = discriminant
				.try_into()
				.expect("Variants array should not possibly be larger than i64::MAX");
			let mut register = |variant: UnionVariantLookupKey, priority: usize| {
				let val = &mut per_direct_union_variant[variant as usize];
				match *val {
					NoneSomeOrConflict::None => {
						*val = NoneSomeOrConflict::Some {
							discriminant_and_schema_node: (discriminant, schema_node),
							priority,
						}
					}
					NoneSomeOrConflict::Some {
						priority: old_priority,
						..
					} => {
						// Favor lowest priority
						match old_priority.cmp(&priority) {
							Ordering::Less => {}
							Ordering::Equal => {
								*val = NoneSomeOrConflict::Conflict {
									priority: old_priority,
								};
							}
							Ordering::Greater => {
								*val = NoneSomeOrConflict::Some {
									priority,
									discriminant_and_schema_node: (discriminant, schema_node),
								};
							}
						}
					}
					NoneSomeOrConflict::Conflict {
						priority: old_priority,
					} => {
						if priority > old_priority {
							*val = NoneSomeOrConflict::Some {
								priority,
								discriminant_and_schema_node: (discriminant, schema_node),
							};
						}
					}
				}
			};
			let mut register_name = |name: &Name| {
				per_name.insert(name.name().to_owned(), schema_node);
				per_name.insert(name.fully_qualified_name().to_owned(), schema_node);
			};
			match schema_node {
				SchemaNode::Null => {
					register(UnionVariantLookupKey::Null, 0);
					register(UnionVariantLookupKey::UnitStruct, 0);
					register(UnionVariantLookupKey::UnitVariant, 2);
				}
				SchemaNode::Boolean => register(UnionVariantLookupKey::Boolean, 0),
				SchemaNode::Int => {
					register(UnionVariantLookupKey::Integer, 0);
					register(UnionVariantLookupKey::Integer4, 0);
					register(UnionVariantLookupKey::Integer8, 1);
				}
				SchemaNode::Long => {
					register(UnionVariantLookupKey::Integer, 0);
					register(UnionVariantLookupKey::Integer4, 1);
					register(UnionVariantLookupKey::Integer8, 0);
				}
				SchemaNode::Float => {
					register(UnionVariantLookupKey::Float4, 0);
					register(UnionVariantLookupKey::Float8, 1);
				}
				SchemaNode::Double => {
					register(UnionVariantLookupKey::Float8, 0);
					register(UnionVariantLookupKey::Float4, 1); // Just for better error
				}
				SchemaNode::Bytes => {
					register(UnionVariantLookupKey::Str, 10);
					register(UnionVariantLookupKey::SliceU8, 0);
				}
				SchemaNode::String => {
					register(UnionVariantLookupKey::Str, 0);
					register(UnionVariantLookupKey::UnitStruct, 0);
					register(UnionVariantLookupKey::SliceU8, 1);
					register(UnionVariantLookupKey::UnitVariant, 1);
				}
				SchemaNode::Array(_) => {}
				SchemaNode::Map(_) => {
					register(UnionVariantLookupKey::StructOrMap, 0);
				}
				SchemaNode::Union(_) => {
					// Union in union is supposedly not allowed so you'd better
					// not rely on looking up through nested unions
				}
				SchemaNode::Enum(Enum { name, .. }) => {
					register_name(name);
					register(UnionVariantLookupKey::Integer, 10);
					register(UnionVariantLookupKey::Integer4, 10);
					register(UnionVariantLookupKey::Integer8, 10);
					register(UnionVariantLookupKey::UnitStruct, 0);
					register(UnionVariantLookupKey::Str, 5);
					register(UnionVariantLookupKey::UnitVariant, 0);
				}
				SchemaNode::Record(Record { name, .. }) => {
					register_name(name);
					register(UnionVariantLookupKey::StructOrMap, 0);
				}
				SchemaNode::Fixed(Fixed { name, .. }) => {
					register_name(name);
					register(UnionVariantLookupKey::Str, 15);
					register(UnionVariantLookupKey::SliceU8, 0);
				}
				SchemaNode::Decimal(Decimal { repr, .. }) => {
					match repr {
						DecimalRepr::Fixed(Fixed { name, .. }) => {
							register_name(name);
						}
						DecimalRepr::Bytes => {}
					}
					register(UnionVariantLookupKey::Integer, 5);
					register(UnionVariantLookupKey::Integer4, 5);
					register(UnionVariantLookupKey::Integer8, 5);
				}
				SchemaNode::Uuid => {
					// A user may assume that uuid::Uuid will serialize to Uuid by default,
					// but since it serializes as &str by default, we in fact can't distinguish
					// between that and &str, so we'll error in case union has both Uuid and String
					// to avoid unexpected behavior on the user side.
					// They may specify using enums.
					register(UnionVariantLookupKey::Str, 0);
				}
				SchemaNode::Date => {
					register(UnionVariantLookupKey::Integer, 0);
					register(UnionVariantLookupKey::Integer4, 0);
					register(UnionVariantLookupKey::Integer8, 1);
				}
				SchemaNode::TimeMillis => {
					register(UnionVariantLookupKey::Integer, 0);
					register(UnionVariantLookupKey::Integer4, 0);
					register(UnionVariantLookupKey::Integer8, 1);
				}
				SchemaNode::TimeMicros => {
					register(UnionVariantLookupKey::Integer, 0);
					register(UnionVariantLookupKey::Integer4, 1);
					register(UnionVariantLookupKey::Integer8, 0);
				}
				SchemaNode::TimestampMillis => {
					register(UnionVariantLookupKey::Integer, 0);
					register(UnionVariantLookupKey::Integer4, 1);
					register(UnionVariantLookupKey::Integer8, 0);
				}
				SchemaNode::TimestampMicros => {
					register(UnionVariantLookupKey::Integer, 0);
					register(UnionVariantLookupKey::Integer4, 1);
					register(UnionVariantLookupKey::Integer8, 0);
				}
				SchemaNode::Duration => {}
			}
		}
		let per_direct_union_variant = per_direct_union_variant.map(|v| match v {
			NoneSomeOrConflict::None => None,
			NoneSomeOrConflict::Some {
				discriminant_and_schema_node,
				..
			} => Some(discriminant_and_schema_node),
			NoneSomeOrConflict::Conflict { .. } => None,
		});
		PerTypeLookup {
			per_name,
			per_direct_union_variant,
		}
	}
}
