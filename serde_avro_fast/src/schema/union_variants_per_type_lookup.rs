//! Fast lookup into unions
//!
//! We build a structure which can lookup in a pre-computed array for each type
//! of query the serializer may make.
//!
//! The contents of this module are very tied to the serializer's behavior

use std::{borrow::Cow, cmp::Ordering, collections::HashMap};

use super::self_referential::*;

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
	SeqOrTupleOrTupleStruct,
}
const N_VARIANTS: usize = 20;

pub(crate) struct PerTypeLookup<'a> {
	per_name: HashMap<Cow<'static, str>, (i64, NodeRef<'a>)>,
	per_direct_union_variant: [Option<(i64, NodeRef<'a>)>; N_VARIANTS],
}
impl<'a> PerTypeLookup<'a> {
	pub(crate) fn placeholder() -> Self {
		Self {
			per_name: Default::default(),
			per_direct_union_variant: Default::default(),
		}
	}

	pub(crate) fn unnamed(
		&self,
		variant: UnionVariantLookupKey,
	) -> Option<(i64, &'a SchemaNode<'a>)> {
		self.per_direct_union_variant[variant as usize].map(|(i, n)| (i, n.as_ref()))
	}
	pub(crate) fn named(&self, name: &str) -> Option<(i64, &'a SchemaNode<'a>)> {
		self.per_name
			.get(name)
			.copied()
			.map(|(i, n)| (i, n.as_ref()))
	}

	/// Constructs the lookup table
	///
	/// Note that the safety/correctness of the self-referential construction
	/// relies on that this function:
	/// - Does not read `per_type_lookup` of the other nodes (doesn't need to do
	///   so anyway)
	pub(crate) fn new(variants: &[NodeRef<'a>]) -> PerTypeLookup<'a> {
		#[derive(Clone, Copy)]
		enum NoneSomeOrConflict<'a> {
			None,
			Some {
				priority: usize,
				discriminant_and_schema_node: (i64, NodeRef<'a>),
			},
			Conflict {
				priority: usize,
			},
		}
		let mut per_direct_union_variant = [NoneSomeOrConflict::None; N_VARIANTS];
		let per_name = std::cell::RefCell::new(HashMap::new());
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
						if priority < old_priority {
							*val = NoneSomeOrConflict::Some {
								priority,
								discriminant_and_schema_node: (discriminant, schema_node),
							};
						}
					}
				}
			};
			let register_name = |name: &Name| {
				let mut per_name = per_name.borrow_mut();
				per_name.insert(
					Cow::Owned(name.name().to_owned()),
					(discriminant, schema_node),
				);
				per_name.insert(
					Cow::Owned(name.fully_qualified_name().to_owned()),
					(discriminant, schema_node),
				);
			};
			let register_type_name = |type_name: &'static str| {
				per_name
					.borrow_mut()
					.insert(Cow::Borrowed(type_name), (discriminant, schema_node));
			};
			// Note that the following list is very coupled with the serializer:
			// every `UnionVariantLookupKey` corresponds to one (or more) function
			// of `Serializer`, and every `register` call corresponds to a capability
			// of that function to serialize that type.
			match schema_node.as_ref() {
				SchemaNode::Null => {
					register_type_name("Null");
					register(UnionVariantLookupKey::Null, 0);
					register(UnionVariantLookupKey::UnitStruct, 0);
					register(UnionVariantLookupKey::UnitVariant, 2);
				}
				SchemaNode::Boolean => {
					register_type_name("Boolean");
					register(UnionVariantLookupKey::Boolean, 0)
				}
				SchemaNode::Int => {
					register_type_name("Int");
					register(UnionVariantLookupKey::Integer, 0);
					register(UnionVariantLookupKey::Integer4, 0);
					register(UnionVariantLookupKey::Integer8, 1);
				}
				SchemaNode::Long => {
					register_type_name("Long");
					register(UnionVariantLookupKey::Integer, 0);
					register(UnionVariantLookupKey::Integer4, 1);
					register(UnionVariantLookupKey::Integer8, 0);
				}
				SchemaNode::Float => {
					register_type_name("Float");
					register(UnionVariantLookupKey::Float4, 0);
					register(UnionVariantLookupKey::Float8, 1);
				}
				SchemaNode::Double => {
					register_type_name("Double");
					register(UnionVariantLookupKey::Float8, 0);
					register(UnionVariantLookupKey::Float4, 1); // Just for better error
				}
				SchemaNode::Bytes => {
					register_type_name("Bytes");
					register(UnionVariantLookupKey::Str, 10);
					register(UnionVariantLookupKey::UnitStruct, 10);
					register(UnionVariantLookupKey::SliceU8, 0);
					register(UnionVariantLookupKey::SeqOrTupleOrTupleStruct, 2);
					register(UnionVariantLookupKey::UnitVariant, 10);
				}
				SchemaNode::String => {
					register_type_name("String");
					register(UnionVariantLookupKey::Str, 0);
					register(UnionVariantLookupKey::UnitStruct, 0);
					register(UnionVariantLookupKey::SliceU8, 1);
					register(UnionVariantLookupKey::UnitVariant, 1);
				}
				SchemaNode::Array(_) => {
					register_type_name("Array");
					register(UnionVariantLookupKey::SeqOrTupleOrTupleStruct, 0);
				}
				SchemaNode::Map(_) => {
					register_type_name("Map");
					register(UnionVariantLookupKey::StructOrMap, 0);
				}
				SchemaNode::Union(_) => {
					// Union in union is supposedly not allowed so you'd better
					// not rely on looking up through nested unions
					register_type_name("Union");
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
					register(UnionVariantLookupKey::SeqOrTupleOrTupleStruct, 2);
				}
				SchemaNode::Decimal(Decimal { repr, .. }) => {
					register_type_name("Decimal");
					match repr {
						DecimalRepr::Fixed(fixed) => {
							register_name(&fixed.as_ref().name);
						}
						DecimalRepr::Bytes => {}
					}
					register(UnionVariantLookupKey::Integer, 5);
					register(UnionVariantLookupKey::Integer4, 5);
					register(UnionVariantLookupKey::Integer8, 5);
					register(UnionVariantLookupKey::Float8, 2);
					register(UnionVariantLookupKey::Str, 20);
				}
				SchemaNode::Uuid => {
					register_type_name("Uuid");
					// A user may assume that uuid::Uuid will serialize to Uuid by default,
					// but since it serializes as &str by default, we in fact can't distinguish
					// between that and &str, so we'll error in case union has both Uuid and String
					// to avoid unexpected behavior on the user side.
					// They may specify using enums.
					register(UnionVariantLookupKey::Str, 0);
				}
				SchemaNode::Date => {
					register_type_name("Date");
					register(UnionVariantLookupKey::Integer, 0);
					register(UnionVariantLookupKey::Integer4, 0);
					register(UnionVariantLookupKey::Integer8, 1);
				}
				SchemaNode::TimeMillis => {
					register_type_name("TimeMillis");
					register(UnionVariantLookupKey::Integer, 0);
					register(UnionVariantLookupKey::Integer4, 0);
					register(UnionVariantLookupKey::Integer8, 1);
				}
				SchemaNode::TimeMicros => {
					register_type_name("TimeMicros");
					register(UnionVariantLookupKey::Integer, 0);
					register(UnionVariantLookupKey::Integer4, 1);
					register(UnionVariantLookupKey::Integer8, 0);
				}
				SchemaNode::TimestampMillis => {
					register_type_name("TimestampMillis");
					register(UnionVariantLookupKey::Integer, 0);
					register(UnionVariantLookupKey::Integer4, 1);
					register(UnionVariantLookupKey::Integer8, 0);
				}
				SchemaNode::TimestampMicros => {
					register_type_name("TimestampMicros");
					register(UnionVariantLookupKey::Integer, 0);
					register(UnionVariantLookupKey::Integer4, 1);
					register(UnionVariantLookupKey::Integer8, 0);
				}
				SchemaNode::Duration => {
					register(UnionVariantLookupKey::StructOrMap, 5);
					register(UnionVariantLookupKey::SeqOrTupleOrTupleStruct, 5);
					register(UnionVariantLookupKey::SliceU8, 5);
				}
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
			per_name: per_name.into_inner(),
			per_direct_union_variant,
		}
	}
}
