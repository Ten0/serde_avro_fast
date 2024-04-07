use super::{
	safe::{LogicalType, RegularType as SafeSchemaType, SchemaNode as SafeSchemaNode},
	union_variants_per_type_lookup::PerTypeLookup as UnionVariantsPerTypeLookup,
	SchemaError,
};

use std::{collections::HashMap, marker::PhantomData};

pub(crate) use super::{Fixed, Name};

/// Main Schema type, opaque representation of an Avro schema
///
/// This is the fully pre-computed type used by the serializer and deserializer.
///
/// To achieve the ideal performance and ease of use via self-referencing
/// nodes, it is built using `unsafe`, so it can only be built through
/// [its safe counterpart](crate::schema::SchemaMut) (via
/// [`.freeze()`](crate::schema::SchemaMut::freeze) or [`TryFrom`]) because it
/// makes the conversion code simple enough that we can reasonably guarantee its
/// correctness despite the usage of `unsafe`.
///
/// It is useful to implement it this way because, due to how referencing via
/// [Names](https://avro.apache.org/docs/current/specification/#names) works in Avro,
/// the most performant representation of an Avro schema is not a tree but a
/// possibly-cyclic general directed graph.
pub struct Schema {
	// First node in the array is considered to be the root
	//
	// This lifetime is fake, but since all elements have to be accessed by the `root` function
	// which will downcast it and we never push anything more in there (which would cause
	// reallocation and invalidate all nodes) this is correct.
	nodes: Vec<SchemaNode<'static>>,
	fingerprint: [u8; 8],
	schema_json: String,
}

impl Schema {
	/// This is private API, you probably intended to call that on an
	/// [`SchemaMut`](crate::schema::SchemaMut) instead of `Schema`.
	///
	/// The Avro schema
	/// is represented internally as a directed graph of nodes, all stored in
	/// [`Schema`].
	///
	/// The root node represents the whole schema.
	pub(crate) fn root<'a>(&'a self) -> NodeRef<'a> {
		// the signature of this function downgrades the fake 'static lifetime in a way
		// that makes it correct
		assert!(
			!self.nodes.is_empty(),
			"Schema must have at least one node (the root)"
		);
		// SAFETY: bounds checked
		unsafe { NodeRef::new(self.nodes.as_ptr() as *mut _) }
	}

	/// SAFETY: this `NodeRef` must never be dereferenced after the
	/// corresponding schema was dropped.
	pub(crate) unsafe fn root_with_fake_static_lifetime(&self) -> NodeRef<'static> {
		assert!(
			!self.nodes.is_empty(),
			"Schema must have at least one node (the root)"
		);
		// SAFETY: bounds checked
		NodeRef::new(self.nodes.as_ptr() as *mut _)
	}

	/// Obtain the JSON for this schema
	pub fn json(&self) -> &str {
		&self.schema_json
	}

	/// Obtain the Rabin fingerprint of the schema
	pub fn rabin_fingerprint(&self) -> &[u8; 8] {
		&self.fingerprint
	}
}

/// A `NodeRef` is a pointer to a node in a [`Schema`]
///
/// This is morally equivalent to `&'a SchemaNode<'a>`, only Rust will not
/// assume as much when it comes to aliasing constraints.
///
/// For ease of use, it can be `Deref`d to a [`SchemaNode`],
/// so this module is responsible for ensuring that no `NodeRef`
/// is leaked that would be incorrect on that regard.
///
/// SAFETY: The invariant that we need to uphold is that with regards to
/// lifetimes, this behaves the same as an `&'a SchemaNode<'a>`.
///
/// We don't directly use references because we need to update the pointees
/// after creating refences to them when building the schema, and that doesn't
/// pass Miri's Stacked Borrows checks.
/// This abstraction should be reasonably ergonomic, but pass miri.
pub(crate) struct NodeRef<'a, N = SchemaNode<'a>> {
	node: std::ptr::NonNull<N>,
	_spooky: PhantomData<&'a N>,
}
impl<'a, N> Copy for NodeRef<'a, N> {}
impl<'a, N> Clone for NodeRef<'a, N> {
	fn clone(&self) -> Self {
		*self
	}
}
/// SAFETY: NonNull is !Send !Sync, but NodeRef is really just a reference, so
/// we can implement Sync and Send
unsafe impl<T: Sync> Sync for NodeRef<'_, T> {}
/// SAFETY: NonNull is !Send !Sync, but NodeRef is really just a reference, so
/// we can implement Sync and Send
unsafe impl<T: Sync> Send for NodeRef<'_, T> {}
impl<N> NodeRef<'static, N> {
	const unsafe fn new(ptr: *mut N) -> Self {
		Self {
			node: std::ptr::NonNull::new_unchecked(ptr),
			_spooky: PhantomData,
		}
	}
	pub(crate) const fn from_static(actually_static: &'static N) -> Self {
		// SAFETY: since it's actually 'static, it always upholds the invariant
		// that we can turn it into a `&'static N`
		unsafe {
			Self {
				node: std::ptr::NonNull::new_unchecked(actually_static as *const N as *mut N),
				_spooky: PhantomData,
			}
		}
	}
}
impl<'a, N> NodeRef<'a, N> {
	/// Compared to `Deref`, this propagates the lifetime of the reference
	pub(crate) fn as_ref(self) -> &'a N {
		// SAFETY: this module is responsible for never leaking a `NodeRef` that
		// isn't tied to the appropriate lifetime
		unsafe { self.node.as_ref() }
	}
}
impl<'a, N> std::ops::Deref for NodeRef<'a, N> {
	type Target = N;
	fn deref(&self) -> &Self::Target {
		self.as_ref()
	}
}

/// A node of an avro schema, borrowed from a [`Schema`].
///
/// This enum is borrowed from a [`Schema`] and is used to navigate it.
///
/// For details about the meaning of the variants, see the
/// [`SchemaNode`](crate::schema::SchemaNode) documentation.
#[non_exhaustive]
pub(crate) enum SchemaNode<'a> {
	Null,
	Boolean,
	Int,
	Long,
	Float,
	Double,
	Bytes,
	String,
	Array(NodeRef<'a>),
	Map(NodeRef<'a>),
	Union(Union<'a>),
	Record(Record<'a>),
	Enum(Enum),
	Fixed(Fixed),
	Decimal(Decimal<'a>),
	Uuid,
	Date,
	TimeMillis,
	TimeMicros,
	TimestampMillis,
	TimestampMicros,
	Duration,
}

/// Component of a [`SchemaNode`]
pub(crate) struct Union<'a> {
	pub(crate) variants: Vec<NodeRef<'a>>,
	pub(crate) per_type_lookup: UnionVariantsPerTypeLookup<'a>,
}

impl std::fmt::Debug for Union<'_> {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		// Skip per_type_lookup for readability
		f.debug_struct("Union")
			.field("variants", &self.variants)
			.finish()
	}
}

/// Component of a [`SchemaNode`]
pub(crate) struct Record<'a> {
	pub(crate) fields: Vec<RecordField<'a>>,
	pub(crate) name: Name,
	pub(crate) per_name_lookup: HashMap<String, usize>,
}

impl<'a> std::fmt::Debug for Record<'a> {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		// Skip per_type_lookup for readability
		f.debug_struct("Record")
			.field("fields", &self.fields)
			.field("name", &self.name)
			.finish()
	}
}

/// Component of a [`SchemaNode`]
#[derive(Debug)]
pub(crate) struct RecordField<'a> {
	pub(crate) name: String,
	pub(crate) schema: NodeRef<'a>,
}

/// Component of a [`SchemaNode`]
#[derive(Clone)]
pub(crate) struct Enum {
	pub(crate) symbols: Vec<String>,
	pub(crate) name: Name,
	pub(crate) per_name_lookup: HashMap<String, usize>,
}

impl std::fmt::Debug for Enum {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		// Skip per_type_lookup for readability
		f.debug_struct("Enum")
			.field("name", &self.name)
			.field("symbols", &self.symbols)
			.finish()
	}
}

/// Component of a [`SchemaNode`]
#[derive(Clone, Debug)]
pub(crate) struct Decimal<'a> {
	/// Unused for now - TODO take this into account when serializing (tolerate
	/// precision loss within the limits of this)
	pub(crate) _precision: usize,
	pub(crate) scale: u32,
	pub(crate) repr: DecimalRepr<'a>,
}
#[derive(Clone, Debug)]
pub(crate) enum DecimalRepr<'a> {
	Bytes,
	Fixed(NodeRef<'a, Fixed>),
}

impl TryFrom<super::safe::SchemaMut> for Schema {
	type Error = SchemaError;
	fn try_from(safe: super::safe::SchemaMut) -> Result<Self, SchemaError> {
		if safe.nodes().is_empty() {
			return Err(SchemaError::new(
				"Schema must have at least one node (the root)",
			));
		}

		// Pre-compute logical types
		enum LogicalTypeResolution {
			NotLogicalType,
			UnresolvedRemapped(usize),
			Resolved(SchemaNode<'static>),
		}
		let mut logical_types = Vec::with_capacity(safe.nodes.len());
		let mut set_decimal_repr_to_fixed: Vec<(usize, usize)> = Vec::new();
		for (i, n) in safe.nodes.iter().enumerate() {
			logical_types.push(match n {
				SafeSchemaNode::LogicalType {
					logical_type,
					inner,
				} => {
					let inner_type = match safe.nodes.get(inner.idx).ok_or_else(|| {
						SchemaError::new("Logical type refers to node that doesn't exist")
					})? {
						SafeSchemaNode::RegularType(inner) => inner,
						SafeSchemaNode::LogicalType {
							logical_type: inner_logical_type,
							..
						} => {
							return Err(SchemaError::msg(format_args!(
								"Immediately-nested logical types: \
									{inner_logical_type:?} in {logical_type:?}"
							)))
						}
					};
					match (logical_type, inner_type) {
						(LogicalType::Decimal(decimal), SafeSchemaType::Bytes) => {
							LogicalTypeResolution::Resolved(SchemaNode::Decimal(Decimal {
								_precision: decimal.precision,
								scale: decimal.scale,
								repr: DecimalRepr::Bytes,
							}))
						}
						(LogicalType::Decimal(decimal), SafeSchemaType::Fixed(_)) => {
							set_decimal_repr_to_fixed.push((i, inner.idx));
							LogicalTypeResolution::Resolved(SchemaNode::Decimal(Decimal {
								_precision: decimal.precision,
								scale: decimal.scale,
								repr: DecimalRepr::Bytes,
							}))
						}
						(LogicalType::Uuid, SafeSchemaType::String) => {
							LogicalTypeResolution::Resolved(SchemaNode::Uuid)
						}
						(LogicalType::Date, SafeSchemaType::Int) => {
							LogicalTypeResolution::Resolved(SchemaNode::Date)
						}
						(LogicalType::TimeMillis, SafeSchemaType::Int) => {
							LogicalTypeResolution::Resolved(SchemaNode::TimeMillis)
						}
						(LogicalType::TimeMicros, SafeSchemaType::Long) => {
							LogicalTypeResolution::Resolved(SchemaNode::TimeMicros)
						}
						(LogicalType::TimestampMillis, SafeSchemaType::Long) => {
							LogicalTypeResolution::Resolved(SchemaNode::TimestampMillis)
						}
						(LogicalType::TimestampMicros, SafeSchemaType::Long) => {
							LogicalTypeResolution::Resolved(SchemaNode::TimestampMicros)
						}
						(LogicalType::Duration, SafeSchemaType::Fixed(fixed))
							if fixed.size == 12 =>
						{
							LogicalTypeResolution::Resolved(SchemaNode::Duration)
						}
						_ => LogicalTypeResolution::UnresolvedRemapped(inner.idx),
					}
				}
				SafeSchemaNode::RegularType(_) => LogicalTypeResolution::NotLogicalType,
			});
		}

		// The `nodes` allocation should never be moved otherwise references will become
		// invalid
		let mut ret = Self {
			nodes: (0..safe.nodes.len()).map(|_| SchemaNode::Null).collect(),
			fingerprint: safe.canonical_form_rabin_fingerprint()?,
			schema_json: match safe.schema_json {
				None => safe.serialize_to_json()?,
				Some(json) => json,
			},
		};
		let len = ret.nodes.len();
		// Let's be extra-sure (second condition is for calls to add)
		assert!(len > 0 && len == safe.nodes.len() && len <= (isize::MAX as usize));
		let storage_start_ptr = ret.nodes.as_mut_ptr();
		let key_to_ref = |schema_key: super::safe::SchemaKey,
		                  logical_types: &[LogicalTypeResolution]|
		 -> Result<NodeRef<'static>, SchemaError> {
			let mut idx = schema_key.idx;
			if idx >= len {
				return Err(SchemaError::msg(format_args!(
					"SchemaKey index {} is out of bounds (len: {})",
					idx, len
				)));
			}
			if let LogicalTypeResolution::UnresolvedRemapped(remapped_idx) = logical_types[idx] {
				idx = remapped_idx;
				// There cannot be nested logical types so there cannot be a second remapping
				// Also we know the index is low enough because that has been checked
				// when loading inner_type above
				// But we're doing unsafe so let's still make extra sure that is true
				assert!(
					idx < len,
					"id should be low enough - bug in serde_avro_fast"
				);
			}
			// SAFETY: see below
			Ok(unsafe { NodeRef::new(storage_start_ptr.add(idx)) })
		};

		// Now we can initialize the nodes
		let mut curr_storage_node_ptr = storage_start_ptr;
		for (i, safe_node) in safe.nodes.into_iter().enumerate() {
			// SAFETY:
			// - The nodes we create here are never moving in memory since the entire vec is
			//   preallocated, and even when moving a vec, the pointed space doesn't move.
			// - The fake `'static` lifetimes are always downgraded before being made
			//   available.
			// - We only use pointers from the point at which we call `as_mut_ptr` so the
			//   compiler will not have aliasing constraints.
			// - We don't dereference the references we create in key_to_node until they
			//   they are all initialized.

			let new_node = match safe_node {
				SafeSchemaNode::LogicalType { .. } => match &mut logical_types[i] {
					LogicalTypeResolution::Resolved(ref mut resolved) => {
						// We can take it, nobody but us reads it
						std::mem::replace(resolved, SchemaNode::Null)
					}
					LogicalTypeResolution::NotLogicalType => unreachable!(),
					LogicalTypeResolution::UnresolvedRemapped(_) => {
						// We're remapping all nodes pointing to this node to another node
						// so we can leave Null here, that won't be used.
						SchemaNode::Null
					}
				},
				SafeSchemaNode::RegularType(regular_type) => match regular_type {
					SafeSchemaType::Null => SchemaNode::Null,
					SafeSchemaType::Boolean => SchemaNode::Boolean,
					SafeSchemaType::Int => SchemaNode::Int,
					SafeSchemaType::Long => SchemaNode::Long,
					SafeSchemaType::Float => SchemaNode::Float,
					SafeSchemaType::Double => SchemaNode::Double,
					SafeSchemaType::Bytes => SchemaNode::Bytes,
					SafeSchemaType::String => SchemaNode::String,
					SafeSchemaType::Array(array) => {
						SchemaNode::Array(key_to_ref(array.items, &logical_types)?)
					}
					SafeSchemaType::Map(map) => {
						SchemaNode::Map(key_to_ref(map.values, &logical_types)?)
					}
					SafeSchemaType::Union(union) => SchemaNode::Union({
						Union {
							variants: {
								let mut variants = Vec::with_capacity(union.variants.len());
								for schema_key in union.variants {
									variants.push(key_to_ref(schema_key, &logical_types)?);
								}
								variants
							},
							per_type_lookup: {
								// Can't be initialized just yet because other nodes
								// may not have been initialized
								UnionVariantsPerTypeLookup::placeholder()
							},
						}
					}),
					SafeSchemaType::Record(record) => SchemaNode::Record(Record {
						per_name_lookup: record
							.fields
							.iter()
							.enumerate()
							.map(|(i, v)| (v.name.clone(), i))
							.collect(),
						fields: {
							let mut fields = Vec::with_capacity(record.fields.len());
							for field in record.fields {
								fields.push(RecordField {
									name: field.name,
									schema: key_to_ref(field.type_, &logical_types)?,
								});
							}
							fields
						},
						name: record.name,
					}),
					SafeSchemaType::Enum(enum_) => SchemaNode::Enum(Enum {
						per_name_lookup: enum_
							.symbols
							.iter()
							.enumerate()
							.map(|(i, v)| (v.clone(), i))
							.collect(),
						symbols: enum_.symbols,
						name: enum_.name,
					}),
					SafeSchemaType::Fixed(fixed) => SchemaNode::Fixed(fixed),
				},
			};
			// SAFETY: see comment at beginning of loop
			unsafe {
				*curr_storage_node_ptr = new_node;
				curr_storage_node_ptr = curr_storage_node_ptr.add(1);
			};
		}
		// Now that all the nodes have been partially we can set the references to
		// `Fixed` for `Decimal` nodes
		for (i, to) in set_decimal_repr_to_fixed {
			// SAFETY: indexes are valid, and there are no live references
			// to this (live NodeRef don't count because they contain pointers, and that is
			// allowed)
			unsafe {
				match *storage_start_ptr.add(i) {
					SchemaNode::Decimal(ref mut decimal) => {
						assert_ne!(i, to, "That would violate aliasing rules");
						match *storage_start_ptr.add(to) {
							SchemaNode::Fixed(ref fixed) => {
								decimal.repr =
									DecimalRepr::Fixed(NodeRef::new(fixed as *const _ as *mut _));
							}
							_ => unreachable!("set_decimal_repr_to_fixed was built incorrectly"),
						}
					}
					_ => unreachable!("set_decimal_repr_to_fixed was built incorrectly"),
				}
			}
		}
		// Now that all the nodes have been **fully** initialized (except their
		// `per_type_lookup` tables) we can initialize the `per_type_lookup` tables
		// Note that this has to be done after all nodes have been initialized because
		// the `per_type_lookup` table may read even the late-initialized fields such as
		// decimal repr.
		curr_storage_node_ptr = storage_start_ptr;
		for _ in 0..len {
			// Safety:
			// - UnionVariantsPerTypeLookup won't ever read `per_type_lookup` of the other
			//   nodes, so there are no aliasing issues. (Tbh I'm not even sure that would
			//   really be an issue because we'd have `& &mut` anyway but with that I'm sure
			//   there isn't an issue)
			unsafe {
				match *curr_storage_node_ptr {
					SchemaNode::Union(Union {
						ref variants,
						ref mut per_type_lookup,
					}) => {
						*per_type_lookup = UnionVariantsPerTypeLookup::new(variants);
					}
					_ => {}
				}
				curr_storage_node_ptr = curr_storage_node_ptr.add(1);
			}
		}
		Ok(ret)
	}
}

impl std::fmt::Debug for Schema {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		<SchemaNode<'_> as std::fmt::Debug>::fmt(self.root().as_ref(), f)
	}
}

impl<N: std::fmt::Debug> std::fmt::Debug for NodeRef<'_, N> {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		<N as std::fmt::Debug>::fmt(self.as_ref(), f)
	}
}

impl<'a> std::fmt::Debug for SchemaNode<'a> {
	fn fmt(&self, f: &mut std::fmt::Formatter) -> ::std::fmt::Result {
		// Avoid going into stack overflow when rendering SchemaNode's debug impl, in
		// case there are loops

		use std::cell::Cell;
		struct SchemaNodeRenderingDepthGuard;
		thread_local! {
			static DEPTH: Cell<u32> = Cell::new(0);
		}
		impl Drop for SchemaNodeRenderingDepthGuard {
			fn drop(&mut self) {
				DEPTH.with(|cell| cell.set(cell.get().checked_sub(1).unwrap()));
			}
		}
		const MAX_DEPTH: u32 = 2;
		let depth = DEPTH.with(|cell| {
			let val = cell.get();
			cell.set(val + 1);
			val
		});
		let _decrement_depth_guard = SchemaNodeRenderingDepthGuard;

		match *self {
			SchemaNode::Null => f.debug_tuple("Null").finish(),
			SchemaNode::Boolean => f.debug_tuple("Boolean").finish(),
			SchemaNode::Int => f.debug_tuple("Int").finish(),
			SchemaNode::Long => f.debug_tuple("Long").finish(),
			SchemaNode::Float => f.debug_tuple("Float").finish(),
			SchemaNode::Double => f.debug_tuple("Double").finish(),
			SchemaNode::Bytes => f.debug_tuple("Bytes").finish(),
			SchemaNode::String => f.debug_tuple("String").finish(),
			SchemaNode::Array(inner) => {
				let mut d = f.debug_tuple("Array");
				if depth < MAX_DEPTH {
					d.field(inner.as_ref());
				}
				d.finish()
			}
			SchemaNode::Map(inner) => {
				let mut d = f.debug_tuple("Map");
				if depth < MAX_DEPTH {
					d.field(inner.as_ref());
				}
				d.finish()
			}
			SchemaNode::Union(ref inner) => {
				let mut d = f.debug_tuple("Union");
				if depth < MAX_DEPTH {
					d.field(inner);
				}
				d.finish()
			}
			SchemaNode::Record(ref inner) => {
				let mut d = f.debug_tuple("Record");
				if depth < MAX_DEPTH {
					d.field(inner);
				}
				d.finish()
			}
			SchemaNode::Enum(ref inner) => {
				let mut d = f.debug_tuple("Enum");
				if depth < MAX_DEPTH {
					d.field(inner);
				}
				d.finish()
			}
			SchemaNode::Fixed(ref inner) => {
				let mut d = f.debug_tuple("Fixed");
				if depth < MAX_DEPTH {
					d.field(inner);
				}
				d.finish()
			}
			SchemaNode::Decimal(ref inner) => {
				let mut d = f.debug_tuple("Decimal");
				if depth < MAX_DEPTH {
					d.field(inner);
				}
				d.finish()
			}
			SchemaNode::Uuid => f.debug_tuple("Uuid").finish(),
			SchemaNode::Date => f.debug_tuple("Date").finish(),
			SchemaNode::TimeMillis => f.debug_tuple("TimeMillis").finish(),
			SchemaNode::TimeMicros => f.debug_tuple("TimeMicros").finish(),
			SchemaNode::TimestampMillis => f.debug_tuple("TimestampMillis").finish(),
			SchemaNode::TimestampMicros => f.debug_tuple("TimestampMicros").finish(),
			SchemaNode::Duration => f.debug_tuple("Duration").finish(),
		}
	}
}
