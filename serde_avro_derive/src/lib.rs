//! Bring automatic Avro Schema generation to [`serde_avro_fast`]
//!
//! See the [`#[derive(BuildSchema)]`](derive@BuildSchema) documentation for
//! more information

pub use serde_avro_fast;

pub use serde_avro_derive_macros::*;

use std::{any::TypeId, collections::HashMap};

use serde_avro_fast::schema::*;

/// We can automatically build a schema for this type (can be `derive`d)
///
/// This trait can be derived using
/// [`#[derive(BuildSchema)]`](derive@BuildSchema)
pub trait BuildSchema {
	/// Build a [`Schema`] for this type
	fn schema() -> Result<Schema, SchemaError> {
		Self::schema_mut().try_into()
	}
	/// Build a [`SchemaMut`] for this type
	fn schema_mut() -> SchemaMut {
		let mut builder = SchemaBuilder::default();
		Self::append_schema(&mut builder);
		SchemaMut::from_nodes(builder.nodes)
	}

	/// Largely internal method to build the schema. Registers the schema within
	/// the builder.
	///
	/// This does not check if this type already exists in the builder, so it
	/// should never be called directly (instead, use
	/// [`SchemaBuilder::find_or_build`])
	///
	/// The [`SchemaNode`] for this type should be put at the current end of the
	/// `nodes` array, and its non-already-built dependencies should be put
	/// after in the array.
	fn append_schema(builder: &mut SchemaBuilder);

	/// Largely internal type used by
	/// [`#[derive(BuildSchema)]`](derive@BuildSchema)
	///
	/// The TypeId of this type will be used to lookup whether the
	/// [`SchemaNode`] for this type has already been built in the
	/// [`SchemaBuilder`].
	///
	/// This indirection is required to allow non-static types to implement
	/// [`BuildSchema`], and also enables using the same node for types that we
	/// know map to the same schema.
	type TypeLookup: std::any::Any;
}

/// Largely internal type used by [`#[derive(BuildSchema)]`](derive@BuildSchema)
///
/// You should typically not use this directly
#[derive(Default)]
#[non_exhaustive]
pub struct SchemaBuilder {
	/// The current set of nodes that have been built
	///
	/// These will be the nodes of the resulting schema.
	///
	/// The first node of the array is the root of the schema.
	pub nodes: Vec<SchemaNode>,
	/// This map maintains the lookup from the TypeId of a type to the index of
	/// the node in `nodes` that represents the schema for that type.
	///
	/// This allows not registering several nodes for the same type if it is
	/// referenced multiple times. This results in a smaller, more efficient
	/// schema (also avoids infinite loops for cyclic types).
	///
	/// Note that it is important for serialization of the Schema to JSON that
	/// the same named schema node is used for the same type, so this map is
	/// necessary. (This is used for de-duplication of named types.)
	pub already_built_types: HashMap<TypeId, SchemaKey>,
}

impl SchemaBuilder {
	/// Reserve a slot in the `nodes` array
	///
	/// After building the `SchemaNode`, it should be put at the corresponding
	/// position in `nodes`.
	pub fn reserve(&mut self) -> usize {
		let idx = self.nodes.len();
		self.nodes.push(RegularType::Null.into());
		idx
	}

	/// If the schema for this type (generic parameter) has already been built
	/// and inserted in the [`nodes`](SchemaBuilder::nodes), return the
	/// [`SchemaKey`] for it.
	///
	/// Otherwise, build the relevant [`SchemaNode`]s, insert them in
	/// [`nodes`](SchemaBuilder::nodes), and return the [`SchemaKey`] for the
	/// newly built schema.
	pub fn find_or_build<T: BuildSchema + ?Sized>(&mut self) -> SchemaKey {
		match self
			.already_built_types
			.entry(TypeId::of::<T::TypeLookup>())
		{
			std::collections::hash_map::Entry::Occupied(entry) => *entry.get(),
			std::collections::hash_map::Entry::Vacant(entry) => {
				let schema_key = SchemaKey::from_idx(self.nodes.len());
				entry.insert(schema_key);
				T::append_schema(self);
				assert!(
					self.nodes.len() > schema_key.idx(),
					"append_schema should always insert at least a node \
					(and its dependencies below itself)"
				);
				schema_key
			}
		}
	}

	/// Insert a new [`SchemaNode`] corresponding to the schema for the type `T`
	/// into [`nodes`](SchemaBuilder::nodes), regardless of whether it has
	/// already been built.
	///
	/// This is only useful if using a type as a base for the definition of
	/// another type, but patching it afterwards (e.g. adding a logical type).
	/// Otherwise, use [`find_or_build`](SchemaBuilder::find_or_build) to avoid
	/// duplicate nodes.
	pub fn build_duplicate<T: BuildSchema + ?Sized>(&mut self) -> SchemaKey {
		let schema_key = SchemaKey::from_idx(self.nodes.len());
		T::append_schema(self);
		assert!(
			self.nodes.len() > schema_key.idx(),
			"append_schema should always insert at least a node \
				(and its dependencies below itself)"
		);
		schema_key
	}

	/// Register a new node for this logical type, where the regular type
	/// specified with `T` is annotated with the logical type specified as the
	/// `logical_type` argument.
	///
	/// `name_override` specifies how to override the name if the underlying
	/// node ends up generating a named node
	pub fn build_logical_type(
		&mut self,
		logical_type: LogicalType,
		inner_type_duplicate: impl FnOnce(&mut Self) -> SchemaKey,
		name_override: impl FnOnce() -> String,
	) -> SchemaKey {
		let inner_type_duplicate_key = inner_type_duplicate(self);
		let node = &mut self.nodes[inner_type_duplicate_key.idx()];
		node.logical_type = Some(logical_type);

		if let Some(name) = node.type_.name_mut() {
			*name = Name::from_fully_qualified_name(name_override());
		}

		inner_type_duplicate_key
	}
}

macro_rules! impl_primitive {
	($($ty:ty, $variant:ident;)+) => {
		$(
			impl BuildSchema for $ty {
				fn append_schema(builder: &mut SchemaBuilder) {
					builder.nodes.push(RegularType::$variant.into());
				}
				type TypeLookup = Self;
			}
		)*
	};
}
impl_primitive!(
	(), Null;
	bool, Boolean;
	i32, Int;
	i64, Long;
	f32, Float;
	f64, Double;
	String, String;
	Vec<u8>, Bytes;
);

macro_rules! impl_forward {
	($($ty:ty, $to:ty;)+) => {
		$(
			impl BuildSchema for $ty {
				fn append_schema(builder: &mut SchemaBuilder) {
					<$to as BuildSchema>::append_schema(builder)
				}
				type TypeLookup = <$to as BuildSchema>::TypeLookup;
			}
		)*
	};
}
impl_forward! {
	str, String;
	[u8], Vec<u8>;
	u16, i32;
	u32, i64;
	u64, i64;
	i8, i32;
	i16, i32;
	usize, i64;
}

macro_rules! impl_ptr {
	($($($ty_path:ident)::+,)+) => {
		$(
			impl<T: BuildSchema + ?Sized> BuildSchema for $($ty_path)::+<T> {
				fn append_schema(builder: &mut SchemaBuilder) {
					<T as BuildSchema>::append_schema(builder)
				}
				type TypeLookup = T::TypeLookup;
			}
		)*
	};
}
impl_ptr! {
	Box,
	std::sync::Arc,
	std::rc::Rc,
	std::cell::RefCell,
	std::cell::Cell,
}
impl<T: BuildSchema + ?Sized> BuildSchema for &'_ T {
	fn append_schema(builder: &mut SchemaBuilder) {
		<T as BuildSchema>::append_schema(builder)
	}
	type TypeLookup = T::TypeLookup;
}
impl<T: BuildSchema + ?Sized> BuildSchema for &'_ mut T {
	fn append_schema(builder: &mut SchemaBuilder) {
		<T as BuildSchema>::append_schema(builder)
	}
	type TypeLookup = T::TypeLookup;
}

impl<T: BuildSchema> BuildSchema for Vec<T> {
	fn append_schema(builder: &mut SchemaBuilder) {
		let reserved_schema_key = builder.reserve();
		let new_node = Array::new(builder.find_or_build::<T>()).into();
		builder.nodes[reserved_schema_key] = new_node;
	}

	type TypeLookup = Vec<T::TypeLookup>;
}

impl<T: BuildSchema> BuildSchema for [T] {
	fn append_schema(builder: &mut SchemaBuilder) {
		<Vec<T> as BuildSchema>::append_schema(builder)
	}
	type TypeLookup = <Vec<T> as BuildSchema>::TypeLookup;
}

impl<T: BuildSchema> BuildSchema for Option<T> {
	fn append_schema(builder: &mut SchemaBuilder) {
		let reserved_schema_key = builder.reserve();
		let new_node = Union::new(vec![
			builder.find_or_build::<()>(),
			builder.find_or_build::<T>(),
		])
		.into();
		builder.nodes[reserved_schema_key] = new_node;
	}

	type TypeLookup = Option<T::TypeLookup>;
}

impl<const N: usize> BuildSchema for [u8; N] {
	fn append_schema(builder: &mut SchemaBuilder) {
		builder.nodes.push(
			Fixed::new(
				Name::from_fully_qualified_name(format!("u8_array_{}", N)),
				N,
			)
			.into(),
		);
	}
	type TypeLookup = Self;
}

impl<S: std::ops::Deref<Target = str>, V: BuildSchema> BuildSchema for HashMap<S, V> {
	fn append_schema(builder: &mut SchemaBuilder) {
		let reserved_schema_key = builder.reserve();
		let new_node = Map::new(builder.find_or_build::<V>()).into();
		builder.nodes[reserved_schema_key] = new_node;
	}
	type TypeLookup = HashMap<String, V::TypeLookup>;
}
impl<S: std::ops::Deref<Target = str>, V: BuildSchema> BuildSchema
	for std::collections::BTreeMap<S, V>
{
	fn append_schema(builder: &mut SchemaBuilder) {
		<HashMap<String, V> as BuildSchema>::append_schema(builder)
	}
	type TypeLookup = <HashMap<String, V> as BuildSchema>::TypeLookup;
}

#[doc(hidden)]
/// Used by the [`BuildSchema!`] derive macro to generate a unique name for a
/// struct when it's generic
pub fn hash_type_id(struct_name: &mut String, type_id: TypeId) {
	use std::{
		fmt::Write,
		hash::{Hash as _, Hasher as _},
	};
	#[allow(deprecated)] // I actually want to not change hasher
	let mut hasher = std::hash::SipHasher::new();
	type_id.hash(&mut hasher);
	write!(struct_name, "_{:016x?}", hasher.finish()).unwrap();
}

#[doc(hidden)]
pub enum LazyNamespace {
	Pending(fn() -> String),
	Computed(String),
}
impl LazyNamespace {
	pub fn new(f: fn() -> String) -> Self {
		Self::Pending(f)
	}
	pub fn get(&mut self) -> &str {
		match self {
			Self::Pending(f) => {
				let n = f();
				*self = Self::Computed(n);
				match self {
					Self::Computed(n) => n,
					_ => unreachable!(),
				}
			}
			Self::Computed(n) => n,
		}
	}
}
