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
pub struct SchemaBuilder {
	pub nodes: Vec<SchemaNode>,
	pub already_built_types: HashMap<TypeId, SchemaKey>,
	_private: (),
}

impl SchemaBuilder {
	/// Reserve a slot in the `nodes` array
	///
	/// After building the `SchemaNode`, it should be put at the corresponding
	/// position in `nodes`.
	pub fn reserve(&mut self) -> usize {
		let idx = self.nodes.len();
		self.nodes.push(SchemaNode::RegularType(RegularType::Null));
		idx
	}

	pub fn find_or_build<T: BuildSchema + ?Sized>(&mut self) -> SchemaKey {
		match self
			.already_built_types
			.entry(TypeId::of::<T::TypeLookup>())
		{
			std::collections::hash_map::Entry::Occupied(entry) => *entry.get(),
			std::collections::hash_map::Entry::Vacant(entry) => {
				let idx = SchemaKey::from_idx(self.nodes.len());
				entry.insert(idx);
				T::append_schema(self);
				assert!(
					self.nodes.len() > idx.idx(),
					"append_schema should always insert at least a node \
					(and its dependencies below itself)"
				);
				idx
			}
		}
	}

	pub fn build_logical_type<T: BuildSchema + ?Sized>(
		&mut self,
		logical_type: LogicalType,
	) -> SchemaKey {
		let reserved_schema_key = self.reserve();
		let new_node = SchemaNode::LogicalType {
			logical_type,
			inner: self.find_or_build::<T>(),
		};
		self.nodes[reserved_schema_key] = new_node;
		SchemaKey::from_idx(reserved_schema_key)
	}
}

macro_rules! impl_primitive {
	($($ty:ty, $variant:ident;)+) => {
		$(
			impl BuildSchema for $ty {
				fn append_schema(builder: &mut SchemaBuilder) {
					builder.nodes.push(SchemaNode::RegularType(RegularType::$variant));
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
		let new_node =
			SchemaNode::RegularType(RegularType::Array(Array::new(builder.find_or_build::<T>())));
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
		let new_node = SchemaNode::RegularType(RegularType::Union(Union::new(vec![
			builder.find_or_build::<()>(),
			builder.find_or_build::<T>(),
		])));
		builder.nodes[reserved_schema_key] = new_node;
	}

	type TypeLookup = Option<T::TypeLookup>;
}

impl<const N: usize> BuildSchema for [u8; N] {
	fn append_schema(builder: &mut SchemaBuilder) {
		builder
			.nodes
			.push(SchemaNode::RegularType(RegularType::Fixed(Fixed::new(
				Name::from_fully_qualified_name(format!("u8_array_{}", N)),
				N,
			))));
	}
	type TypeLookup = Self;
}

impl<S: std::ops::Deref<Target = str>, V: BuildSchema> BuildSchema for HashMap<S, V> {
	fn append_schema(builder: &mut SchemaBuilder) {
		let reserved_schema_key = builder.reserve();
		let new_node =
			SchemaNode::RegularType(RegularType::Map(Map::new(builder.find_or_build::<V>())));
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
