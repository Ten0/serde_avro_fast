pub use serde_avro_fast;

pub use serde_avro_derive_macros::*;

use std::{any::TypeId, collections::HashMap};

use serde_avro_fast::schema::*;

/// We can automatically build a schema for this type (can be `derive`d)
///
/// This trait can be derived using `#[derive(Schema)]` from the
/// [`serde_avro_derive`](https://docs.rs/serde_avro_derive/) crate
pub trait BuildSchema {
	/// Obtain the [`struct@Schema`] for this type
	fn schema() -> Schema {
		Self::schema_mut()
			.try_into()
			.expect("Schema derive generated invalid schema")
	}
	/// Obtain the [`SchemaMut`] for this type
	fn schema_mut() -> SchemaMut {
		let mut builder = SchemaBuilder::default();
		assert_eq!(Self::build_schema(&mut builder).idx(), 0);
		SchemaMut::from_nodes(builder.nodes)
	}

	/// Largely internal method to build the schema. Registers the schema with
	/// the builder.
	fn build_schema(builder: &mut SchemaBuilder) -> SchemaKey;
	type TypeLookup: std::any::Any;
}

#[derive(Default)]
pub struct SchemaBuilder {
	pub nodes: Vec<SchemaNode>,
	pub already_built: HashMap<TypeId, SchemaKey>,
	_private: (),
}

impl SchemaBuilder {
	pub fn reserve(&mut self) -> SchemaKey {
		let idx = self.nodes.len();
		self.nodes.push(SchemaNode::RegularType(RegularType::Null));
		SchemaKey::from_idx(idx)
	}

	pub fn find_or_build<T: BuildSchema>(&mut self) -> SchemaKey {
		match self.already_built.entry(TypeId::of::<T::TypeLookup>()) {
			std::collections::hash_map::Entry::Occupied(entry) => *entry.get(),
			std::collections::hash_map::Entry::Vacant(entry) => {
				let expected_idx = SchemaKey::from_idx(self.nodes.len());
				entry.insert(expected_idx);
				let idx = T::build_schema(self);
				assert_eq!(idx, expected_idx);
				idx
			}
		}
	}
}

macro_rules! impl_primitive {
	($($ty:ty, $variant:ident;)+) => {
		$(
			impl BuildSchema for $ty {
				fn build_schema(builder: &mut SchemaBuilder) -> SchemaKey {
					let schema_key = SchemaKey::from_idx(builder.nodes.len());
					builder.nodes.push(SchemaNode::RegularType(RegularType::$variant));
					schema_key
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

macro_rules! delegate_impl {
	($($ty:ty, $to:ty;)+) => {
		$(
			impl BuildSchema for $ty {
				fn build_schema(builder: &mut SchemaBuilder) -> SchemaKey {
					<$to as BuildSchema>::build_schema(builder)
				}
				type TypeLookup = <$to as BuildSchema>::TypeLookup;
			}
		)*
	};
}
delegate_impl! {
	&'_ str, String;
	&'_ [u8], Vec<u8>;
	u16, i32;
	u32, i64;
	u64, i64;
	i8, i32;
	i16, i32;
}

impl<T: BuildSchema> BuildSchema for Vec<T> {
	fn build_schema(builder: &mut SchemaBuilder) -> SchemaKey {
		let reserved_schema_key = builder.reserve();
		let new_node =
			SchemaNode::RegularType(RegularType::Array(Array::new(builder.find_or_build::<T>())));
		builder.nodes[reserved_schema_key.idx()] = new_node;
		reserved_schema_key
	}

	type TypeLookup = Vec<T::TypeLookup>;
}

impl<T: BuildSchema> BuildSchema for &'_ [T] {
	fn build_schema(builder: &mut SchemaBuilder) -> SchemaKey {
		<Vec<T> as BuildSchema>::build_schema(builder)
	}
	type TypeLookup = <Vec<T> as BuildSchema>::TypeLookup;
}

impl<T: BuildSchema> BuildSchema for Option<T> {
	fn build_schema(builder: &mut SchemaBuilder) -> SchemaKey {
		let reserved_schema_key = builder.reserve();
		let new_node = SchemaNode::RegularType(RegularType::Union(Union::new(vec![
			builder.find_or_build::<()>(),
			builder.find_or_build::<T>(),
		])));
		builder.nodes[reserved_schema_key.idx()] = new_node;
		reserved_schema_key
	}

	type TypeLookup = Option<T::TypeLookup>;
}

impl<const N: usize> BuildSchema for [u8; N] {
	fn build_schema(builder: &mut SchemaBuilder) -> SchemaKey {
		let schema_key = SchemaKey::from_idx(builder.nodes.len());
		builder
			.nodes
			.push(SchemaNode::RegularType(RegularType::Fixed(Fixed::new(
				Name::from_fully_qualified_name(format!("u8_array_{}", N)),
				N,
			))));
		schema_key
	}
	type TypeLookup = Self;
}

impl<S: std::ops::Deref<Target = str>, V: BuildSchema> BuildSchema for HashMap<S, V> {
	fn build_schema(builder: &mut SchemaBuilder) -> SchemaKey {
		let reserved_schema_key = builder.reserve();
		let new_node =
			SchemaNode::RegularType(RegularType::Map(Map::new(builder.find_or_build::<V>())));
		builder.nodes[reserved_schema_key.idx()] = new_node;
		reserved_schema_key
	}
	type TypeLookup = HashMap<String, V::TypeLookup>;
}
impl<S: std::ops::Deref<Target = str>, V: BuildSchema> BuildSchema
	for std::collections::BTreeMap<S, V>
{
	fn build_schema(builder: &mut SchemaBuilder) -> SchemaKey {
		<HashMap<String, V> as BuildSchema>::build_schema(builder)
	}
	type TypeLookup = <HashMap<String, V> as BuildSchema>::TypeLookup;
}
