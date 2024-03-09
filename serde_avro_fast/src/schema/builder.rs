use std::{any::TypeId, collections::HashMap};

use crate::schema::*;

/// We can automatically build a schema for this type (can be `derive`d)
///
/// This trait can be derived using `#[derive(Schema)]` from the
/// [`serde_avro_derive`](https://docs.rs/serde_avro_derive/) crate
pub trait BuildSchema {
	fn schema() -> Schema {
		Self::schema_mut()
			.try_into()
			.expect("Schema derive generated invalid schema")
	}
	fn schema_mut() -> SchemaMut;
}
impl<T: BuildSchemaInner> BuildSchema for T {
	fn schema_mut() -> SchemaMut {
		let mut builder = SchemaBuilder::default();
		assert_eq!(T::build(&mut builder).idx(), 0);
		SchemaMut::from_nodes(builder.nodes)
	}
}

#[derive(Default)]
pub struct SchemaBuilder {
	pub nodes: Vec<SchemaNode>,
	pub already_built: HashMap<TypeId, SchemaKey>,
}

impl SchemaBuilder {
	pub fn reserve(&mut self) -> SchemaKey {
		let idx = self.nodes.len();
		self.nodes.push(SchemaNode::RegularType(RegularType::Null));
		SchemaKey::from_idx(idx)
	}
}

pub trait BuildSchemaInner {
	fn build(builder: &mut SchemaBuilder) -> SchemaKey;
	type TypeLookup: std::any::Any;
}

pub fn node_idx<T: BuildSchemaInner>(builder: &mut SchemaBuilder) -> SchemaKey {
	match builder.already_built.entry(TypeId::of::<T::TypeLookup>()) {
		std::collections::hash_map::Entry::Occupied(entry) => *entry.get(),
		std::collections::hash_map::Entry::Vacant(entry) => {
			let expected_idx = SchemaKey::from_idx(builder.nodes.len());
			entry.insert(expected_idx);
			let idx = T::build(builder);
			assert_eq!(idx, expected_idx);
			idx
		}
	}
}

macro_rules! impl_primitive {
	($($ty:ty, $variant:ident;)+) => {
		$(
			impl BuildSchemaInner for $ty {
				fn build(builder: &mut SchemaBuilder) -> SchemaKey {
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
			impl BuildSchemaInner for $ty {
				fn build(builder: &mut SchemaBuilder) -> SchemaKey {
					<$to as BuildSchemaInner>::build(builder)
				}
				type TypeLookup = <$to as BuildSchemaInner>::TypeLookup;
			}
		)*
	};
}
delegate_impl! {
	&'_ str, String;
	&'_ [u8], Vec<u8>;
}

impl<T: BuildSchemaInner> BuildSchemaInner for Vec<T> {
	fn build(builder: &mut SchemaBuilder) -> SchemaKey {
		let reserved_schema_key = builder.reserve();
		let new_node =
			SchemaNode::RegularType(RegularType::Array(Array::new(node_idx::<T>(builder))));
		builder.nodes[reserved_schema_key.idx()] = new_node;
		reserved_schema_key
	}

	type TypeLookup = Vec<T::TypeLookup>;
}

impl<T: BuildSchemaInner> BuildSchemaInner for &'_ [T] {
	fn build(builder: &mut SchemaBuilder) -> SchemaKey {
		<Vec<T> as BuildSchemaInner>::build(builder)
	}
	type TypeLookup = <Vec<T> as BuildSchemaInner>::TypeLookup;
}

impl<T: BuildSchemaInner> BuildSchemaInner for Option<T> {
	fn build(builder: &mut SchemaBuilder) -> SchemaKey {
		let reserved_schema_key = builder.reserve();
		let new_node = SchemaNode::RegularType(RegularType::Union(Union::new(vec![
			node_idx::<()>(builder),
			node_idx::<T>(builder),
		])));
		builder.nodes[reserved_schema_key.idx()] = new_node;
		reserved_schema_key
	}

	type TypeLookup = Option<T::TypeLookup>;
}

impl<const N: usize> BuildSchemaInner for [u8; N] {
	fn build(builder: &mut SchemaBuilder) -> SchemaKey {
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

impl<V: BuildSchemaInner> BuildSchemaInner for HashMap<String, V> {
	fn build(builder: &mut SchemaBuilder) -> SchemaKey {
		let reserved_schema_key = builder.reserve();
		let new_node = SchemaNode::RegularType(RegularType::Map(Map::new(node_idx::<V>(builder))));
		builder.nodes[reserved_schema_key.idx()] = new_node;
		reserved_schema_key
	}
	type TypeLookup = HashMap<String, V::TypeLookup>;
}
impl<V: BuildSchemaInner> BuildSchemaInner for std::collections::BTreeMap<String, V> {
	fn build(builder: &mut SchemaBuilder) -> SchemaKey {
		<HashMap<String, V> as BuildSchemaInner>::build(builder)
	}
	type TypeLookup = <HashMap<String, V> as BuildSchemaInner>::TypeLookup;
}
