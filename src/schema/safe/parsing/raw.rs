use std::borrow::Cow;

use serde::de::*;

#[derive(serde_derive::Deserialize, Clone, Copy, Debug)]
#[serde(rename_all = "kebab-case")]
pub(super) enum Type {
	Null,
	Boolean,
	Int,
	Long,
	Float,
	Double,
	Bytes,
	String,
	Array,
	Map,
	Record,
	Enum,
	Fixed,
}

pub(super) enum SchemaNode<'a> {
	TypeOnly(Type),
	Ref(&'a str),
	Object(SchemaNodeObject<'a>),
	Union(Vec<SchemaNode<'a>>),
}

#[derive(serde_derive::Deserialize)]
pub(super) struct SchemaNodeObject<'a> {
	pub(super) type_: Type,
	#[serde(skip_serializing)]
	pub(super) logical_type: Option<&'a str>,
	pub(super) name: Option<String>,
	pub(super) namespace: Option<String>,
	pub(super) fields: Option<Vec<Field<'a>>>,
	pub(super) symbols: Option<Vec<String>>,
	pub(super) items: Option<Box<SchemaNode<'a>>>,
	pub(super) values: Option<Box<SchemaNode<'a>>>,
	pub(super) size: Option<usize>,
}

#[derive(serde_derive::Deserialize)]
struct Field<'a> {
	#[serde(borrow)]
	pub(super) name: Cow<'a, str>,
	#[serde(rename = "type")]
	pub(super) type_: SchemaNode<'a>,
}

impl<'de> Deserialize<'de> for SchemaNode<'de> {
	fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
	where
		D: Deserializer<'de>,
	{
		struct SchemaNodeVisitor<'de>(std::marker::PhantomData<&'de ()>);
		impl<'de> Visitor<'de> for SchemaNodeVisitor<'de> {
			type Value = SchemaNode<'de>;

			fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
				write!(
					formatter,
					"A string (type) or an object with a `type` field or an array (union)"
				)
			}

			fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
			where
				E: Error,
			{
				// That's a type right away, or a ref.
				Ok(
					match Type::deserialize(value::StrDeserializer::<FailedDeserialization>::new(v))
					{
						Ok(type_) => SchemaNode::TypeOnly(type_),
						Err(FailedDeserialization) => SchemaNode::Ref(v),
					},
				)
			}

			fn visit_seq<A>(self, seq: A) -> Result<Self::Value, A::Error>
			where
				A: SeqAccess<'de>,
			{
				// That's a union.
				Ok(SchemaNode::Union(Deserialize::deserialize(
					serde::de::value::SeqAccessDeserializer::new(seq),
				)?))
			}

			fn visit_map<A>(self, map: A) -> Result<Self::Value, A::Error>
			where
				A: MapAccess<'de>,
			{
				// That's an object.
				Ok(SchemaNode::Object(Deserialize::deserialize(
					serde::de::value::MapAccessDeserializer::new(map),
				)?))
			}
		}
		deserializer.deserialize_any(SchemaNodeVisitor(std::marker::PhantomData))
	}
}

#[derive(thiserror::Error, Debug)]
#[error("Failed to deserialize")]
struct FailedDeserialization;
impl Error for FailedDeserialization {
	fn custom<T: std::fmt::Display>(_msg: T) -> Self {
		FailedDeserialization
	}
}
