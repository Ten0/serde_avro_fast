use serde::de::*;

pub(super) enum SchemaNode<'a> {
	Type(Type),
	Ref(std::borrow::Cow<'a, str>),
	Object(Box<SchemaNodeObject<'a>>),
	Union(Vec<SchemaNode<'a>>),
}

#[derive(serde_derive::Deserialize, Clone, Copy, Debug)]
#[serde(rename_all = "kebab-case")]
pub(super) enum Type {
	// Primitive types
	Null,
	Boolean,
	Int,
	Long,
	Float,
	Double,
	Bytes,
	String,
	// Complex types
	Array,
	Map,
	Record,
	Enum,
	Fixed,
}

#[derive(serde_derive::Deserialize)]
#[serde(rename_all = "camelCase")]
#[serde(bound = "'a: 'de, 'de: 'a")]
pub(super) struct SchemaNodeObject<'a> {
	/// Like the reference Java implementation, we won't allow the `type` field
	/// to be a nested `SchemaNodeObject`
	/// https://github.com/apache/avro/blob/06c8b5ddfa3540b466b144503b150e30bf8afc15/lang/java/avro/src/main/java/org/apache/avro/Schema.java#L1830
	/// If we did, it's unclear in node like this:
	/// `{"type":{"type": "fixed", "name":"foo"}, "logicalType":"decimal"}`
	/// whether the `foo` name would refer to the fixed type or the logical
	/// type.
	/// Because there's no reference implementation to decide this (because Java
	/// just rejects this), we'll be conservative here and reject it as well
	/// until this is *specified*.
	#[serde(rename = "type")]
	pub(super) type_: Type,
	pub(super) logical_type: Option<BorrowedCowIfPossible<'a>>,
	/// For named types
	pub(super) name: Option<BorrowedCowIfPossible<'a>>,
	/// For named types
	pub(super) namespace: Option<BorrowedCowIfPossible<'a>>,
	/// For record type
	pub(super) fields: Option<Vec<Field<'a>>>,
	/// For enum type
	pub(super) symbols: Option<Vec<BorrowedCowIfPossible<'a>>>,
	/// For array type
	pub(super) items: Option<SchemaNode<'a>>,
	/// For map type
	pub(super) values: Option<SchemaNode<'a>>,
	/// For fixed type
	pub(super) size: Option<usize>,
	/// For decimal logical type
	pub(super) precision: Option<usize>,
	/// For decimal logical type
	pub(super) scale: Option<u32>,
}

#[derive(serde_derive::Deserialize)]
#[serde(bound = "'a: 'de")]
pub(super) struct Field<'a> {
	#[serde(borrow)]
	pub(super) name: BorrowedCowIfPossible<'a>,
	#[serde(rename = "type")]
	pub(super) type_: SchemaNode<'a>,
}

#[derive(serde_derive::Deserialize)]
pub(super) struct BorrowedCowIfPossible<'a>(#[serde(borrow)] pub(crate) std::borrow::Cow<'a, str>);

impl<'de> Deserialize<'de> for SchemaNode<'de> {
	fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
	where
		D: Deserializer<'de>,
	{
		struct SchemaNodeVisitor<'de>(std::marker::PhantomData<&'de ()>);
		impl<'de> Visitor<'de> for SchemaNodeVisitor<'de> {
			type Value = SchemaNode<'de>;

			fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
				write!(
					formatter,
					"A string (type) or an object with a `type` field or an array (union)"
				)
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

			fn visit_seq<A>(self, seq: A) -> Result<Self::Value, A::Error>
			where
				A: SeqAccess<'de>,
			{
				// That's a union.
				Ok(SchemaNode::Union(Deserialize::deserialize(
					serde::de::value::SeqAccessDeserializer::new(seq),
				)?))
			}

			fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
			where
				E: Error,
			{
				// That's a type right away, or a ref.
				Ok(
					match Type::deserialize(value::StrDeserializer::<FailedDeserialization>::new(v))
					{
						Ok(type_) => SchemaNode::Type(type_),
						Err(FailedDeserialization) => SchemaNode::Ref(v.to_owned().into()),
					},
				)
			}

			fn visit_borrowed_str<E>(self, v: &'de str) -> Result<Self::Value, E>
			where
				E: Error,
			{
				// That's a type right away, or a ref.
				Ok(
					match Type::deserialize(value::StrDeserializer::<FailedDeserialization>::new(v))
					{
						Ok(type_) => SchemaNode::Type(type_),
						Err(FailedDeserialization) => SchemaNode::Ref(v.into()),
					},
				)
			}

			fn visit_string<E>(self, v: String) -> Result<Self::Value, E>
			where
				E: Error,
			{
				// That's a type right away, or a ref.
				Ok(
					match Type::deserialize(value::StrDeserializer::<FailedDeserialization>::new(
						v.as_str(),
					)) {
						Ok(type_) => SchemaNode::Type(type_),
						Err(FailedDeserialization) => SchemaNode::Ref(v.into()),
					},
				)
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
