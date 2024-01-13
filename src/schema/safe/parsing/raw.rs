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
		let mut out = SchemaNode {
			type_: None,
			logical_type: None,
			name: None,
			namespace: None,
			fields: None,
			symbols: None,
			items: None,
			values: None,
			size: None,
		};
		struct SchemaNodeVisitor<'n, 'a>(&'n mut SchemaNode<'a>);
		impl<'n, 'de> Visitor<'de> for SchemaNodeVisitor<'n, 'de> {
			type Value = ();

			fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
				write!(
					formatter,
					"A borrowed &str or an object with a `type` field"
				)
			}

			fn visit_borrowed_str<E>(self, v: &'de str) -> Result<Self::Value, E>
			where
				E: Error,
			{
				self.0.type_ = Some(v);
				Ok(())
			}

			/*fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
			where
				E: Error,
			{
				// That's a type right away
				self.0.type_ = Some(Type::deserialize(value::StrDeserializer::new(v))?);
				Ok(())
			}*/
		}
		deserializer.deserialize_any(SchemaNodeVisitor(&mut out))?;
		Ok(out)
	}
}
