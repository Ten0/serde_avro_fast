use super::*;

pub struct DatumDeserializer<'r, 's, R> {
	pub(super) state: &'r mut DeserializerState<'s, R>,
	pub(super) schema_node: &'s SchemaNode,
}

impl<'de, R: Read<'de>> Deserializer<'de> for DatumDeserializer<'_, '_, R> {
	type Error = DeError;

	fn deserialize_any<V>(self, visitor: V) -> Result<V::Value, Self::Error>
	where
		V: Visitor<'de>,
	{
		match *self.schema_node {
			SchemaNode::Null => visitor.visit_unit(),
			SchemaNode::Boolean => read_bool(self.state, visitor),
			SchemaNode::Int => visitor.visit_i32(self.state.read_varint()?),
			SchemaNode::Long => visitor.visit_i64(self.state.read_varint()?),
			SchemaNode::Float => visitor.visit_f32(self.state.read_const_size_buf(f32::from_le_bytes)?),
			SchemaNode::Double => visitor.visit_f64(self.state.read_const_size_buf(f64::from_le_bytes)?),
			SchemaNode::Bytes => read_length_delimited(self.state, BytesVisitor(visitor)),
			SchemaNode::String => read_length_delimited(self.state, StringVisitor(visitor)),
			SchemaNode::Array(elements_schema) => visitor.visit_seq(ArraySeqAccess {
				element_schema: &self.state.schema[elements_schema],
				block_reader: BlockReader::new(self.state),
			}),
			SchemaNode::Map(elements_schema) => visitor.visit_map(MapSeqAccess {
				element_schema: &self.state.schema[elements_schema],
				block_reader: BlockReader::new(self.state),
			}),
			SchemaNode::Union(ref union_schema) => DatumDeserializer {
				schema_node: read_union_discriminant(self.state, union_schema)?,
				state: self.state,
			}
			.deserialize_any(visitor),
			SchemaNode::Record(ref record) => todo!(),
			SchemaNode::Enum { ref symbols } => todo!(),
			SchemaNode::Fixed { size } => todo!(),
			SchemaNode::Decimal {
				precision,
				scale,
				inner,
			} => todo!(),
			SchemaNode::Uuid => todo!(),
			SchemaNode::Date => todo!(),
			SchemaNode::TimeMillis => todo!(),
			SchemaNode::TimeMicros => todo!(),
			SchemaNode::TimestampMillis => todo!(),
			SchemaNode::TimestampMicros => todo!(),
			SchemaNode::Duration => todo!(),
		}
	}

	serde::forward_to_deserialize_any! {
		bool i8 i16 i32 i64 i128 u8 u16 u32 u64 u128 f32 f64 char // str string
		bytes byte_buf //option unit unit_struct newtype_struct seq tuple
		//tuple_struct map struct enum identifier ignored_any
	}

	fn deserialize_str<V>(self, visitor: V) -> Result<V::Value, Self::Error>
	where
		V: Visitor<'de>,
	{
		// If we get hinted on str, we may attempt to deserialize byte arrays as utf-8 encoded strings
		match self.schema_node {
			SchemaNode::String => read_length_delimited(self.state, StringVisitor(visitor)),
			SchemaNode::Bytes => read_length_delimited(self.state, StringVisitor(visitor)),
			_ => self.deserialize_any(visitor),
		}
	}

	fn deserialize_string<V>(self, visitor: V) -> Result<V::Value, Self::Error>
	where
		V: Visitor<'de>,
	{
		self.deserialize_str(visitor)
	}

	fn deserialize_option<V>(self, visitor: V) -> Result<V::Value, Self::Error>
	where
		V: Visitor<'de>,
	{
		match self.schema_node {
			SchemaNode::Null => visitor.visit_none(),
			SchemaNode::Union(union_schema) => {
				let union_discriminant: usize = read_discriminant(self.state)?;
				match union_schema
					.variants
					.get(union_discriminant)
					.map(|&schema_key| &self.state.schema[schema_key])
				{
					None => Err(Error::custom("Could not find union discriminant in schema")),
					Some(SchemaNode::Null) => visitor.visit_none(),
					Some(variant_schema) => visitor.visit_some(Self {
						state: self.state,
						schema_node: variant_schema,
					}),
				}
			}
			_ => self.deserialize_any(visitor),
		}
	}

	fn deserialize_unit<V>(self, visitor: V) -> Result<V::Value, Self::Error>
	where
		V: Visitor<'de>,
	{
		todo!()
	}

	fn deserialize_unit_struct<V>(self, name: &'static str, visitor: V) -> Result<V::Value, Self::Error>
	where
		V: Visitor<'de>,
	{
		todo!()
	}

	fn deserialize_newtype_struct<V>(self, name: &'static str, visitor: V) -> Result<V::Value, Self::Error>
	where
		V: Visitor<'de>,
	{
		todo!()
	}

	fn deserialize_seq<V>(self, visitor: V) -> Result<V::Value, Self::Error>
	where
		V: Visitor<'de>,
	{
		todo!()
	}

	fn deserialize_tuple<V>(self, len: usize, visitor: V) -> Result<V::Value, Self::Error>
	where
		V: Visitor<'de>,
	{
		todo!()
	}

	fn deserialize_tuple_struct<V>(self, name: &'static str, len: usize, visitor: V) -> Result<V::Value, Self::Error>
	where
		V: Visitor<'de>,
	{
		todo!()
	}

	fn deserialize_map<V>(self, visitor: V) -> Result<V::Value, Self::Error>
	where
		V: Visitor<'de>,
	{
		todo!()
	}

	fn deserialize_struct<V>(
		self,
		name: &'static str,
		fields: &'static [&'static str],
		visitor: V,
	) -> Result<V::Value, Self::Error>
	where
		V: Visitor<'de>,
	{
		todo!()
	}

	fn deserialize_enum<V>(
		self,
		name: &'static str,
		variants: &'static [&'static str],
		visitor: V,
	) -> Result<V::Value, Self::Error>
	where
		V: Visitor<'de>,
	{
		todo!()
	}

	fn deserialize_identifier<V>(self, visitor: V) -> Result<V::Value, Self::Error>
	where
		V: Visitor<'de>,
	{
		todo!()
	}

	fn deserialize_ignored_any<V>(self, visitor: V) -> Result<V::Value, Self::Error>
	where
		V: Visitor<'de>,
	{
		todo!()
	}
}
