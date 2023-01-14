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
			SchemaNode::Map(elements_schema) => visitor.visit_map(MapMapAccess {
				element_schema: &self.state.schema[elements_schema],
				block_reader: BlockReader::new(self.state),
			}),
			SchemaNode::Union(ref union_schema) => DatumDeserializer {
				schema_node: read_union_discriminant(self.state, union_schema)?,
				state: self.state,
			}
			.deserialize_any(visitor),
			SchemaNode::Record(ref record_schema) => visitor.visit_map(RecordMapAccess {
				record_fields: record_schema.fields.iter(),
				state: self.state,
			}),
			SchemaNode::Enum { ref symbols } => read_enum(self.state, symbols, visitor),
			SchemaNode::Fixed { size } => self.state.read_slice(size, BytesVisitor(visitor)),
			SchemaNode::Decimal {
				precision,
				scale,
				inner,
			} => todo!(),
			SchemaNode::Uuid => read_length_delimited(self.state, StringVisitor(visitor)),
			SchemaNode::Date => visitor.visit_i32(self.state.read_varint()?),
			SchemaNode::TimeMillis => visitor.visit_i32(self.state.read_varint()?),
			SchemaNode::TimeMicros => visitor.visit_i64(self.state.read_varint()?),
			SchemaNode::TimestampMillis => visitor.visit_i64(self.state.read_varint()?),
			SchemaNode::TimestampMicros => visitor.visit_i64(self.state.read_varint()?),
			SchemaNode::Duration => visitor.visit_map(DurationMapAndSeqAccess {
				duration_buf: &self.state.read_const_size_buf::<_, 12>(std::convert::identity)?,
			}),
		}
	}

	serde::forward_to_deserialize_any! {
		bool i8 i16 i32 i64 i128 u8 u16 u32 u64 u128 f32 f64 char
		unit unit_struct newtype_struct
		//tuple_struct map struct enum identifier ignored_any
	}

	fn deserialize_str<V>(self, visitor: V) -> Result<V::Value, Self::Error>
	where
		V: Visitor<'de>,
	{
		// If we get hinted on str, we may attempt to deserialize byte arrays as utf-8 encoded strings
		match *self.schema_node {
			SchemaNode::String => read_length_delimited(self.state, StringVisitor(visitor)),
			SchemaNode::Bytes => read_length_delimited(self.state, StringVisitor(visitor)),
			SchemaNode::Fixed { size } => self.state.read_slice(size, StringVisitor(visitor)),
			_ => self.deserialize_any(visitor),
		}
	}

	fn deserialize_string<V>(self, visitor: V) -> Result<V::Value, Self::Error>
	where
		V: Visitor<'de>,
	{
		self.deserialize_str(visitor)
	}

	fn deserialize_bytes<V>(self, visitor: V) -> Result<V::Value, Self::Error>
	where
		V: Visitor<'de>,
	{
		match *self.schema_node {
			SchemaNode::Bytes => read_length_delimited(self.state, BytesVisitor(visitor)),
			SchemaNode::Duration => self.state.read_slice(12, BytesVisitor(visitor)),
			_ => self.deserialize_any(visitor),
		}
	}

	fn deserialize_byte_buf<V>(self, visitor: V) -> Result<V::Value, Self::Error>
	where
		V: Visitor<'de>,
	{
		self.deserialize_bytes(visitor)
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

	fn deserialize_seq<V>(self, visitor: V) -> Result<V::Value, Self::Error>
	where
		V: Visitor<'de>,
	{
		// TODO deserialize map as [(key,value)]
		match *self.schema_node {
			SchemaNode::Array(elements_schema) => visitor.visit_seq(ArraySeqAccess {
				element_schema: &self.state.schema[elements_schema],
				block_reader: BlockReader::new(self.state),
			}),
			SchemaNode::Duration => visitor.visit_seq(DurationMapAndSeqAccess {
				duration_buf: &self.state.read_const_size_buf::<_, 12>(std::convert::identity)?,
			}),
			_ => self.deserialize_any(visitor),
		}
	}

	fn deserialize_tuple<V>(self, len: usize, visitor: V) -> Result<V::Value, Self::Error>
	where
		V: Visitor<'de>,
	{
		// Allows deserializing Duration as (u32, u32, u32)
		match *self.schema_node {
			SchemaNode::Array(elements_schema) => visitor.visit_seq(ArraySeqAccess {
				element_schema: &self.state.schema[elements_schema],
				block_reader: BlockReader::new(self.state),
			}),
			SchemaNode::Duration if len == 3 => visitor.visit_seq(DurationMapAndSeqAccess {
				duration_buf: &self.state.read_const_size_buf::<_, 12>(std::convert::identity)?,
			}),
			_ => self.deserialize_any(visitor),
		}
	}

	fn deserialize_tuple_struct<V>(self, _: &'static str, len: usize, visitor: V) -> Result<V::Value, Self::Error>
	where
		V: Visitor<'de>,
	{
		self.deserialize_tuple(len, visitor)
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
		// TODO skip more efficiently using blocks size hints
		self.deserialize_any(visitor)
	}
}
