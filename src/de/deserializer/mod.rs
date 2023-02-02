mod types;
mod unit_variant_enum_access;

use {types::*, unit_variant_enum_access::UnitVariantEnumAccess};

use super::*;

/// Can't be instantiated directly - has to be constructed from a
/// [`DeserializerState`]
pub struct DatumDeserializer<'r, 's, R> {
	pub(super) state: &'r mut DeserializerState<'s, R>,
	pub(super) schema_node: &'s SchemaNode<'s>,
}

impl<'de, R: ReadSlice<'de>> Deserializer<'de> for DatumDeserializer<'_, '_, R> {
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
			SchemaNode::Float => {
				visitor.visit_f32(f32::from_le_bytes(self.state.read_const_size_buf()?))
			}
			SchemaNode::Double => {
				visitor.visit_f64(f64::from_le_bytes(self.state.read_const_size_buf()?))
			}
			SchemaNode::Bytes => read_length_delimited(self.state, BytesVisitor(visitor)),
			SchemaNode::String => read_length_delimited(self.state, StringVisitor(visitor)),
			SchemaNode::Array(elements_schema) => visitor.visit_seq(ArraySeqAccess {
				elements_schema,
				block_reader: BlockReader::new(self.state),
			}),
			SchemaNode::Map(elements_schema) => visitor.visit_map(MapMapAccess {
				element_schema: elements_schema,
				block_reader: BlockReader::new(self.state),
			}),
			SchemaNode::Union(ref union) => DatumDeserializer {
				schema_node: read_union_discriminant(self.state, union)?,
				state: self.state,
			}
			.deserialize_any(visitor),
			SchemaNode::Record(ref record) => visitor.visit_map(RecordMapAccess {
				record_fields: record.fields.iter(),
				state: self.state,
			}),
			SchemaNode::Enum(ref enum_) => read_enum_as_str(self.state, &enum_.symbols, visitor),
			SchemaNode::Fixed(ref fixed) => {
				self.state.read_slice(fixed.size, BytesVisitor(visitor))
			}
			SchemaNode::Decimal(ref decimal) => read_decimal(
				self.state,
				decimal.scale,
				decimal.inner,
				VisitorHint::Str,
				visitor,
			),
			SchemaNode::Uuid => read_length_delimited(self.state, StringVisitor(visitor)),
			SchemaNode::Date => visitor.visit_i32(self.state.read_varint()?),
			SchemaNode::TimeMillis => visitor.visit_i32(self.state.read_varint()?),
			SchemaNode::TimeMicros => visitor.visit_i64(self.state.read_varint()?),
			SchemaNode::TimestampMillis => visitor.visit_i64(self.state.read_varint()?),
			SchemaNode::TimestampMicros => visitor.visit_i64(self.state.read_varint()?),
			SchemaNode::Duration => visitor.visit_map(DurationMapAndSeqAccess {
				duration_buf: &self.state.read_const_size_buf::<12>()?,
			}),
		}
	}

	serde::forward_to_deserialize_any! {
		bool i8 i16 i32 u8 u16 u32 f32 char
		unit unit_struct newtype_struct identifier
	}

	fn deserialize_u64<V>(self, visitor: V) -> Result<V::Value, Self::Error>
	where
		V: Visitor<'de>,
	{
		// Allow deserializing discriminants without making the string lookup for enums
		match *self.schema_node {
			SchemaNode::Enum(_) => {
				let discriminant: i64 = self.state.read_varint()?;
				visitor.visit_u64(discriminant.try_into().map_err(|e| {
					DeError::custom(format_args!("Got negative enum discriminant: {e}"))
				})?)
			}
			SchemaNode::Decimal(ref decimal) => read_decimal(
				self.state,
				decimal.scale,
				decimal.inner,
				VisitorHint::U64,
				visitor,
			),
			_ => self.deserialize_any(visitor),
		}
	}

	fn deserialize_i64<V>(self, visitor: V) -> Result<V::Value, Self::Error>
	where
		V: Visitor<'de>,
	{
		match *self.schema_node {
			SchemaNode::Long => visitor.visit_i64(self.state.read_varint()?),
			SchemaNode::Decimal(ref decimal) => read_decimal(
				self.state,
				decimal.scale,
				decimal.inner,
				VisitorHint::I64,
				visitor,
			),
			_ => self.deserialize_any(visitor),
		}
	}

	fn deserialize_u128<V>(self, visitor: V) -> Result<V::Value, Self::Error>
	where
		V: Visitor<'de>,
	{
		match *self.schema_node {
			SchemaNode::Decimal(ref decimal) => read_decimal(
				self.state,
				decimal.scale,
				decimal.inner,
				VisitorHint::U128,
				visitor,
			),
			_ => self.deserialize_any(visitor),
		}
	}

	fn deserialize_i128<V>(self, visitor: V) -> Result<V::Value, Self::Error>
	where
		V: Visitor<'de>,
	{
		match *self.schema_node {
			SchemaNode::Decimal(ref decimal) => read_decimal(
				self.state,
				decimal.scale,
				decimal.inner,
				VisitorHint::I128,
				visitor,
			),
			_ => self.deserialize_any(visitor),
		}
	}

	fn deserialize_f64<V>(self, visitor: V) -> Result<V::Value, Self::Error>
	where
		V: Visitor<'de>,
	{
		match *self.schema_node {
			SchemaNode::Double => {
				visitor.visit_f64(f64::from_le_bytes(self.state.read_const_size_buf()?))
			}
			SchemaNode::Decimal(ref decimal) => read_decimal(
				self.state,
				decimal.scale,
				decimal.inner,
				VisitorHint::F64,
				visitor,
			),
			_ => self.deserialize_any(visitor),
		}
	}

	fn deserialize_str<V>(self, visitor: V) -> Result<V::Value, Self::Error>
	where
		V: Visitor<'de>,
	{
		// If we get hinted on str, we may attempt to deserialize byte arrays as utf-8
		// encoded strings
		match *self.schema_node {
			SchemaNode::String => read_length_delimited(self.state, StringVisitor(visitor)),
			SchemaNode::Bytes => read_length_delimited(self.state, StringVisitor(visitor)),
			SchemaNode::Fixed(ref fixed) => {
				self.state.read_slice(fixed.size, StringVisitor(visitor))
			}
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
			SchemaNode::Union(union) => {
				let union_discriminant: usize = read_discriminant(self.state)?;
				match union
					.variants
					.get(union_discriminant)
					.map(|&schema_key| schema_key)
				{
					None => Err(DeError::new("Could not find union discriminant in schema")),
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
				elements_schema,
				block_reader: BlockReader::new(self.state),
			}),
			SchemaNode::Duration => visitor.visit_seq(DurationMapAndSeqAccess {
				duration_buf: &self.state.read_const_size_buf::<12>()?,
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
				elements_schema,
				block_reader: BlockReader::new(self.state),
			}),
			SchemaNode::Duration if len == 3 => visitor.visit_seq(DurationMapAndSeqAccess {
				duration_buf: &self.state.read_const_size_buf::<12>()?,
			}),
			_ => self.deserialize_any(visitor),
		}
	}

	fn deserialize_tuple_struct<V>(
		self,
		_: &'static str,
		len: usize,
		visitor: V,
	) -> Result<V::Value, Self::Error>
	where
		V: Visitor<'de>,
	{
		self.deserialize_tuple(len, visitor)
	}

	fn deserialize_map<V>(self, visitor: V) -> Result<V::Value, Self::Error>
	where
		V: Visitor<'de>,
	{
		// TODO [{key, value}] could be deserialized into a map
		self.deserialize_any(visitor)
	}

	fn deserialize_struct<V>(
		self,
		_: &'static str,
		_: &'static [&'static str],
		visitor: V,
	) -> Result<V::Value, Self::Error>
	where
		V: Visitor<'de>,
	{
		self.deserialize_map(visitor)
	}

	fn deserialize_enum<V>(
		self,
		_: &'static str,
		_: &'static [&'static str],
		visitor: V,
	) -> Result<V::Value, Self::Error>
	where
		V: Visitor<'de>,
	{
		match *self.schema_node {
			SchemaNode::Union(ref union) => visitor.visit_enum(UnionEnumAccess {
				state: self.state,
				union,
			}),
			_ => visitor.visit_enum(UnitVariantEnumAccess {
				state: self.state,
				schema_node: self.schema_node,
			}),
		}
	}

	fn deserialize_ignored_any<V>(self, visitor: V) -> Result<V::Value, Self::Error>
	where
		V: Visitor<'de>,
	{
		// TODO skip more efficiently using blocks size hints
		self.deserialize_any(visitor)
	}
}
