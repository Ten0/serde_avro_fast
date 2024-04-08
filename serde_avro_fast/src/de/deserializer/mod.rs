mod allowed_depth;
mod types;
mod unit_variant_enum_access;

use {types::*, unit_variant_enum_access::UnitVariantEnumAccess};

pub(crate) use allowed_depth::AllowedDepth;

use super::*;

/// Can't be instantiated directly - has to be constructed from a
/// [`DeserializerState`]
pub struct DatumDeserializer<'r, 's, R> {
	pub(super) state: &'r mut DeserializerState<'s, R>,
	pub(super) schema_node: &'s SchemaNode<'s>,
	pub(super) allowed_depth: AllowedDepth,
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
				elements_schema: elements_schema.as_ref(),
				block_reader: BlockReader::new(self.state, self.allowed_depth.dec()?),
			}),
			SchemaNode::Map(elements_schema) => visitor.visit_map(MapMapAccess {
				elements_schema: elements_schema.as_ref(),
				block_reader: BlockReader::new(self.state, self.allowed_depth.dec()?),
			}),
			SchemaNode::Union(ref union) => Self {
				schema_node: read_union_discriminant(self.state, union)?,
				state: self.state,
				allowed_depth: self.allowed_depth.dec()?,
			}
			.deserialize_any(visitor),
			SchemaNode::Record(ref record) => {
				// NB: infinite recursion is prevented here by the fact we prevent constructing
				// a schema that contains a record that always ends up containing itself
				visitor.visit_map(RecordMapAccess {
					record_fields: record.fields.iter(),
					state: self.state,
					allowed_depth: self.allowed_depth.dec()?,
				})
			}
			SchemaNode::Enum(ref enum_) => read_enum_as_str(self.state, &enum_.symbols, visitor),
			SchemaNode::Fixed(ref fixed) => {
				self.state.read_slice(fixed.size, BytesVisitor(visitor))
			}
			SchemaNode::Decimal(ref decimal) => {
				read_decimal(self.state, decimal, VisitorHint::Str, visitor)
			}
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
		unit unit_struct newtype_struct
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
			SchemaNode::Decimal(ref decimal) => {
				read_decimal(self.state, decimal, VisitorHint::U64, visitor)
			}
			_ => self.deserialize_any(visitor),
		}
	}

	fn deserialize_i64<V>(self, visitor: V) -> Result<V::Value, Self::Error>
	where
		V: Visitor<'de>,
	{
		match *self.schema_node {
			SchemaNode::Long => visitor.visit_i64(self.state.read_varint()?),
			SchemaNode::Decimal(ref decimal) => {
				read_decimal(self.state, decimal, VisitorHint::I64, visitor)
			}
			_ => self.deserialize_any(visitor),
		}
	}

	fn deserialize_u128<V>(self, visitor: V) -> Result<V::Value, Self::Error>
	where
		V: Visitor<'de>,
	{
		match *self.schema_node {
			SchemaNode::Decimal(ref decimal) => {
				read_decimal(self.state, decimal, VisitorHint::U128, visitor)
			}
			_ => self.deserialize_any(visitor),
		}
	}

	fn deserialize_i128<V>(self, visitor: V) -> Result<V::Value, Self::Error>
	where
		V: Visitor<'de>,
	{
		match *self.schema_node {
			SchemaNode::Decimal(ref decimal) => {
				read_decimal(self.state, decimal, VisitorHint::I128, visitor)
			}
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
			SchemaNode::Decimal(ref decimal) => {
				read_decimal(self.state, decimal, VisitorHint::F64, visitor)
			}
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
					.map(|&schema_key| schema_key.as_ref())
				{
					None => Err(DeError::new("Could not find union discriminant in schema")),
					Some(SchemaNode::Null) => visitor.visit_none(),
					Some(variant_schema)
						if union.variants.len() == 2
							&& matches!(
								*union.variants[1 - union_discriminant],
								SchemaNode::Null
							) =>
					{
						visitor.visit_some(DatumDeserializer {
							state: self.state,
							schema_node: variant_schema,
							allowed_depth: self.allowed_depth.dec()?,
						})
					}
					Some(variant_schema) => {
						visitor.visit_some(FavorSchemaTypeNameIfEnumHintDatumDeserializer {
							inner: DatumDeserializer {
								state: self.state,
								schema_node: variant_schema,
								allowed_depth: self.allowed_depth.dec()?,
							},
						})
					}
				}
			}
			_ => visitor.visit_some(self),
		}
	}

	fn deserialize_seq<V>(self, visitor: V) -> Result<V::Value, Self::Error>
	where
		V: Visitor<'de>,
	{
		// TODO deserialize map as [(key,value)]
		// Until then, this can be worked around using the `serde-tuple-vec-map` crate
		match *self.schema_node {
			SchemaNode::Array(elements_schema) => visitor.visit_seq(ArraySeqAccess {
				elements_schema: elements_schema.as_ref(),
				block_reader: BlockReader::new(self.state, self.allowed_depth.dec()?),
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
				elements_schema: elements_schema.as_ref(),
				block_reader: BlockReader::new(self.state, self.allowed_depth.dec()?),
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
			SchemaNode::Union(ref union) => visitor.visit_enum(SchemaTypeNameEnumAccess {
				variant_schema: read_union_discriminant(self.state, union)?,
				state: self.state,
				allowed_depth: self.allowed_depth.dec()?,
			}),
			ref possible_unit_variant_identifier @ (SchemaNode::Int
			| SchemaNode::Long
			| SchemaNode::Bytes
			| SchemaNode::String
			| SchemaNode::Enum(_)
			| SchemaNode::Fixed(_)) => visitor.visit_enum(UnitVariantEnumAccess {
				state: self.state,
				schema_node: possible_unit_variant_identifier,
				allowed_depth: self.allowed_depth.dec()?,
			}),
			ref not_unit_variant_identifier @ (SchemaNode::Null
			| SchemaNode::Boolean
			| SchemaNode::Float
			| SchemaNode::Double
			| SchemaNode::Array(_)
			| SchemaNode::Map(_)
			| SchemaNode::Record(_)
			| SchemaNode::Decimal(_)
			| SchemaNode::Uuid
			| SchemaNode::Date
			| SchemaNode::TimeMillis
			| SchemaNode::TimeMicros
			| SchemaNode::TimestampMillis
			| SchemaNode::TimestampMicros
			| SchemaNode::Duration) => visitor.visit_enum(SchemaTypeNameEnumAccess {
				state: self.state,
				variant_schema: not_unit_variant_identifier,
				allowed_depth: self.allowed_depth.dec()?,
			}),
		}
	}

	fn deserialize_identifier<V>(self, visitor: V) -> Result<V::Value, Self::Error>
	where
		V: Visitor<'de>,
	{
		match *self.schema_node {
			SchemaNode::Int => visitor.visit_u64({
				let val: i32 = self.state.read_varint()?;
				val.try_into()
					.map_err(|_| DeError::new("Failed to convert i32 to u64 for enum identifier"))?
			}),
			SchemaNode::Long => visitor.visit_u64({
				let val: i64 = self.state.read_varint()?;
				val.try_into()
					.map_err(|_| DeError::new("Failed to convert i64 to u64 for enum identifier"))?
			}),
			_ => self.deserialize_any(visitor),
		}
	}

	fn deserialize_ignored_any<V>(self, visitor: V) -> Result<V::Value, Self::Error>
	where
		V: Visitor<'de>,
	{
		// The main thing we can skip here for performance is utf8 decoding of strings.
		// However we still need to drive the deserializer mostly normally to properly
		// advance the reader.

		// TODO skip more efficiently using blocks size hints
		// https://stackoverflow.com/a/42247224/3799609

		// Ideally this would also specialize if we have Seek on our generic reader but
		// we don't have specialization

		match *self.schema_node {
			SchemaNode::String => read_length_delimited(self.state, BytesVisitor(visitor)),
			_ => self.deserialize_any(visitor),
		}
	}
}
