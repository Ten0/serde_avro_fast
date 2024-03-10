use super::*;

pub(in super::super) fn read_union_discriminant<'de, 's, R>(
	state: &mut DeserializerState<'s, R>,
	union: &'s Union<'s>,
) -> Result<&'s SchemaNode<'s>, DeError>
where
	R: ReadSlice<'de>,
{
	let union_discriminant: usize = read_discriminant(state)?;
	match union.variants.get(union_discriminant) {
		None => Err(DeError::new("Could not find union discriminant in schema")),
		Some(&variant_schema) => Ok(variant_schema.as_ref()),
	}
}

pub(in super::super) struct SchemaTypeNameEnumAccess<'r, 's, R> {
	pub(in super::super) state: &'r mut DeserializerState<'s, R>,
	pub(in super::super) variant_schema: &'s SchemaNode<'s>,
	pub(in super::super) allowed_depth: AllowedDepth,
}

impl<'de, 'r, 's, R> EnumAccess<'de> for SchemaTypeNameEnumAccess<'r, 's, R>
where
	R: ReadSlice<'de>,
{
	type Error = DeError;
	type Variant = SchemaTypeNameVariantAccess<'r, 's, R>;

	fn variant_seed<V>(self, seed: V) -> Result<(V::Value, Self::Variant), Self::Error>
	where
		V: DeserializeSeed<'de>,
	{
		seed.deserialize(SchemaTypeNameDeserializer {
			schema_node: self.variant_schema,
		})
		.map(|value| {
			(
				value,
				SchemaTypeNameVariantAccess {
					datum_deserializer: DatumDeserializer {
						state: self.state,
						schema_node: self.variant_schema,
						allowed_depth: self.allowed_depth,
					},
				},
			)
		})
	}
}

/// Implemented this way instead of using serde's StrDeserializer to help it get
/// inlined so that const propagation will get rid of the string matching
struct SchemaTypeNameDeserializer<'s> {
	schema_node: &'s SchemaNode<'s>,
}

impl<'de> Deserializer<'de> for SchemaTypeNameDeserializer<'_> {
	type Error = DeError;

	fn deserialize_any<V>(self, visitor: V) -> Result<V::Value, Self::Error>
	where
		V: Visitor<'de>,
	{
		visitor.visit_str(match self.schema_node {
			SchemaNode::Null => "Null",
			SchemaNode::Boolean => "Boolean",
			SchemaNode::Int => "Int",
			SchemaNode::Long => "Long",
			SchemaNode::Float => "Float",
			SchemaNode::Double => "Double",
			SchemaNode::Bytes => "Bytes",
			SchemaNode::String => "String",
			SchemaNode::Array(_) => "Array",
			SchemaNode::Map(_) => "Map",
			SchemaNode::Union(_) => {
				// Supposedly disallowed but easy to support if we get to this point
				"Union"
			}
			SchemaNode::Record(record) => record.name.fully_qualified_name(),
			SchemaNode::Enum(enum_) => enum_.name.fully_qualified_name(),
			SchemaNode::Fixed(fixed) => fixed.name.fully_qualified_name(),
			SchemaNode::Decimal(Decimal {
				repr: DecimalRepr::Fixed(fixed),
				..
			}) => fixed.name.fully_qualified_name(),
			SchemaNode::Decimal(Decimal {
				repr: DecimalRepr::Bytes,
				..
			}) => "Decimal",
			SchemaNode::Uuid => "Uuid",
			SchemaNode::Date => "Date",
			SchemaNode::TimeMillis => "TimeMillis",
			SchemaNode::TimeMicros => "TimeMicros",
			SchemaNode::TimestampMillis => "TimestampMillis",
			SchemaNode::TimestampMicros => "TimestampMicros",
			SchemaNode::Duration => "Duration",
		})
	}

	serde::forward_to_deserialize_any! {
		bool i8 i16 i32 i64 i128 u8 u16 u32 u64 u128 f32 f64 char str string
		bytes byte_buf option unit unit_struct newtype_struct seq tuple
		tuple_struct map struct enum identifier ignored_any
	}
}

pub struct SchemaTypeNameVariantAccess<'r, 's, R> {
	datum_deserializer: DatumDeserializer<'r, 's, R>,
}

impl<'de, R> VariantAccess<'de> for SchemaTypeNameVariantAccess<'_, '_, R>
where
	R: ReadSlice<'de>,
{
	type Error = DeError;

	fn unit_variant(self) -> Result<(), Self::Error> {
		self.datum_deserializer
			.deserialize_ignored_any(IgnoredAny)?;
		Ok(())
	}

	fn newtype_variant_seed<T>(self, seed: T) -> Result<T::Value, Self::Error>
	where
		T: DeserializeSeed<'de>,
	{
		seed.deserialize(self.datum_deserializer)
	}

	fn tuple_variant<V>(self, len: usize, visitor: V) -> Result<V::Value, Self::Error>
	where
		V: Visitor<'de>,
	{
		self.datum_deserializer.deserialize_tuple(len, visitor)
	}

	fn struct_variant<V>(
		self,
		_fields: &'static [&'static str],
		visitor: V,
	) -> Result<V::Value, Self::Error>
	where
		V: Visitor<'de>,
	{
		self.datum_deserializer.deserialize_map(visitor)
	}
}

pub(in super::super) struct FavorSchemaTypeNameIfEnumHintDatumDeserializer<'r, 's, R> {
	pub(in super::super) inner: DatumDeserializer<'r, 's, R>,
}

macro_rules! forward_to_inner_deserializer {
	($($f: ident($($arg: ident: $ty: ty),*))*) => {
		$(
			#[inline]
			fn $f<V>(self, $($arg: $ty,)* visitor: V) -> Result<V::Value, Self::Error>
			where
				V: Visitor<'de>,
			{
				self.inner.$f($($arg,)* visitor)
			}
		)*
	};
}

impl<'de, R: ReadSlice<'de>> Deserializer<'de>
	for FavorSchemaTypeNameIfEnumHintDatumDeserializer<'_, '_, R>
{
	type Error = DeError;

	fn deserialize_enum<V>(
		self,
		_name: &'static str,
		_variants: &'static [&'static str],
		visitor: V,
	) -> Result<V::Value, Self::Error>
	where
		V: Visitor<'de>,
	{
		visitor.visit_enum(SchemaTypeNameEnumAccess {
			state: self.inner.state,
			variant_schema: self.inner.schema_node,
			allowed_depth: self.inner.allowed_depth,
		})
	}

	forward_to_inner_deserializer! {
		deserialize_any()
		deserialize_bool()
		deserialize_i8()
		deserialize_i16()
		deserialize_i32()
		deserialize_i64()
		deserialize_i128()
		deserialize_u8()
		deserialize_u16()
		deserialize_u32()
		deserialize_u64()
		deserialize_u128()
		deserialize_f32()
		deserialize_f64()
		deserialize_char()
		deserialize_str()
		deserialize_string()
		deserialize_bytes()
		deserialize_byte_buf()
		deserialize_option()
		deserialize_unit()
		deserialize_unit_struct(name: &'static str)
		deserialize_newtype_struct(name: &'static str)
		deserialize_seq()
		deserialize_tuple(len: usize)
		deserialize_tuple_struct(name: &'static str, len: usize)
		deserialize_map()
		deserialize_struct(name: &'static str, fields: &'static [&'static str])
		deserialize_identifier()
		deserialize_ignored_any()
	}

	#[inline]
	fn is_human_readable(&self) -> bool {
		self.inner.is_human_readable()
	}
}
