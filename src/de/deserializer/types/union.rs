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
		Some(&variant_schema) => Ok(variant_schema),
	}
}

pub(in super::super) struct UnionEnumAccess<'r, 's, R> {
	pub(in super::super) state: &'r mut DeserializerState<'s, R>,
	pub(in super::super) union: &'s Union<'s>,
}

impl<'r, 's, 'de, R: ReadSlice<'de>> EnumAccess<'de> for UnionEnumAccess<'r, 's, R>
where
	R: ReadSlice<'de>,
{
	type Error = DeError;
	type Variant = UnionAsEnumVariantAccess<'r, 's, R>;

	fn variant_seed<V>(self, seed: V) -> Result<(V::Value, Self::Variant), Self::Error>
	where
		V: DeserializeSeed<'de>,
	{
		let schema_node = read_union_discriminant(self.state, self.union)?;
		seed.deserialize(SchemaNameDeserializer { schema_node })
			.map(|value| {
				(
					value,
					UnionAsEnumVariantAccess {
						datum_deserializer: DatumDeserializer {
							state: self.state,
							schema_node,
						},
					},
				)
			})
	}
}

/// Implemented this way instead of using serde's StrDeserializer to help it get
/// inlined so that const propagation will get rid of the string matching
struct SchemaNameDeserializer<'s> {
	schema_node: &'s SchemaNode<'s>,
}

impl<'de> Deserializer<'de> for SchemaNameDeserializer<'_> {
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
			SchemaNode::Decimal(_) => "Decimal",
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

pub(in super::super) struct UnionAsEnumVariantAccess<'r, 's, R> {
	datum_deserializer: DatumDeserializer<'r, 's, R>,
}

impl<'de, R> VariantAccess<'de> for UnionAsEnumVariantAccess<'_, '_, R>
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
