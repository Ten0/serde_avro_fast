use super::*;

pub(super) struct UnitVariantEnumAccess<'r, 's, R> {
	pub(super) state: &'r mut DeserializerState<'s, R>,
	pub(super) schema_node: &'s SchemaNode<'s>,
	pub(super) allowed_depth: AllowedDepth,
}

impl<'de, R: ReadSlice<'de>> EnumAccess<'de> for UnitVariantEnumAccess<'_, '_, R>
where
	R: ReadSlice<'de>,
{
	type Error = DeError;
	type Variant = private::UnitOnly;

	fn variant_seed<V>(self, seed: V) -> Result<(V::Value, Self::Variant), Self::Error>
	where
		V: DeserializeSeed<'de>,
	{
		seed.deserialize(DatumDeserializer {
			state: self.state,
			schema_node: self.schema_node,
			allowed_depth: self.allowed_depth,
		})
		.map(|value| (value, private::UnitOnly))
	}
}

// Inspired from serde's:

mod private {
	use super::*;

	pub(in super::super) struct UnitOnly;

	impl<'de> VariantAccess<'de> for UnitOnly {
		type Error = DeError;

		fn unit_variant(self) -> Result<(), Self::Error> {
			Ok(())
		}

		fn newtype_variant_seed<T>(self, _seed: T) -> Result<T::Value, Self::Error>
		where
			T: DeserializeSeed<'de>,
		{
			Err(Error::invalid_type(
				Unexpected::UnitVariant,
				&"newtype variant",
			))
		}

		fn tuple_variant<V>(self, _len: usize, _visitor: V) -> Result<V::Value, Self::Error>
		where
			V: Visitor<'de>,
		{
			Err(Error::invalid_type(
				Unexpected::UnitVariant,
				&"tuple variant",
			))
		}

		fn struct_variant<V>(
			self,
			_fields: &'static [&'static str],
			_visitor: V,
		) -> Result<V::Value, Self::Error>
		where
			V: Visitor<'de>,
		{
			Err(Error::invalid_type(
				Unexpected::UnitVariant,
				&"struct variant",
			))
		}
	}
}
