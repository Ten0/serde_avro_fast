use super::*;

pub(in super::super) struct RecordMapAccess<'r, 's, R> {
	pub(in super::super) state: &'r mut DeserializerState<'s, R>,
	pub(in super::super) record_fields: std::slice::Iter<'s, RecordField<'s>>,
	pub(in super::super) allowed_depth: AllowedDepth,
}
impl<'de, R: ReadSlice<'de>> MapAccess<'de> for RecordMapAccess<'_, '_, R> {
	type Error = DeError;

	fn next_key_seed<T>(&mut self, seed: T) -> Result<Option<T::Value>, Self::Error>
	where
		T: DeserializeSeed<'de>,
	{
		Ok(match self.record_fields.as_slice().first() {
			None => None,
			Some(field) => Some(seed.deserialize(value::StrDeserializer::new(&field.name))?),
		})
	}

	fn next_value_seed<V>(&mut self, seed: V) -> Result<V::Value, Self::Error>
	where
		V: DeserializeSeed<'de>,
	{
		seed.deserialize(DatumDeserializer {
			schema_node: self
				.record_fields
				.next()
				.expect("Called next_value without seed returning Some before")
				.schema
				.as_ref(),
			state: self.state,
			allowed_depth: self.allowed_depth,
		})
	}
}
