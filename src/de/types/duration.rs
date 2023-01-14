use super::*;

pub(in super::super) struct DurationMapAndSeqAccess<'d> {
	pub(in super::super) duration_buf: &'d [u8],
}
impl<'de> MapAccess<'de> for DurationMapAndSeqAccess<'_> {
	type Error = DeError;

	fn next_key_seed<T>(&mut self, seed: T) -> Result<Option<T::Value>, Self::Error>
	where
		T: DeserializeSeed<'de>,
	{
		seed.deserialize(match self.duration_buf.len() {
			0 => return Ok(None),
			12 => DurationFieldNameDeserializer::Months,
			8 => DurationFieldNameDeserializer::Days,
			4 => DurationFieldNameDeserializer::Milliseconds,
			_ => unreachable!("We are building this with 12 and pulling 4 by 4"),
		})
		.map(Some)
	}

	fn next_value_seed<V>(&mut self, seed: V) -> Result<V::Value, Self::Error>
	where
		V: DeserializeSeed<'de>,
	{
		let (curr, rest) = self.duration_buf.split_at(4);
		self.duration_buf = rest;
		seed.deserialize(value::U32Deserializer::new(u32::from_le_bytes(
			curr.try_into().unwrap(),
		)))
	}
}

enum DurationFieldNameDeserializer {
	Months,
	Days,
	Milliseconds,
}

impl<'de> Deserializer<'de> for DurationFieldNameDeserializer {
	type Error = DeError;

	fn deserialize_any<V>(self, visitor: V) -> Result<V::Value, Self::Error>
	where
		V: Visitor<'de>,
	{
		match self {
			Self::Months => visitor.visit_str("months"),
			Self::Days => visitor.visit_str("days"),
			Self::Milliseconds => visitor.visit_str("milliseconds"),
		}
	}

	// in case someone wants to avoid the str lookup
	fn deserialize_u64<V>(self, visitor: V) -> Result<V::Value, Self::Error>
	where
		V: Visitor<'de>,
	{
		match self {
			Self::Months => visitor.visit_u64(0),
			Self::Days => visitor.visit_u64(1),
			Self::Milliseconds => visitor.visit_u64(2),
		}
	}

	serde::forward_to_deserialize_any! {
		bool i8 i16 i32 i64 i128 u8 u16 u32 u128 f32 f64 char str string
		bytes byte_buf option unit unit_struct newtype_struct seq tuple
		tuple_struct map struct enum identifier ignored_any
	}
}

impl<'de> SeqAccess<'de> for DurationMapAndSeqAccess<'_> {
	type Error = DeError;

	fn next_element_seed<V>(&mut self, seed: V) -> Result<Option<V::Value>, Self::Error>
	where
		V: DeserializeSeed<'de>,
	{
		if self.duration_buf.len() >= 4 {
			let (curr, rest) = self.duration_buf.split_at(4);
			self.duration_buf = rest;
			seed.deserialize(value::U32Deserializer::new(u32::from_le_bytes(
				curr.try_into().unwrap(),
			)))
			.map(Some)
		} else {
			Ok(None)
		}
	}

	fn size_hint(&self) -> Option<usize> {
		Some(self.duration_buf.len() / 4)
	}
}
