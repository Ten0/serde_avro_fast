mod error;
mod read;
mod types;

use {error::DeError, read::*, types::*};

use crate::Schema;

use serde::de::*;

pub struct DatumDeserializer<'s, 'r, R> {
	schema: &'s Schema,
	reader: &'r mut ReaderAndConfig<R>,
}
struct ReaderAndConfig<R> {
	reader: R,
	max_seq_size: usize,
}

impl<'de, R: Read<'de>> Deserializer<'de> for DatumDeserializer<'_, '_, R> {
	type Error = DeError;

	fn deserialize_any<V>(self, visitor: V) -> Result<V::Value, Self::Error>
	where
		V: Visitor<'de>,
	{
		match self.schema {
			Schema::Null => self.deserialize_unit(visitor),
			Schema::Boolean => read_bool(self.reader, visitor),
			Schema::Int => visitor.visit_i32(self.reader.read_varint()?),
			Schema::Long => visitor.visit_i64(self.reader.read_varint()?),
			Schema::Float => visitor.visit_f32(self.reader.read_const_size_buf(f32::from_le_bytes)?),
			Schema::Double => visitor.visit_f64(self.reader.read_const_size_buf(f64::from_le_bytes)?),
			Schema::Bytes => read_length_delimited(self.reader, BytesVisitor(visitor)),
			Schema::String => read_length_delimited(self.reader, StringVisitor(visitor)),
			Schema::Array(elements_schema) => visitor.visit_seq(ArraySeqAccess {
				element_schema: &**elements_schema,
				block_reader: BlockReader::new(self.reader),
			}),
			Schema::Map(elements_schema) => visitor.visit_map(MapSeqAccess {
				element_schema: &**elements_schema,
				block_reader: BlockReader::new(self.reader),
			}),
			Schema::Union(_) => todo!(),
			Schema::Record {
				name,
				aliases,
				doc,
				fields,
				lookup,
				attributes,
			} => todo!(),
			Schema::Enum {
				name,
				aliases,
				doc,
				symbols,
				attributes,
			} => todo!(),
			Schema::Fixed {
				name,
				aliases,
				doc,
				size,
				attributes,
			} => todo!(),
			Schema::Decimal {
				precision,
				scale,
				inner,
			} => todo!(),
			Schema::Uuid => todo!(),
			Schema::Date => todo!(),
			Schema::TimeMillis => todo!(),
			Schema::TimeMicros => todo!(),
			Schema::TimestampMillis => todo!(),
			Schema::TimestampMicros => todo!(),
			Schema::Duration => todo!(),
			Schema::Ref { name } => todo!(),
		}
	}

	serde::forward_to_deserialize_any! {
		bool i8 i16 i32 i64 i128 u8 u16 u32 u64 u128 f32 f64
		//char str string
		//bytes byte_buf option unit unit_struct newtype_struct seq tuple
		//tuple_struct map struct enum identifier ignored_any
	}

	fn deserialize_char<V>(self, visitor: V) -> Result<V::Value, Self::Error>
	where
		V: Visitor<'de>,
	{
		todo!()
	}

	fn deserialize_str<V>(self, visitor: V) -> Result<V::Value, Self::Error>
	where
		V: Visitor<'de>,
	{
		todo!()
	}

	fn deserialize_string<V>(self, visitor: V) -> Result<V::Value, Self::Error>
	where
		V: Visitor<'de>,
	{
		todo!()
	}

	fn deserialize_bytes<V>(self, visitor: V) -> Result<V::Value, Self::Error>
	where
		V: Visitor<'de>,
	{
		todo!()
	}

	fn deserialize_byte_buf<V>(self, visitor: V) -> Result<V::Value, Self::Error>
	where
		V: Visitor<'de>,
	{
		todo!()
	}

	fn deserialize_option<V>(self, visitor: V) -> Result<V::Value, Self::Error>
	where
		V: Visitor<'de>,
	{
		todo!()
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

impl<R> std::ops::Deref for ReaderAndConfig<R> {
	type Target = R;
	fn deref(&self) -> &Self::Target {
		&self.reader
	}
}

impl<R> std::ops::DerefMut for ReaderAndConfig<R> {
	fn deref_mut(&mut self) -> &mut Self::Target {
		&mut self.reader
	}
}
