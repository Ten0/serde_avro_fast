use super::*;

use std::num::NonZeroUsize;

fn read_block_len<'de, R>(state: &mut DeserializerState<R>) -> Result<Option<NonZeroUsize>, DeError>
where
	R: ReadSlice<'de>,
{
	let len: i64 = state.read_varint()?;
	let res;
	if len < 0 {
		// res = -len, properly handling i64::MIN
		res = u64::from_ne_bytes(len.to_ne_bytes()).wrapping_neg();
		// Drop the number of bytes in the block to properly advance the reader
		// Since we don't use that value, decode as u64 instead of i64 (skip zigzag
		// decoding) TODO enable fast skipping when encountering
		// `deserialize_ignored_any`
		let _: u64 = state.read_varint()?;
	} else {
		res = len as u64;
	}
	res.try_into()
		.map_err(|e| DeError::custom(format_args!("Invalid array length in stream: {e}")))
		.map(NonZeroUsize::new)
}

pub(in super::super) struct BlockReader<'r, 's, R> {
	current_block_len: usize,
	n_read: usize,
	reader: &'r mut DeserializerState<'s, R>,
	allowed_depth: AllowedDepth,
}
impl<'r, 's, R> BlockReader<'r, 's, R> {
	pub(in super::super) fn new(
		reader: &'r mut DeserializerState<'s, R>,
		allowed_depth: AllowedDepth,
	) -> Self {
		Self {
			reader,
			current_block_len: 0,
			n_read: 0,
			allowed_depth,
		}
	}
	fn has_more<'de>(&mut self) -> Result<bool, DeError>
	where
		R: ReadSlice<'de>,
	{
		self.current_block_len = match self.current_block_len.checked_sub(1) {
			None => {
				let new_len = read_block_len(self.reader)?;
				match new_len {
					None => return Ok(false),
					Some(new_len) => {
						let l = new_len.get();
						let n_read = self.n_read.saturating_add(l);
						if n_read > self.reader.config.max_seq_size {
							return Err(DeError::new(
								"Exceeding max sequence size while deserializing",
							));
						}
						self.n_read = n_read;
						l - 1
					}
				}
			}
			Some(new_len) => new_len,
		};
		Ok(true)
	}
}

pub(in super::super) struct ArraySeqAccess<'r, 's, R> {
	pub(in super::super) block_reader: BlockReader<'r, 's, R>,
	pub(in super::super) elements_schema: &'s SchemaNode<'s>,
}
impl<'de, R: ReadSlice<'de>> SeqAccess<'de> for ArraySeqAccess<'_, '_, R> {
	type Error = DeError;

	fn next_element_seed<T>(&mut self, seed: T) -> Result<Option<T::Value>, Self::Error>
	where
		T: DeserializeSeed<'de>,
	{
		if !self.block_reader.has_more()? {
			return Ok(None);
		}
		Ok(Some(seed.deserialize(DatumDeserializer {
			schema_node: self.elements_schema,
			state: self.block_reader.reader,
			allowed_depth: self.block_reader.allowed_depth,
		})?))
	}
}

pub(in super::super) struct MapMapAccess<'r, 's, R> {
	pub(in super::super) block_reader: BlockReader<'r, 's, R>,
	pub(in super::super) elements_schema: &'s SchemaNode<'s>,
}
impl<'de, R: ReadSlice<'de>> MapAccess<'de> for MapMapAccess<'_, '_, R> {
	type Error = DeError;

	fn next_key_seed<T>(&mut self, seed: T) -> Result<Option<T::Value>, Self::Error>
	where
		T: DeserializeSeed<'de>,
	{
		if !self.block_reader.has_more()? {
			return Ok(None);
		}

		Ok(Some(seed.deserialize(StringDeserializer {
			reader: self.block_reader.reader,
		})?))
	}

	fn next_value_seed<V>(&mut self, seed: V) -> Result<V::Value, Self::Error>
	where
		V: DeserializeSeed<'de>,
	{
		seed.deserialize(DatumDeserializer {
			schema_node: self.elements_schema,
			state: self.block_reader.reader,
			allowed_depth: self.block_reader.allowed_depth,
		})
	}
}

struct StringDeserializer<'r, 's, R> {
	reader: &'r mut DeserializerState<'s, R>,
}
impl<'de, R: ReadSlice<'de>> Deserializer<'de> for StringDeserializer<'_, '_, R> {
	type Error = DeError;

	fn deserialize_any<V>(self, visitor: V) -> Result<V::Value, Self::Error>
	where
		V: Visitor<'de>,
	{
		read_length_delimited(self.reader, StringVisitor(visitor))
	}

	serde::forward_to_deserialize_any! {
		bool i8 i16 i32 i64 i128 u8 u16 u32 u64 u128 f32 f64 char str string
		bytes byte_buf option unit unit_struct newtype_struct seq tuple
		tuple_struct map struct enum identifier ignored_any
	}
}
