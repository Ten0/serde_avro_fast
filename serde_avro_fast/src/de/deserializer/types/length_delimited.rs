use super::*;

pub(super) fn read_len<'de, R>(state: &mut DeserializerState<R>) -> Result<usize, DeError>
where
	R: ReadSlice<'de>,
{
	state
		.read_varint::<i64>()?
		.try_into()
		.map_err(|e| DeError::custom(format_args!("Invalid buffer length in stream: {e}")))
}

pub(in super::super) fn read_length_delimited<'de, R, BV>(
	state: &mut DeserializerState<R>,
	visitor: BV,
) -> Result<BV::Value, DeError>
where
	R: ReadSlice<'de>,
	BV: ReadVisitor<'de>,
{
	let len = read_len(state)?;
	state.read_slice(len, visitor)
}

pub(in super::super) struct BytesVisitor<V>(pub(in super::super) V);
impl<'de, V: Visitor<'de>> ReadVisitor<'de> for BytesVisitor<V> {
	type Value = V::Value;
	fn visit(self, bytes: &[u8]) -> Result<Self::Value, DeError> {
		self.0.visit_bytes(bytes)
	}
	fn visit_borrowed(self, bytes: &'de [u8]) -> Result<Self::Value, DeError> {
		self.0.visit_borrowed_bytes(bytes)
	}
}

pub(in super::super) struct StringVisitor<V>(pub(in super::super) V);
impl<'de, V: Visitor<'de>> ReadVisitor<'de> for StringVisitor<V> {
	type Value = V::Value;
	fn visit(self, bytes: &[u8]) -> Result<Self::Value, DeError> {
		self.0.visit_str(parse_str(bytes)?)
	}
	fn visit_borrowed(self, bytes: &'de [u8]) -> Result<Self::Value, DeError> {
		self.0.visit_borrowed_str(parse_str(bytes)?)
	}
}
fn parse_str(bytes: &[u8]) -> Result<&str, DeError> {
	std::str::from_utf8(bytes)
		.map_err(|e| DeError::custom(format_args!("String is not valid utf-8: {e}")))
}
