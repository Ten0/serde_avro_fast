use super::*;

pub(in super::super) fn read_discriminant<'de, R>(
	state: &mut DeserializerState<R>,
) -> Result<usize, DeError>
where
	R: ReadSlice<'de>,
{
	let union_discriminant: i64 = state.read_varint()?;
	union_discriminant
		.try_into()
		.map_err(|e| Error::custom(format_args!("Discriminant is too large in schema: {e}")))
}
