use super::*;

pub(in super::super) fn read_bool<'de, R, V>(reader: &mut ReaderAndConfig<R>, visitor: V) -> Result<V::Value, DeError>
where
	R: Read<'de>,
	V: Visitor<'de>,
{
	visitor.visit_bool(reader.read_slice(1, |s: &[u8]| match s[0] {
		0 => Ok(false),
		1 => Ok(true),
		other => Err(DeError::custom(format_args!(
			"Invalid byte value when deserializing boolean: {:?}",
			other
		))),
	})?)
}
