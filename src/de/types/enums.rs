use super::*;

pub(in super::super) fn read_enum<'de, R, V>(
	state: &mut DeserializerState<R>,
	symbols: &[String],
	visitor: V,
) -> Result<V::Value, DeError>
where
	R: Read<'de>,
	V: Visitor<'de>,
{
	let enum_discriminant = read_discriminant(state)?;
	match symbols.get(enum_discriminant) {
		None => Err(Error::custom("Could not find enum discriminant in schema")),
		Some(variant_schema) => Ok(visitor.visit_enum(value::StrDeserializer::new(variant_schema))?),
	}
}
