use super::*;

pub(in super::super) fn read_enum_as_str<'de, R, V>(
	state: &mut DeserializerState<R>,
	symbols: &[String],
	visitor: V,
) -> Result<V::Value, DeError>
where
	R: ReadSlice<'de>,
	V: Visitor<'de>,
{
	let enum_discriminant = read_discriminant(state)?;
	match symbols.get(enum_discriminant) {
		None => Err(DeError::new("Could not find enum discriminant in schema")),
		Some(variant_schema) => Ok({
			// If we were to visit borrowed here we'd always have to tie the lifetime of the
			// serializer to the lifetime of the schema, which would otherwise be a bother
			// in a lot of cases.
			// In order to avoid the allocation here, it is consequently required that the
			// user defines an enum.
			visitor.visit_str(variant_schema)?
		}),
	}
}
