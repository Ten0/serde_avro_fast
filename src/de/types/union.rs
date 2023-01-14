use super::*;

pub(in super::super) fn read_union_discriminant<'de, 's, R>(
	state: &mut DeserializerState<'s, R>,
	union_schema: &'s UnionSchema,
) -> Result<&'s SchemaNode, DeError>
where
	R: Read<'de>,
{
	let union_discriminant: usize = read_discriminant(state)?;
	match union_schema.variants.get(union_discriminant) {
		None => Err(Error::custom("Could not find union discriminant in schema")),
		Some(variant_schema) => Ok(&state.schema[*variant_schema]),
	}
}
