pub mod de;
pub mod schema;

pub use schema::Schema;

pub fn datum_from_slice<'a, T>(slice: &'a [u8], schema: &Schema) -> Result<T, de::DeError>
where
	T: serde::Deserialize<'a>,
{
	serde::Deserialize::deserialize(de::DeserializerState::from_slice(slice, &schema).deserializer())
}

pub fn datum_from_reader<R, T>(reader: R, schema: &Schema) -> Result<T, de::DeError>
where
	T: serde::de::DeserializeOwned,
	R: std::io::Read,
{
	serde::Deserialize::deserialize(de::DeserializerState::from_reader(reader, &schema).deserializer())
}
