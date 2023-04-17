use super::*;

/// Deserialize from an avro
/// [single object encoding](https://avro.apache.org/docs/current/specification/#single-object-encoding) slice
///
/// This is zero-alloc.
///
/// Your structure may contain `&'a str`s that will end up pointing directly
/// into this slice for ideal performance.
pub fn from_single_object_slice<'a, T>(slice: &'a [u8], schema: &Schema) -> Result<T, de::DeError>
where
	T: serde::Deserialize<'a>,
{
	let header: &[u8; 10] = slice
		.get(0..10)
		.ok_or_else(|| de::DeError::new("Slice is too short for single object encoding header"))?
		.try_into()
		.unwrap();
	check_header(header, schema)?;
	from_datum_slice(&slice[10..], schema)
}

/// Deserialize from an avro
/// [single object encoding](https://avro.apache.org/docs/current/specification/#single-object-encoding) `impl Read`
///
/// If deserializing from a slice, a `Vec`, ... prefer using `from_datum_slice`,
/// as it will be more performant and enable you to borrow `&str`s from the
/// original slice.
pub fn from_single_object_reader<R, T>(mut reader: R, schema: &Schema) -> Result<T, de::DeError>
where
	T: serde::de::DeserializeOwned,
	R: std::io::Read,
{
	let mut header_buf = [0u8; 10];
	reader
		.read_exact(&mut header_buf)
		.map_err(de::DeError::io)?;
	check_header(&header_buf, schema)?;
	from_datum_reader(reader, schema)
}

fn check_header(slice: &[u8; 10], schema: &Schema) -> Result<(), de::DeError> {
	if slice[0..2] != [0xC3, 0x01] {
		return Err(de::DeError::new(
			"Single object slice does not respect C3 01 header",
		));
	}
	if &slice[2..10] != schema.rabin_fingerprint() {
		return Err(de::DeError::new(
			"Single object encoding fingerprint header does not match with schema fingerprint",
		));
	}
	Ok(())
}

/// Serialize to an avro
/// [single object encoding](https://avro.apache.org/docs/current/specification/#single-object-encoding)
///
/// to the provided writer
pub fn to_single_object<T, W>(
	value: &T,
	mut writer: W,
	schema: &Schema,
) -> Result<(), ser::SerError>
where
	T: serde::Serialize + ?Sized,
	W: std::io::Write,
{
	writer.write_all(&[0xC3, 0x01]).map_err(ser::SerError::io)?;
	writer
		.write_all(schema.rabin_fingerprint())
		.map_err(ser::SerError::io)?;
	to_datum(value, writer, schema)
}

/// Serialize to an avro
/// [single object encoding](https://avro.apache.org/docs/current/specification/#single-object-encoding)
///
/// to a newly allocated Vec
///
/// Note that unless you would otherwise allocate a `Vec` anyway, it will be
/// more efficient to use [`to_single_object`] instead.
pub fn to_single_object_vec<T>(value: &T, schema: &Schema) -> Result<Vec<u8>, ser::SerError>
where
	T: serde::Serialize + ?Sized,
{
	let mut buf = Vec::new();
	to_single_object(value, &mut buf, schema)?;
	Ok(buf)
}
