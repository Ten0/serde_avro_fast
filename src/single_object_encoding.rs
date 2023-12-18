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
/// [single object encoding](https://avro.apache.org/docs/current/specification/#single-object-encoding) `impl BufRead`
///
/// If you only have an `impl Read`, wrap it in a
/// [`BufReader`](std::io::BufReader) first.
///
/// If deserializing from a slice, a `Vec`, ... prefer using `from_datum_slice`,
/// as it will be more performant and enable you to borrow `&str`s from the
/// original slice.
pub fn from_single_object_reader<R, T>(mut reader: R, schema: &Schema) -> Result<T, de::DeError>
where
	T: serde::de::DeserializeOwned,
	R: std::io::BufRead,
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
///
/// [`SerializerConfig`](ser::SerializerConfig) can be built from a schema:
/// ```
/// # use serde_avro_fast::{ser, Schema};
/// let schema: Schema = r#""int""#.parse().unwrap();
/// let serializer_config = &mut ser::SerializerConfig::new(&schema);
///
/// let mut serialized: Vec<u8> =
/// 	serde_avro_fast::to_single_object_vec(&3, serializer_config).unwrap();
/// assert_eq!(
/// 	serialized,
/// 	&[0xC3, 0x01, 143, 92, 57, 63, 26, 213, 117, 114, 6]
/// );
///
/// // reuse config and output buffer across serializations for ideal performance (~40% perf gain)
/// serialized.clear();
/// let serialized = serde_avro_fast::to_single_object(&4, serialized, serializer_config).unwrap();
/// assert_eq!(
/// 	serialized,
/// 	&[0xC3, 0x01, 143, 92, 57, 63, 26, 213, 117, 114, 8]
/// );
/// ```
pub fn to_single_object<T, W>(
	value: &T,
	mut writer: W,
	serializer_config: &mut ser::SerializerConfig<'_>,
) -> Result<W, ser::SerError>
where
	T: serde::Serialize + ?Sized,
	W: std::io::Write,
{
	writer.write_all(&[0xC3, 0x01]).map_err(ser::SerError::io)?;
	writer
		.write_all(serializer_config.schema().rabin_fingerprint())
		.map_err(ser::SerError::io)?;
	to_datum(value, writer, serializer_config)
}

/// Serialize to an avro
/// [single object encoding](https://avro.apache.org/docs/current/specification/#single-object-encoding)
///
/// to a newly allocated Vec
///
/// Note that unless you would otherwise allocate a `Vec` anyway, it will be
/// more efficient to use [`to_single_object`] instead.
///
/// See [`to_single_object`] for more details.
pub fn to_single_object_vec<T>(
	value: &T,
	serializer_config: &mut ser::SerializerConfig<'_>,
) -> Result<Vec<u8>, ser::SerError>
where
	T: serde::Serialize + ?Sized,
{
	let mut buf = Vec::new();
	to_single_object(value, &mut buf, serializer_config)?;
	Ok(buf)
}
