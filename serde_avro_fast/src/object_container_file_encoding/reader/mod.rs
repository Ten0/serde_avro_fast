mod decompression;

use crate::{
	de::{
		read::{Read, ReadSlice},
		DeError,
	},
	object_container_file_encoding::{CompressionCodec, Metadata, HEADER_CONST, METADATA_SCHEMA},
	*,
};

use {
	decompression::DecompressionState,
	serde::{
		de::{DeserializeOwned, DeserializeSeed},
		Deserialize,
	},
	std::{marker::PhantomData, sync::Arc},
};

/// Reader for [object container files](https://avro.apache.org/docs/current/specification/#object-container-files)
///
/// # Example
/// ```
/// let avro_object_container_file_encoded: &[u8] = &[
/// 	79, 98, 106, 1, 4, 22, 97, 118, 114, 111, 46, 115, 99, 104, 101, 109, 97, 222, 1, 123, 34,
/// 	116, 121, 112, 101, 34, 58, 34, 114, 101, 99, 111, 114, 100, 34, 44, 34, 110, 97, 109, 101,
/// 	34, 58, 34, 116, 101, 115, 116, 34, 44, 34, 102, 105, 101, 108, 100, 115, 34, 58, 91, 123,
/// 	34, 110, 97, 109, 101, 34, 58, 34, 97, 34, 44, 34, 116, 121, 112, 101, 34, 58, 34, 108,
/// 	111, 110, 103, 34, 44, 34, 100, 101, 102, 97, 117, 108, 116, 34, 58, 52, 50, 125, 44, 123,
/// 	34, 110, 97, 109, 101, 34, 58, 34, 98, 34, 44, 34, 116, 121, 112, 101, 34, 58, 34, 115,
/// 	116, 114, 105, 110, 103, 34, 125, 93, 125, 20, 97, 118, 114, 111, 46, 99, 111, 100, 101,
/// 	99, 8, 110, 117, 108, 108, 0, 94, 61, 54, 221, 190, 207, 108, 180, 158, 57, 114, 40, 173,
/// 	199, 228, 239, 4, 20, 54, 6, 102, 111, 111, 84, 6, 98, 97, 114, 94, 61, 54, 221, 190, 207,
/// 	108, 180, 158, 57, 114, 40, 173, 199, 228, 239,
/// ];
///
/// #[derive(serde_derive::Deserialize, Debug, PartialEq, Eq)]
/// struct SchemaRecord<'a> {
/// 	a: i64,
/// 	b: &'a str,
/// }
///
/// let mut reader = serde_avro_fast::object_container_file_encoding::Reader::from_slice(
/// 	avro_object_container_file_encoded,
/// )
/// .expect("Failed to initialize reader");
///
/// let expected = vec![
/// 	SchemaRecord { a: 27, b: "foo" },
/// 	SchemaRecord { a: 42, b: "bar" },
/// ];
/// let res: Vec<SchemaRecord> = reader
/// 	.deserialize_borrowed::<SchemaRecord>() // Only use `_borrowed` if data is not compressed
/// 	.collect::<Result<_, _>>()
/// 	.expect("Failed to deserialize a record");
///
/// assert_eq!(expected, res);
/// ```
///
/// # Notes
///
/// Works from either slices or arbitrary `impl BufRead`s.
///
/// If you only have an `impl Read`, wrap it in a
/// [`BufReader`](std::io::BufReader) first.
///
/// Slice version enables borrowing from the input if there is no compression
/// involved.
pub struct Reader<R: de::read::take::Take> {
	// the 'static here is fake, it in fact is bound to `Schema` not being dropped
	// struct fields are dropped in order of declaration, so this is dropped before schema
	reader_state: ReaderState<'static, R>,
	compression_codec: CompressionCodec,
	sync_marker: [u8; 16],
	/// If we hit an IO error, we yield it once, then for following calls to
	/// `deserialize_next` we pretend that we reached EOF. This is because IO
	/// errors will typically reproduce at every call, and we don't want to keep
	/// yielding the same error over and over again if the caller happens to try
	/// to recover from deserialization errors.
	pretend_eof_because_yielded_unrecoverable_error: bool,
	/// This has to be stored in here and be the last field because the Reader
	/// is self referential: it stores references to inside Schema.
	schema: Arc<Schema>,
}

/// Errors that may happen when attempting to construct a [`Reader`]
#[derive(Debug, thiserror::Error)]
pub enum FailedToInitializeReader {
	/// Does not begin by `Obj1` as per spec
	#[error("Reader input is not an avro object container file: could not match the header")]
	NotAvroObjectContainerFile,
	/// The `Avro` object container file header could not be deserialized as per
	/// the avro schema of the header defined by the specification
	#[error("Failed to validate avro object container file header: {}", _0)]
	FailedToDeserializeHeader(DeError),
	/// The avro schema in the header could not be parsed
	///
	/// This could be due to an invalid schema, or a schema that is not
	/// supported by this library
	#[error("Failed to parse schema in avro object container file: {}", _0)]
	FailedToParseSchema(schema::SchemaError),
}

impl<'a> Reader<de::read::SliceRead<'a>> {
	/// Initialize a `Reader` from a slice
	///
	/// Useful if the entire file has already been loaded in memory and you wish
	/// to deserialize borrowing from this slice.
	///
	/// Note that deserialization will only be able to borrow from this slice if
	/// there is no compression codec. To be safe that it works in both cases,
	/// you may use `Cow<str>` tagged with `#[serde(borrow)]`.
	pub fn from_slice(slice: &'a [u8]) -> Result<Self, FailedToInitializeReader> {
		Self::new(de::read::SliceRead::new(slice))
	}
}

impl<R: std::io::BufRead> Reader<de::read::ReaderRead<R>> {
	/// Initialize a `Reader` from any `impl BufRead`
	///
	/// Note that if your reader has [`Read`](std::io::Read) but not
	/// [`BufRead`](std::io::BufRead), you may simply wrap
	/// it into a [`std::io::BufReader`].
	///
	/// Note that this will start reading from the `reader` during
	/// initialization.
	pub fn from_reader(reader: R) -> Result<Self, FailedToInitializeReader> {
		Self::new(de::read::ReaderRead::new(reader))
	}
}

impl<R> Reader<R>
where
	R: Read + de::read::take::Take + std::io::BufRead,
	<R as de::read::take::Take>::Take: std::io::BufRead,
{
	/// You should typically use `from_slice` or `from_reader` instead
	pub fn new<'de>(reader: R) -> Result<Self, FailedToInitializeReader>
	where
		R: ReadSlice<'de>,
	{
		Self::new_and_metadata::<()>(reader).map(|(reader, ())| reader)
	}

	/// Build a `Reader`, also extracting custom metadata in addition to the
	/// avro-reserved metadata
	///
	/// Note that if your reader is not a slice reader, you should provide a
	/// type `M` that implements [`serde::de::DeserializeOwned`], otherwise
	/// deserialization may fail.
	pub fn new_and_metadata<'de, M>(mut reader: R) -> Result<(Self, M), FailedToInitializeReader>
	where
		R: ReadSlice<'de>,
		M: Deserialize<'de>,
	{
		if reader
			.read_const_size_buf::<4>()
			.map_err(FailedToInitializeReader::FailedToDeserializeHeader)?
			!= HEADER_CONST
		{
			return Err(FailedToInitializeReader::NotAvroObjectContainerFile);
		}

		let mut metadata_deserializer_config =
			de::DeserializerConfig::from_schema_node(METADATA_SCHEMA);
		metadata_deserializer_config.max_seq_size = 1_000;
		let mut metadata_deserializer_state =
			de::DeserializerState::with_config(reader, metadata_deserializer_config);
		let metadata: Metadata<String, M> =
			serde::Deserialize::deserialize(metadata_deserializer_state.deserializer())
				.map_err(FailedToInitializeReader::FailedToDeserializeHeader)?;
		reader = metadata_deserializer_state.into_reader();
		let schema: Arc<Schema> = Arc::new(
			metadata
				.schema
				.parse()
				.map_err(FailedToInitializeReader::FailedToParseSchema)?,
		);

		let sync_marker = reader
			.read_const_size_buf::<16>()
			.map_err(FailedToInitializeReader::FailedToDeserializeHeader)?;

		// Safety: we don't drop the schema until this is dropped
		// This is useful to be able to store a DeserializerState directly in here,
		// which will avoid additional &mut levels, allowing for highest performance and
		// ergonomics
		let schema_root = unsafe { schema.root_with_fake_static_lifetime() };

		Ok((
			Self {
				reader_state: ReaderState::NotInBlock {
					reader,
					config: de::DeserializerConfig::from_schema_node(schema_root),
					decompression_buffer: Vec::new(),
				},
				compression_codec: metadata.codec,
				sync_marker,
				pretend_eof_because_yielded_unrecoverable_error: false,
				schema,
			},
			metadata.user_metadata,
		))
	}

	/// Iterator over the deserialized values
	pub fn deserialize<'r, 'rs, T: DeserializeOwned>(
		&'r mut self,
	) -> impl Iterator<Item = Result<T, DeError>> + 'r
	where
		R: ReadSlice<'rs>,
		<R as de::read::take::Take>::Take: ReadSlice<'rs>,
	{
		self.deserialize_inner()
	}

	/// Iterator over the deserialized values
	///
	/// This can only be used if reading directly from a slice (Reader built via
	/// [`Reader::from_slice`], `R = `[`SliceRead<'_>`](de::read::SliceRead))
	///
	/// Note that this may fail if the provided `T` requires to borrow from the
	/// input and the blocks are compressed. (`deserialize_next` typechecks that
	/// we have `DeserializeOwned` to make sure that is never the case).
	pub fn deserialize_borrowed<'r, 'de, T: Deserialize<'de>>(
		&'r mut self,
	) -> impl Iterator<Item = Result<T, DeError>> + 'r
	where
		R: ReadSlice<'de> + IsSliceRead,
		<R as de::read::take::Take>::Take: ReadSlice<'de>,
	{
		Self::deserialize_inner::<T>(self)
	}

	/// Iterator over the deserialized values
	///
	/// Note that this may fail if the provided `T` requires to borrow from the
	/// input and the input is actually an `impl BufRead`, or if the blocks are
	/// compressed. (`deserialize_next` typechecks that we have
	/// `DeserializeOwned` to make sure that is never the case).
	fn deserialize_inner<'r, 'de, T: Deserialize<'de>>(
		&'r mut self,
	) -> impl Iterator<Item = Result<T, DeError>> + 'r
	where
		R: ReadSlice<'de>,
		<R as de::read::take::Take>::Take: ReadSlice<'de>,
	{
		std::iter::from_fn(|| self.deserialize_seed_next(PhantomData::<T>).transpose())
	}

	/// Attempt to deserialize the next value
	pub fn deserialize_next<'a, T: DeserializeOwned>(&mut self) -> Result<Option<T>, DeError>
	where
		R: ReadSlice<'a>,
		<R as de::read::take::Take>::Take: ReadSlice<'a>,
	{
		self.deserialize_seed_next(PhantomData::<T>)
	}

	/// Attempt to deserialize the next value
	///
	/// This can only be used if reading directly from a slice (Reader built via
	/// [`Reader::from_slice`], `R = `[`SliceRead<'_>`](de::read::SliceRead)).
	///
	/// Note that this may fail if the provided `T` requires to borrow from the
	/// input and the blocks are compressed. (`deserialize_next` typechecks that
	/// we have `DeserializeOwned` to make sure that is never the case).
	pub fn deserialize_next_borrowed<'de, T: Deserialize<'de>>(
		&mut self,
	) -> Result<Option<T>, DeError>
	where
		R: ReadSlice<'de> + IsSliceRead,
		<R as de::read::take::Take>::Take: ReadSlice<'de>,
	{
		self.deserialize_seed_next(PhantomData::<T>)
	}

	/// Attempt to deserialize the next value via the advanced
	/// [`DeserializeSeed`] serde API
	///
	/// A typical user should not need this.
	///
	/// This may be useful for transcoding.
	pub fn deserialize_seed_next<'de, S: DeserializeSeed<'de>>(
		&mut self,
		deserialize_seed: S,
	) -> Result<Option<S::Value>, DeError>
	where
		R: ReadSlice<'de>,
		<R as de::read::take::Take>::Take: ReadSlice<'de>,
	{
		if self.pretend_eof_because_yielded_unrecoverable_error {
			return Ok(None);
		}
		let res = self.deserialize_next_inner(deserialize_seed);
		if let Err(ref de_error) = res {
			if de_error.io_error().is_some() || matches!(self.reader_state, ReaderState::Broken) {
				// we yield this error once, then for following calls to
				// `deserialize_next` we pretend that we reached EOF. This is
				// because IO errors will typically reproduce at every call, and we
				// don't want to keep yielding the same error over and over again
				// if the caller happens to try to recover from deserialization
				// errors.
				self.pretend_eof_because_yielded_unrecoverable_error = true;
			}
		}
		res
	}

	/// Attempt to deserialize the next value
	///
	/// Note that this may fail if the provided `seed` requires to borrow from
	/// the input and the input is actually an `impl BufRead`, or if the blocks
	/// are compressed. (`deserialize_next` typechecks that we have
	/// `DeserializeOwned` to make sure that is never the case)
	fn deserialize_next_inner<'de, S: DeserializeSeed<'de>>(
		&mut self,
		deserialize_seed: S,
	) -> Result<Option<S::Value>, DeError>
	where
		R: ReadSlice<'de>,
		<R as de::read::take::Take>::Take: ReadSlice<'de>,
	{
		loop {
			match &mut self.reader_state {
				ReaderState::Broken => {
					return Err(DeError::new(
						"Object container file reader is broken after error",
					))
				}
				ReaderState::NotInBlock { reader, .. } => {
					if reader
						.fill_buf()
						.map(|b| b.is_empty())
						.map_err(DeError::io)?
					{
						// Reader is empty, we're done reading
						break Ok(None);
					}
					let (mut reader, config, decompression_buffer) =
						match std::mem::replace(&mut self.reader_state, ReaderState::Broken) {
							ReaderState::NotInBlock {
								reader,
								config,
								decompression_buffer,
							} => (reader, config, decompression_buffer),
							_ => unreachable!(),
						};
					let n_objects_in_block: i64 = reader.read_varint()?;
					let n_objects_in_block: usize = n_objects_in_block
						.try_into()
						.map_err(|_| DeError::new("Invalid container file block object count"))?;
					let block_size: i64 = reader.read_varint()?;
					let block_size: usize = block_size
						.try_into()
						.map_err(|_| DeError::new("Invalid container file block size in bytes"))?;
					let codec_data = self.compression_codec.state(
						reader,
						config,
						decompression_buffer,
						block_size,
					)?;
					self.reader_state = ReaderState::InBlock {
						codec_data,
						n_objects_in_block,
					};
				}
				ReaderState::InBlock {
					codec_data,
					n_objects_in_block,
				} => match n_objects_in_block.checked_sub(1) {
					None => match std::mem::replace(&mut self.reader_state, ReaderState::Broken) {
						ReaderState::InBlock {
							codec_data,
							n_objects_in_block: _,
						} => {
							let (mut reader, config, decompression_buffer) =
								codec_data.into_source_reader_and_config()?;
							let sync_marker = reader.read_const_size_buf::<16>()?;
							if sync_marker != self.sync_marker {
								return Err(DeError::new("Incorrect sync marker at end of block"));
							}
							self.reader_state = ReaderState::NotInBlock {
								reader,
								config,
								decompression_buffer,
							}
						}
						_ => unreachable!(),
					},
					Some(next_n_in_block) => {
						*n_objects_in_block = next_n_in_block;
						break match codec_data {
							DecompressionState::Null {
								deserializer_state, ..
							} => deserialize_seed.deserialize(deserializer_state.deserializer()),
							#[cfg(any(
								feature = "deflate",
								feature = "bzip2",
								feature = "xz",
								feature = "zstandard"
							))]
							DecompressionState::BufReader {
								deserializer_state, ..
							} => deserialize_seed.deserialize(deserializer_state.deserializer()),
							#[cfg(feature = "snappy")]
							DecompressionState::DecompressedOnConstruction {
								deserializer_state,
								..
							} => deserialize_seed.deserialize(deserializer_state.deserializer()),
						}
						.map(Some);
					}
				},
			}
		}
	}

	/// Get the schema used for deserialization
	///
	/// It was read from the header of the object container file.
	pub fn schema(&self) -> &Arc<Schema> {
		&self.schema
	}
}

enum ReaderState<'s, R: de::read::take::Take> {
	Broken,
	NotInBlock {
		reader: R,
		config: de::DeserializerConfig<'s>,
		decompression_buffer: Vec<u8>,
	},
	InBlock {
		codec_data: DecompressionState<'s, R>,
		n_objects_in_block: usize,
	},
}

mod private {
	/// Implemented only on [`SliceRead<'_>`](crate::de::read::SliceRead)
	///
	/// We need this trait to enforce that `deserialize_borrowed` and
	/// `deserialize_next_borrowed` are only callable when `R = SliceRead<'de>`,
	/// not on arbitrary BufReads.
	///
	/// We have to use this trait instead of implementing directly on
	/// `Reader<de::read::SliceRead<'a>>` because otherwise the compiler
	/// complains that "hidden type for `impl Iterator<Item = Result<T,
	/// de::error::DeError>> + 'r` captures lifetime that does not appear in
	/// bounds"
	pub trait IsSliceRead {}
}
use private::IsSliceRead;
impl IsSliceRead for de::read::SliceRead<'_> {}
