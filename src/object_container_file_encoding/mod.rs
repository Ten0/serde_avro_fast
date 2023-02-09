//! Support for [object container files](https://avro.apache.org/docs/current/specification/#object-container-files)

mod compression_codec;

use {
	super::*,
	de::{
		read::{Read, ReadSlice},
		DeError,
	},
};

use {
	compression_codec::{CompressionCodec, CompressionCodecState},
	serde::{de::DeserializeOwned, Deserialize},
};

/// Reader for [object container files](https://avro.apache.org/docs/current/specification/#object-container-files)
///
/// Works from either slices or arbitrary `impl Read`s.
///
/// Slice version enables borrowing from the input if there is no compression
/// involved.
pub struct Reader<R: de::read::Take> {
	// the 'static here is fake, it in fact is bound to `Schema` not being dropped
	// struct fields are dropped in order of declaration, so this is dropped before schema
	reader_state: ReaderState<'static, R>,
	compression_codec: CompressionCodec,
	sync_marker: [u8; 16],
	_schema: Schema,
}

/// Errors that may happen when attempting to construct a [`Reader`]
#[derive(Debug, thiserror::Error)]
pub enum FailedToInitializeReader {
	/// Does not begin by `Obj1` as per spec
	#[error("Reader input is not an avro object container file: could not match the header")]
	NotAvroObjectContainerFile,
	#[error("Failed to validate avro object container file header: {}", _0)]
	FailedToDeserializeHeader(DeError),
	#[error("Failed to parse schema in avro object container file: {}", _0)]
	FailedToParseSchema(schema::ParseSchemaError),
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
	R: Read + de::read::Take + std::io::BufRead,
	<R as de::read::Take>::Take: std::io::BufRead,
{
	/// You should typically use `from_slice` or `from_reader` instead
	pub fn new<'de>(mut reader: R) -> Result<Self, FailedToInitializeReader>
	where
		R: ReadSlice<'de>,
	{
		if reader
			.read_const_size_buf::<4>()
			.map_err(FailedToInitializeReader::FailedToDeserializeHeader)?
			!= [b'O', b'b', b'j', 1u8]
		{
			return Err(FailedToInitializeReader::NotAvroObjectContainerFile);
		}

		#[derive(serde_derive::Deserialize)]
		struct Metadata {
			#[serde(rename = "avro.schema")]
			schema: String,
			#[serde(rename = "avro.codec")]
			codec: CompressionCodec,
		}

		let mut metadata_deserializer_config = de::DeserializerConfig::from_schema_node(
			&schema::SchemaNode::Map(&schema::SchemaNode::Bytes),
		);
		metadata_deserializer_config.max_seq_size = 1_000;
		let mut metadata_deserializer_state =
			de::DeserializerState::with_config(reader, metadata_deserializer_config);
		let metadata: Metadata =
			serde::Deserialize::deserialize(metadata_deserializer_state.deserializer())
				.map_err(FailedToInitializeReader::FailedToDeserializeHeader)?;
		reader = metadata_deserializer_state.into_reader();
		let schema: Schema = metadata
			.schema
			.parse()
			.map_err(FailedToInitializeReader::FailedToParseSchema)?;

		let sync_marker = reader
			.read_const_size_buf::<16>()
			.map_err(FailedToInitializeReader::FailedToDeserializeHeader)?;

		// Safety: we don't drop the schema until this is dropped
		// This is useful to be able to store a DeserializerState directly in here,
		// which will avoid additional &mut levels, allowing for highest performance and
		// ergonomics
		let schema_root: &'static schema::SchemaNode<'static> = unsafe {
			let schema = &*(&schema as *const Schema);
			let a: *const schema::SchemaNode<'_> = schema.root() as *const schema::SchemaNode<'_>;
			let b: *const schema::SchemaNode<'static> = a as *const _;
			&*b
		};

		Ok(Self {
			reader_state: ReaderState::NotInBlock {
				reader,
				config: de::DeserializerConfig::from_schema_node(schema_root),
			},
			compression_codec: metadata.codec,
			sync_marker,
			_schema: schema,
		})
	}

	/// Iterator over the deserialized values
	pub fn deserialize<'r, 'rs, T: DeserializeOwned>(
		&'r mut self,
	) -> impl Iterator<Item = Result<T, DeError>> + 'r
	where
		<R as de::read::Take>::Take: ReadSlice<'rs>,
	{
		self.deserialize_borrowed()
	}

	/// Iterator over the deserialized values
	///
	/// Note that this may fail if the provided `T` requires to borrow from the
	/// input and the input is actually an `impl Read`, or if the blocks are
	/// compressed. (`deserialize_next` typechecks that we have
	/// `DeserializeOwned` to make sure that is never the case)
	pub fn deserialize_borrowed<'r, 'de, T: Deserialize<'de>>(
		&'r mut self,
	) -> impl Iterator<Item = Result<T, DeError>> + 'r
	where
		<R as de::read::Take>::Take: ReadSlice<'de>,
	{
		std::iter::from_fn(|| self.deserialize_next_borrowed().transpose())
	}

	/// Attempt to deserialize the next value
	pub fn deserialize_next<'a, T: DeserializeOwned>(&mut self) -> Result<Option<T>, DeError>
	where
		<R as de::read::Take>::Take: ReadSlice<'a>,
	{
		self.deserialize_next_borrowed()
	}

	/// Attempt to deserialize the next value
	///
	/// Note that this may fail if the provided `T` requires to borrow from the
	/// input and the input is actually an `impl Read`, or if the blocks are
	/// compressed. (`deserialize_next` typechecks that we have
	/// `DeserializeOwned` to make sure that is never the case)
	pub fn deserialize_next_borrowed<'de, T: Deserialize<'de>>(
		&mut self,
	) -> Result<Option<T>, DeError>
	where
		<R as de::read::Take>::Take: ReadSlice<'de>,
	{
		loop {
			match &mut self.reader_state {
				ReaderState::Broken => {
					return Err(DeError::new(
						"Object container file reader is broken after error",
					))
				}
				ReaderState::NotInBlock { reader, config: _ } => {
					if reader
						.fill_buf()
						.map(|b| b.is_empty())
						.map_err(DeError::io)?
					{
						// Reader is empty, we're done reading
						break Ok(None);
					}
					let (mut reader, config) =
						match std::mem::replace(&mut self.reader_state, ReaderState::Broken) {
							ReaderState::NotInBlock { reader, config } => (reader, config),
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
					let codec_data = self.compression_codec.state(reader, config, block_size)?;
					self.reader_state = ReaderState::InBlock {
						codec_data,
						n_objects_in_block,
					};
				}
				ReaderState::InBlock {
					codec_data,
					n_objects_in_block,
				} => match n_objects_in_block.checked_sub(1) {
					None => {
						match std::mem::replace(&mut self.reader_state, ReaderState::Broken) {
							ReaderState::InBlock {
								codec_data,
								n_objects_in_block: _,
							} => {
								let (reader, config) = codec_data.into_source_reader_and_config();
								let mut reader =
									de::read::IntoLeftAfterTake::into_left_after_take(reader)?;
								let sync_marker = reader.read_const_size_buf::<16>()?;
								if sync_marker != self.sync_marker {
									return Err(DeError::new(
										"Incorrect sync marker at end of block",
									));
								}
								self.reader_state = ReaderState::NotInBlock { reader, config }
							}
							_ => unreachable!(),
						}
						return Ok(None);
					}
					Some(next_n_in_block) => {
						*n_objects_in_block = next_n_in_block;
						break match codec_data {
							CompressionCodecState::Null { deserializer_state } => {
								T::deserialize(deserializer_state.deserializer())
							}
							CompressionCodecState::Deflate { deserializer_state } => {
								T::deserialize(deserializer_state.deserializer())
							}
						}
						.map(Some);
					}
				},
			}
		}
	}
}

enum ReaderState<'s, R: de::read::Take> {
	Broken,
	NotInBlock {
		reader: R,
		config: de::DeserializerConfig<'s>,
	},
	InBlock {
		codec_data: CompressionCodecState<'s, R::Take>,
		n_objects_in_block: usize,
	},
}
