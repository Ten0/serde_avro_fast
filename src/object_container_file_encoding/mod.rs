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

pub struct Reader<R: de::read::Take> {
	// the 'static here is fake, it in fact is bound to `Schema` not being dropped
	// struct fields are dropped in order of declaration, so this is dropped before schema
	reader_state: ReaderState<'static, R>,
	compression_codec: CompressionCodec,
	sync_marker: [u8; 16],
	_schema: Schema,
}

pub enum FailedToInitializeReader {
	/// Does not begin by `Obj1` as per spec
	NotAvroObjectContainerFile,
	FailedToDeserializeHeader(DeError),
	FailedToParseSchema(schema::ParseSchemaError),
}

impl<R> Reader<R>
where
	R: Read + de::read::Take + std::io::BufRead,
	<R as de::read::Take>::Take: std::io::BufRead,
{
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

		#[derive(serde::Deserialize)]
		struct Metadata {
			#[serde(rename = "avro.schema")]
			schema: String,
			#[serde(rename = "avro.codec")]
			codec: CompressionCodec,
		}

		let mut metadata_deserializer_config =
			de::DeserializerConfig::from_schema_node(&schema::SchemaNode::Map(&schema::SchemaNode::Bytes));
		metadata_deserializer_config.max_seq_size = 1_000;
		let mut metadata_deserializer_state = de::DeserializerState::with_config(reader, metadata_deserializer_config);
		let metadata: Metadata = serde::Deserialize::deserialize(metadata_deserializer_state.deserializer())
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
		// This is useful to be able to store a DeserializerState directly in here, which will avoid additional
		// &mut levels, allowing for highest performance and ergonomics
		// TODO replace this with crate `ouroboros`
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
	pub fn deserialize<'r, 'rs, T: DeserializeOwned>(&'r mut self) -> DeserializeIterator<'r, 'rs, R, T>
	where
		<R as de::read::Take>::Take: ReadSlice<'rs>,
	{
		self.deserialize_borrowed()
	}

	/// Iterator over the deserialized values
	///
	/// Note that this may fail if the provided `T` requires to borrow from the input
	/// and the input is actually an `impl Read`, or if the blocks are compressed.
	/// (`deserialize_next` typechecks that we have `DeserializeOwned` to make sure that is never the case)
	pub fn deserialize_borrowed<'r, 'de, T: Deserialize<'de>>(&'r mut self) -> DeserializeIterator<'r, 'de, R, T>
	where
		<R as de::read::Take>::Take: ReadSlice<'de>,
	{
		DeserializeIterator {
			reader: self,
			target: std::marker::PhantomData,
			lifetime: std::marker::PhantomData,
		}
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
	/// Note that this may fail if the provided `T` requires to borrow from the input
	/// and the input is actually an `impl Read`, or if the blocks are compressed.
	/// (`deserialize_next` typechecks that we have `DeserializeOwned` to make sure that is never the case)
	pub fn deserialize_next_borrowed<'de, T: Deserialize<'de>>(&mut self) -> Result<Option<T>, DeError>
	where
		<R as de::read::Take>::Take: ReadSlice<'de>,
	{
		loop {
			match &mut self.reader_state {
				ReaderState::Broken => return Err(DeError::new("Object container file reader is broken after error")),
				ReaderState::NotInBlock { reader, config: _ } => {
					if reader.fill_buf().map(|b| b.is_empty()).map_err(DeError::io)? {
						// Reader is empty, we're done reading
						break Ok(None);
					}
					let (mut reader, config) = match std::mem::replace(&mut self.reader_state, ReaderState::Broken) {
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
								let mut reader = de::read::IntoLeftAfterTake::into_left_after_take(reader)?;
								let sync_marker = reader.read_const_size_buf::<16>()?;
								if sync_marker != self.sync_marker {
									return Err(DeError::new("Incorrect sync marker at end of block"));
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

pub struct DeserializeIterator<'r, 'de, R: de::read::Take, T> {
	reader: &'r mut Reader<R>,
	target: std::marker::PhantomData<T>,
	lifetime: std::marker::PhantomData<&'de ()>,
}
impl<'de, R, T> Iterator for DeserializeIterator<'_, 'de, R, T>
where
	R: Read + de::read::Take + std::io::BufRead,
	<R as de::read::Take>::Take: ReadSlice<'de> + std::io::BufRead,
	T: Deserialize<'de>,
{
	type Item = Result<T, de::DeError>;
	fn next(&mut self) -> Option<Self::Item> {
		self.reader.deserialize_next_borrowed().transpose()
	}

	fn size_hint(&self) -> (usize, Option<usize>) {
		(
			match self.reader.reader_state {
				ReaderState::InBlock { n_objects_in_block, .. } => n_objects_in_block,
				_ => 0,
			},
			None,
		)
	}
}
