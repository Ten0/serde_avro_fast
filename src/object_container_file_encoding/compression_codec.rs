use crate::de::{self, DeserializerConfig, DeserializerState};

/// The compression codec used to compress blocks.
#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum CompressionCodec {
	/// The `Null` codec simply passes through data uncompressed.
	Null,
	/// The `Deflate` codec writes the data block using the deflate algorithm
	/// as specified in RFC 1951, and typically implemented using the zlib library.
	/// Note that this format (unlike the "zlib format" in RFC 1950) does not have a checksum.
	Deflate,
	#[cfg(feature = "snappy")]
	/// The `Snappy` codec uses Google's [Snappy](http://google.github.io/snappy/)
	/// compression library. Each compressed block is followed by the 4-byte, big-endian
	/// CRC32 checksum of the uncompressed data in the block.
	Snappy,
	#[cfg(feature = "zstandard")]
	Zstandard,
	#[cfg(feature = "bzip")]
	/// The `BZip2` codec uses [BZip2](https://sourceware.org/bzip2/)
	/// compression library.
	Bzip2,
	#[cfg(feature = "xz")]
	/// The `Xz` codec uses [Xz utils](https://tukaani.org/xz/)
	/// compression library.
	Xz,
}

impl CompressionCodec {
	pub(super) fn state<'de, 's, R>(
		self,
		reader: R,
		config: DeserializerConfig<'s>,
		block_size: usize,
	) -> Result<CompressionCodecState<R::Take>, de::DeError>
	where
		R: de::read::Take,
		<R as de::read::Take>::Take: de::read::ReadSlice<'de> + std::io::BufRead,
	{
		Ok(match self {
			CompressionCodec::Null => CompressionCodecState::Null {
				deserializer_state: de::DeserializerState::with_config(
					de::read::Take::take(reader, block_size)?,
					config,
				),
			},
			CompressionCodec::Deflate => CompressionCodecState::Deflate {
				deserializer_state: de::DeserializerState::with_config(
					de::read::ReaderRead::new(flate2::bufread::DeflateDecoder::new(de::read::Take::take(
						reader, block_size,
					)?)),
					config,
				),
			},
		})
	}
}

pub(super) enum CompressionCodecState<'s, R> {
	Null {
		deserializer_state: DeserializerState<'s, R>,
	},
	Deflate {
		deserializer_state: DeserializerState<'s, de::read::ReaderRead<flate2::bufread::DeflateDecoder<R>>>,
	},
}

impl<'s, R> CompressionCodecState<'s, R> {
	pub(super) fn into_source_reader_and_config(self) -> (R, DeserializerConfig<'s>) {
		match self {
			CompressionCodecState::Null { deserializer_state } => deserializer_state.into_inner(),
			CompressionCodecState::Deflate { deserializer_state } => {
				let (reader, config) = deserializer_state.into_inner();
				(reader.into_inner().into_inner(), config)
			}
		}
	}
}
