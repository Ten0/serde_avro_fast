use crate::de::{self, read::take::IntoLeftAfterTake, DeserializerConfig, DeserializerState};

/// The compression codec used to compress blocks.
#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Deserialize)]
#[serde(rename_all = "kebab-case")]
#[non_exhaustive]
pub enum CompressionCodec {
	/// The `Null` codec simply passes through data uncompressed.
	Null,
	#[cfg(feature = "deflate")]
	/// The `Deflate` codec writes the data block using the deflate algorithm
	/// as specified in RFC 1951, and typically implemented using the zlib
	/// library. Note that this format (unlike the "zlib format" in RFC 1950)
	/// does not have a checksum.
	Deflate,
	#[cfg(feature = "bzip2")]
	/// The `BZip2` codec uses [BZip2](https://sourceware.org/bzip2/)
	/// compression library.
	Bzip2,
	#[cfg(feature = "snappy")]
	/// The `Snappy` codec uses Google's [Snappy](http://google.github.io/snappy/)
	/// compression algorithm. Each compressed block is followed by the 4-byte,
	/// big-endian CRC32 checksum of the uncompressed data in the block.
	Snappy,
	#[cfg(feature = "xz")]
	/// The `Xz` codec uses [Xz utils](https://tukaani.org/xz/)
	/// compression library.
	Xz,
	#[cfg(feature = "zstandard")]
	// The `zstandard` codec uses Facebook’s [Zstandard](https://facebook.github.io/zstd/) compression library
	Zstandard,
}
// TODO add support for these compression protocols below (and declare features
// and relevant additional dependencies)

impl CompressionCodec {
	pub(super) fn state<'de, 's, R>(
		self,
		reader: R,
		config: DeserializerConfig<'s>,
		decompression_buffer: Vec<u8>,
		block_size: usize,
	) -> Result<CompressionCodecState<R>, de::DeError>
	where
		R: de::read::take::Take + de::read::ReadSlice<'de>,
		<R as de::read::take::Take>::Take: de::read::ReadSlice<'de> + std::io::BufRead,
	{
		Ok(match self {
			CompressionCodec::Null => CompressionCodecState::Null {
				deserializer_state: de::DeserializerState::with_config(
					de::read::take::Take::take(reader, block_size)?,
					config,
				),
				decompression_buffer,
			},
			#[cfg(feature = "deflate")]
			CompressionCodec::Deflate => CompressionCodecState::Deflate {
				deserializer_state: de::DeserializerState::with_config(
					de::read::ReaderRead::new(flate2::bufread::DeflateDecoder::new(
						de::read::take::Take::take(reader, block_size)?,
					)),
					config,
				),
				decompression_buffer,
			},
			#[cfg(feature = "bzip2")]
			CompressionCodec::Bzip2 => CompressionCodecState::Bzip2 {
				deserializer_state: de::DeserializerState::with_config(
					de::read::ReaderRead::new(bzip2::bufread::BzDecoder::new(
						de::read::take::Take::take(reader, block_size)?,
					)),
					config,
				),
				decompression_buffer,
			},
			#[cfg(feature = "snappy")]
			CompressionCodec::Snappy => {
				// Snappy does not support block decompression in the format used by Avro.
				// This should be fine because avro blocks themselves should typically be of a
				// reasonable size
				let block_raw_size = block_size.checked_sub(4).ok_or_else(|| {
					de::DeError::new(
						"Incorrect block size for Snappy compression: should be at least 4 for CRC",
					)
				})?;
				let mut reader = reader;
				let mut decompression_buffer = decompression_buffer;
				/// Workaround a rust type inference limitation
				fn fix_closure_late_bound_lifetime_inference<F, T>(f: F) -> F
				where
					F: FnOnce(&[u8]) -> T,
				{
					f
				}
				de::read::ReadSlice::read_slice(
					&mut reader,
					block_raw_size,
					fix_closure_late_bound_lifetime_inference(|compressed_slice| {
						fn snappy_to_de_error(snappy_error: snap::Error) -> de::DeError {
							<de::DeError as serde::de::Error>::custom(format_args!(
								"Snappy decompression error: {snappy_error}"
							))
						}
						decompression_buffer.resize(
							snap::raw::decompress_len(compressed_slice)
								.map_err(snappy_to_de_error)?,
							0,
						);
						snap::raw::Decoder::new()
							.decompress(compressed_slice, &mut decompression_buffer)
							.map_err(snappy_to_de_error)?;
						Ok(())
					}),
				)?;
				let actual_crc32 = crc32fast::hash(&decompression_buffer);
				let expected_crc32 =
					u32::from_be_bytes(de::read::Read::read_const_size_buf(&mut reader)?);
				if actual_crc32 != expected_crc32 {
					return Err(de::DeError::new(
						"Incorrect extra CRC32 of decompressed data when using Snappy compression codec",
					));
				}
				CompressionCodecState::Snappy {
					deserializer_state: de::DeserializerState::with_config(
						de::read::ReaderRead::new(std::io::Cursor::new(decompression_buffer)),
						config,
					),
					source_reader: reader,
				}
			}
			#[cfg(feature = "xz")]
			CompressionCodec::Xz => CompressionCodecState::Xz {
				deserializer_state: de::DeserializerState::with_config(
					de::read::ReaderRead::new(xz2::bufread::XzDecoder::new(
						de::read::take::Take::take(reader, block_size)?,
					)),
					config,
				),
				decompression_buffer,
			},
			#[cfg(feature = "zstandard")]
			CompressionCodec::Zstandard => CompressionCodecState::Zstandard {
				deserializer_state: de::DeserializerState::with_config(
					de::read::ReaderRead::new(
						zstd::stream::read::Decoder::with_buffer(de::read::take::Take::take(
							reader, block_size,
						)?)
						.map_err(de::DeError::io)?,
					),
					config,
				),
				decompression_buffer,
			},
		})
	}
}

pub(super) enum CompressionCodecState<'s, R: de::read::take::Take> {
	Null {
		deserializer_state: DeserializerState<'s, R::Take>,
		decompression_buffer: Vec<u8>,
	},
	#[cfg(feature = "deflate")]
	Deflate {
		deserializer_state:
			DeserializerState<'s, de::read::ReaderRead<flate2::bufread::DeflateDecoder<R::Take>>>,
		decompression_buffer: Vec<u8>,
	},
	#[cfg(feature = "bzip2")]
	Bzip2 {
		deserializer_state:
			DeserializerState<'s, de::read::ReaderRead<bzip2::bufread::BzDecoder<R::Take>>>,
		decompression_buffer: Vec<u8>,
	},
	#[cfg(feature = "snappy")]
	Snappy {
		deserializer_state: DeserializerState<'s, de::read::ReaderRead<std::io::Cursor<Vec<u8>>>>,
		source_reader: R,
	},
	#[cfg(feature = "xz")]
	Xz {
		deserializer_state:
			DeserializerState<'s, de::read::ReaderRead<xz2::bufread::XzDecoder<R::Take>>>,
		decompression_buffer: Vec<u8>,
	},
	#[cfg(feature = "zstandard")]
	Zstandard {
		deserializer_state: DeserializerState<
			's,
			de::read::ReaderRead<zstd::stream::read::Decoder<'static, R::Take>>,
		>,
		decompression_buffer: Vec<u8>,
	},
}

impl<'s, R: de::read::take::Take> CompressionCodecState<'s, R> {
	pub(super) fn into_source_reader_and_config(
		self,
	) -> Result<(R, DeserializerConfig<'s>, Vec<u8>), de::DeError> {
		Ok(match self {
			CompressionCodecState::Null {
				deserializer_state,
				decompression_buffer,
			} => {
				let (reader, config) = deserializer_state.into_inner();
				(reader.into_left_after_take()?, config, decompression_buffer)
			}
			#[cfg(feature = "deflate")]
			CompressionCodecState::Deflate {
				deserializer_state,
				decompression_buffer,
			} => {
				let (reader, config) = deserializer_state.into_inner();
				(
					reader.into_inner().into_inner().into_left_after_take()?,
					config,
					decompression_buffer,
				)
			}
			#[cfg(feature = "bzip2")]
			CompressionCodecState::Bzip2 {
				deserializer_state,
				decompression_buffer,
			} => {
				let (reader, config) = deserializer_state.into_inner();
				(
					reader.into_inner().into_inner().into_left_after_take()?,
					config,
					decompression_buffer,
				)
			}
			#[cfg(feature = "snappy")]
			CompressionCodecState::Snappy {
				deserializer_state,
				source_reader,
			} => {
				let (reader, config) = deserializer_state.into_inner();
				(source_reader, config, reader.into_inner().into_inner())
			}
			#[cfg(feature = "xz2")]
			CompressionCodecState::Xz {
				deserializer_state,
				decompression_buffer,
			} => {
				let (reader, config) = deserializer_state.into_inner();
				(
					reader.into_inner().into_inner().into_left_after_take()?,
					config,
					decompression_buffer,
				)
			}
			#[cfg(feature = "zstandard")]
			CompressionCodecState::Zstandard {
				deserializer_state,
				decompression_buffer,
			} => {
				let (reader, config) = deserializer_state.into_inner();
				let mut reader = reader.into_inner();
				// With zstandard, we need to manually drive the reader to the end by asking to
				// deserialize the rest of the data. If the serialized avro is correct, this
				// should not yield anything, but if we don't, it won't read the last bytes of
				// the compressed data, resulting in an error when checking that there's no data
				// left in the block.
				// https://github.com/gyscos/zstd-rs/issues/255
				let mut drive_reader_to_end_buf = [0];
				let read = std::io::Read::read(&mut reader, &mut drive_reader_to_end_buf).map_err(
					|e| {
						<de::DeError as serde::de::Error>::custom(format_args!(
							"Zstandard error when driving decompressor to end: {e}"
						))
					},
				)?;
				if read != 0 {
					return Err(de::DeError::new(
						"Zstandard decompression error: There's decompressed data left in the \
							block after reading the whole avro block out of it",
					));
				}
				(
					reader.finish().into_left_after_take()?,
					config,
					decompression_buffer,
				)
			}
		})
	}
}
