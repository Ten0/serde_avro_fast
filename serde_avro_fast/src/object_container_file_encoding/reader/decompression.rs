use crate::{
	de::{self, read::take::IntoLeftAfterTake, DeserializerConfig, DeserializerState},
	object_container_file_encoding::CompressionCodec,
};

impl CompressionCodec {
	pub(super) fn state<'de, 's, R>(
		self,
		reader: R,
		config: DeserializerConfig<'s>,
		decompression_buffer: Vec<u8>,
		block_size: usize,
	) -> Result<DecompressionState<R>, de::DeError>
	where
		R: de::read::take::Take + de::read::ReadSlice<'de>,
		<R as de::read::take::Take>::Take: de::read::ReadSlice<'de> + std::io::BufRead,
	{
		Ok(match self {
			CompressionCodec::Null => DecompressionState::Null {
				deserializer_state: de::DeserializerState::with_config(
					de::read::take::Take::take(reader, block_size)?,
					config,
				),
				decompression_buffer,
			},
			#[cfg(feature = "deflate")]
			CompressionCodec::Deflate => DecompressionState::BufReader {
				deserializer_state: de::DeserializerState::with_config(
					de::read::ReaderRead::new(std::io::BufReader::new(
						DecompressionReaderForBufReader::Deflate(
							flate2::bufread::DeflateDecoder::new(de::read::take::Take::take(
								reader, block_size,
							)?),
						),
					)),
					config,
				),
				decompression_buffer,
			},
			#[cfg(feature = "bzip2")]
			CompressionCodec::Bzip2 => DecompressionState::BufReader {
				deserializer_state: de::DeserializerState::with_config(
					de::read::ReaderRead::new(std::io::BufReader::new(
						DecompressionReaderForBufReader::Bzip2(bzip2::bufread::BzDecoder::new(
							de::read::take::Take::take(reader, block_size)?,
						)),
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
						let written = snap::raw::Decoder::new()
							.decompress(compressed_slice, &mut decompression_buffer)
							.map_err(snappy_to_de_error)?;
						if written != decompression_buffer.len() {
							return Err(de::DeError::new(
								"Snappy decompression error: incorrect decompressed size",
							));
						}
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
				DecompressionState::DecompressedOnConstruction {
					deserializer_state: de::DeserializerState::with_config(
						de::read::ReaderRead::new(std::io::Cursor::new(decompression_buffer)),
						config,
					),
					source_reader: reader,
				}
			}
			#[cfg(feature = "xz")]
			CompressionCodec::Xz => DecompressionState::BufReader {
				deserializer_state: de::DeserializerState::with_config(
					de::read::ReaderRead::new(std::io::BufReader::new(
						DecompressionReaderForBufReader::Xz(xz2::bufread::XzDecoder::new(
							de::read::take::Take::take(reader, block_size)?,
						)),
					)),
					config,
				),
				decompression_buffer,
			},
			#[cfg(feature = "zstandard")]
			CompressionCodec::Zstandard => DecompressionState::BufReader {
				deserializer_state: de::DeserializerState::with_config(
					de::read::ReaderRead::new(std::io::BufReader::new(
						DecompressionReaderForBufReader::Zstandard(
							zstd::stream::read::Decoder::with_buffer(de::read::take::Take::take(
								reader, block_size,
							)?)
							.map_err(de::DeError::io)?,
						),
					)),
					config,
				),
				decompression_buffer,
			},
		})
	}
}

pub(super) enum DecompressionState<'s, R: de::read::take::Take> {
	Null {
		deserializer_state: DeserializerState<'s, R::Take>,
		decompression_buffer: Vec<u8>,
	},
	/// Any decompression protocol that is decompressed on the fly to a
	/// BufReader as that buffer gets emptied by the deserializer (streaming
	/// inside block)
	/// The dispatch is inexpensive because it's only done every time the buffer
	/// is emptied, which is not very often
	#[cfg(any(
		feature = "deflate",
		feature = "bzip2",
		feature = "xz",
		feature = "zstandard"
	))]
	BufReader {
		deserializer_state: DeserializerState<
			's,
			de::read::ReaderRead<std::io::BufReader<DecompressionReaderForBufReader<R::Take>>>,
		>,
		decompression_buffer: Vec<u8>,
	},
	/// Any decompression protocol that is decompressed as we build the
	/// `DecompressionState` It is decompressed in a `Vec<u8>` and then we use a
	/// `Cursor` to plug it in a `Deserializer`
	#[cfg(feature = "snappy")]
	DecompressedOnConstruction {
		deserializer_state: DeserializerState<'s, de::read::ReaderRead<std::io::Cursor<Vec<u8>>>>,
		source_reader: R,
	},
}

pub(super) enum DecompressionReaderForBufReader<R: std::io::BufRead> {
	#[cfg(feature = "deflate")]
	Deflate(flate2::bufread::DeflateDecoder<R>),
	#[cfg(feature = "bzip2")]
	Bzip2(bzip2::bufread::BzDecoder<R>),
	#[cfg(feature = "xz")]
	Xz(xz2::bufread::XzDecoder<R>),
	#[cfg(feature = "zstandard")]
	Zstandard(zstd::stream::read::Decoder<'static, R>),
}

impl<'s, R: de::read::take::Take> DecompressionState<'s, R> {
	pub(super) fn into_source_reader_and_config(
		self,
	) -> Result<(R, DeserializerConfig<'s>, Vec<u8>), de::DeError> {
		Ok(match self {
			DecompressionState::Null {
				deserializer_state,
				decompression_buffer,
			} => {
				let (reader, config) = deserializer_state.into_inner();
				(reader.into_left_after_take()?, config, decompression_buffer)
			}
			#[cfg(any(
				feature = "deflate",
				feature = "bzip2",
				feature = "xz",
				feature = "zstandard"
			))]
			DecompressionState::BufReader {
				deserializer_state,
				decompression_buffer,
			} => {
				let (reader, config) = deserializer_state.into_inner();
				(
					(match reader.into_inner().into_inner() {
						#[cfg(feature = "deflate")]
						DecompressionReaderForBufReader::Deflate(reader) => reader.into_inner(),
						#[cfg(feature = "bzip2")]
						DecompressionReaderForBufReader::Bzip2(reader) => reader.into_inner(),
						#[cfg(feature = "xz")]
						DecompressionReaderForBufReader::Xz(reader) => reader.into_inner(),
						#[cfg(feature = "zstandard")]
						DecompressionReaderForBufReader::Zstandard(mut reader) => {
							// With zstandard, we need to manually drive the reader to the end by
							// asking to deserialize the rest of the data. If the serialized avro is
							// correct, this should not yield anything, but if we don't, it won't
							// read the last bytes of the compressed data, resulting in an error
							// when checking that there's no data left in the block.
							// https://github.com/gyscos/zstd-rs/issues/255
							let mut drive_reader_to_end_buf = [0];
							let read =
								std::io::Read::read(&mut reader, &mut drive_reader_to_end_buf)
									.map_err(|e| {
										de::DeError::custom_io(
											"Zstandard error when driving decompressor to end",
											e,
										)
									})?;
							if read != 0 {
								return Err(de::DeError::new(
									"Zstandard decompression error: There's \
									decompressed data left in the \
									block after reading the whole avro block out of it",
								));
							}
							reader.finish()
						}
					})
					.into_left_after_take()?,
					config,
					decompression_buffer,
				)
			}
			#[cfg(feature = "snappy")]
			DecompressionState::DecompressedOnConstruction {
				deserializer_state,
				source_reader,
			} => {
				let (reader, config) = deserializer_state.into_inner();
				(source_reader, config, reader.into_inner().into_inner())
			}
		})
	}
}

macro_rules! dispatch {
	($self: ident, $function: ident ($($arg:ident)*)) => {
		match $self {
			#[cfg(feature = "deflate")]
			DecompressionReaderForBufReader::Deflate(reader) => reader.$function($($arg)*),
			#[cfg(feature = "bzip2")]
			DecompressionReaderForBufReader::Bzip2(reader) => reader.$function($($arg)*),
			#[cfg(feature = "xz")]
			DecompressionReaderForBufReader::Xz(reader) => reader.$function($($arg)*),
			#[cfg(feature = "zstandard")]
			DecompressionReaderForBufReader::Zstandard(reader) => reader.$function($($arg)*),
		}
	};
}
impl<R: std::io::BufRead> std::io::Read for DecompressionReaderForBufReader<R> {
	fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
		dispatch!(self, read(buf))
	}
	fn read_vectored(&mut self, bufs: &mut [std::io::IoSliceMut<'_>]) -> std::io::Result<usize> {
		dispatch!(self, read_vectored(bufs))
	}
}
