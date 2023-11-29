use crate::{object_container_file_encoding::CompressionCodec, ser::SerError};

pub(super) struct CompressionCodecState {
	output_vec: Vec<u8>,
	kind: Kind,
}

impl CompressionCodecState {
	pub(super) fn new(compression_codec: CompressionCodec) -> Self {
		Self {
			output_vec: Vec::new(),
			kind: match compression_codec {
				CompressionCodec::Null => Kind::Null,
				#[cfg(feature = "deflate")]
				CompressionCodec::Deflate => Kind::Deflate {
					compress: flate2::Compress::new(flate2::Compression::default(), false),
				},
				#[cfg(feature = "bzip2")]
				CompressionCodec::Bzip2 => Kind::Bzip2,
				#[cfg(feature = "snappy")]
				CompressionCodec::Snappy => Kind::Snappy {
					encoder: snap::raw::Encoder::new(),
				},
				#[cfg(feature = "xz")]
				CompressionCodec::Xz => Kind::Xz,
				#[cfg(feature = "zstandard")]
				CompressionCodec::Zstandard => Kind::Zstandard,
			},
		}
	}
}

enum Kind {
	Null,
	#[cfg(feature = "deflate")]
	Deflate {
		compress: flate2::Compress,
	},
	#[cfg(feature = "bzip2")]
	Bzip2,
	#[cfg(feature = "snappy")]
	Snappy {
		encoder: snap::raw::Encoder,
	},
	#[cfg(feature = "xz")]
	Xz,
	#[cfg(feature = "zstandard")]
	Zstandard,
}

impl CompressionCodecState {
	/// If none, this means the codec is Null and the original
	/// buffer should be used instead
	pub(super) fn compressed_buffer(&self) -> Option<&[u8]> {
		match self.kind {
			Kind::Null => None,
			_ => Some(&self.output_vec),
		}
	}

	pub(super) fn encode(&mut self, input: &[u8]) -> Result<(), SerError> {
		fn error(protocol: &str, error: &dyn std::fmt::Display) -> SerError {
			<SerError as serde::ser::Error>::custom(format_args!(
				"{protocol} decompression error: {error}"
			))
		}
		match &mut self.kind {
			Kind::Null => {}
			#[cfg(feature = "deflate")]
			Kind::Deflate { compress } => {
				compress.reset();
				if self.output_vec.is_empty() {
					// Default buffer length in flate2
					self.output_vec.resize(32 * 1024, 0);
				}
				let mut input = input;
				while !input.is_empty() {
					let before_in = compress.total_in() as usize;
					let status = compress
						.compress(
							input,
							&mut self.output_vec[before_in..],
							flate2::FlushCompress::Finish,
						)
						.map_err(|deflate_error| error("Deflate", &deflate_error))?;
					let after_in = compress.total_in() as usize;
					let written = after_in - before_in;
					match status {
						flate2::Status::Ok => {
							// There may be more to write.
							input = &input[written..];
							assert!(written > 0);
							if !input.is_empty() {
								self.output_vec.reserve(self.output_vec.len());
							}
						}
						flate2::Status::BufError => {
							// miniz_oxide documents that this can only happen:
							// If the size of the output slice is empty or no progress was made due
							// to lack of expected input data, or if called without MZFlush::Finish
							// after the compression was already finished.
							return Err(error("Deflate", &"got BufError from flate2"));
						}
						flate2::Status::StreamEnd => {
							assert_eq!(input.len(), written as usize);
							self.output_vec.truncate(after_in);
							break;
						}
					}
				}
			}
			#[cfg(feature = "bzip2")]
			Kind::Bzip2 => {
				todo!()
			}
			#[cfg(feature = "snappy")]
			Kind::Snappy { ref mut encoder } => {
				self.output_vec
					.resize(snap::raw::max_compress_len(input.len()), 0);
				let n = encoder
					.compress(input, &mut self.output_vec)
					.map_err(|snappy_error| error("Snappy", &snappy_error))?;
				self.output_vec.truncate(n);
				self.output_vec
					.extend(crc32fast::hash(&input).to_be_bytes());
			}
			#[cfg(feature = "xz")]
			Kind::Xz => {
				todo!()
			}
			#[cfg(feature = "zstandard")]
			Kind::Zstandard => {
				todo!()
			}
		}
		Ok(())
	}
}
