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
				CompressionCodec::Bzip2 => Kind::Bzip2 { len: 0 },
				#[cfg(feature = "snappy")]
				CompressionCodec::Snappy => Kind::Snappy {
					encoder: snap::raw::Encoder::new(),
				},
				#[cfg(feature = "xz")]
				CompressionCodec::Xz => Kind::Xz { len: 0 },
				#[cfg(feature = "zstandard")]
				CompressionCodec::Zstandard => Kind::Zstandard { encoder: None },
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
	Bzip2 {
		len: usize,
	},
	#[cfg(feature = "snappy")]
	Snappy {
		encoder: snap::raw::Encoder,
	},
	#[cfg(feature = "xz")]
	Xz {
		len: usize,
	},
	#[cfg(feature = "zstandard")]
	Zstandard {
		encoder: Option<zstd::stream::raw::Encoder<'static>>,
	},
}

impl CompressionCodecState {
	/// If none, this means the codec is Null and the original
	/// buffer should be used instead
	pub(super) fn compressed_buffer(&self) -> Option<&[u8]> {
		match self.kind {
			Kind::Null => None,
			#[cfg(feature = "deflate")]
			Kind::Deflate { ref compress } => Some(&self.output_vec[..compress.total_out() as usize]),
			#[cfg(feature = "bzip2")]
			Kind::Bzip2 { len } => Some(&self.output_vec[..len]),
			#[cfg(feature = "snappy")]
			Kind::Snappy { .. } => Some(&self.output_vec),
			#[cfg(feature = "xz")]
			Kind::Xz { len } => Some(&self.output_vec[..len]),
			#[cfg(feature = "zstandard")]
			Kind::Zstandard { .. } => Some(&self.output_vec),
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
				loop {
					let before_in = compress.total_in() as usize;
					let status = compress
						.compress(
							input,
							&mut self.output_vec[compress.total_out() as usize..],
							flate2::FlushCompress::Finish,
						)
						.map_err(|deflate_error| error("Deflate", &deflate_error))?;
					let written = compress.total_in() as usize - before_in;
					match status {
						flate2::Status::Ok => {
							// There may be more to write.
							// That may be true even if the input is empty, because flate2
							// may have buffered some input.
							input = &input[written..];
							self.output_vec.resize(self.output_vec.len() * 2, 0);
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
							break;
						}
					}
				}
			}
			#[cfg(feature = "bzip2")]
			Kind::Bzip2 { len } => {
				let mut compress = bzip2::Compress::new(bzip2::Compression::default(), {
					// Default in BufRead::bzencoder
					30
				});
				if self.output_vec.is_empty() {
					self.output_vec.resize(32 * 1024, 0);
				}
				let mut input = input;
				loop {
					let before_in = compress.total_in() as usize;
					let status = compress
						.compress(
							input,
							&mut self.output_vec[compress.total_out() as usize..],
							bzip2::Action::Finish,
						)
						.map_err(|deflate_error| error("Bzip2", &deflate_error))?;
					let written = compress.total_in() as usize - before_in;
					match status {
						bzip2::Status::MemNeeded => {
							// There may be more to write.
							// That may be true even if the input is empty, because bzip2
							// may have buffered some input.
							input = &input[written..];
							self.output_vec.resize(self.output_vec.len() * 2, 0);
						}
						bzip2::Status::FlushOk | bzip2::Status::RunOk | bzip2::Status::Ok => {
							return Err(error(
								"Bzip2",
								&format_args!("got unexpected status from bzip2: {status:?}"),
							));
						}
						bzip2::Status::FinishOk | bzip2::Status::StreamEnd => {
							assert_eq!(input.len(), written as usize);
							*len = compress.total_out() as usize;
							break;
						}
					}
				}
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
			Kind::Xz { len } => {
				let mut compress =
					xz2::stream::Stream::new_easy_encoder(6, xz2::stream::Check::Crc64)
						.map_err(|err| error("Xz", &err))?;
				if self.output_vec.is_empty() {
					self.output_vec.resize(32 * 1024, 0);
				}
				let mut input = input;
				loop {
					let before_in = compress.total_in() as usize;
					let status = compress
						.process(
							input,
							&mut self.output_vec[compress.total_out() as usize..],
							xz2::stream::Action::Finish,
						)
						.map_err(|deflate_error| error("Bzip2", &deflate_error))?;
					let written = compress.total_in() as usize - before_in;
					match status {
						xz2::stream::Status::MemNeeded => {
							// There may be more to write.
							// That may be true even if the input is empty, because bzip2
							// may have buffered some input.
							input = &input[written..];
							self.output_vec.resize(self.output_vec.len() * 2, 0);
						}
						xz2::stream::Status::Ok | xz2::stream::Status::GetCheck => {
							return Err(error(
								"Xz",
								&format_args!("got unexpected status from xz2: {status:?}"),
							));
						}
						xz2::stream::Status::StreamEnd => {
							assert_eq!(input.len(), written as usize);
							*len = compress.total_out() as usize;
							break;
						}
					}
				}
			}
			#[cfg(feature = "zstandard")]
			Kind::Zstandard { encoder } => {
				use zstd::stream::raw::Operation;
				let encoder = match encoder {
					None => {
						*encoder = Some(zstd::stream::raw::Encoder::new(0).map_err(|err| {
							error("zstandard", &format_args!("zstd error on init: {err}"))
						})?);
						encoder.as_mut().unwrap()
					}
					Some(encoder) => {
						encoder.reinit().map_err(|err| {
							error("zstandard", &format_args!("zstd error on reinit: {err}"))
						})?;
						encoder
					}
				};

				if self.output_vec.is_empty() {
					// Default buffer length in flate2
					self.output_vec.resize(32 * 1024, 0);
				}
				let mut input = zstd::stream::raw::InBuffer::around(input);
				let mut output = zstd::stream::raw::OutBuffer::around(&mut self.output_vec);
				let mut finished_frame = false;
				while input.pos() < input.src.len() {
					finished_frame = encoder.run(&mut input, &mut output).map_err(|err| {
						error("zstandard", &format_args!("zstd error on run: {err}"))
					})? == 0;
				}
				loop {
					if encoder.finish(&mut output, finished_frame).map_err(|err| {
						error("zstandard", &format_args!("zstd error on finish: {err}"))
					})? == 0
					{
						break;
					}
				}
				let pos = output.pos();
				self.output_vec.truncate(pos);
			}
		}
		Ok(())
	}
}
