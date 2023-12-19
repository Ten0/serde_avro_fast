//! Support for [object container files](https://avro.apache.org/docs/current/specification/#object-container-files)
//!
//! This is typically what you want when reading/writing avro files with
//! multiple objects.
//!
//! See [`Reader`] and [`Writer`] documentations for their respective examples.

mod reader;
mod writer;

pub use {reader::*, writer::*};

use std::num::NonZeroU8;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub enum Compression {
	// The `Null` codec simply passes through data uncompressed.
	Null,
	#[cfg(feature = "deflate")]
	/// The `Deflate` codec writes the data block using the deflate algorithm
	/// as specified in RFC 1951, and typically implemented using the zlib
	/// library. Note that this format (unlike the "zlib format" in RFC 1950)
	/// does not have a checksum.
	Deflate {
		level: CompressionLevel,
	},
	#[cfg(feature = "bzip2")]
	/// The `BZip2` codec uses [BZip2](https://sourceware.org/bzip2/)
	/// compression library.
	Bzip2 {
		level: CompressionLevel,
	},
	#[cfg(feature = "snappy")]
	/// The `Snappy` codec uses Google's [Snappy](http://google.github.io/snappy/)
	/// compression algorithm. Each compressed block is followed by the 4-byte,
	/// big-endian CRC32 checksum of the uncompressed data in the block.
	Snappy,
	#[cfg(feature = "xz")]
	/// The `Xz` codec uses [Xz utils](https://tukaani.org/xz/)
	/// compression library.
	Xz {
		level: CompressionLevel,
	},
	#[cfg(feature = "zstandard")]
	// The `zstandard` codec uses Facebook’s [Zstandard](https://facebook.github.io/zstd/) compression library
	Zstandard {
		level: CompressionLevel,
	},
}

/// Compression level to use for the compression algorithm
///
/// You may either specify a given number (1-9) or use the default compression
/// level.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct CompressionLevel {
	repr: NonZeroU8,
}
impl CompressionLevel {
	/// Specifies the compression level that will be used for the compression
	/// algorithms
	///
	/// Panics if `level` is lower than `1` or greater than `9`
	///
	/// This is because all algorithms expect compression levels between `1`
	/// (fast compression) and `9` (take as long as you'd like).
	pub const fn new(level: u8) -> Self {
		match NonZeroU8::new(level) {
			Some(n) if n.get() < 10 => Self { repr: n },
			_ => panic!("Compression level must be between 1 and 9"),
		}
	}

	/// Use the default compression level of the considered algorithm
	pub const fn default() -> Self {
		Self {
			repr: match NonZeroU8::new(u8::MAX) {
				Some(nonzero) => nonzero,
				None => unreachable!(),
			},
		}
	}

	#[allow(unused)]
	/// may be unused depending on which compression codecs features are enabled
	fn instantiate<T: Default, C: From<u8>, F: FnOnce(C) -> T>(self, f: F) -> T {
		match self.repr.get() {
			u8::MAX => T::default(),
			specified_compression_level => f(specified_compression_level.into()),
		}
	}

	#[allow(unused)]
	/// may be unused depending on which compression codecs features are enabled
	fn instantiate_nb<C: From<u8>>(self, default: C) -> C {
		match self.repr.get() {
			u8::MAX => default,
			specified_compression_level => specified_compression_level.into(),
		}
	}
}
impl Default for CompressionLevel {
	fn default() -> Self {
		CompressionLevel::default()
	}
}
impl std::fmt::Debug for CompressionLevel {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match self.repr.get() {
			u8::MAX => write!(f, "Default"),
			_ => write!(f, "{}", self.repr),
		}
	}
}

impl Compression {
	fn codec(&self) -> CompressionCodec {
		match self {
			Compression::Null => CompressionCodec::Null,
			#[cfg(feature = "deflate")]
			Compression::Deflate { .. } => CompressionCodec::Deflate,
			#[cfg(feature = "bzip2")]
			Compression::Bzip2 { .. } => CompressionCodec::Bzip2,
			#[cfg(feature = "snappy")]
			Compression::Snappy => CompressionCodec::Snappy,
			#[cfg(feature = "xz")]
			Compression::Xz { .. } => CompressionCodec::Xz,
			#[cfg(feature = "zstandard")]
			Compression::Zstandard { .. } => CompressionCodec::Zstandard,
		}
	}
}

/// The compression codec used to compress blocks.
#[derive(Clone, Copy, Debug, Eq, PartialEq, serde_derive::Deserialize, serde_derive::Serialize)]
#[serde(rename_all = "kebab-case")]
#[non_exhaustive]
enum CompressionCodec {
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

const HEADER_CONST: [u8; 4] = [b'O', b'b', b'j', 1u8];

#[derive(serde_derive::Deserialize, serde_derive::Serialize)]
struct Metadata<S, M> {
	#[serde(rename = "avro.schema")]
	schema: S,
	#[serde(rename = "avro.codec")]
	codec: CompressionCodec,
	#[serde(flatten)]
	user_metadata: M,
}
const METADATA_SCHEMA: &crate::schema::SchemaNode =
	&crate::schema::SchemaNode::Map(&crate::schema::SchemaNode::Bytes);

#[test]
fn compression_codec_serializes_properly() {
	let codec = CompressionCodec::Null;
	let serialized = serde_json::to_string(&codec).unwrap();
	assert_eq!(serialized, "\"null\"");

	#[cfg(feature = "deflate")]
	{
		let codec = CompressionCodec::Deflate;
		let serialized = serde_json::to_string(&codec).unwrap();
		assert_eq!(serialized, "\"deflate\"");
	}

	#[cfg(feature = "bzip2")]
	{
		let codec = CompressionCodec::Bzip2;
		let serialized = serde_json::to_string(&codec).unwrap();
		assert_eq!(serialized, "\"bzip2\"");
	}

	#[cfg(feature = "snappy")]
	{
		let codec = CompressionCodec::Snappy;
		let serialized = serde_json::to_string(&codec).unwrap();
		assert_eq!(serialized, "\"snappy\"");
	}

	#[cfg(feature = "xz")]
	{
		let codec = CompressionCodec::Xz;
		let serialized = serde_json::to_string(&codec).unwrap();
		assert_eq!(serialized, "\"xz\"");
	}

	#[cfg(feature = "zstandard")]
	{
		let codec = CompressionCodec::Zstandard;
		let serialized = serde_json::to_string(&codec).unwrap();
		assert_eq!(serialized, "\"zstandard\"");
	}
}
