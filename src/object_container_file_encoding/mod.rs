//! Support for [object container files](https://avro.apache.org/docs/current/specification/#object-container-files)
//!
//! This is typically what you want when reading/writing avro files with
//! multiple objects.
//!
//! See [`Reader`] and [`Writer`] documentations for their respective examples.

mod reader;
mod writer;

pub use {reader::*, writer::*};

/// The compression codec used to compress blocks.
#[derive(Clone, Copy, Debug, Eq, PartialEq, serde_derive::Deserialize, serde_derive::Serialize)]
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
