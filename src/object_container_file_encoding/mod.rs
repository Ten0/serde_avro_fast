//! Support for [object container files](https://avro.apache.org/docs/current/specification/#object-container-files)
//!
//! This is typically what you want when reading/writing avro files with
//! multiple objects.
//!
//! # Reader example
//! ```
//! let avro_object_container_file_encoded: &[u8] = &[
//! 	79, 98, 106, 1, 4, 22, 97, 118, 114, 111, 46, 115, 99, 104, 101, 109, 97, 222, 1, 123, 34,
//! 	116, 121, 112, 101, 34, 58, 34, 114, 101, 99, 111, 114, 100, 34, 44, 34, 110, 97, 109, 101,
//! 	34, 58, 34, 116, 101, 115, 116, 34, 44, 34, 102, 105, 101, 108, 100, 115, 34, 58, 91, 123,
//! 	34, 110, 97, 109, 101, 34, 58, 34, 97, 34, 44, 34, 116, 121, 112, 101, 34, 58, 34, 108,
//! 	111, 110, 103, 34, 44, 34, 100, 101, 102, 97, 117, 108, 116, 34, 58, 52, 50, 125, 44, 123,
//! 	34, 110, 97, 109, 101, 34, 58, 34, 98, 34, 44, 34, 116, 121, 112, 101, 34, 58, 34, 115,
//! 	116, 114, 105, 110, 103, 34, 125, 93, 125, 20, 97, 118, 114, 111, 46, 99, 111, 100, 101,
//! 	99, 8, 110, 117, 108, 108, 0, 94, 61, 54, 221, 190, 207, 108, 180, 158, 57, 114, 40, 173,
//! 	199, 228, 239, 4, 20, 54, 6, 102, 111, 111, 84, 6, 98, 97, 114, 94, 61, 54, 221, 190, 207,
//! 	108, 180, 158, 57, 114, 40, 173, 199, 228, 239,
//! ];
//!
//! #[derive(serde_derive::Deserialize, Debug, PartialEq, Eq)]
//! struct SchemaRecord<'a> {
//! 	a: i64,
//! 	b: &'a str,
//! }
//!
//! let mut reader = serde_avro_fast::object_container_file_encoding::Reader::from_slice(
//! 	avro_object_container_file_encoded,
//! )
//! .expect("Failed to initialize reader");
//!
//! let expected = vec![
//! 	SchemaRecord { a: 27, b: "foo" },
//! 	SchemaRecord { a: 42, b: "bar" },
//! ];
//! let res: Vec<SchemaRecord> = reader
//! 	.deserialize_borrowed::<SchemaRecord>()
//! 	.collect::<Result<_, _>>()
//! 	.expect("Failed to deserialize a record");
//!
//! assert_eq!(expected, res);
//! ```

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
