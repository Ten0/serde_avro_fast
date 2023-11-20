use serde_avro_fast::object_container_file_encoding::CompressionCodec;

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
