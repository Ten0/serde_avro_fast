use serde_avro_fast::object_container_file_encoding::CompressionCodec;

#[test]
fn compression_codec_serializes_properly() {
	let codec = CompressionCodec::Null;
	let serialized = serde_json::to_string(&codec).unwrap();
	assert_eq!(serialized, "\"null\"");
	let codec = CompressionCodec::Deflate;
	let serialized = serde_json::to_string(&codec).unwrap();
	assert_eq!(serialized, "\"deflate\"");
	let codec = CompressionCodec::Bzip2;
	let serialized = serde_json::to_string(&codec).unwrap();
	assert_eq!(serialized, "\"bzip2\"");
	let codec = CompressionCodec::Snappy;
	let serialized = serde_json::to_string(&codec).unwrap();
	assert_eq!(serialized, "\"snappy\"");
	let codec = CompressionCodec::Xz;
	let serialized = serde_json::to_string(&codec).unwrap();
	assert_eq!(serialized, "\"xz\"");
	let codec = CompressionCodec::Zstandard;
	let serialized = serde_json::to_string(&codec).unwrap();
	assert_eq!(serialized, "\"zstandard\"");
}
