//! Tests are ported over from from
//! https://github.com/apache/avro/blob/6d90ec4b1c4ba47dba16650c54b4c15265016190/lang/rust/avro/src/reader.rs#L470
//! updated to match this crate's interface

use {
	serde_avro_fast::{
		from_datum_reader, from_datum_slice,
		object_container_file_encoding::{Compression, CompressionLevel, Reader, WriterBuilder},
		ser::SerializerConfig,
		Schema,
	},
	std::borrow::Cow,
};

use {
	pretty_assertions::assert_eq,
	serde::{Deserialize, Serialize},
};

const SCHEMA: &str = r#"
    {
      "type": "record",
      "name": "test",
      "fields": [
        {
          "name": "a",
          "type": "long",
          "default": 42
        },
        {
          "name": "b",
          "type": "string"
        }
      ]
    }
    "#;
const UNION_SCHEMA: &str = r#"["null", "long"]"#;
const ENCODED: &[u8] = &[
	79, 98, 106, 1, 4, 22, 97, 118, 114, 111, 46, 115, 99, 104, 101, 109, 97, 222, 1, 123, 34, 116,
	121, 112, 101, 34, 58, 34, 114, 101, 99, 111, 114, 100, 34, 44, 34, 110, 97, 109, 101, 34, 58,
	34, 116, 101, 115, 116, 34, 44, 34, 102, 105, 101, 108, 100, 115, 34, 58, 91, 123, 34, 110, 97,
	109, 101, 34, 58, 34, 97, 34, 44, 34, 116, 121, 112, 101, 34, 58, 34, 108, 111, 110, 103, 34,
	44, 34, 100, 101, 102, 97, 117, 108, 116, 34, 58, 52, 50, 125, 44, 123, 34, 110, 97, 109, 101,
	34, 58, 34, 98, 34, 44, 34, 116, 121, 112, 101, 34, 58, 34, 115, 116, 114, 105, 110, 103, 34,
	125, 93, 125, 20, 97, 118, 114, 111, 46, 99, 111, 100, 101, 99, 8, 110, 117, 108, 108, 0, 94,
	61, 54, 221, 190, 207, 108, 180, 158, 57, 114, 40, 173, 199, 228, 239, 4, 20, 54, 6, 102, 111,
	111, 84, 6, 98, 97, 114, 94, 61, 54, 221, 190, 207, 108, 180, 158, 57, 114, 40, 173, 199, 228,
	239,
];

#[derive(Deserialize, Serialize, Debug, PartialEq, Eq)]
struct SchemaRecord<'a> {
	a: i64,
	#[serde(borrow)]
	b: Cow<'a, str>,
}

#[test]
fn test_from_avro_datum() {
	let schema: Schema = SCHEMA.parse().unwrap();
	let encoded: &'static [u8] = &[54, 6, 102, 111, 111];

	assert_eq!(
		from_datum_slice::<SchemaRecord>(encoded, &schema).unwrap(),
		SchemaRecord {
			a: 27,
			b: "foo".into()
		}
	);
}

#[test]
fn test_from_avro_datum_with_union_to_struct() {
	const TEST_RECORD_SCHEMA: &str = r#"
    {
      "type": "record",
      "name": "test",
      "fields": [
        {
          "name": "a",
          "type": "long",
          "default": 42
        },
        {
          "name": "b",
          "type": "string"
        },
        {
            "name": "a_nullable_array",
            "type": ["null", {"type": "array", "items": {"type": "string"}}],
            "default": null
        },
        {
            "name": "a_nullable_boolean",
            "type": ["null", {"type": "boolean"}],
            "default": null
        },
        {
            "name": "a_nullable_string",
            "type": ["null", {"type": "string"}],
            "default": null
        }
      ]
    }
    "#;
	#[derive(Default, Debug, Deserialize, PartialEq, Eq)]
	struct TestRecord3240 {
		a: i64,
		b: String,
		a_nullable_array: Option<Vec<String>>,
		// we are missing the 'a_nullable_boolean' field to simulate missing keys
		// a_nullable_boolean: Option<bool>,
		a_nullable_string: Option<String>,
	}

	let schema: Schema = TEST_RECORD_SCHEMA.parse().unwrap();
	// This is actually a test originally introduced to check for
	// https://github.com/apache/avro/pull/1379 but I don't think it's correct:
	// https://github.com/apache/avro/pull/1379#issuecomment-1412608332
	// so I'm not adding support for this - still testing that it works if adding
	// the necessary zeroes in the encoded below
	let encoded: &'static [u8] = &[54, 6, 102, 111, 111, 0, 0, 0];

	let expected_record = TestRecord3240 {
		a: 27i64,
		b: String::from("foo"),
		a_nullable_array: None,
		a_nullable_string: None,
	};

	assert_eq!(
		from_datum_reader::<&[u8], TestRecord3240>(encoded, &schema).unwrap(),
		expected_record
	);
}

#[test]
fn test_null_union() {
	let schema: Schema = UNION_SCHEMA.parse().unwrap();
	let encoded: &'static [u8] = &[2, 0];

	assert_eq!(from_datum_slice::<i64>(encoded, &schema).unwrap(), 0);
}

#[test]
fn test_reader_iterator() {
	//let schema: Schema = SCHEMA.parse().unwrap();
	let mut reader = Reader::from_slice(ENCODED).unwrap();

	let expected = &[
		SchemaRecord {
			a: 27,
			b: "foo".into(),
		},
		SchemaRecord {
			a: 42,
			b: "bar".into(),
		},
	];
	let res: Vec<SchemaRecord> = reader
		.deserialize_borrowed::<SchemaRecord>()
		.collect::<Result<_, _>>()
		.unwrap();
	std::mem::drop(reader);

	assert_eq!(expected.as_slice(), res.as_slice());
	assert!(res.iter().all(|r| matches!(r.b, Cow::Borrowed(_))));
}

fn round_trip_writer(compression_codec: Compression, approx_block_size: u32) {
	let input = &[
		SchemaRecord {
			a: 27,
			b: "foo".into(),
		},
		SchemaRecord {
			a: 42,
			b: "bar".into(),
		},
	];

	let schema: Schema = SCHEMA.parse().unwrap();

	let mut serializer_config = SerializerConfig::new(&schema);
	let mut writer = WriterBuilder::new(&mut serializer_config)
		.compression(compression_codec)
		.approx_block_size(approx_block_size)
		.build(Vec::new())
		.unwrap();
	writer.serialize_all(input.iter()).unwrap();
	let serialized = writer.into_inner().unwrap();

	let mut reader = Reader::from_slice(&serialized).unwrap();
	let res: Vec<SchemaRecord> = reader
		.deserialize_borrowed::<SchemaRecord>()
		.collect::<Result<_, _>>()
		.unwrap();

	assert_eq!(input.as_slice(), res.as_slice());
	match compression_codec {
		Compression::Null => assert!(res.iter().all(|r| matches!(r.b, Cow::Borrowed(_)))),
		_ => assert!(res.iter().all(|r| matches!(r.b, Cow::Owned(_)))),
	}
}

#[test]
fn test_writer_no_compression_regular_block_size() {
	round_trip_writer(Compression::Null, 64 * 1024);
}

#[test]
fn test_writer_no_compression_small_block_size() {
	round_trip_writer(Compression::Null, 1);
}

#[cfg(feature = "snappy")]
#[test]
fn test_writer_snappy() {
	round_trip_writer(Compression::Snappy, 64 * 1024);
	round_trip_writer(Compression::Snappy, 1);
}

#[cfg(feature = "deflate")]
#[test]
fn test_writer_deflate() {
	round_trip_writer(
		Compression::Deflate {
			level: CompressionLevel::default(),
		},
		64 * 1024,
	);
	round_trip_writer(
		Compression::Deflate {
			level: CompressionLevel::default(),
		},
		1,
	);
}

#[cfg(feature = "bzip2")]
#[test]
fn test_writer_bzip2() {
	round_trip_writer(
		Compression::Bzip2 {
			level: CompressionLevel::default(),
		},
		64 * 1024,
	);
	round_trip_writer(
		Compression::Bzip2 {
			level: CompressionLevel::default(),
		},
		1,
	);
}

#[cfg(feature = "xz")]
#[test]
fn test_writer_xz() {
	round_trip_writer(
		Compression::Xz {
			level: CompressionLevel::default(),
		},
		64 * 1024,
	);
	round_trip_writer(
		Compression::Xz {
			level: CompressionLevel::default(),
		},
		1,
	);
}

#[cfg(feature = "zstandard")]
#[test]
fn test_writer_zstandard() {
	round_trip_writer(
		Compression::Zstandard {
			level: CompressionLevel::default(),
		},
		64 * 1024,
	);
	round_trip_writer(
		Compression::Zstandard {
			level: CompressionLevel::default(),
		},
		1,
	);
}

#[test]
fn test_reader_invalid_header() {
	//let schema: Schema = SCHEMA.parse().unwrap();
	let invalid = &ENCODED[1..];
	assert!(matches!(
		Reader::from_slice(invalid),
		Err(serde_avro_fast::object_container_file_encoding::FailedToInitializeReader::NotAvroObjectContainerFile),
	));
}

#[test]
fn test_reader_invalid_block() {
	//let schema: Schema = SCHEMA.parse().unwrap();
	let invalid = &ENCODED[0..(ENCODED.len() - 19)];
	let mut reader = Reader::from_slice(invalid).unwrap();
	// TODO more precise err matching
	let res: Result<Vec<SchemaRecord>, _> = reader.deserialize_borrowed().collect();
	assert!(res.is_err());
}

#[test]
fn test_reader_empty_buffer() {
	let empty: &[u8] = &[];
	assert!(matches!(
		Reader::from_slice(empty),
		Err(serde_avro_fast::object_container_file_encoding::FailedToInitializeReader::FailedToDeserializeHeader(_)),
	));
}

#[test]
fn test_reader_only_header() {
	let invalid = &ENCODED[..165];
	let mut reader = Reader::from_slice(invalid).unwrap();
	// TODO more precise err matching
	let res: Result<Vec<SchemaRecord>, _> = reader.deserialize_borrowed().collect();
	assert!(res.is_err());
}

const SNAPPY_COMPRESSED_AVRO: &[u8] = &[
	79, 98, 106, 1, 4, 22, 97, 118, 114, 111, 46, 115, 99, 104, 101, 109, 97, 210, 1, 123, 34, 102,
	105, 101, 108, 100, 115, 34, 58, 91, 123, 34, 110, 97, 109, 101, 34, 58, 34, 110, 117, 109, 34,
	44, 34, 116, 121, 112, 101, 34, 58, 34, 115, 116, 114, 105, 110, 103, 34, 125, 93, 44, 34, 110,
	97, 109, 101, 34, 58, 34, 101, 118, 101, 110, 116, 34, 44, 34, 110, 97, 109, 101, 115, 112, 97,
	99, 101, 34, 58, 34, 101, 120, 97, 109, 112, 108, 101, 110, 97, 109, 101, 115, 112, 97, 99,
	101, 34, 44, 34, 116, 121, 112, 101, 34, 58, 34, 114, 101, 99, 111, 114, 100, 34, 125, 20, 97,
	118, 114, 111, 46, 99, 111, 100, 101, 99, 12, 115, 110, 97, 112, 112, 121, 0, 213, 209, 241,
	208, 200, 110, 164, 47, 203, 25, 90, 235, 161, 167, 195, 177, 2, 20, 4, 12, 6, 49, 50, 51, 115,
	38, 58, 0, 213, 209, 241, 208, 200, 110, 164, 47, 203, 25, 90, 235, 161, 167, 195, 177,
];
#[cfg(not(feature = "snappy"))]
#[test]
fn test_avro_3549_read_not_enabled_codec() {
	if let Err(err) = Reader::from_slice(SNAPPY_COMPRESSED_AVRO) {
		assert_eq!("Failed to validate avro object container file header: unknown variant `snappy`, expected `null` or `deflate`", err.to_string());
	} else {
		panic!("Expected an error in the reading of the codec!");
	}
}
#[cfg(feature = "snappy")]
#[test]
fn test_snappy() {
	let mut reader = Reader::from_slice(SNAPPY_COMPRESSED_AVRO).unwrap();
	let expected: Vec<serde_json::Value> = vec![serde_json::json!({"num": "123"})];
	let res: Vec<serde_json::Value> = reader
		.deserialize::<serde_json::Value>()
		.collect::<Result<_, _>>()
		.unwrap();

	assert_eq!(expected, res);
}
