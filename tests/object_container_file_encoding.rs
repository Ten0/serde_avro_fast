//! Tests are ported over from from
//! https://github.com/apache/avro/blob/6d90ec4b1c4ba47dba16650c54b4c15265016190/lang/rust/avro/src/reader.rs#L470
//! updated to match this crate's interface

use serde_avro_fast::{from_datum_reader, from_datum_slice, object_container_file_encoding::Reader, Schema};

use {pretty_assertions::assert_eq, serde::Deserialize};

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
	79u8, 98u8, 106u8, 1u8, 4u8, 22u8, 97u8, 118u8, 114u8, 111u8, 46u8, 115u8, 99u8, 104u8, 101u8, 109u8, 97u8, 222u8,
	1u8, 123u8, 34u8, 116u8, 121u8, 112u8, 101u8, 34u8, 58u8, 34u8, 114u8, 101u8, 99u8, 111u8, 114u8, 100u8, 34u8,
	44u8, 34u8, 110u8, 97u8, 109u8, 101u8, 34u8, 58u8, 34u8, 116u8, 101u8, 115u8, 116u8, 34u8, 44u8, 34u8, 102u8,
	105u8, 101u8, 108u8, 100u8, 115u8, 34u8, 58u8, 91u8, 123u8, 34u8, 110u8, 97u8, 109u8, 101u8, 34u8, 58u8, 34u8,
	97u8, 34u8, 44u8, 34u8, 116u8, 121u8, 112u8, 101u8, 34u8, 58u8, 34u8, 108u8, 111u8, 110u8, 103u8, 34u8, 44u8, 34u8,
	100u8, 101u8, 102u8, 97u8, 117u8, 108u8, 116u8, 34u8, 58u8, 52u8, 50u8, 125u8, 44u8, 123u8, 34u8, 110u8, 97u8,
	109u8, 101u8, 34u8, 58u8, 34u8, 98u8, 34u8, 44u8, 34u8, 116u8, 121u8, 112u8, 101u8, 34u8, 58u8, 34u8, 115u8, 116u8,
	114u8, 105u8, 110u8, 103u8, 34u8, 125u8, 93u8, 125u8, 20u8, 97u8, 118u8, 114u8, 111u8, 46u8, 99u8, 111u8, 100u8,
	101u8, 99u8, 8u8, 110u8, 117u8, 108u8, 108u8, 0u8, 94u8, 61u8, 54u8, 221u8, 190u8, 207u8, 108u8, 180u8, 158u8,
	57u8, 114u8, 40u8, 173u8, 199u8, 228u8, 239u8, 4u8, 20u8, 54u8, 6u8, 102u8, 111u8, 111u8, 84u8, 6u8, 98u8, 97u8,
	114u8, 94u8, 61u8, 54u8, 221u8, 190u8, 207u8, 108u8, 180u8, 158u8, 57u8, 114u8, 40u8, 173u8, 199u8, 228u8, 239u8,
];

#[derive(Deserialize, Debug, PartialEq, Eq)]
struct SchemaRecord<'a> {
	a: i64,
	b: &'a str,
}

#[test]
fn test_from_avro_datum() {
	let schema: Schema = SCHEMA.parse().unwrap();
	let encoded: &'static [u8] = &[54, 6, 102, 111, 111];

	assert_eq!(
		from_datum_slice::<SchemaRecord>(encoded, &schema).unwrap(),
		SchemaRecord { a: 27, b: "foo" }
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
	// so I'm not adding support for this - still testing that it works if adding the necessary zeroes
	// in the encoded below
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

	let expected = vec![SchemaRecord { a: 27, b: "foo" }, SchemaRecord { a: 42, b: "bar" }];
	let res: Vec<SchemaRecord> = reader
		.deserialize_borrowed::<SchemaRecord>()
		.collect::<Result<_, _>>()
		.unwrap();

	assert_eq!(expected, res);
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

#[cfg(not(feature = "snappy"))]
#[test]
fn test_avro_3549_read_not_enabled_codec() {
	let snappy_compressed_avro: &[u8] = &[
		79, 98, 106, 1, 4, 22, 97, 118, 114, 111, 46, 115, 99, 104, 101, 109, 97, 210, 1, 123, 34, 102, 105, 101, 108,
		100, 115, 34, 58, 91, 123, 34, 110, 97, 109, 101, 34, 58, 34, 110, 117, 109, 34, 44, 34, 116, 121, 112, 101,
		34, 58, 34, 115, 116, 114, 105, 110, 103, 34, 125, 93, 44, 34, 110, 97, 109, 101, 34, 58, 34, 101, 118, 101,
		110, 116, 34, 44, 34, 110, 97, 109, 101, 115, 112, 97, 99, 101, 34, 58, 34, 101, 120, 97, 109, 112, 108, 101,
		110, 97, 109, 101, 115, 112, 97, 99, 101, 34, 44, 34, 116, 121, 112, 101, 34, 58, 34, 114, 101, 99, 111, 114,
		100, 34, 125, 20, 97, 118, 114, 111, 46, 99, 111, 100, 101, 99, 12, 115, 110, 97, 112, 112, 121, 0, 213, 209,
		241, 208, 200, 110, 164, 47, 203, 25, 90, 235, 161, 167, 195, 177, 2, 20, 4, 12, 6, 49, 50, 51, 115, 38, 58, 0,
		213, 209, 241, 208, 200, 110, 164, 47, 203, 25, 90, 235, 161, 167, 195, 177,
	];

	if let Err(err) = Reader::from_slice(snappy_compressed_avro) {
		assert_eq!("Failed to validate avro object container file header: unknown variant `snappy`, expected `null` or `deflate`", err.to_string());
	} else {
		panic!("Expected an error in the reading of the codec!");
	}
}
