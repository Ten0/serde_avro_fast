#![allow(missing_docs)]

use serde_avro_fast::Schema;

const SCHEMA: &str = r#"
{
	"fields": [
		{
			"type": {"type": "array", "items": "int"},
			"name": "a"
		},
		{
			"type": {"type": "array", "items": "int"},
			"name": "b"
		},
		{
			"type": {"type": "array", "items": "int"},
			"name": "cd"
		}
	],
	"type": "record",
	"name": "test_skip"
}
"#;

#[derive(Debug, PartialEq, Eq, serde::Deserialize)]
struct TestSkip {
	a: Vec<i32>,
	cd: Vec<i32>,
}

#[test]
fn skip_block() {
	let schema: Schema = SCHEMA.parse().unwrap();
	let input: &[u8] = &[1, 2, 20, 0, 1, 2, 30, 1, 4, 31, 32, 0, 4, 40, 50, 0, 0xFF];
	let expected = TestSkip {
		a: vec![10],
		cd: vec![20, 25],
	};

	let deserialized: TestSkip = serde_avro_fast::from_datum_slice(input, &schema).unwrap();
	assert_eq!(deserialized, expected);

	let mut reader = &input[..];
	let deserialized: TestSkip = serde_avro_fast::from_datum_reader(&mut reader, &schema).unwrap();
	assert_eq!(deserialized, expected);
	// Also make sure that the reader stopped at the end of the block
	assert_eq!(reader, &[0xFF]);
}
