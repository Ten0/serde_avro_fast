use serde_avro_fast::{
	from_single_object_reader, from_single_object_slice, schema::SchemaMut, Schema,
};

use apache_avro::types::Value;

use {lazy_static::lazy_static, pretty_assertions::assert_eq, serde::Deserialize};

const SCHEMA_STR: &str = r#"
	{
		"type":"record",
		"name":"TestSingleObjectWriterSerialize",
		"fields":[
			{
				"name":"a",
				"type":"long"
			},
			{
				"name":"b",
				"type":"double"
			},
			{
				"name":"c",
				"type":{
					"type":"array",
					"items":"string"
				}
			}
		]
	}
	"#;

lazy_static! {
	static ref SCHEMA: Schema = SCHEMA_STR.parse().unwrap();
	static ref EDITABLE_SCHEMA: SchemaMut = SCHEMA_STR.parse().unwrap();
	static ref APACHE_SCHEMA: apache_avro::Schema =
		apache_avro::Schema::parse_str(SCHEMA_STR).unwrap();
}

#[derive(Deserialize, Clone, PartialEq, Debug)]
struct TestSingleObjectReader {
	a: i64,
	b: f64,
	c: Vec<String>,
}

impl From<TestSingleObjectReader> for Value {
	fn from(obj: TestSingleObjectReader) -> Value {
		Value::Record(vec![
			("a".into(), obj.a.into()),
			("b".into(), obj.b.into()),
			(
				"c".into(),
				Value::Array(obj.c.into_iter().map(|s| s.into()).collect()),
			),
		])
	}
}

fn apache_encode(
	value: impl Into<Value>,
	schema: &apache_avro::Schema,
	out: &mut Vec<u8>,
) -> apache_avro::AvroResult<()> {
	out.extend_from_slice(&apache_avro::to_avro_datum(schema, value.into())?);
	Ok(())
}

#[test]
fn test_avro_3507_single_object_reader() {
	let expected_value = TestSingleObjectReader {
		a: 42,
		b: 3.33,
		c: vec!["cat".into(), "dog".into()],
	};
	let mut to_read = Vec::<u8>::new();
	to_read.extend_from_slice(&[0xC3, 0x01]);
	to_read.extend_from_slice(
		&APACHE_SCHEMA
			.fingerprint::<apache_avro::rabin::Rabin>()
			.bytes[..],
	);
	apache_encode(expected_value.clone(), &APACHE_SCHEMA, &mut to_read)
		.expect("Encode should succeed");
	let val: TestSingleObjectReader =
		from_single_object_slice(to_read.as_slice(), &SCHEMA).unwrap();
	assert_eq!(expected_value, val);
}

#[test]
fn avro_3642_test_single_object_reader_incomplete_reads() {
	use std::io::Read;
	let expected_value = TestSingleObjectReader {
		a: 42,
		b: 3.33,
		c: vec!["cat".into(), "dog".into()],
	};
	// The two-byte marker, to show that the message uses this single-record format
	let to_read_1 = &[0xC3, 0x01];
	let mut to_read_2 = Vec::<u8>::new();
	to_read_2.extend_from_slice(
		&APACHE_SCHEMA
			.fingerprint::<apache_avro::rabin::Rabin>()
			.bytes[..],
	);
	let mut to_read_3 = Vec::<u8>::new();
	apache_encode(expected_value.clone(), &APACHE_SCHEMA, &mut to_read_3)
		.expect("Encode should succeed");
	let to_read = (to_read_1).chain(&to_read_2[..]).chain(&to_read_3[..]);
	let val: TestSingleObjectReader = from_single_object_reader(to_read, &SCHEMA).unwrap();
	assert_eq!(expected_value, val);
}
