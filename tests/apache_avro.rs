//! Port of tests from the apache-avro library
//! https://github.com/apache/avro/blob/5016cd5c3f2054ebacce7983785c228798e47f59/lang/rust/avro/tests/io.rs

use {
	apache_avro::{to_avro_datum, types::Value, Schema},
	lazy_static::lazy_static,
	pretty_assertions::assert_eq,
};

lazy_static! {
	static ref SCHEMAS_TO_VALIDATE: Vec<(&'static str, Value)> = vec![
		(r#""null""#, Value::Null),
		(r#""boolean""#, Value::Boolean(true)),
		(r#""string""#, Value::String("adsfasdf09809dsf-=adsf".to_string())),
		(r#""bytes""#, Value::Bytes("12345abcd".to_string().into_bytes())),
		(r#""int""#, Value::Int(1234)),
		(r#""long""#, Value::Long(1234)),
		(r#""float""#, Value::Float(1234.0)),
		(r#""double""#, Value::Double(1234.0)),
		(r#"{"type": "fixed", "name": "Test", "size": 1}"#, Value::Fixed(1, vec![b'B'])),
		(r#"{"type": "enum", "name": "Test", "symbols": ["A", "B"]}"#, Value::Enum(1, "B".to_string())),
		(r#"{"type": "array", "items": "long"}"#, Value::Array(vec![Value::Long(1), Value::Long(3), Value::Long(2)])),
		(r#"{"type": "map", "values": "long"}"#, Value::Map([("a".to_string(), Value::Long(1i64)), ("b".to_string(), Value::Long(3i64)), ("c".to_string(), Value::Long(2i64))].iter().cloned().collect())),
		(r#"["string", "null", "long"]"#, Value::Union(1, Box::new(Value::Null))),
		(r#"{"type": "record", "name": "Test", "fields": [{"name": "f", "type": "long"}]}"#, Value::Record(vec![("f".to_string(), Value::Long(1))]))
	];

	static ref BINARY_ENCODINGS: Vec<(i64, Vec<u8>)> = vec![
		(0, vec![0x00]),
		(-1, vec![0x01]),
		(1, vec![0x02]),
		(-2, vec![0x03]),
		(2, vec![0x04]),
		(-64, vec![0x7f]),
		(64, vec![0x80, 0x01]),
		(8192, vec![0x80, 0x80, 0x01]),
		(-8193, vec![0x81, 0x80, 0x01]),
	];

	static ref DEFAULT_VALUE_EXAMPLES: Vec<(&'static str, &'static str, Value)> = vec![
		(r#""null""#, "null", Value::Null),
		(r#""boolean""#, "true", Value::Boolean(true)),
		(r#""string""#, r#""foo""#, Value::String("foo".to_string())),
		(r#""bytes""#, r#""a""#, Value::Bytes(vec![97])), // ASCII 'a' => one byte
		(r#""bytes""#, r#""\u00FF""#, Value::Bytes(vec![195, 191])), // The value is between U+0080 and U+07FF => two bytes
		(r#""int""#, "5", Value::Int(5)),
		(r#""long""#, "5", Value::Long(5)),
		(r#""float""#, "1.1", Value::Float(1.1)),
		(r#""double""#, "1.1", Value::Double(1.1)),
		(r#"{"type": "fixed", "name": "F", "size": 2}"#, r#""a""#, Value::Fixed(1, vec![97])), // ASCII 'a' => one byte
		(r#"{"type": "fixed", "name": "F", "size": 2}"#, r#""\u00FF""#, Value::Fixed(2, vec![195, 191])), // The value is between U+0080 and U+07FF => two bytes
		(r#"{"type": "enum", "name": "F", "symbols": ["FOO", "BAR"]}"#, r#""FOO""#, Value::Enum(0, "FOO".to_string())),
		(r#"{"type": "array", "items": "int"}"#, "[1, 2, 3]", Value::Array(vec![Value::Int(1), Value::Int(2), Value::Int(3)])),
		(r#"{"type": "map", "values": "int"}"#, r#"{"a": 1, "b": 2}"#, Value::Map([("a".to_string(), Value::Int(1)), ("b".to_string(), Value::Int(2))].iter().cloned().collect())),
		(r#"["int", "null"]"#, "5", Value::Union(0, Box::new(Value::Int(5)))),
		(r#"{"type": "record", "name": "F", "fields": [{"name": "A", "type": "int"}]}"#, r#"{"A": 5}"#,Value::Record(vec![("A".to_string(), Value::Int(5))])),
		(r#"["null", "int"]"#, "null", Value::Union(0, Box::new(Value::Null))),
	];

	static ref LONG_RECORD_SCHEMA: Schema = Schema::parse_str(r#"
    {
        "type": "record",
        "name": "Test",
        "fields": [
            {"name": "A", "type": "int"},
            {"name": "B", "type": "int"},
            {"name": "C", "type": "int"},
            {"name": "D", "type": "int"},
            {"name": "E", "type": "int"},
            {"name": "F", "type": "int"},
            {"name": "G", "type": "int"}
        ]
    }
    "#).unwrap();

	static ref LONG_RECORD_DATUM: Value = Value::Record(vec![
		("A".to_string(), Value::Int(1)),
		("B".to_string(), Value::Int(2)),
		("C".to_string(), Value::Int(3)),
		("D".to_string(), Value::Int(4)),
		("E".to_string(), Value::Int(5)),
		("F".to_string(), Value::Int(6)),
		("G".to_string(), Value::Int(7)),
	]);
}

pub fn from_avro_datum(schema: &Schema, slice: &[u8]) -> Value {
	let sjv: serde_json::Value =
		serde::Deserialize::deserialize(serde_avro_fast::de::ReaderAndConfig::from_slice(slice).deserializer(schema))
			.unwrap();
	let avro_value = apache_avro::to_value(sjv).unwrap();
	let avro_value_reinterpreted = avro_value.resolve(schema).unwrap();
	avro_value_reinterpreted
}

macro_rules! tests {
	($($idx: tt)*) => {
		paste::paste! {
			$(
				#[test]
				fn [<test_validate_ $idx>]() {
					let (raw_schema, value) = &SCHEMAS_TO_VALIDATE[$idx];
					let schema = Schema::parse_str(raw_schema).unwrap();
					assert!(
						value.validate(&schema),
						"value {:?} does not validate schema: {}",
						value,
						raw_schema
					);
				}
			)*

			$(
				#[test]
				fn [<test_round_trip_ $idx>]() {
					let (raw_schema, value) = &SCHEMAS_TO_VALIDATE[$idx];
					println!("{raw_schema}");
					let schema = Schema::parse_str(raw_schema).unwrap();
					let encoded = to_avro_datum(&schema, value.clone()).unwrap();
					let decoded = from_avro_datum(&schema, &encoded);
					assert_eq!(value, &decoded);
				}
			)*
		}


		#[test]
		fn all_test_cases_are_tested() {
			let indexes = &[$($idx,)*];
			assert!(indexes.len() == SCHEMAS_TO_VALIDATE.len() && indexes.iter().zip(0..SCHEMAS_TO_VALIDATE.len()).all(|(a, b)| *a == b))
		}
	};
}
tests! { 00 01 02 03 04 05 06 07 08 09 10 11 12 13 }
