//! Port of tests from the apache-avro library
//! https://github.com/apache/avro/blob/5016cd5c3f2054ebacce7983785c228798e47f59/lang/rust/avro/tests/io.rs

use {
	apache_avro::{types::Value, Schema},
	lazy_static::lazy_static,
	pretty_assertions::assert_eq,
};

lazy_static! {
	static ref SCHEMAS_TO_VALIDATE: Vec<(&'static str, Value)> = vec![
		(r#""null""#, Value::Null),
		(r#""boolean""#, Value::Boolean(true)),
		(
			r#""string""#,
			Value::String("adsfasdf09809dsf-=adsf".to_string())
		),
		(
			r#""bytes""#,
			Value::Bytes("12345abcd".to_string().into_bytes())
		),
		(r#""int""#, Value::Int(1234)),
		(r#""long""#, Value::Long(1234)),
		(r#""float""#, Value::Float(1234.0)),
		(r#""double""#, Value::Double(1234.0)),
		(
			r#"{"type": "fixed", "name": "Test", "size": 1}"#,
			Value::Fixed(1, vec![b'B'])
		),
		(
			r#"{"type": "enum", "name": "Test", "symbols": ["A", "B"]}"#,
			Value::Enum(1, "B".to_string())
		),
		(
			r#"{"type": "array", "items": "long"}"#,
			Value::Array(vec![Value::Long(1), Value::Long(3), Value::Long(2)])
		),
		(
			r#"{"type": "map", "values": "long"}"#,
			Value::Map(
				[
					("a".to_string(), Value::Long(1i64)),
					("b".to_string(), Value::Long(3i64)),
					("c".to_string(), Value::Long(2i64))
				]
				.iter()
				.cloned()
				.collect()
			)
		),
		(
			r#"["string", "null", "long"]"#,
			Value::Union(1, Box::new(Value::Null))
		),
		(
			r#"{"type": "record", "name": "Test", "fields": [{"name": "f", "type": "long"}]}"#,
			Value::Record(vec![("f".to_string(), Value::Long(1))])
		),
	];
}

pub fn from_avro_datum_fast<T: serde::de::DeserializeOwned + serde::Serialize>(
	schema: &Schema,
	slice: &[u8],
) -> Value {
	let fast_schema = serde_avro_fast::Schema::from_apache_schema(schema).unwrap();
	let sjv: T = serde_avro_fast::from_datum_slice(slice, &fast_schema).unwrap();
	println!("{}", serde_json::to_string_pretty(&sjv).unwrap());
	let avro_value = apache_avro::to_value(sjv).unwrap();
	dbg!(&avro_value);
	let avro_value_reinterpreted = match (avro_value, schema) {
		(Value::Bytes(v), Schema::Fixed { size, .. }) => {
			assert_eq!(*size, v.len());
			Value::Fixed(*size, v)
		}
		(avro_value, schema) => avro_value.resolve(schema).unwrap(),
	};
	avro_value_reinterpreted
}

fn test_round_trip_apache_fast<T: serde::de::DeserializeOwned + serde::Serialize>(
	&(raw_schema, ref value): &(&str, Value),
) {
	println!("{raw_schema}");
	let schema = Schema::parse_str(raw_schema).unwrap();

	let encoded = apache_avro::to_avro_datum(&schema, value.clone()).unwrap();
	let decoded = from_avro_datum_fast::<T>(&schema, &encoded);
	assert_eq!(*value, decoded);
}

macro_rules! tests {
	($($type_: ty => $($idx: expr)+,)+) => {
		paste::paste! {
			$(
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
					fn [<test_round_trip_apache_fast_ $idx>]() {
						test_round_trip_apache_fast::<$type_>(&SCHEMAS_TO_VALIDATE[$idx]);
					}
				)*
			)*
		}

		#[test]
		fn all_tested() {
			let mut tested = vec![$($($idx,)*)*];
			tested.sort();
			assert_eq!(tested, (0..SCHEMAS_TO_VALIDATE.len()).collect::<Vec<_>>());
		}
	};
}
tests! {
	serde_json::Value => 00 01 02 04 05 06 07 09 10 11 12 13,
	serde_bytes::ByteBuf => 03 08,
}

#[test]
fn test_decimal() {
	let schema: serde_avro_fast::Schema =
		r#"{"type": "bytes", "logicalType": "decimal", "precision": 4, "scale": 1}"#
			.parse()
			.unwrap();
	use serde_avro_fast::schema::{DecimalRepr, SchemaNode};
	dbg!(schema.root());
	assert!(matches!(
		schema.root(),
		SchemaNode::Decimal(serde_avro_fast::schema::Decimal {
			precision: 4,
			scale: 1,
			repr: DecimalRepr::Bytes
		})
	));
	let deserialized: f64 = serde_avro_fast::from_datum_slice(&[2, 2], &schema).unwrap();
	assert_eq!(deserialized, 0.2);
	let deserialized: String = serde_avro_fast::from_datum_slice(&[2, 2], &schema).unwrap();
	assert_eq!(deserialized, "0.2");
	let deserialized: rust_decimal::Decimal =
		serde_avro_fast::from_datum_slice(&[2, 2], &schema).unwrap();
	assert_eq!(deserialized, "0.2".parse().unwrap());
}
