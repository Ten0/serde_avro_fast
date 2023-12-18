//! Port of tests from the apache-avro library
//! https://github.com/apache/avro/blob/5016cd5c3f2054ebacce7983785c228798e47f59/lang/rust/avro/tests/io.rs

use {
	apache_avro::{types::Value, Schema},
	lazy_static::lazy_static,
	pretty_assertions::assert_eq,
	rand::prelude::*,
	serde_avro_fast::ser::SerializerConfig,
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
		(
			r#"{"type": "record", "name": "LongerRecord", "fields": [{"name": "f", "type": "long"}, {"name": "g", "type": "long"}, {"name": "h", "type": "string"}]}"#,
			Value::Record(vec![
				("f".to_string(), Value::Long(1)),
				("g".to_string(), Value::Long(2)),
				("h".to_string(), Value::String("Abc".to_owned())),
			])
		),
		(
			r#"{"name": "null_or_string","type": ["null", "string"], "default": null}"#,
			Value::Union(1, Box::new(Value::String("value".to_string())))
		),
	];
}

pub fn from_avro_datum_fast<T: serde::de::DeserializeOwned + serde::Serialize>(
	schema: &Schema,
	fast_schema: &serde_avro_fast::Schema,
	slice: &[u8],
) -> Value {
	let sjv: T = serde_avro_fast::from_datum_slice(slice, fast_schema).unwrap();
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
	let fast_schema = serde_avro_fast::Schema::from_apache_schema(&schema).unwrap();

	let encoded = apache_avro::to_avro_datum(&schema, value.clone()).unwrap();
	let decoded = from_avro_datum_fast::<T>(&schema, &fast_schema, &encoded);
	assert_eq!(*value, decoded);
}

fn test_round_trip_fast_apache<T: serde::de::DeserializeOwned + serde::Serialize>(
	&(raw_schema, ref value): &(&str, Value),
) {
	println!("{raw_schema}");
	let schema = Schema::parse_str(raw_schema).unwrap();
	let fast_schema = serde_avro_fast::Schema::from_apache_schema(&schema).unwrap();
	let serializer_config = &mut SerializerConfig::new(&fast_schema);

	let json_for_value = apache_avro::from_value::<T>(value).unwrap();
	println!("{}", serde_json::to_string_pretty(&json_for_value).unwrap());

	let mut encoded = Vec::new();
	match (serde_json::to_value(&json_for_value), &fast_schema.root()) {
		(
			Ok(serde_json::Value::Object(obj)),
			serde_avro_fast::schema::SchemaNode::Record { .. },
		) => {
			// Test that it works with random ordering
			let mut keys: Vec<(_, _)> = obj.into_iter().collect();
			let mut prev = None;
			for _ in 0..10 {
				encoded.clear();
				keys.shuffle(&mut rand::thread_rng());
				tuple_vec_map::serialize(
					&keys,
					serde_avro_fast::ser::SerializerState::from_writer(
						&mut encoded,
						serializer_config,
					)
					.serializer(),
				)
				.unwrap();
				if let Some(prevv) = prev {
					assert_eq!(encoded, prevv);
				}
				prev = Some(encoded.clone());
			}
		}
		_ => {
			serde_avro_fast::to_datum(&json_for_value, &mut encoded, serializer_config).unwrap();
		}
	}
	let decoded = apache_avro::from_avro_datum(&schema, &mut encoded.as_slice(), None).unwrap();
	assert_eq!(*value, decoded);
}

fn test_round_trip_fast_fast<T: serde::de::DeserializeOwned + serde::Serialize>(
	&(raw_schema, ref value): &(&str, Value),
) {
	println!("{raw_schema}");
	let schema = Schema::parse_str(raw_schema).unwrap();
	let fast_schema = serde_avro_fast::Schema::from_apache_schema(&schema).unwrap();
	let serializer_config = &mut SerializerConfig::new(&fast_schema);

	let json_for_value = apache_avro::from_value::<T>(value).unwrap();
	println!("{}", serde_json::to_string_pretty(&json_for_value).unwrap());

	let mut encoded = Vec::new();
	serde_avro_fast::to_datum(&json_for_value, &mut encoded, serializer_config).unwrap();
	let decoded = from_avro_datum_fast::<T>(&schema, &fast_schema, &encoded);
	assert_eq!(*value, decoded);
}

macro_rules! tests {
	($($type_: ty, $name: ident => $($idx: expr)+,)+) => {
		paste::paste! {
			$(
				$(
					#[test]
					fn [<test_validate_ $name $idx>]() {
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
					fn [<test_round_trip_apache_fast_ $name $idx>]() {
						test_round_trip_apache_fast::<$type_>(&SCHEMAS_TO_VALIDATE[$idx]);
					}
				)*

				$(
					#[test]
					fn [<test_round_trip_fast_apache_ $name $idx>]() {
						test_round_trip_fast_apache::<$type_>(&SCHEMAS_TO_VALIDATE[$idx]);
					}
				)*

				$(
					#[test]
					fn [<test_round_trip_fast_fast_ $name $idx>]() {
						test_round_trip_fast_fast::<$type_>(&SCHEMAS_TO_VALIDATE[$idx]);
					}
				)*
			)*
		}

		#[test]
		fn all_tested() {
			let mut tested = vec![$($($idx,)*)*];
			tested.sort_unstable();
			tested.dedup();
			assert_eq!(tested, (0..SCHEMAS_TO_VALIDATE.len()).collect::<Vec<_>>());
		}
	};
}
tests! {
	serde_json::Value, sjv => 00 01 02 04 05 06 07 10 11 12 13 14 15,
	serde_bytes::ByteBuf, byte_buf => 03 08,
	AB, ab => 09,
	Option<String>, option_string => 15,
}
#[derive(serde::Serialize, serde::Deserialize)]
enum AB {
	A,
	B,
}

#[test]
fn test_decimal() {
	let schema: serde_avro_fast::Schema =
		r#"{"type": "bytes", "logicalType": "decimal", "precision": 4, "scale": 1}"#
			.parse()
			.unwrap();
	let serializer_config = &mut SerializerConfig::new(&schema);
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

	// 0.2
	let deserialized: f64 = serde_avro_fast::from_datum_slice(&[2, 2], &schema).unwrap();
	assert_eq!(deserialized, 0.2);
	let deserialized: String = serde_avro_fast::from_datum_slice(&[2, 2], &schema).unwrap();
	assert_eq!(deserialized, "0.2");
	let deserialized: rust_decimal::Decimal =
		serde_avro_fast::from_datum_slice(&[2, 2], &schema).unwrap();
	assert_eq!(deserialized, "0.2".parse().unwrap());
	assert_eq!(
		serde_avro_fast::to_datum_vec(&deserialized, serializer_config).unwrap(),
		[2, 2]
	);

	// - 0.2
	let deserialized: f64 = serde_avro_fast::from_datum_slice(&[2, 0xFE], &schema).unwrap();
	assert_eq!(deserialized, -0.2);
	let deserialized: String = serde_avro_fast::from_datum_slice(&[2, 0xFE], &schema).unwrap();
	assert_eq!(deserialized, "-0.2");
	let deserialized: rust_decimal::Decimal =
		serde_avro_fast::from_datum_slice(&[2, 0xFE], &schema).unwrap();
	assert_eq!(deserialized, "-0.2".parse().unwrap());
	assert_eq!(
		serde_avro_fast::to_datum_vec(&deserialized, serializer_config).unwrap(),
		[2, 0xFE]
	);
}

#[test]
fn test_bytes_with_serde_json_value() {
	let (schema, value) = &SCHEMAS_TO_VALIDATE[3];
	let schema = Schema::parse_str(schema).unwrap();
	let encoded = apache_avro::to_avro_datum(&schema, value.clone()).unwrap();
	let schema = serde_avro_fast::Schema::from_apache_schema(&schema).unwrap();

	let decoded: serde_json::Value = match value {
		Value::Bytes(b) => b.into_iter().map(|b| *b as u64).collect(),
		_ => unreachable!(),
	};
	let config = &mut serde_avro_fast::ser::SerializerConfig::new(&schema);
	config.allow_slow_sequence_to_bytes();
	let mut serializer_state =
		serde_avro_fast::ser::SerializerState::from_writer(Vec::new(), config);
	serde::Serialize::serialize(&decoded, serializer_state.serializer()).unwrap();
	let serialized = serializer_state.into_writer();

	assert_eq!(serialized, encoded);
}

#[test]
fn test_fixed_with_serde_json_value() {
	let (schema, value) = &SCHEMAS_TO_VALIDATE[8];
	let schema = Schema::parse_str(schema).unwrap();
	let encoded = apache_avro::to_avro_datum(&schema, value.clone()).unwrap();
	let schema = serde_avro_fast::Schema::from_apache_schema(&schema).unwrap();

	let decoded: serde_json::Value = match value {
		Value::Fixed(_, b) => b.into_iter().map(|b| *b as u64).collect(),
		_ => unreachable!(),
	};
	let config = &mut serde_avro_fast::ser::SerializerConfig::new(&schema);
	config.allow_slow_sequence_to_bytes();
	let mut serializer_state =
		serde_avro_fast::ser::SerializerState::from_writer(Vec::new(), config);
	serde::Serialize::serialize(&decoded, serializer_state.serializer()).unwrap();
	let serialized = serializer_state.into_writer();

	assert_eq!(serialized, encoded);
}
