//! Port of tests from the apache-avro library
//! https://github.com/apache/avro/blob/5016cd5c3f2054ebacce7983785c228798e47f59/lang/rust/avro/tests/io.rs

#![allow(clippy::zero_prefixed_literal)]

use {
	apache_avro::{types::Value, Schema},
	lazy_static::lazy_static,
	pretty_assertions::assert_eq,
	rand::prelude::*,
	serde_avro_fast::{schema::SchemaMut, ser::SerializerConfig},
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
	let fast_schema: serde_avro_fast::Schema = raw_schema.parse().unwrap();

	let encoded = apache_avro::to_avro_datum(&schema, value.clone()).unwrap();
	let decoded = from_avro_datum_fast::<T>(&schema, &fast_schema, &encoded);
	assert_eq!(*value, decoded);
}

fn test_round_trip_fast_apache<T: serde::de::DeserializeOwned + serde::Serialize>(
	&(raw_schema, ref value): &(&str, Value),
) {
	println!("{raw_schema}");
	let schema = Schema::parse_str(raw_schema).unwrap();
	let fast_schema: serde_avro_fast::Schema = raw_schema.parse().unwrap();
	let serializer_config = &mut SerializerConfig::new(&fast_schema);

	let json_for_value = apache_avro::from_value::<T>(value).unwrap();
	println!("{}", serde_json::to_string_pretty(&json_for_value).unwrap());

	let mut encoded = Vec::new();
	match (serde_json::to_value(&json_for_value), &schema) {
		(Ok(serde_json::Value::Object(obj)), Schema::Record { .. }) => {
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
	let fast_schema: serde_avro_fast::Schema = raw_schema.parse().unwrap();
	let serializer_config = &mut SerializerConfig::new(&fast_schema);

	let json_for_value = apache_avro::from_value::<T>(value).unwrap();
	println!("{}", serde_json::to_string_pretty(&json_for_value).unwrap());

	let mut encoded = Vec::new();
	serde_avro_fast::to_datum(&json_for_value, &mut encoded, serializer_config).unwrap();
	let decoded = from_avro_datum_fast::<T>(&schema, &fast_schema, &encoded);
	assert_eq!(*value, decoded);
}

/// Skip apache avro and just do a local round trip - for cases when apache avro
/// is buggy
fn test_schema_parse_round_trip(raw_schema: &str) -> (String, [u8; 8]) {
	let mut fast_schema: SchemaMut = raw_schema.parse().unwrap();
	let fast_fingerprint = fast_schema.canonical_form_rabin_fingerprint().unwrap();

	fast_schema.nodes_mut(); // Forget original json
	let serialized_schema = serde_json::to_string_pretty(&fast_schema).unwrap();
	let fast_schema_2: SchemaMut = serialized_schema.parse().unwrap();
	let serialized_schema_2 = serde_json::to_string_pretty(&fast_schema_2).unwrap();
	assert_eq!(serialized_schema, serialized_schema_2);
	println!("{}", &serialized_schema);
	assert_eq!(
		fast_schema_2.canonical_form_rabin_fingerprint().unwrap(),
		fast_fingerprint
	);

	(serialized_schema, fast_fingerprint)
}
/// Make sure that parsed schema -> serialized schema -> apache schema agrees
/// with apache parsed schema
fn test_schema_fingerprint_and_parse_round_trip(raw_schema: &str) {
	let (serialized_schema, fast_fingerprint) = test_schema_parse_round_trip(raw_schema);

	let schema = Schema::parse_str(raw_schema).unwrap();

	let apache_from_serialized = Schema::parse_str(&serialized_schema).unwrap();
	assert_eq!(apache_from_serialized, schema);

	let apache_finterprint = schema.fingerprint::<apache_avro::rabin::Rabin>().bytes;
	assert_eq!(apache_finterprint, fast_fingerprint);
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

				$(
					#[test]
					fn [<test_schema_fingerprint_and_parse_round_trip_ $name $idx>]() {
						test_schema_fingerprint_and_parse_round_trip(SCHEMAS_TO_VALIDATE[$idx].0);
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
	use serde_avro_fast::schema::*;
	let editable_schema: SchemaMut =
		r#"{"type": "bytes", "logicalType": "decimal", "precision": 4, "scale": 1}"#
			.parse()
			.unwrap();
	dbg!(editable_schema.root());
	assert!(matches!(
		*editable_schema.root(),
		SchemaNode::LogicalType {
			logical_type: LogicalType::Decimal(Decimal {
				scale: 1,
				precision: 4,
				..
			}),
			inner
		} if matches!(editable_schema[inner], SchemaNode::RegularType(RegularType::Bytes))
	));
	let schema = editable_schema.try_into().unwrap();
	let serializer_config = &mut SerializerConfig::new(&schema);

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

	assert_eq!(
		serde_avro_fast::to_datum_vec(
			&rust_decimal::Decimal::from_str_exact("-12.8").unwrap(),
			&mut SerializerConfig::new(
				&r#"{"type": {"type":"fixed","size":3,"name":"f"}, "logicalType": "decimal", "precision": 123, "scale": 1}"#.parse().unwrap()
			)
		)
		.unwrap(),
		[255, 255, 128]
	);
}

#[test]
fn test_bytes_with_serde_json_value() {
	let (raw_schema, value) = &SCHEMAS_TO_VALIDATE[3];
	let schema = Schema::parse_str(raw_schema).unwrap();
	let encoded = apache_avro::to_avro_datum(&schema, value.clone()).unwrap();
	let schema: serde_avro_fast::Schema = raw_schema.parse().unwrap();

	let decoded: serde_json::Value = match value {
		Value::Bytes(b) => b.iter().map(|&b| b as u64).collect(),
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
	let (raw_schema, value) = &SCHEMAS_TO_VALIDATE[8];
	let schema = Schema::parse_str(raw_schema).unwrap();
	let encoded = apache_avro::to_avro_datum(&schema, value.clone()).unwrap();
	let schema: serde_avro_fast::Schema = raw_schema.parse().unwrap();

	let decoded: serde_json::Value = match value {
		Value::Fixed(_, b) => b.iter().map(|&b| b as u64).collect(),
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
fn complex_schema_parsing_serialization_round_trip() {
	let (serialized, fingerprint) = test_schema_parse_round_trip(
		r#"
		[
			{
				"type": "fixed",
				"name": "fiiixed",
				"size": 12
			},
			{
				"type": "record",
				"name": "Test",
				"fields": [
					{
						"name": "f",
						"type": {
							"type": "record",
							"name": "a.Test2",
							"fields": [
								{
									"name": "Test2 inner",
									"type": {
										"type": "fixed",
										"size": 12,
										"name": "test2_inner"
									}
								},
								{
									"name": "the_fiixed",
									"type": ".fiiixed"
								}
							]
						}
					},
					{
						"name": "f2",
						"type": "a.Test2"
					},
					{
						"name": "f3",
						"type": {
							"type": "record",
							"name": "f3",
							"namespace": "",
							"fields": [
								{
									"name": "f3fiiixed",
									"type": "fiiixed"
								},
								{
									"name": "f3_2",
									"type": "a.test2_inner"
								}
							]
						}
					},
					{
						"name": "f4",
						"type": "a.test2_inner"
					}
				]
			}
		]
	"#,
	);
	assert_eq!(
		serialized,
		r#"[
  {
    "type": "fixed",
    "name": "fiiixed",
    "size": 12
  },
  {
    "type": "record",
    "name": "Test",
    "fields": [
      {
        "name": "f",
        "type": {
          "type": "record",
          "name": "a.Test2",
          "fields": [
            {
              "name": "Test2 inner",
              "type": {
                "type": "fixed",
                "name": "test2_inner",
                "size": 12
              }
            },
            {
              "name": "the_fiixed",
              "type": ".fiiixed"
            }
          ]
        }
      },
      {
        "name": "f2",
        "type": "a.Test2"
      },
      {
        "name": "f3",
        "type": {
          "type": "record",
          "name": "f3",
          "fields": [
            {
              "name": "f3fiiixed",
              "type": "fiiixed"
            },
            {
              "name": "f3_2",
              "type": "a.test2_inner"
            }
          ]
        }
      },
      {
        "name": "f4",
        "type": "a.test2_inner"
      }
    ]
  }
]"#
	);
	assert_eq!(fingerprint, [18, 207, 199, 195, 150, 81, 210, 28]);
}
