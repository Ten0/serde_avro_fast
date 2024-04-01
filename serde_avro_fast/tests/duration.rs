use serde_avro_fast::{from_datum_slice, ser::SerializerConfig, to_datum_vec, Schema};

fn test<'de, T: serde::Serialize + serde::Deserialize<'de> + PartialEq + std::fmt::Debug>(
	datum: &'de [u8],
	rust_value: T,
	avro_value: apache_avro::Duration,
	schema: &Schema,
	apache_schema: &apache_avro::Schema,
) {
	assert_eq!(from_datum_slice::<T>(datum, schema).unwrap(), rust_value);
	assert_eq!(
		to_datum_vec(&rust_value, &mut SerializerConfig::new(schema)).unwrap(),
		datum
	);
	let avro_value = apache_avro::types::Value::Duration(avro_value);
	assert_eq!(
		apache_avro::from_avro_datum(apache_schema, &mut &*datum, None).unwrap(),
		avro_value
	);
	assert_eq!(
		apache_avro::to_avro_datum(apache_schema, avro_value).unwrap(),
		datum
	);
}

#[derive(serde_derive::Serialize, serde_derive::Deserialize, PartialEq)]
struct Duration {
	months: u32,
	days: u32,
	milliseconds: u32,
}

impl std::fmt::Debug for Duration {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		f.debug_struct("Duration")
			.field("months", &format_args!("0x{:x}", self.months))
			.field("days", &&format_args!("0x{:x}", self.days))
			.field("milliseconds", &&format_args!("0x{:x}", self.milliseconds))
			.finish()
	}
}

#[test]
fn decimal() {
	let raw_schema =
		r#"{"type":{"name":"duration","type":"fixed","size":12},"logicalType":"duration"}"#;
	let apache_schema = apache_avro::Schema::parse_str(raw_schema).unwrap();
	let schema: Schema = raw_schema.parse().unwrap();

	test(
		&(1..13).collect::<Vec<u8>>(),
		Duration {
			months: 0x04030201,
			days: 0x08070605,
			milliseconds: 0x0C0B0A09,
		},
		apache_avro::Duration::new(
			apache_avro::Months::new(0x04030201),
			apache_avro::Days::new(0x08070605),
			apache_avro::Millis::new(0x0C0B0A09),
		),
		&schema,
		&apache_schema,
	);

	test::<(u32, u32, u32)>(
		&(1..13).collect::<Vec<u8>>(),
		(0x04030201, 0x08070605, 0x0C0B0A09),
		apache_avro::Duration::new(
			apache_avro::Months::new(0x04030201),
			apache_avro::Days::new(0x08070605),
			apache_avro::Millis::new(0x0C0B0A09),
		),
		&schema,
		&apache_schema,
	);

	test::<[u32; 3]>(
		&(1..13).collect::<Vec<u8>>(),
		[0x04030201, 0x08070605, 0x0C0B0A09],
		apache_avro::Duration::new(
			apache_avro::Months::new(0x04030201),
			apache_avro::Days::new(0x08070605),
			apache_avro::Millis::new(0x0C0B0A09),
		),
		&schema,
		&apache_schema,
	);

	test::<serde_bytes::ByteBuf>(
		&(1..13).collect::<Vec<u8>>(),
		serde_bytes::ByteBuf::from((1..13).collect::<Vec<u8>>()),
		apache_avro::Duration::new(
			apache_avro::Months::new(0x04030201),
			apache_avro::Days::new(0x08070605),
			apache_avro::Millis::new(0x0C0B0A09),
		),
		&schema,
		&apache_schema,
	);
}
