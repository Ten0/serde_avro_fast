//! This benchmakes apache_avro for comparison
//! Benches are again largely stolen from there

#![allow(missing_docs)]

use criterion::BenchmarkId;

use criterion::{criterion_group, criterion_main, Criterion};

const RAW_SMALL_SCHEMA: &str = r#"
{
	"namespace": "test",
	"type": "record",
	"name": "Test",
	"fields": [
		{
			"type": {
				"type": "string"
			},
			"name": "field"
		}
	]
}
"#;

#[derive(serde_derive::Deserialize)]
#[allow(unused)]
struct SmallStruct<'a> {
	field: &'a str,
}
#[derive(serde_derive::Deserialize)]
#[allow(unused)]
struct SmallStructApache {
	field: String,
}

const RAW_BIG_SCHEMA: &str = r#"
{
	"namespace": "my.example",
	"type": "record",
	"name": "userInfo",
	"fields": [
		{
			"default": "NONE",
			"type": "string",
			"name": "username"
		},
		{
			"default": -1,
			"type": "int",
			"name": "age"
		},
		{
			"default": "NONE",
			"type": "string",
			"name": "phone"
		},
		{
			"default": "NONE",
			"type": "string",
			"name": "housenum"
		},
		{
			"default": {},
			"type": {
				"fields": [
					{
						"default": "NONE",
						"type": "string",
						"name": "street"
					},
					{
						"default": "NONE",
						"type": "string",
						"name": "city"
					},
					{
						"default": "NONE",
						"type": "string",
						"name": "state_prov"
					},
					{
						"default": "NONE",
						"type": "string",
						"name": "country"
					},
					{
						"default": "NONE",
						"type": "string",
						"name": "zip"
					}
				],
				"type": "record",
				"name": "mailing_address"
			},
			"name": "address"
		}
	]
}
"#;

const RAW_ADDRESS_SCHEMA: &str = r#"
{
	"fields": [
		{
			"default": "NONE",
			"type": "string",
			"name": "street"
		},
		{
			"default": "NONE",
			"type": "string",
			"name": "city"
		},
		{
			"default": "NONE",
			"type": "string",
			"name": "state_prov"
		},
		{
			"default": "NONE",
			"type": "string",
			"name": "country"
		},
		{
			"default": "NONE",
			"type": "string",
			"name": "zip"
		}
	],
	"type": "record",
	"name": "mailing_address"
}
"#;

#[derive(serde_derive::Deserialize)]
#[allow(unused)]
struct BigStruct<'a> {
	username: &'a str,
	age: u32,
	phone: &'a str,
	housenum: &'a str,
	address: Address<'a>,
}

#[derive(serde_derive::Deserialize)]
#[allow(unused)]
struct Address<'a> {
	street: &'a str,
	city: &'a str,
	state_prov: &'a str,
	country: &'a str,
	zip: &'a str,
}

#[derive(serde_derive::Deserialize)]
#[allow(unused)]
struct BigStructApache {
	username: String,
	age: u32,
	phone: String,
	housenum: String,
	address: AddressApache,
}

#[derive(serde_derive::Deserialize)]
#[allow(unused)]
struct AddressApache {
	street: String,
	city: String,
	state_prov: String,
	country: String,
	zip: String,
}

fn make_small_record() -> anyhow::Result<(
	apache_avro::Schema,
	serde_avro_fast::Schema,
	apache_avro::types::Value,
)> {
	let small_schema = apache_avro::Schema::parse_str(RAW_SMALL_SCHEMA)?;
	let fast_schema = RAW_SMALL_SCHEMA.parse()?;
	let small_record = {
		let mut small_record = apache_avro::types::Record::new(&small_schema).unwrap();
		small_record.put("field", "foo");
		small_record.into()
	};
	Ok((small_schema, fast_schema, small_record))
}

fn make_big_record() -> anyhow::Result<(
	apache_avro::Schema,
	serde_avro_fast::Schema,
	apache_avro::types::Value,
)> {
	let big_schema = apache_avro::Schema::parse_str(RAW_BIG_SCHEMA)?;
	let fast_schema = RAW_BIG_SCHEMA.parse()?;
	let address_schema = apache_avro::Schema::parse_str(RAW_ADDRESS_SCHEMA)?;
	let mut address = apache_avro::types::Record::new(&address_schema).unwrap();
	address.put("street", "street");
	address.put("city", "city");
	address.put("state_prov", "state_prov");
	address.put("country", "country");
	address.put("zip", "zip");

	let big_record = {
		let mut big_record = apache_avro::types::Record::new(&big_schema).unwrap();
		big_record.put("username", "username");
		big_record.put("age", 10i32);
		big_record.put("phone", "000000000");
		big_record.put("housenum", "0000");
		big_record.put("address", address);
		big_record.into()
	};

	Ok((big_schema, fast_schema, big_record))
}

fn bench_small_schema_read_record(c: &mut Criterion) {
	let (schema, fast_schema, record) = make_small_record().unwrap();
	let datum = apache_avro::to_avro_datum(&schema, record).unwrap();
	c.bench_with_input(
		BenchmarkId::new("apache_avro", "small"),
		&datum.as_slice(),
		|b, &datum| {
			b.iter(|| {
				let value = apache_avro::from_avro_datum(&schema, &mut &*datum, None).unwrap();
				apache_avro::from_value::<SmallStructApache>(&value).unwrap()
			})
		},
	);
	c.bench_with_input(
		BenchmarkId::new("serde_avro_fast", "small"),
		&datum.as_slice(),
		|b, &datum| {
			b.iter(|| {
				serde_avro_fast::from_datum_slice::<SmallStruct>(datum, &fast_schema).unwrap()
			})
		},
	);
}

fn bench_big_schema_read_record(c: &mut Criterion) {
	let (schema, fast_schema, record) = make_big_record().unwrap();
	let datum = apache_avro::to_avro_datum(&schema, record).unwrap();
	c.bench_with_input(
		BenchmarkId::new("apache_avro", "big"),
		&datum.as_slice(),
		|b, &datum| {
			b.iter(|| {
				let value = apache_avro::from_avro_datum(&schema, &mut &*datum, None).unwrap();
				apache_avro::from_value::<BigStructApache>(&value).unwrap()
			})
		},
	);
	c.bench_with_input(
		BenchmarkId::new("serde_avro_fast", "big"),
		&datum.as_slice(),
		|b, &datum| {
			b.iter(|| serde_avro_fast::from_datum_slice::<BigStruct>(datum, &fast_schema).unwrap())
		},
	);
}
criterion_group!(
	benches,
	bench_small_schema_read_record,
	bench_big_schema_read_record
);
criterion_main!(benches);
