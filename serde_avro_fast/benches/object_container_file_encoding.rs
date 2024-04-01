#![allow(missing_docs)]

use serde_avro_fast::object_container_file_encoding::{Compression, CompressionLevel};

use criterion::BenchmarkId;

use criterion::{criterion_group, criterion_main, Criterion};

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

#[derive(serde_derive::Deserialize, serde_derive::Serialize)]
#[allow(unused)]
struct BigStruct<'a> {
	username: &'a str,
	age: u32,
	phone: &'a str,
	housenum: &'a str,
	address: Address<'a>,
}

#[derive(serde_derive::Deserialize, serde_derive::Serialize)]
#[allow(unused)]
struct Address<'a> {
	street: &'a str,
	city: &'a str,
	state_prov: &'a str,
	country: &'a str,
	zip: &'a str,
}

#[derive(serde_derive::Deserialize, serde_derive::Serialize)]
#[allow(unused)]
struct BigStructOwned {
	username: String,
	age: u32,
	phone: String,
	housenum: String,
	address: AddressOwned,
}

#[derive(serde_derive::Deserialize, serde_derive::Serialize)]
#[allow(unused)]
struct AddressOwned {
	street: String,
	city: String,
	state_prov: String,
	country: String,
	zip: String,
}

fn bench_object_container_file_serialization(c: &mut Criterion) {
	let apache_schema = apache_avro::Schema::parse_str(RAW_BIG_SCHEMA).unwrap();
	let schema: serde_avro_fast::Schema = RAW_BIG_SCHEMA.parse().unwrap();
	let inputs: Vec<BigStruct> = (0..100000)
		.map(|i| BigStruct {
			username: "John Doe",
			age: i,
			phone: "555-555-5555",
			housenum: "123",
			address: Address {
				street: "123 Fake St",
				city: "Springfield",
				state_prov: "IL",
				country: "USA",
				zip: "12345",
			},
		})
		.collect();
	for &(name, compression_codec, apache_codec) in &[
		("null", Compression::Null, apache_avro::Codec::Null),
		#[cfg(feature = "deflate")]
		(
			"deflate",
			Compression::Deflate {
				level: CompressionLevel::default(),
			},
			apache_avro::Codec::Deflate,
		),
		#[cfg(feature = "bzip2")]
		(
			"bzip2",
			Compression::Bzip2 {
				level: CompressionLevel::default(),
			},
			apache_avro::Codec::Bzip2,
		),
		#[cfg(feature = "snappy")]
		("snappy", Compression::Snappy, apache_avro::Codec::Snappy),
		#[cfg(feature = "xz")]
		(
			"xz",
			Compression::Xz {
				level: CompressionLevel::default(),
			},
			apache_avro::Codec::Xz,
		),
		#[cfg(feature = "zstandard")]
		(
			"zstandard",
			Compression::Zstandard {
				level: CompressionLevel::default(),
			},
			apache_avro::Codec::Zstandard,
		),
	] {
		c.bench_with_input(
			BenchmarkId::new("serde_avro_fast_object_container_file_serialization", name),
			&inputs.as_slice(),
			|b, &inputs| {
				b.iter(|| {
					serde_avro_fast::object_container_file_encoding::write_all(
						&schema,
						compression_codec,
						Vec::new(),
						inputs,
					)
				})
			},
		);
		c.bench_with_input(
			BenchmarkId::new("apache_avro_object_container_file_serialization", name),
			&inputs.as_slice(),
			|b, &inputs| {
				b.iter(|| {
					let mut writer =
						apache_avro::Writer::with_codec(&apache_schema, Vec::new(), apache_codec);
					for input in inputs {
						writer.append_ser(input).unwrap();
					}
					writer.into_inner().unwrap()
				})
			},
		);
	}
}

fn bench_object_container_file_deserialization(c: &mut Criterion) {
	let schema: serde_avro_fast::Schema = RAW_BIG_SCHEMA.parse().unwrap();
	for &(name, codec) in &[
		("null", Compression::Null),
		#[cfg(feature = "deflate")]
		(
			"deflate",
			Compression::Deflate {
				level: CompressionLevel::default(),
			},
		),
		#[cfg(feature = "bzip2")]
		(
			"bzip2",
			Compression::Bzip2 {
				level: CompressionLevel::default(),
			},
		),
		#[cfg(feature = "snappy")]
		("snappy", Compression::Snappy),
		#[cfg(feature = "xz")]
		(
			"xz",
			Compression::Xz {
				level: CompressionLevel::default(),
			},
		),
		#[cfg(feature = "zstandard")]
		(
			"zstandard",
			Compression::Zstandard {
				level: CompressionLevel::default(),
			},
		),
	] {
		let serialized = serde_avro_fast::object_container_file_encoding::write_all(
			&schema,
			codec,
			Vec::new(),
			(0..100000).map(|i| BigStruct {
				username: "John Doe",
				age: i,
				phone: "555-555-5555",
				housenum: "123",
				address: Address {
					street: "123 Fake St",
					city: "Springfield",
					state_prov: "IL",
					country: "USA",
					zip: "12345",
				},
			}),
		)
		.unwrap();
		c.bench_with_input(
			BenchmarkId::new(
				"serde_avro_fast_object_container_file_deserialization",
				name,
			),
			&serialized,
			|b, inputs| {
				b.iter(|| {
					let mut n = 0u64;
					serde_avro_fast::object_container_file_encoding::Reader::from_slice(
						inputs.as_slice(),
					)
					.unwrap()
					.deserialize::<BigStructOwned>()
					.try_for_each(|r| {
						let s = r?;
						n += s.age as u64;
						Ok::<_, serde_avro_fast::de::DeError>(())
					})
					.unwrap();
					n
				})
			},
		);
		c.bench_with_input(
			BenchmarkId::new("apache_avro_object_container_file_deserialization", name),
			&serialized,
			|b, inputs| {
				b.iter(|| {
					let mut n = 0u64;
					apache_avro::Reader::new(inputs.as_slice())
						.unwrap()
						.try_for_each(|res| {
							let value = res?;
							let deserialized: BigStructOwned = apache_avro::from_value(&value)?;
							n += deserialized.age as u64;
							Ok::<_, apache_avro::Error>(())
						})
						.unwrap();
					n
				})
			},
		);
	}
}

criterion_group!(
	benches,
	bench_object_container_file_serialization,
	bench_object_container_file_deserialization
);
criterion_main!(benches);
