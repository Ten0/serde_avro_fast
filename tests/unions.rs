use serde_avro_fast::{from_datum_slice, to_datum_vec, Schema};

const SCHEMA: &str = r#"[
	"string",
	"null",
	"long",
	{
		"type": "array",
		"items": "string"
	},
	{
		"type":"record",
		"name":"Record1",
		"fields":[
			{
				"name":"a",
				"type":"long"
			}
		]
	},
	{
		"type":"record",
		"namespace": "some_namespace",
		"name":"Record2",
		"fields":[
			{
				"name":"b",
				"type":"long"
			}
		]
	}
]"#;

#[derive(serde_derive::Deserialize, serde_derive::Serialize, Debug, PartialEq)]
enum Union<'a> {
	Null,
	Array(#[serde(borrow)] Vec<&'a str>),
	Record1 {
		a: i64,
	},
	#[serde(rename = "some_namespace.Record2")]
	Record2(Record2),
	String,
	Long(u64),
}

#[derive(serde_derive::Deserialize, serde_derive::Serialize, Debug, PartialEq)]
struct Record2 {
	b: i64,
}

fn test<'de, T: serde::Serialize + serde::Deserialize<'de> + PartialEq + std::fmt::Debug>(
	datum: &'de [u8],
	rust_value: T,
	schema: &Schema,
) {
	assert_eq!(from_datum_slice::<T>(datum, schema).unwrap(), rust_value);
	assert_eq!(to_datum_vec(&rust_value, schema).unwrap(), datum);
}

#[test]
fn union_as_enum() {
	let schema: Schema = SCHEMA.parse().unwrap();
	let test = |s: &'static [u8], value: Union<'static>| test::<Union<'static>>(s, value, &schema);
	assert_eq!(
		from_datum_slice::<Union>(&[0, 2, b'a'], &schema).unwrap(),
		Union::String
	);
	assert_eq!(
		from_datum_slice::<Union>(&[2], &schema).unwrap(),
		Union::Null
	);
	test(&[4, 2], Union::Long(1));
	test(&[6, 4, 2, b'a', 2, b'b', 0], Union::Array(vec!["a", "b"]));
	test(&[8, 1], Union::Record1 { a: -1 });
	test(&[10, 3], Union::Record2(Record2 { b: -2 }));
}

#[test]
fn union_straight_to_actual_type() {
	let schema: Schema = SCHEMA.parse().unwrap();
	assert_eq!(
		from_datum_slice::<&str>(&[0, 2, b'a'], &schema).unwrap(),
		"a"
	);
	assert_eq!(from_datum_slice::<()>(&[2], &schema).unwrap(), ());
	assert_eq!(from_datum_slice::<i64>(&[4, 2], &schema).unwrap(), 1);
	assert_eq!(
		from_datum_slice::<Vec<&str>>(&[6, 4, 2, b'a', 2, b'b', 0], &schema).unwrap(),
		vec!["a", "b"]
	);
	assert_eq!(
		from_datum_slice::<Record2>(&[10, 3], &schema).unwrap(),
		Record2 { b: -2 }
	);
}

#[test]
fn option_complex() {
	let schema: Schema = SCHEMA.parse().unwrap();
	let test = |s: &'static [u8], value: Option<Union<'static>>| {
		test::<Option<Union<'static>>>(s, value, &schema)
	};
	assert_eq!(
		from_datum_slice::<Option<Union>>(&[0, 2, b'a'], &schema).unwrap(),
		Some(Union::String)
	);
	test(&[2], None);
}

#[test]
fn option_simple() {
	let schema: Schema = r#"["string", "null"]"#.parse().unwrap();
	assert_eq!(
		from_datum_slice::<Option<&str>>(&[2], &schema).unwrap(),
		None
	);
	assert_eq!(
		from_datum_slice::<Option<&str>>(&[0, 2, b'a'], &schema).unwrap(),
		Some("a")
	);
}

#[test]
fn option_of_enum_union_single() {
	let schema: Schema = r#"["string", "null"]"#.parse().unwrap();
	#[derive(serde_derive::Deserialize, PartialEq, Debug)]
	enum WhatDoWeDoHere {
		A,
	}
	assert_eq!(
		from_datum_slice::<Option<WhatDoWeDoHere>>(&[0, 2, b'A'], &schema).unwrap(),
		Some(WhatDoWeDoHere::A),
	);
}

#[test]
fn option_of_enum_union_multi() {
	let schema: Schema =
		r#"["string", "null", {"name":"AnEnum", "type": "enum", "symbols": ["A", "B"]}]"#
			.parse()
			.unwrap();
	#[derive(serde_derive::Deserialize, PartialEq, Debug)]
	enum WhatDoWeDoHere {
		A,
	}
	#[derive(serde_derive::Deserialize, PartialEq, Debug)]
	enum StringOrAnEnum {
		String(WhatDoWeDoHere),
		AnEnum(WhatDoWeDoHere),
	}
	assert_eq!(
		from_datum_slice::<Option<StringOrAnEnum>>(&[0, 2, b'A'], &schema).unwrap(),
		Some(StringOrAnEnum::String(WhatDoWeDoHere::A)),
	);
	assert_eq!(
		from_datum_slice::<Option<StringOrAnEnum>>(&[4, 0], &schema).unwrap(),
		Some(StringOrAnEnum::AnEnum(WhatDoWeDoHere::A)),
	);
}
