use serde_avro_fast::{from_datum_slice, Schema};

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
		"name":"Record2",
		"fields":[
			{
				"name":"b",
				"type":"long"
			}
		]
	}
]"#;

#[derive(serde_derive::Deserialize, Debug, PartialEq)]
enum Union<'a> {
	Null,
	Array(#[serde(borrow)] Vec<&'a str>),
	Record1 { a: i64 },
	Record2(Record2),
	String,
	Long(u64),
}

#[derive(serde_derive::Deserialize, Debug, PartialEq)]
struct Record2 {
	b: i64,
}

#[test]
fn union_as_enum() {
	let schema: Schema = SCHEMA.parse().unwrap();
	let fds = |s: &'static [u8]| from_datum_slice::<Union<'static>>(s, &schema).unwrap();
	assert_eq!(fds(&[0, 2, b'a']), Union::String);
	assert_eq!(fds(&[2]), Union::Null);
	assert_eq!(fds(&[4, 2]), Union::Long(1));
	assert_eq!(
		fds(&[6, 4, 2, b'a', 2, b'b', 0]),
		Union::Array(vec!["a", "b"])
	);
	assert_eq!(fds(&[8, 1]), Union::Record1 { a: -1 });
	assert_eq!(fds(&[10, 3]), Union::Record2(Record2 { b: -2 }));
}

#[test]
fn option() {
	let schema: Schema = SCHEMA.parse().unwrap();
	let fds = |s: &'static [u8]| from_datum_slice::<Option<Union<'static>>>(s, &schema).unwrap();
	assert_eq!(fds(&[0, 2, b'a']), Some(Union::String));
	assert_eq!(fds(&[2]), None);
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
fn option_only_behavior() {
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
fn option_enum_behavior() {
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
