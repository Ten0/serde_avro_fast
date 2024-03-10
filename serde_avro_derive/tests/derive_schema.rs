use serde_avro_derive::BuildSchema;

use pretty_assertions::assert_eq;

fn test<T: BuildSchema>(expected: &str) {
	let schema = clean_schema(&serde_json::to_string_pretty(&T::schema_mut()).unwrap());
	println!("{schema}");
	assert_eq!(schema, expected);

	// Round trip
	let schema_mut: serde_avro_fast::schema::SchemaMut = schema.parse().unwrap();
	dbg!(&schema_mut);
	let schema2 = clean_schema(&serde_json::to_string_pretty(&schema_mut).unwrap());
	assert_eq!(schema2, expected);
	let _schema: serde_avro_fast::Schema = schema_mut.try_into().unwrap();
}

fn clean_schema(schema: &str) -> String {
	lazy_static::lazy_static! {
		static ref REGEX: regex::Regex = regex::Regex::new(r#""(derive_schema\.[^_]+_)\w{16}""#).unwrap();
	}
	REGEX
		.replace_all(schema, r#""${1}TYPEIDHASH""#)
		.into_owned()
}

#[derive(serde_avro_derive::Schema)]
#[allow(unused)]
struct Bar {
	a: i32,
	b: String,
}

#[test]
fn primitives() {
	test::<Bar>(
		r#"{
  "type": "record",
  "name": "derive_schema.Bar",
  "fields": [
    {
      "name": "a",
      "type": "int"
    },
    {
      "name": "b",
      "type": "string"
    }
  ]
}"#,
	);
}

#[derive(serde_avro_derive::Schema)]
struct Foo {
	#[allow(unused)]
	primitives: Bar,
}

#[test]
fn substruct() {
	test::<Foo>(
		r#"{
  "type": "record",
  "name": "derive_schema.Foo",
  "fields": [
    {
      "name": "primitives",
      "type": {
        "type": "record",
        "name": "Bar",
        "fields": [
          {
            "name": "a",
            "type": "int"
          },
          {
            "name": "b",
            "type": "string"
          }
        ]
      }
    }
  ]
}"#,
	);
}

#[derive(serde_avro_derive::Schema)]
#[allow(unused)]
struct Complex {
	s1: Foo,
	s2: Foo,
	vec: Vec<String>,
}

#[test]
fn complex() {
	test::<Complex>(
		r#"{
  "type": "record",
  "name": "derive_schema.Complex",
  "fields": [
    {
      "name": "s1",
      "type": {
        "type": "record",
        "name": "Foo",
        "fields": [
          {
            "name": "primitives",
            "type": {
              "type": "record",
              "name": "Bar",
              "fields": [
                {
                  "name": "a",
                  "type": "int"
                },
                {
                  "name": "b",
                  "type": "string"
                }
              ]
            }
          }
        ]
      }
    },
    {
      "name": "s2",
      "type": "Foo"
    },
    {
      "name": "vec",
      "type": {
        "type": "array",
        "items": "string"
      }
    }
  ]
}"#,
	);
}

#[derive(serde_avro_derive::Schema)]
#[allow(unused)]
struct Generics<'a, F> {
	s1: F,
	s2: &'a F,
	s: &'a str,
}

#[test]
fn generics() {
	test::<Generics<'_, Bar>>(
		r#"{
  "type": "record",
  "name": "derive_schema.Generics_TYPEIDHASH",
  "fields": [
    {
      "name": "s1",
      "type": {
        "type": "record",
        "name": "Bar",
        "fields": [
          {
            "name": "a",
            "type": "int"
          },
          {
            "name": "b",
            "type": "string"
          }
        ]
      }
    },
    {
      "name": "s2",
      "type": "Bar"
    },
    {
      "name": "s",
      "type": "string"
    }
  ]
}"#,
	);
}

#[derive(serde_avro_derive::Schema)]
#[allow(unused)]
struct Lifetimes<'a, 'b> {
	s: &'a [&'b str],
	#[avro_schema(skip)]
	z: String,
}

#[test]
fn lifetimes() {
	test::<Lifetimes<'_, '_>>(
		r#"{
  "type": "record",
  "name": "derive_schema.Lifetimes",
  "fields": [
    {
      "name": "s",
      "type": {
        "type": "array",
        "items": "string"
      }
    }
  ]
}"#,
	);
}

#[derive(serde_avro_derive::Schema)]
#[allow(unused)]
struct LogicalTypes<'a> {
	#[avro_schema(logical_type = Uuid)]
	uuid: &'a str,
	#[avro_schema(logical_type = Decimal, scale = 1, precision = 4)]
	decimal: f64,
	#[avro_schema(logical_type = CustomLogicalType)]
	custom: &'a str,
}

#[test]
fn logical_types() {
	test::<LogicalTypes<'_>>(
		r#"{
  "type": "record",
  "name": "derive_schema.LogicalTypes",
  "fields": [
    {
      "name": "uuid",
      "type": {
        "logicalType": "uuid",
        "type": "string"
      }
    },
    {
      "name": "decimal",
      "type": {
        "logicalType": "decimal",
        "type": "double",
        "scale": 1,
        "precision": 4
      }
    },
    {
      "name": "custom",
      "type": {
        "logicalType": "CustomLogicalType",
        "type": "string"
      }
    }
  ]
}"#,
	);
}
