use serde_avro_derive::BuildSchema;

use pretty_assertions::assert_eq;

fn test<T: BuildSchema>(expected: &str) {
	let schema = serde_json::to_string_pretty(&T::schema_mut()).unwrap();
	println!("{schema}");
	assert_eq!(schema, expected);
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
  "name": "derive_schema.Generics_62462e653c3a8376",
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
