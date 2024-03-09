use serde_avro_derive::BuildSchema;

use pretty_assertions::assert_eq;

#[derive(serde_avro_derive::Schema)]
struct Foo {
	#[allow(unused)]
	primitives: Bar,
}

#[derive(serde_avro_derive::Schema)]
#[allow(unused)]
struct Bar {
	a: i32,
	b: String,
}

#[derive(serde_avro_derive::Schema)]
#[allow(unused)]
struct Complex {
	s1: Foo,
	s2: Foo,
	vec: Vec<String>,
}

fn test<T: BuildSchema>(expected: &str) {
	let schema = serde_json::to_string_pretty(&T::schema_mut()).unwrap();
	println!("{schema}");
	assert_eq!(schema, expected);
}

#[test]
fn primitives() {
	test::<Bar>(
		r#"{
  "type": "record",
  "name": "basic.Bar",
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

#[test]
fn substruct() {
	test::<Foo>(
		r#"{
  "type": "record",
  "name": "basic.Foo",
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

#[test]
fn complex() {
	test::<Complex>(
		r#"{
  "type": "record",
  "name": "basic.Complex",
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
