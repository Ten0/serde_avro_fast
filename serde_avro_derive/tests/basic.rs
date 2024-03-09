use serde_avro_fast::schema::BuildSchema;

#[derive(serde_avro_derive::Schema)]
#[allow(unused)]
struct Primitives {
	a: i32,
	b: String,
}

#[derive(serde_avro_derive::Schema)]
struct SubStruct {
	#[allow(unused)]
	primitives: Primitives,
}

#[derive(serde_avro_derive::Schema)]
#[allow(unused)]
struct TopStruct {
	s1: SubStruct,
	s2: SubStruct,
	vec: Vec<String>,
}

fn test<T: BuildSchema>(expected: &str) {
	let schema = serde_json::to_string_pretty(&T::schema_mut()).unwrap();
	println!("{schema}");
	assert_eq!(schema, expected);
}

#[test]
fn primitives() {
	test::<Primitives>(
		r#"{
  "type": "record",
  "name": "basic.Primitives",
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
fn substruct_and_vec() {
	test::<TopStruct>(
		r#"{
  "type": "record",
  "name": "basic.TopStruct",
  "fields": [
    {
      "name": "s1",
      "type": {
        "type": "record",
        "name": "SubStruct",
        "fields": [
          {
            "name": "primitives",
            "type": {
              "type": "record",
              "name": "Primitives",
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
      "type": "SubStruct"
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
