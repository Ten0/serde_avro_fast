use serde_avro_derive::BuildSchema;

use pretty_assertions::assert_eq;

fn test<T: BuildSchema>(expected: &str) {
	let schema_raw = serde_json::to_string_pretty(&T::schema_mut()).unwrap();
	let schema = clean_schema(&schema_raw);
	println!("{schema}");
	assert_eq!(schema, expected);

	// Round trip
	let schema_mut: serde_avro_fast::schema::SchemaMut = schema_raw.parse().unwrap();
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

#[derive(BuildSchema)]
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

#[derive(BuildSchema)]
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

#[derive(BuildSchema)]
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

#[derive(BuildSchema)]
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

#[derive(BuildSchema)]
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

#[derive(BuildSchema)]
#[allow(unused)]
struct LogicalTypes<'a> {
	#[avro_schema(logical_type = "Uuid")]
	uuid: &'a str,
	#[avro_schema(logical_type = r#"decimal"#, scale = 1, precision = 4)]
	decimal: f64,
	#[avro_schema(logical_type = r#"custom-logical-type"#, has_same_type_as = "String")]
	custom: MyCustomString,
}
struct MyCustomString {
	_inner: String,
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
        "logicalType": "custom-logical-type",
        "type": "string"
      }
    }
  ]
}"#,
	);
}

#[derive(BuildSchema)]
#[allow(unused)]
enum Ip {
	Ipv4([u8; 4]),
	Ipv6([u8; 16]),
	Normal(String),
}

#[test]
fn ip_enum() {
	test::<Ip>(
		r#"[
  {
    "type": "fixed",
    "name": "derive_schema.Ip.Ipv4",
    "size": 4
  },
  {
    "type": "fixed",
    "name": "derive_schema.Ip.Ipv6",
    "size": 16
  },
  "string"
]"#,
	);
}

#[derive(BuildSchema)]
#[allow(unused)]
enum FooEnum {
	Bar,
	Baz,
}

#[test]
fn foo_enum() {
	test::<FooEnum>(
		r#"{
  "type": "enum",
  "name": "derive_schema.FooEnum",
  "symbols": [
    "Bar",
    "Baz"
  ]
}"#,
	);
}

#[derive(BuildSchema)]
#[allow(unused)]
struct NewType(Box<[u8; 3]>);

#[test]
fn newtype() {
	test::<NewType>(
		r#"{
  "type": "fixed",
  "name": "derive_schema.NewType",
  "size": 3
}"#,
	);
}

#[derive(BuildSchema)]
#[avro_schema(namespace = "namespace_override")]
#[allow(unused)]
struct NewTypeNamespace([u8; 3]);

#[test]
fn newtype_namespace() {
	test::<NewTypeNamespace>(
		r#"{
  "type": "fixed",
  "name": "namespace_override.NewTypeNamespace",
  "size": 3
}"#,
	);
}

#[derive(BuildSchema)]
#[avro_schema(namespace = "namespace_override")]
#[allow(unused)]
enum FooEnumNamespace {
	Bar,
	Baz,
}

#[test]
fn foo_enum_namespace() {
	test::<FooEnumNamespace>(
		r#"{
  "type": "enum",
  "name": "namespace_override.FooEnumNamespace",
  "symbols": [
    "Bar",
    "Baz"
  ]
}"#,
	);
}

#[derive(BuildSchema)]
#[avro_schema(namespace = "namespace_override")]
#[allow(unused)]
enum IpNamespace {
	Ipv4([u8; 4]),
	Ipv6([u8; 16]),
	Normal(String),
}

#[test]
fn ip_enum_namespace() {
	test::<IpNamespace>(
		r#"[
  {
    "type": "fixed",
    "name": "namespace_override.IpNamespace.Ipv4",
    "size": 4
  },
  {
    "type": "fixed",
    "name": "namespace_override.IpNamespace.Ipv6",
    "size": 16
  },
  "string"
]"#,
	);
}

#[derive(BuildSchema)]
#[avro_schema(name = Name, namespace = "namespace")]
#[allow(unused)]
struct NameOverride {
	inner: i32,
}

#[test]
fn name_override() {
	test::<NameOverride>(
		r#"{
  "type": "record",
  "name": "namespace.Name",
  "fields": [
    {
      "name": "inner",
      "type": "int"
    }
  ]
}"#,
	);
}
