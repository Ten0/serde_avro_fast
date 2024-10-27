#![allow(unused)]

use serde_avro_derive::BuildSchema;

#[derive(BuildSchema)]
struct A(i32);

#[derive(BuildSchema)]
struct B(i32);

#[derive(BuildSchema)]
struct Foo {
	a: A,
	b: B,
}

#[test]
fn direct_lookup_newtype() {
	assert_eq!(Foo::schema().unwrap().json(), "{\"type\":\"record\",\"name\":\"direct_lookup_newtype.Foo\",\"fields\":[{\"name\":\"a\",\"type\":\"int\"},{\"name\":\"b\",\"type\":\"int\"}]}");
}
