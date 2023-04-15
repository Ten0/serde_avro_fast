//! # Getting started
//!
//! ```
//! let schema: serde_avro_fast::Schema = r#"
//! {
//! 	"namespace": "test",
//! 	"type": "record",
//! 	"name": "Test",
//! 	"fields": [
//! 		{
//! 			"type": {
//! 				"type": "string"
//! 			},
//! 			"name": "field"
//! 		}
//! 	]
//! }
//! "#
//! .parse()
//! .expect("Failed to parse schema");
//!
//! #[derive(serde_derive::Deserialize, Debug, PartialEq)]
//! struct Test<'a> {
//! 	field: &'a str,
//! }
//!
//! let avro_datum = &[6, 102, 111, 111];
//! assert_eq!(
//! 	serde_avro_fast::from_datum_slice::<Test>(avro_datum, &schema)
//! 		.expect("Failed to deserialize"),
//! 	Test { field: "foo" }
//! );
//! ```
//! # An idiomatic (re)implementation of serde/avro (de)serialization
//!
//! At the time of writing, the other existing libraries for [Avro](https://avro.apache.org/docs/current/specification/)
//! (de)serialization do tons of unnecessary allocations, `HashMap` lookups,
//! etc... for every record they encounter.
//!
//! This version is a more idiomatic implementation, both with regards to Rust
//! and to [`serde`].
//!
//! It is consequently >10x more performant (cf benchmarks):
//! ```txt
//! apache_avro/small       time:   [386.57 ns 387.04 ns 387.52 ns]
//! serde_avro_fast/small   time:   [19.367 ns 19.388 ns 19.413 ns] <- x20 improvement
//!
//! apache_avro/big         time:   [1.8618 µs 1.8652 µs 1.8701 µs]
//! serde_avro_fast/big     time:   [165.87 ns 166.92 ns 168.09 ns] <- x11 improvement
//! ```
//!
//! It currently has a dependency on [`apache_avro`], to parse the schema and
//! obtain its fingerprint.

pub mod de;
pub mod schema;
pub mod ser;

pub use schema::Schema;

mod single_object_encoding;
pub use single_object_encoding::{from_single_object_reader, from_single_object_slice};

pub mod object_container_file_encoding;

/// Because we use some of its types (namely, schema), reexport it in case
/// interop is needed by users
pub use apache_avro;

/// Deserialize from an avro "datum" (raw data, no headers...) slice
///
/// This is zero-alloc.
///
/// Your structure may contain `&'a str`s that will end up pointing directly
/// into this slice for ideal performance.
pub fn from_datum_slice<'a, T>(slice: &'a [u8], schema: &Schema) -> Result<T, de::DeError>
where
	T: serde::Deserialize<'a>,
{
	serde::Deserialize::deserialize(
		de::DeserializerState::from_slice(slice, &schema).deserializer(),
	)
}

/// Deserialize from an avro "datum" (raw data, no headers...) `impl Read`
///
/// If deserializing from a slice, a `Vec`, ... prefer using `from_datum_slice`,
/// as it will be more performant and enable you to borrow `&str`s from the
/// original slice.
pub fn from_datum_reader<R, T>(reader: R, schema: &Schema) -> Result<T, de::DeError>
where
	T: serde::de::DeserializeOwned,
	R: std::io::Read,
{
	serde::Deserialize::deserialize(
		de::DeserializerState::from_reader(reader, &schema).deserializer(),
	)
}

/// Serialize an avro "datum" (raw data, no headers...)
///
/// to the provided writer
pub fn to_datum<T, W>(value: &T, writer: W, schema: &Schema) -> Result<(), ser::SerError>
where
	T: serde::Serialize + ?Sized,
	W: std::io::Write,
{
	serde::Serialize::serialize(
		value,
		ser::SerializerState::from_writer(writer, schema).serializer(),
	)
}
