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
//! #[derive(serde_derive::Serialize, serde_derive::Deserialize, Debug, PartialEq)]
//! struct Test<'a> {
//! 	field: &'a str,
//! }
//!
//! let rust_value = Test { field: "foo" };
//! let avro_datum = &[6, 102, 111, 111];
//!
//! // Avro datum deserialization
//! assert_eq!(
//! 	serde_avro_fast::from_datum_slice::<Test>(avro_datum, &schema)
//! 		.expect("Failed to deserialize"),
//! 	rust_value
//! );
//!
//! // Avro datum serialization
//! assert_eq!(
//! 	serde_avro_fast::to_datum(
//! 		&rust_value,
//! 		Vec::new(),
//! 		&mut serde_avro_fast::ser::SerializerConfig::new(&schema)
//! 	)
//! 	.expect("Failed to serialize"),
//! 	avro_datum
//! );
//! ```
//!
//! # Object container file encoding
//! Otherwise called "avro files", avro object container files contain a header
//! that holds the schema, followed by an arbitrary number of avro objects.
//!
//! For this use-case, please see the [`object_container_file_encoding`] module
//! documentation.
//!
//! # Deriving schema from Rust structs
//!
//! If the Rust program is the source of truth for the schema definition, it is
//! useful to define the schema as a derive on the relevant Rust structs.
//! This can be achieved using the [`serde_avro_derive`](https://docs.rs/serde_avro_derive/)
//! crate:
//!
//! ```
//! use serde_avro_derive::BuildSchema;
//!
//! #[derive(BuildSchema)]
//! struct Foo {
//! 	primitives: Bar,
//! }
//!
//! #[derive(BuildSchema)]
//! struct Bar {
//! 	a: i32,
//! 	b: String,
//! }
//!
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! let schema: serde_avro_fast::Schema = Foo::schema()?;
//!
//! // This will generate the following schema:
//! let _schema_str = r#"{
//!   "type": "record",
//!   "name": "crate_name.path.to.Foo",
//!   "fields": [{
//!     "name": "primitives",
//!     "type": {
//!       "type": "record",
//!       "name": "Bar",
//!       "fields": [
//!         { "name": "a", "type": "int" },
//!         { "name": "b", "type": "string" }
//!       ]
//!     }
//!   }]
//! }"#;
//! # assert_eq!(schema.json(), {
//! # 	let mut serializer = serde_json::Serializer::new(Vec::new());
//! # 	serde_transcode::transcode(
//! # 		&mut serde_json::Deserializer::from_str(&_schema_str.replace("crate_name.path.to.", "rust_out.")),
//! # 		&mut serializer,
//! # 	)
//! # 	.unwrap();
//! # 	String::from_utf8(serializer.into_inner()).unwrap()
//! # });
//! # Ok(())
//! # }
//! ```
//! See the [`serde_avro_derive`](https://docs.rs/serde_avro_derive/) documentation
//! for more details.
//!
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

// Get docs.rs to display all compression methods and corresponding feature flags.
// That is used jointly with `package.metadata.docs.rs` in the `Cargo.toml`
#![cfg_attr(docsrs, feature(doc_auto_cfg))]

pub mod de;
pub mod schema;
pub mod ser;

pub use schema::Schema;

mod single_object_encoding;
pub use single_object_encoding::{
	from_single_object_reader, from_single_object_slice, to_single_object, to_single_object_vec,
};

pub mod object_container_file_encoding;

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
	serde::Deserialize::deserialize(de::DeserializerState::from_slice(slice, schema).deserializer())
}

/// Deserialize from an avro "datum" (raw data, no headers...) `impl BufRead`
///
/// If you only have an `impl Read`, wrap it in a
/// [`BufReader`](std::io::BufReader) first.
///
/// If deserializing from a slice, a `Vec`, ... prefer using `from_datum_slice`,
/// as it will be more performant and enable you to borrow `&str`s from the
/// original slice.
pub fn from_datum_reader<R, T>(reader: R, schema: &Schema) -> Result<T, de::DeError>
where
	T: serde::de::DeserializeOwned,
	R: std::io::BufRead,
{
	serde::Deserialize::deserialize(
		de::DeserializerState::from_reader(reader, schema).deserializer(),
	)
}

/// Serialize an avro "datum" (raw data, no headers...)
///
/// to the provided writer
///
/// [`SerializerConfig`](ser::SerializerConfig) can be built from a schema:
/// ```
/// # use serde_avro_fast::{ser, Schema};
/// let schema: Schema = r#""int""#.parse().unwrap();
/// let serializer_config = &mut ser::SerializerConfig::new(&schema);
///
/// let mut serialized: Vec<u8> = serde_avro_fast::to_datum_vec(&3, serializer_config).unwrap();
/// assert_eq!(serialized, &[6]);
///
/// // reuse config and output buffer across serializations for ideal performance (~40% perf gain)
/// serialized.clear();
/// let serialized = serde_avro_fast::to_datum(&4, serialized, serializer_config).unwrap();
/// assert_eq!(serialized, &[8]);
/// ```
pub fn to_datum<T, W>(
	value: &T,
	writer: W,
	serializer_config: &mut ser::SerializerConfig<'_>,
) -> Result<W, ser::SerError>
where
	T: serde::Serialize + ?Sized,
	W: std::io::Write,
{
	let mut serializer_state = ser::SerializerState::from_writer(writer, serializer_config);
	serde::Serialize::serialize(value, serializer_state.serializer())?;
	Ok(serializer_state.into_writer())
}

/// Serialize an avro "datum" (raw data, no headers...)
///
/// to a newly allocated Vec
///
/// Note that unless you would otherwise allocate a new `Vec` anyway, it will be
/// more efficient to use [`to_datum`] instead.
///
/// See [`to_datum`] for more details.
pub fn to_datum_vec<T>(
	value: &T,
	serializer_config: &mut ser::SerializerConfig<'_>,
) -> Result<Vec<u8>, ser::SerError>
where
	T: serde::Serialize + ?Sized,
{
	to_datum(value, Vec::new(), serializer_config)
}
