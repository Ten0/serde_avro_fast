//! Internal macros crate for the `serde_avro_derive` crate
//!
//! Use [`serde_avro_derive`](https://docs.rs/serde_avro_derive/) instead of using this crate directly

mod schema;

use darling::FromDeriveInput;

#[proc_macro_derive(Schema, attributes(avro_schema))]
/// Derive the ability to build an Avro schema for a type
///
/// # Example
/// ```
/// #[derive(serde_avro_derive::Schema)]
/// struct Foo {
/// 	primitives: Bar,
/// }
///
/// #[derive(serde_avro_derive::Schema)]
/// #[allow(unused)]
/// struct Bar {
/// 	a: i32,
/// 	b: String,
/// }
///
/// // The [`serde_avro_fast::schema::BuildSchema`] implementation will
/// // generate the following schema:
///
/// let schema = r#"{
///   "type": "record",
///   "name": "rust_out.Foo",
///   "fields": [
///     {
///       "name": "primitives",
///       "type": {
///         "type": "record",
///         "name": "Bar",
///         "fields": [
///           {
///             "name": "a",
///             "type": "int"
///           },
///           {
///             "name": "b",
///             "type": "string"
///           }
///         ]
///       }
///     }
///   ]
/// }"#;
/// # use serde_avro_derive::BuildSchema;
/// # let actual_schema = serde_json::to_string_pretty(&Foo::schema_mut()).unwrap();
/// # assert_eq!(actual_schema, schema);
/// ```
pub fn schema_derive(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
	let derive_input = syn::parse_macro_input!(input as syn::DeriveInput);

	match FromDeriveInput::from_derive_input(&derive_input).map(schema::schema_impl) {
		Err(e) => e.write_errors().into(),
		Ok(Ok(tokens)) => tokens.into(),
		Ok(Err(e)) => e.into_compile_error().into(),
	}
}
