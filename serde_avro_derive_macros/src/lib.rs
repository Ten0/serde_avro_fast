//! Internal macros crate for the `serde_avro_derive` crate
//!
//! Use [`serde_avro_derive`](https://docs.rs/serde_avro_derive/) instead of using this crate directly

mod build_schema;

use darling::FromDeriveInput;

#[proc_macro_derive(BuildSchema, attributes(avro_schema))]
/// Derive the ability to build an Avro schema for a type
/// (implements `BuildSchema`)
///
/// # Example
/// ```
/// use serde_avro_derive::BuildSchema;
///
/// #[derive(BuildSchema)]
/// struct Foo {
/// 	primitives: Bar,
/// }
///
/// #[derive(BuildSchema)]
/// struct Bar {
/// 	a: i32,
/// 	b: String,
/// }
///
/// let schema = Foo::schema();
///
/// // The [`serde_avro_fast::schema::BuildSchema`] implementation will
/// // generate the following schema:
/// let schema_str = r#"{
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
/// // Note that the `rust_out` namespace here is only due to the fact this is a doctest:
/// // the name will always be crate_name.path.to.module.Foo
/// // (but for doctests the crate is called rust_out and the struct is at top level)
/// # let actual_schema = serde_json::to_string_pretty(&Foo::schema_mut()).unwrap();
/// # assert_eq!(actual_schema, schema_str);
/// ```
pub fn build_schema_derive(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
	let derive_input = syn::parse_macro_input!(input as syn::DeriveInput);

	match FromDeriveInput::from_derive_input(&derive_input).map(build_schema::schema_impl) {
		Err(e) => e.write_errors().into(),
		Ok(Ok(tokens)) => tokens.into(),
		Ok(Err(e)) => e.into_compile_error().into(),
	}
}
