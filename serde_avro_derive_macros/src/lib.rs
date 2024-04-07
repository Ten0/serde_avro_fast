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
///   "name": "crate_name.path.to.Foo",
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
///
/// # let actual_schema = serde_json::to_string_pretty(&Foo::schema_mut())
/// #     .unwrap()
/// #     .replace("rust_out.", "crate_name.path.to.");
/// assert_eq!(actual_schema, schema_str);
/// ```
///
/// # Customize field schema
///
/// Field attributes can be used to specify logical type or override the
/// schema that a given field will produce:
/// ```
/// use serde_avro_derive::BuildSchema;
///
/// #[derive(BuildSchema)]
/// #[allow(unused)]
/// struct LogicalTypes<'a> {
/// 	#[avro_schema(logical_type = "Uuid")]
/// 	uuid: &'a str,
/// 	#[avro_schema(logical_type = "decimal", scale = 1, precision = 4)]
/// 	decimal: f64,
/// 	#[avro_schema(scale = 1, precision = 4)]
/// 	implicit_decimal: Decimal, // logical type is inferred because of the name of the type
/// 	#[avro_schema(logical_type = "custom-logical-type", has_same_type_as = "String")]
/// 	custom: MyCustomString,
/// }
/// struct MyCustomString(String);
/// struct Decimal {
/// 	_repr: (),
/// }
///
/// let expected_schema = r#"{
///   "type": "record",
///   "name": "crate_name.path.to.LogicalTypes",
///   "fields": [
///     {
///       "name": "uuid",
///       "type": {
///         "logicalType": "uuid",
///         "type": "string"
///       }
///     },
///     {
///       "name": "decimal",
///       "type": {
///         "logicalType": "decimal",
///         "type": "double",
///         "scale": 1,
///         "precision": 4
///       }
///     },
///     {
///       "name": "implicit_decimal",
///       "type": {
///         "logicalType": "decimal",
///         "type": "bytes",
///         "scale": 1,
///         "precision": 4
///       }
///     },
///     {
///       "name": "custom",
///       "type": {
///         "logicalType": "custom-logical-type",
///         "type": "string"
///       }
///     }
///   ]
/// }"#;
///
/// # let actual_schema = serde_json::to_string_pretty(&LogicalTypes::schema_mut())
/// #     .unwrap()
/// #     .replace("rust_out.", "crate_name.path.to.");
/// assert_eq!(actual_schema, expected_schema);
/// ```
///
/// # Namespace
///
/// The namespace will be inferred from the module path of the type being
/// derived. However it is possible to override this using the
/// `#[avro_schema(namespace = "my.namespace")]` attribute:
/// ```
/// use serde_avro_derive::BuildSchema;
///
/// #[derive(BuildSchema)]
/// #[avro_schema(namespace = "my.namespace")]
/// struct Foo {
/// 	bar: i32,
/// }
///
/// let schema = Foo::schema();
///
/// // The [`serde_avro_fast::schema::BuildSchema`] implementation will
/// // generate the following schema:
/// let schema_str = r#"{
///   "type": "record",
///   "name": "my.namespace.Foo",
///   "fields": [
///     {
///       "name": "bar",
///       "type": "int"
///     }
///   ]
/// }"#;
///
/// # let actual_schema = serde_json::to_string_pretty(&Foo::schema_mut())
/// #     .unwrap();
/// assert_eq!(actual_schema, schema_str);
/// ```
///
/// # Generics
///
/// Generics are supported - see
/// [the `tests` module](https://github.com/Ten0/serde_avro_fast/blob/master/serde_avro_derive/tests/derive_schema.rs)
/// for more advanced examples
pub fn build_schema_derive(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
	let derive_input = syn::parse_macro_input!(input as syn::DeriveInput);

	match FromDeriveInput::from_derive_input(&derive_input).map(build_schema::schema_impl) {
		Err(e) => e.write_errors().into(),
		Ok(Ok(tokens)) => tokens.into(),
		Ok(Err(e)) => e.into_compile_error().into(),
	}
}
