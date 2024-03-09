mod schema;

use darling::FromDeriveInput;

#[proc_macro_derive(Schema, attributes(avro_schema))]
pub fn schema_derive(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
	let derive_input = syn::parse_macro_input!(input as syn::DeriveInput);

	match FromDeriveInput::from_derive_input(&derive_input).map(schema::schema_impl) {
		Err(e) => e.write_errors().into(),
		Ok(Ok(tokens)) => tokens.into(),
		Ok(Err(e)) => e.into_compile_error().into(),
	}
}
