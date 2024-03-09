use {
	proc_macro2::{Span, TokenStream},
	quote::{format_ident, quote},
	syn::{
		visit_mut::{self, VisitMut},
		Error,
	},
};

#[derive(darling::FromDeriveInput)]
#[darling(attributes(avro_schema), supports(struct_named))]
pub(crate) struct SchemaDeriveInput {
	pub(super) ident: proc_macro2::Ident,
	pub(super) data: darling::ast::Data<(), SchemaDeriveField>,
	pub(super) generics: syn::Generics,
}

#[derive(darling::FromField)]
#[darling(attributes(avro_schema))]
pub(crate) struct SchemaDeriveField {
	pub(super) ident: Option<proc_macro2::Ident>,
	pub(super) ty: syn::Type,
}

pub(crate) fn schema_impl(input: SchemaDeriveInput) -> Result<TokenStream, Error> {
	let fields = input
		.data
		.take_struct()
		.expect("Supports directive should prevent enums");

	let ident = &input.ident;
	let struct_name = ident.to_string();
	let (impl_generics, ty_generics, where_clause) = input.generics.split_for_impl();

	let field_names = fields
		.iter()
		.map(|f| f.ident.as_ref().map(|i| i.to_string()))
		.collect::<Option<Vec<_>>>()
		.ok_or_else(|| Error::new(Span::call_site(), "Unnamed fields are not supported"))?;
	let field_types = fields.iter().map(|f| &f.ty);

	let mut generics_staticified = input.generics.clone();
	TurnLifetimesToStatic.visit_generics_mut(&mut generics_staticified);
	let (_, ty_generics_staticified, _) = generics_staticified.split_for_impl();

	Ok(quote! {
		const _: () = {
			use serde_avro_fast::schema::{self, builder};

			impl #impl_generics builder::BuildSchemaInner for #ident #ty_generics #where_clause {
				fn build(builder: &mut builder::SchemaBuilder) -> schema::SchemaKey {
					let reserved_schema_key = builder.reserve();
					let mut struct_name = module_path!().replace("::", ".");
					struct_name.push('.');
					struct_name.push_str(#struct_name);
					let new_node = schema::SchemaNode::RegularType(schema::RegularType::Record(
						schema::Record::new(
							schema::Name::from_fully_qualified_name(struct_name),
							vec![#(
								schema::RecordField::new(
									#field_names,
									builder::node_idx::<#field_types>(builder),
								),
							)*],
						),
					));
					builder.nodes[reserved_schema_key.idx()] = new_node;
					reserved_schema_key
				}

				type TypeLookup = #ident #ty_generics_staticified;
			}
		};
	})
}

struct TurnLifetimesToStatic;
impl VisitMut for TurnLifetimesToStatic {
	fn visit_lifetime_mut(&mut self, i: &mut syn::Lifetime) {
		i.ident = format_ident!("static");
		visit_mut::visit_lifetime_mut(self, i)
	}
}
