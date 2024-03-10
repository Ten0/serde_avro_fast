use {
	proc_macro2::{Span, TokenStream},
	quote::{format_ident, quote},
	syn::{
		parse_quote,
		visit::{self, Visit},
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
	let mut generics = input.generics;

	let mut added_where_clause_predicate_for_types: std::collections::HashSet<_> =
		Default::default();
	let field_types = fields
		.iter()
		.map(|f| {
			let mut ty = &f.ty;
			while let syn::Type::Reference(r) = ty {
				// This allows not requiring the user to specify that T: 'a
				// as an explicit where predicate, and simplifies the calls
				ty = &r.elem;
			}
			if !generics.params.is_empty() {
				let mut is_relevant_generic = IsRelevantGeneric {
					generics: &generics,
					result: false,
				};
				is_relevant_generic.visit_type(ty);
				if is_relevant_generic.result {
					if added_where_clause_predicate_for_types.insert(ty) {
						generics
							.make_where_clause()
							.predicates
							.push(parse_quote!(#ty: serde_avro_derive::BuildSchema));
					}
				}
			}
			ty
		})
		.collect::<Vec<_>>();

	let (impl_generics, ty_generics, where_clause) = generics.split_for_impl();

	let field_names = fields
		.iter()
		.map(|f| f.ident.as_ref().map(|i| i.to_string()))
		.collect::<Option<Vec<_>>>()
		.ok_or_else(|| Error::new(Span::call_site(), "Unnamed fields are not supported"))?;

	let has_generics = !generics.params.is_empty();
	let (type_lookup, type_lookup_decl): (syn::Type, _) = match has_generics {
		false => (parse_quote!(Self), None),
		true => {
			// The struct we are deriving on is generic, but we need the TypeLookup to be
			// 'static otherwise it won't implement `Any`, so we need to generate a
			// dedicated struct for it.
			let type_lookup_ident = format_ident!("{ident}TypeLookup");
			let type_params: Vec<syn::Ident> = (0..generics.params.len())
				.map(|i| format_ident!("T{}", i))
				.collect();
			let struct_decl = syn::ItemStruct {
				attrs: Default::default(),
				vis: syn::Visibility::Inherited,
				struct_token: syn::token::Struct::default(),
				ident: type_lookup_ident.clone(),
				generics: syn::Generics {
					lt_token: Some(Default::default()),
					params: type_params
						.iter()
						.map(|ident| -> syn::GenericParam { parse_quote!(#ident) })
						.collect(),
					gt_token: Some(Default::default()),
					where_clause: None,
				},
				fields: syn::Fields::Named(syn::FieldsNamed {
					named: fields
						.iter()
						.zip(&type_params)
						.map(|(field, ident)| syn::Field {
							attrs: Default::default(),
							vis: syn::Visibility::Inherited,
							ident: field.ident.clone(),
							colon_token: Some(Default::default()),
							ty: { parse_quote!(#ident) },
							mutability: syn::FieldMutability::None,
						})
						.collect(),
					brace_token: Default::default(),
				}),
				semi_token: None,
			};
			let type_lookup = syn::PathSegment {
				ident: type_lookup_ident,
				arguments: syn::PathArguments::AngleBracketed(
					syn::AngleBracketedGenericArguments {
						args: field_types
							.iter()
							.map(|ty| -> syn::GenericArgument {
								parse_quote!(<#ty as serde_avro_derive::BuildSchema>::TypeLookup)
							})
							.collect(),
						colon2_token: Default::default(),
						lt_token: Default::default(),
						gt_token: Default::default(),
					},
				),
			};
			(parse_quote!(#type_lookup), Some(struct_decl))
		}
	};

	let add_type_id_to_fqn = if has_generics {
		quote! {
			serde_avro_derive::hash_type_id(
				&mut struct_name,
				std::any::TypeId::of::<<Self as serde_avro_derive::BuildSchema>::TypeLookup>(),
			);
		}
	} else {
		quote! {}
	};

	Ok(quote! {
		const _: () = {
			use serde_avro_derive::serde_avro_fast::schema;

			impl #impl_generics serde_avro_derive::BuildSchema for #ident #ty_generics #where_clause {
				fn append_schema(builder: &mut serde_avro_derive::SchemaBuilder) {
					let reserved_schema_key = builder.reserve();
					let mut struct_name = module_path!().replace("::", ".");
					struct_name.push('.');
					struct_name.push_str(#struct_name);
					#add_type_id_to_fqn
					let new_node = schema::SchemaNode::RegularType(schema::RegularType::Record(
						schema::Record::new(
							schema::Name::from_fully_qualified_name(struct_name),
							vec![#(
								schema::RecordField::new(
									#field_names,
									builder.find_or_build::<#field_types>(),
								),
							)*],
						),
					));
					builder.nodes[reserved_schema_key] = new_node;
				}

				type TypeLookup = #type_lookup;
			}

			#type_lookup_decl
		};
	})
}

struct IsRelevantGeneric<'a> {
	generics: &'a syn::Generics,
	result: bool,
}
impl Visit<'_> for IsRelevantGeneric<'_> {
	fn visit_type(&mut self, v: &syn::Type) {
		match v {
			syn::Type::Path(v) => {
				if let Some(v) = v.path.get_ident() {
					if self.generics.params.iter().any(|p| match p {
						syn::GenericParam::Type(t) => t.ident == *v,
						_ => false,
					}) {
						self.result = true;
					}
				}
			}
			_ => {}
		}
		visit::visit_type(self, v);
	}
	fn visit_lifetime(&mut self, v: &syn::Lifetime) {
		if self.generics.params.iter().any(|p| match p {
			syn::GenericParam::Lifetime(l) => l.lifetime == *v,
			_ => false,
		}) {
			self.result = true;
		}
		visit::visit_lifetime(self, v)
	}
	fn visit_const_param(&mut self, v: &syn::ConstParam) {
		if self.generics.params.iter().any(|p| match p {
			syn::GenericParam::Const(c) => c == v,
			_ => false,
		}) {
			self.result = true;
		}
		visit::visit_const_param(self, v)
	}
}
