use super::*;

/// Returned boolean is `has_non_lifetime_generics`
pub(super) fn build_type_lookup(
	type_ident: &syn::Ident,
	generics: &syn::Generics,
	field_idents: Option<&[&syn::Ident]>,
	field_types: &[Cow<'_, syn::Type>],
) -> (syn::Type, Option<syn::ItemStruct>, bool) {
	let has_non_lifetime_generics = generics
		.params
		.iter()
		.any(|gp| !matches!(gp, syn::GenericParam::Lifetime(_)));
	let (type_lookup, type_lookup_decl): (syn::Type, _) = match has_non_lifetime_generics {
		false => {
			let type_lookup = if generics.params.is_empty() {
				parse_quote!(Self)
			} else {
				let mut generics_static = generics.clone();
				TurnLifetimesToStatic.visit_generics_mut(&mut generics_static);
				let (_, ty_generics, _) = generics_static.split_for_impl();
				parse_quote!(#type_ident #ty_generics)
			};
			(type_lookup, None)
		}
		true => {
			// The struct we are deriving on is generic, but we need the TypeLookup to be
			// 'static otherwise it won't implement `Any`, so we need to generate a
			// dedicated struct for it.

			// E.g., for a struct
			// struct Foo<Bar> {
			//     f1: Bar,
			//     f2: Baz;
			// }
			// We'll generate
			// struct FooTypeLookup<T0, T1> {
			//     f1: T0,
			//     f2: T1,
			// }
			// and then use type TypeLookup =
			//   TypeLookup<
			//       <Bar as BuildSchema>::TypeLookup,
			//       <Baz as BuildSchema>::TypeLookup,
			//   >;
			let type_lookup_ident = format_ident!("{type_ident}TypeLookup");
			let type_params: Vec<syn::Ident> = (0..field_types.len())
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
				fields: match field_idents {
					Some(field_idents) => syn::Fields::Named(syn::FieldsNamed {
						named: field_idents
							.iter()
							.zip(&type_params)
							.map(|(field_ident, type_ident)| syn::Field {
								attrs: Default::default(),
								vis: syn::Visibility::Inherited,
								ident: Some((**field_ident).clone()),
								colon_token: Some(Default::default()),
								ty: { parse_quote!(#type_ident) },
								mutability: syn::FieldMutability::None,
							})
							.collect(),
						brace_token: Default::default(),
					}),
					None => syn::Fields::Unnamed(syn::FieldsUnnamed {
						unnamed: type_params
							.iter()
							.map(|type_ident| syn::Field {
								attrs: Default::default(),
								vis: syn::Visibility::Inherited,
								ident: None,
								colon_token: Some(Default::default()),
								ty: { parse_quote!(#type_ident) },
								mutability: syn::FieldMutability::None,
							})
							.collect(),
						paren_token: Default::default(),
					}),
				},
				semi_token: field_idents.is_none().then(Default::default),
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
	(type_lookup, type_lookup_decl, has_non_lifetime_generics)
}

struct TurnLifetimesToStatic;
impl VisitMut for TurnLifetimesToStatic {
	fn visit_lifetime_mut(&mut self, i: &mut syn::Lifetime) {
		i.ident = format_ident!("static");
		visit_mut::visit_lifetime_mut(self, i)
	}
}
