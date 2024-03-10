use {
	proc_macro2::{Span, TokenStream},
	quote::{format_ident, quote},
	syn::{
		parse_quote,
		visit::{self, Visit},
		visit_mut::{self, VisitMut},
		Error,
	},
};

#[derive(darling::FromDeriveInput)]
#[darling(attributes(avro_schema), supports(struct_named))]
pub(crate) struct SchemaDeriveInput {
	ident: proc_macro2::Ident,
	data: darling::ast::Data<(), SchemaDeriveField>,
	generics: syn::Generics,
}

#[derive(darling::FromField)]
#[darling(attributes(avro_schema))]
pub(crate) struct SchemaDeriveField {
	ident: Option<proc_macro2::Ident>,
	ty: syn::Type,

	skip: darling::util::Flag,
	logical_type: Option<syn::Ident>,
	scale: Option<WithMetaPath<syn::LitInt>>,
	precision: Option<WithMetaPath<syn::LitInt>>,
}

pub(crate) fn schema_impl(input: SchemaDeriveInput) -> Result<TokenStream, Error> {
	let mut errors = TokenStream::default();

	let mut fields = input
		.data
		.take_struct()
		.expect("Supports directive should prevent enums");
	fields.fields.retain(|f| !f.skip.is_present());

	let struct_ident = &input.ident;
	let struct_name = struct_ident.to_string();
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

	let field_instantiations = fields.iter().zip(&field_types).map(|(field, ty)| {
		let mut logical_type_ident = field.logical_type.as_ref();
		if logical_type_ident.is_none() {
			if let syn::Type::Path(path) = &field.ty {
				if let Some(last_type_ident) = path.path.segments.last().map(|s| &s.ident) {
					let last_type_str = last_type_ident.to_string();
					match last_type_str.as_str() {
						"Uuid" => logical_type_ident = Some(last_type_ident),
						_ => {}
					}
				}
			}
		}
		match logical_type_ident {
			None => quote! { builder.find_or_build::<#ty>() },
			Some(logical_type_ident) => {
				let logical_type_str = logical_type_ident.to_string();
				let mut logical_type = if [
					"Decimal",
					"Uuid",
					"Date",
					"TimeMillis",
					"TimeMicros",
					"TimestampMillis",
					"TimestampMicros",
					"Duration",
				]
				.contains(&logical_type_str.as_str())
				{
					// This is a known logical type
					quote! { schema::LogicalType::#logical_type_ident }
				} else {
					quote! { schema::LogicalType::Unknown(
						#logical_type_str.to_owned()
					) }
				};
				if logical_type_str == "Decimal" {
					let zero = parse_quote!(0);
					let mut error = |missing_field: &str| {
						errors.extend(
							Error::new_spanned(
								logical_type_ident,
								format_args!(
									"`Decimal` logical type requires \
										`{missing_field}` attribute to be set"
								),
							)
							.to_compile_error(),
						);
						&zero
					};
					let scale = field
						.scale
						.as_ref()
						.map_or_else(|| error("scale"), |w| &w.value);
					let precision = field
						.precision
						.as_ref()
						.map_or_else(|| error("precision"), |w| &w.value);
					logical_type.extend(quote! {
						(schema::Decimal::new(#scale, #precision))
					});
				} else {
					let mut error = |field_that_should_not_be_here: &WithMetaPath<syn::LitInt>| {
						errors.extend(
							Error::new_spanned(
								&field_that_should_not_be_here.path,
								format_args!(
									"`{}` attribute is not relevant for `{}` logical type",
									darling::util::path_to_string(
										&field_that_should_not_be_here.path
									),
									logical_type_str
								),
							)
							.to_compile_error(),
						);
					};
					if let Some(f) = &field.scale {
						error(&f);
					}
					if let Some(f) = &field.precision {
						error(&f);
					}
				}
				quote! { builder.build_logical_type::<#ty>(#logical_type) }
			}
		}
	});

	let field_names = fields
		.iter()
		.map(|f| f.ident.as_ref().map(|i| i.to_string()))
		.collect::<Option<Vec<_>>>()
		.ok_or_else(|| Error::new(Span::call_site(), "Unnamed fields are not supported"))?;

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
				parse_quote!(#struct_ident #ty_generics)
			};
			(type_lookup, None)
		}
		true => {
			// The struct we are deriving on is generic, but we need the TypeLookup to be
			// 'static otherwise it won't implement `Any`, so we need to generate a
			// dedicated struct for it.

			// E.g., for a struct
			// struct Foo<Bar> {
			// 	f1: Bar,
			// 	f2: Baz;
			// }
			// We'll generate
			// struct FooTypeLookup<T0, T1> {
			// 	f1: T0,
			// 	f1: T1,
			// }
			// and then use type TypeLookup =
			//   TypeLookup<
			//       <Bar as BuildSchema>::TypeLookup,
			//       <Baz as BuildSchema>::TypeLookup,
			//   >;
			let type_lookup_ident = format_ident!("{struct_ident}TypeLookup");
			let type_params: Vec<syn::Ident> =
				(0..fields.len()).map(|i| format_ident!("T{}", i)).collect();
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

	let add_type_id_to_fqn = if has_non_lifetime_generics {
		quote! {
			serde_avro_derive::hash_type_id(
				&mut struct_name,
				std::any::TypeId::of::<<Self as serde_avro_derive::BuildSchema>::TypeLookup>(),
			);
		}
	} else {
		quote! {}
	};

	let (impl_generics, ty_generics, where_clause) = generics.split_for_impl();

	Ok(quote! {
		const _: () = {
			use serde_avro_derive::serde_avro_fast::schema;

			impl #impl_generics serde_avro_derive::BuildSchema for #struct_ident #ty_generics #where_clause {
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
									#field_instantiations,
								),
							)*],
						),
					));
					builder.nodes[reserved_schema_key] = new_node;
				}

				type TypeLookup = #type_lookup;
			}

			#type_lookup_decl

			#errors
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

struct TurnLifetimesToStatic;
impl VisitMut for TurnLifetimesToStatic {
	fn visit_lifetime_mut(&mut self, i: &mut syn::Lifetime) {
		i.ident = format_ident!("static");
		visit_mut::visit_lifetime_mut(self, i)
	}
}

struct WithMetaPath<T> {
	path: syn::Path,
	value: T,
}
impl<T: darling::FromMeta> darling::FromMeta for WithMetaPath<T> {
	fn from_meta(meta: &syn::Meta) -> Result<Self, darling::Error> {
		Ok(Self {
			value: <T as darling::FromMeta>::from_meta(meta)?,
			path: meta.path().clone(),
		})
	}
}
