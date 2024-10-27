mod field_types_and_instantiations;
mod type_lookup;

use field_types_and_instantiations::*;

use {
	darling::ast::Style,
	heck::ToPascalCase as _,
	proc_macro2::{Span, TokenStream},
	quote::{format_ident, quote},
	std::borrow::Cow,
	syn::{
		ext::IdentExt as _,
		parse_quote, parse_quote_spanned,
		visit::{self, Visit},
		visit_mut::{self, VisitMut},
		Error,
	},
};

#[derive(darling::FromDeriveInput)]
#[darling(
	attributes(avro_schema),
	supports(struct_named, struct_newtype, enum_unit, enum_newtype)
)]
pub(crate) struct SchemaDeriveInput {
	ident: proc_macro2::Ident,
	data: darling::ast::Data<SchemaDeriveVariant, SchemaDeriveField>,
	generics: syn::Generics,

	namespace: Option<String>,
	name: Option<proc_macro2::Ident>,
}

#[derive(darling::FromField, Debug)]
#[darling(attributes(avro_schema))]
pub(crate) struct SchemaDeriveField {
	ident: Option<proc_macro2::Ident>,
	ty: syn::Type,

	skip: darling::util::Flag,

	logical_type: Option<syn::LitStr>,
	scale: Option<WithMetaPath<syn::LitInt>>,
	precision: Option<WithMetaPath<syn::LitInt>>,

	has_same_type_as: Option<syn::Type>,
}

#[derive(darling::FromVariant, Debug)]
#[darling(attributes(avro_schema))]
pub(crate) struct SchemaDeriveVariant {
	ident: proc_macro2::Ident,
	fields: darling::ast::Fields<SchemaDeriveField>,

	skip: darling::util::Flag,
}

pub(crate) fn schema_impl(input: SchemaDeriveInput) -> Result<TokenStream, Error> {
	let struct_or_enum = {
		let mut d = input.data;
		match &mut d {
			darling::ast::Data::Struct(fields) => {
				fields.fields.retain(|f| !f.skip.is_present());
			}
			darling::ast::Data::Enum(enum_variants) => {
				enum_variants.retain(|f| !f.skip.is_present());
			}
		}
		d
	};

	let type_ident = &input.ident;
	let name_ident = input.name.as_ref().unwrap_or(type_ident);
	let compute_namespace_expr = quote! { module_path!().replace("::", ".") };
	let type_name_var = match &input.namespace {
		None => {
			let type_name_str = format!(".{}", name_ident);
			quote! {
				let mut type_name = #compute_namespace_expr;
				type_name.push_str(#type_name_str);
			}
		}
		Some(namespace) => {
			let type_name = if namespace.is_empty() {
				name_ident.to_string()
			} else {
				format!("{}.{}", namespace, name_ident)
			};
			quote! {
				let mut type_name = #type_name.to_owned();
			}
		}
	};
	let lazy_compute_namespace = || {
		quote! {
			let mut namespace = serde_avro_derive::LazyNamespace::new(|| #compute_namespace_expr);
		}
	};

	let mut errors = TokenStream::default();
	let mut generics = input.generics;

	let mut field_types_and_instantiations = FieldTypeAndInstantiationsBuilder {
		generics: &mut generics,
		added_where_clause_predicate_for_types: Default::default(),
		errors: &mut errors,
		namespace: &input.namespace,
		expand_namespace_var: false,
		has_direct_lookup: false,
	};

	let type_lookup: syn::Type;
	let type_lookup_decl: Option<syn::ItemStruct>;
	let append_schema_body: TokenStream = match struct_or_enum {
		darling::ast::Data::Struct(fields) => {
			// We support two scenarios: newtype structs (forward to inner type) and named
			// structs (avro record)
			if fields.len() == 1 && fields.fields[0].ident.is_none() {
				// newtype struct
				let field = &fields.fields[0];
				let (field_type, field_instantiation) = field_types_and_instantiations
					.field_type_and_instantiation(
						field,
						FieldKind::NewtypeStruct {
							struct_name: name_ident,
						},
					);
				if field_types_and_instantiations.has_direct_lookup {
					// This single field is a direct lookup (no logical type,
					// etc), so this is the special case we need to just
					// expand into forwarding.
					type_lookup = parse_quote! { <#field_type as serde_avro_derive::BuildSchema>::TypeLookup };
					type_lookup_decl = None;
					quote! {
						<#field_type as serde_avro_derive::BuildSchema>::append_schema(builder);
					}
				} else {
					let namespace_var = field_types_and_instantiations
						.expand_namespace_var
						.then(lazy_compute_namespace);
					(type_lookup, type_lookup_decl, _) = type_lookup::build_type_lookup(
						type_ident,
						&generics,
						None,
						std::slice::from_ref(&field_type),
					);
					quote! {
						let n_nodes = builder.nodes.len();
						#namespace_var
						let new_node_key = #field_instantiation;
						assert_eq!(n_nodes, new_node_key.idx());
					}
				}
			} else {
				// named struct
				let field_idents = fields
					.iter()
					.map(|f| f.ident.as_ref())
					.collect::<Option<Vec<_>>>()
					.ok_or_else(|| {
						Error::new(Span::call_site(), "Unnamed fields are not supported")
					})?;

				let (field_types, field_instantiations): (Vec<Cow<syn::Type>>, Vec<TokenStream>) =
					fields
						.iter()
						.zip(&field_idents)
						.map(|(field, field_name)| {
							field_types_and_instantiations.field_type_and_instantiation(
								field,
								FieldKind::StructField {
									struct_name: name_ident,
									field_name,
								},
							)
						})
						.unzip();

				let field_names = field_idents.iter().map(|ident| ident.unraw().to_string());

				let has_non_lifetime_generics;
				(type_lookup, type_lookup_decl, has_non_lifetime_generics) =
					type_lookup::build_type_lookup(
						type_ident,
						&generics,
						Some(field_idents.as_slice()),
						&field_types,
					);

				let add_type_id_to_fqn = has_non_lifetime_generics.then(|| {
					quote! {
						serde_avro_derive::hash_type_id(
							&mut type_name,
							std::any::TypeId::of::<<Self as serde_avro_derive::BuildSchema>::TypeLookup>(),
						);
					}
				});

				quote! {
					let reserved_schema_key = builder.reserve();
					#type_name_var
					#add_type_id_to_fqn
					let fields = vec![#(
						schema::RecordField::new(
							#field_names,
							#field_instantiations,
						),
					)*];
					let new_node = schema::Record::new(
						schema::Name::from_fully_qualified_name(type_name),
						fields,
					);
					builder.nodes[reserved_schema_key] = new_node.into();
				}
			}
		}
		darling::ast::Data::Enum(variants) => {
			if variants.iter().all(|v| v.fields.is_empty()) {
				// Only unit variants
				type_lookup = parse_quote!(Self);
				type_lookup_decl = None;
				let variants = variants.iter().map(|v| v.ident.to_string());
				quote! {
					#type_name_var
					builder.nodes.push(
						schema::Enum::new(
							schema::Name::from_fully_qualified_name(type_name),
							vec![#(#variants.to_owned(),)*],
						)
						.into()
					);
				}
			} else {
				let mut field_types = Vec::new();
				let mut type_lookup_field_names = Vec::new();

				let mut has_unit = 0;

				let variant_instantiations: Vec<TokenStream> = variants
					.iter()
					.map(|v| {
						let mut style = v.fields.style;
						if v.fields.is_empty() {
							style = Style::Unit;
						}
						match style {
							Style::Unit => {
								match has_unit {
									0 => {
										has_unit = 1;
									}
									1 => {
										field_types_and_instantiations.errors.extend(
											Error::new_spanned(
												&v.ident,
												"If not all variants are unit variants \
												(which would be an Avro enum), only one unit variant \
												is allowed in the Avro union (that will map to Null \
												in the Union)",
											)
											.to_compile_error(),
										);
										has_unit = 2;
									}
									_ => {}
								}
								quote! { builder.find_or_build::<()>() }
							}
							Style::Tuple => {
								let mut fields = v.fields.iter();
								let field = fields
									.next()
									.expect("Style should be Unit if there are no fields");
								if fields.next().is_some() {
									field_types_and_instantiations.errors.extend(
										Error::new_spanned(
											&v.ident,
											"Tuple variants may only contain one \
												field for Avro Union schema generation",
										)
										.to_compile_error(),
									);
								}
								if field.skip.is_present() {
									field_types_and_instantiations.errors.extend(
										Error::new(
											field.skip.span(),
											"`skip` attribute should be set on the enum \
												variant, not on the inner field",
										)
										.to_compile_error(),
									);
								}
								let (field_type, field_instantiation) =
									field_types_and_instantiations.field_type_and_instantiation(
										field,
										FieldKind::NewtypeVariant {
											enum_name: type_ident,
											variant_name: &v.ident,
										},
									);
								field_types.push(field_type);
								type_lookup_field_names.push(&v.ident);
								field_instantiation
							}
							Style::Struct => {
								panic!(
									"Struct variant style at this point should have been prevented \
										by the lack of 'enum_struct' in 'supports'"
								)
							}
						}
					})
					.collect();

				let namespace_var = field_types_and_instantiations
					.expand_namespace_var
					.then(lazy_compute_namespace);

				(type_lookup, type_lookup_decl, _) = type_lookup::build_type_lookup(
					type_ident,
					&generics,
					Some(&type_lookup_field_names),
					&field_types,
				);

				quote! {
					let reserved_schema_key = builder.reserve();
					#namespace_var
					let new_node = schema::Union::new(vec![
						#(#variant_instantiations,)*
					]);
					builder.nodes[reserved_schema_key] = new_node.into();
				}
			}
		}
	};

	let (impl_generics, ty_generics, where_clause) = generics.split_for_impl();

	Ok(quote! {
		const _: () = {
			use serde_avro_derive::serde_avro_fast::schema;

			impl #impl_generics serde_avro_derive::BuildSchema for #type_ident #ty_generics #where_clause {
				fn append_schema(builder: &mut serde_avro_derive::SchemaBuilder) {
					#append_schema_body
				}

				type TypeLookup = #type_lookup;
			}

			#type_lookup_decl

			#errors
		};
	})
}

#[derive(Debug)]
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
