use super::*;

pub(super) struct FieldTypeAndInstantiationsBuilder<'t, 'm> {
	pub(super) generics: &'m mut syn::Generics,
	pub(super) added_where_clause_predicate_for_types:
		std::collections::HashSet<Cow<'t, syn::Type>>,
	pub(super) errors: &'m mut TokenStream,
	pub(super) namespace: &'m Option<String>,
	pub(super) expand_namespace_var: bool,
	/// Whether at least one of the fields id directly looked up in the schema,
	/// without necessarily inserting a new node. This is useful for newtype
	/// struct serialization without logical type, whose type (and lookup)
	/// should be directly the inner one
	pub(super) has_direct_lookup: bool,
}

pub(super) enum OverrideFixedName<'a> {
	NewtypeStruct {
		struct_name: &'a syn::Ident,
	},
	NewtypeVariant {
		enum_name: &'a syn::Ident,
		variant_name: &'a syn::Ident,
	},
}

impl<'t> FieldTypeAndInstantiationsBuilder<'t, '_> {
	pub(super) fn field_type_and_instantiation(
		&mut self,
		field: &'t SchemaDeriveField,
		override_fixed_name: Option<OverrideFixedName<'_>>,
	) -> (Cow<'t, syn::Type>, TokenStream) {
		// Choose type
		let mut ty = field.has_same_type_as.as_ref().unwrap_or(&field.ty);
		loop {
			match ty {
				syn::Type::Reference(reference) => {
					// This allows not requiring the user to specify that T: 'a
					// as an explicit where predicate, and simplifies the calls
					ty = &reference.elem;
					continue;
				}
				syn::Type::Path(p) => {
					if let Some(last) = p.path.segments.last() {
						let last_str = last.ident.to_string();
						if ["Box", "Arc", "Rc", "RefCell", "Cell"].contains(&last_str.as_str()) {
							if let syn::PathArguments::AngleBracketed(arguments) = &last.arguments {
								if arguments.args.len() == 1 {
									if let syn::GenericArgument::Type(inner_type) =
										arguments.args.first().unwrap()
									{
										ty = inner_type;
										continue;
									}
								}
							}
						}
					}
				}
				_ => {}
			}
			break;
		}
		while let syn::Type::Reference(reference) = ty {
			ty = &reference.elem;
		}
		let mut ty = Cow::Borrowed(ty);

		fn regular_field_instantiation(ty: &syn::Type) -> TokenStream {
			quote! { builder.find_or_build::<#ty>() }
		}

		let mut override_regular_field_instantiation = None;
		if let Some(override_fixed_name) = override_fixed_name {
			// If the type is a [u8; N] that should map to a Fixed, we should not just send
			// towards the implementation for [u8; N], but instead create a new node for it,
			// overriding the name
			// This is done with:
			// `struct NewType([u8; 4]);`
			// `enum Ip { V4([u8; 4]), V6([u8; 16]) }`
			if let syn::Type::Array(array) = &*ty {
				if let syn::Type::Path(path) = &*array.elem {
					if path.path.is_ident("u8") {
						let len = match &array.len {
							syn::Expr::Lit(syn::ExprLit {
								lit: syn::Lit::Int(i),
								..
							}) => match i.base10_parse::<usize>() {
								Ok(len) => len,
								Err(e) => {
									self.errors.extend(e.to_compile_error());
									0
								}
							},
							_ => {
								self.errors.extend(
									Error::new_spanned(
										&array.len,
										"Fixed array length must be a constant integer",
									)
									.to_compile_error(),
								);
								0
							}
						};
						let fixed_name = match self.namespace {
							None => {
								self.expand_namespace_var = true;
								let pattern = match override_fixed_name {
									OverrideFixedName::NewtypeStruct { struct_name } => {
										format!(r#"{{}}.{struct_name}"#)
									}
									OverrideFixedName::NewtypeVariant {
										enum_name,
										variant_name,
									} => format!(r#"{{}}.{enum_name}.{variant_name}"#),
								};
								quote! {
									format!(#pattern, namespace)
								}
							}
							Some(namespace) => {
								let namespace_prefix = if namespace.is_empty() {
									"".to_owned()
								} else {
									format!("{}.", namespace)
								};
								let type_name = match override_fixed_name {
									OverrideFixedName::NewtypeStruct { struct_name } => {
										format!("{}{}", namespace_prefix, struct_name)
									}
									OverrideFixedName::NewtypeVariant {
										enum_name,
										variant_name,
									} => format!(
										"{}{}.{}",
										namespace_prefix, enum_name, variant_name
									),
								};
								quote! { #type_name.to_owned() }
							}
						};
						override_regular_field_instantiation = Some(quote! {
							{
								let schema_key = schema::SchemaKey::from_idx(builder.nodes.len());
								builder.nodes.push(schema::SchemaNode::RegularType(
									schema::RegularType::Fixed(
										schema::Fixed::new(
											schema::Name::from_fully_qualified_name(#fixed_name),
											#len
										)
									)
								));
								schema_key
							}
						});
					}
				}
			}
		}

		// Identify logical types and prepare field instantiation
		let mut logical_type_litstr = field.logical_type.as_ref().map(Cow::Borrowed);
		let mut inferred_decimal_logical_type = false;
		if logical_type_litstr.is_none() {
			if let syn::Type::Path(path) = &field.ty {
				if let Some(last_type_ident) = path.path.segments.last().map(|s| &s.ident) {
					let last_type_str = last_type_ident.to_string();
					let from_last_type = || {
						Some(Cow::Owned(syn::LitStr::new(
							&last_type_str,
							last_type_ident.span(),
						)))
					};
					match last_type_str.as_str() {
						"Uuid" => {
							logical_type_litstr = from_last_type();
						}
						"Decimal" => {
							inferred_decimal_logical_type = true;
							logical_type_litstr = from_last_type();
						}
						_ => {}
					}
				}
			}
		}
		// If it's a logical type, wrap it
		let field_instantiation = match logical_type_litstr.as_deref() {
			None => override_regular_field_instantiation.unwrap_or_else(|| {
				self.has_direct_lookup = true;
				regular_field_instantiation(&ty)
			}),
			Some(logical_type_litstr) => {
				let logical_type_str_raw = logical_type_litstr.value();
				let logical_type_str_pascal = logical_type_str_raw.to_pascal_case();
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
				.contains(&logical_type_str_pascal.as_str())
				{
					// This is a known logical type
					let logical_type_ident_pascal =
						syn::Ident::new(&logical_type_str_pascal, logical_type_litstr.span());
					quote! { schema::LogicalType::#logical_type_ident_pascal }
				} else {
					quote! { schema::LogicalType::Unknown(
						schema::UnknownLogicalType::new(#logical_type_litstr)
					) }
				};
				if logical_type_str_pascal == "Decimal" {
					if inferred_decimal_logical_type {
						ty = Cow::Owned(
							// "A `decimal` logical type annotates Avro `bytes` or `fixed`
							// types". Because we need to choose an arbitrary one as we
							// picked `decimal` because the type was named `Decimal`, we'll
							// choose Bytes as we have no information to accurately decide
							// the attributes we would give to a `Fixed`.
							parse_quote_spanned!(logical_type_litstr.span() => Vec<u8>),
						);
					}
					let zero = parse_quote!(0);
					let mut error = |missing_field: &str| {
						self.errors.extend(
							Error::new_spanned(
								logical_type_litstr,
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
					match logical_type_str_pascal.as_str() {
						"TimestampMillis" | "TimestampMicros" | "TimeMicros" => {
							if !matches!(&*ty, syn::Type::Path(p) if p.path.is_ident("i64")) {
								ty = Cow::Owned(
									parse_quote_spanned!(logical_type_litstr.span() => i64),
								);
							}
						}
						"TimeMillis" => {
							if !matches!(&*ty, syn::Type::Path(p) if p.path.is_ident("i32")) {
								ty = Cow::Owned(
									parse_quote_spanned!(logical_type_litstr.span() => i32),
								);
							}
						}
						"Uuid" => {
							if !matches!(&*ty, syn::Type::Path(p) if p.path.is_ident("String")) {
								// It is specified that
								// "A uuid logical type annotates an Avro string"
								ty = Cow::Owned(
									parse_quote_spanned!(logical_type_litstr.span() => String),
								);
							}
						}
						"Date" => {
							if !matches!(&*ty, syn::Type::Path(p) if p.path.is_ident("i32")) {
								ty = Cow::Owned(
									parse_quote_spanned!(logical_type_litstr.span() => i32),
								);
							}
						}
						_ => {}
					}
					let mut error = |field_that_should_not_be_here: &WithMetaPath<syn::LitInt>| {
						self.errors.extend(
							Error::new_spanned(
								&field_that_should_not_be_here.path,
								format_args!(
									"`{}` attribute is not relevant for `{}` logical type",
									darling::util::path_to_string(
										&field_that_should_not_be_here.path
									),
									logical_type_str_raw
								),
							)
							.to_compile_error(),
						);
					};
					if let Some(f) = &field.scale {
						error(f);
					}
					if let Some(f) = &field.precision {
						error(f);
					}
				}
				let instantiation = override_regular_field_instantiation
					.unwrap_or_else(|| regular_field_instantiation(&ty));
				quote! { builder.build_logical_type(#logical_type, |builder| #instantiation) }
			}
		};

		// Add relevant where clause if not already present
		if !self.generics.params.is_empty() {
			let mut is_relevant_generic = IsRelevantGeneric {
				generics: &*self.generics,
				result: false,
			};
			is_relevant_generic.visit_type(&*ty);
			if is_relevant_generic.result {
				if self
					.added_where_clause_predicate_for_types
					.insert(ty.clone())
				{
					self.generics
						.make_where_clause()
						.predicates
						.push(parse_quote!(#ty: serde_avro_derive::BuildSchema));
				}
			}
		}

		(ty, field_instantiation)
	}
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
