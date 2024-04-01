//! Navigate, modify and initialize the [`Schema`]

mod error;
mod safe;
pub(crate) mod self_referential;
mod union_variants_per_type_lookup;

pub use {error::SchemaError, safe::*, self_referential::Schema};

pub(crate) use union_variants_per_type_lookup::UnionVariantLookupKey;

impl std::str::FromStr for Schema {
	type Err = SchemaError;
	fn from_str(s: &str) -> Result<Self, Self::Err> {
		let safe_schema: safe::SchemaMut = s.parse()?;
		safe_schema.try_into()
	}
}

/// Component of a [`SchemaMut`]
#[derive(Clone, Debug)]
pub struct Fixed {
	/// The size in bytes of the *fixed* type
	pub size: usize,
	/// The name of the *fixed* type, including the namespace
	pub name: Name,
	pub(crate) _private: (),
}
impl Fixed {
	/// `name` is name of the *fixed* type, including the namespace, `size` is
	/// the size in bytes of the fixed type
	pub fn new(name: Name, size: usize) -> Self {
		Self {
			size,
			name,
			_private: (),
		}
	}
}

/// Schema component for named nodes of a [`SchemaMut`]
///
/// This holds both the "name" and the "namespace".
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct Name {
	fully_qualified_name: String,
	namespace_delimiter_idx: Option<usize>,
}

impl std::fmt::Debug for Name {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		std::fmt::Debug::fmt(&self.fully_qualified_name, f)
	}
}

impl Name {
	/// The rightmost component of the fully qualified name
	///
	/// e.g. in `a.b.c` it's `c`
	pub fn name(&self) -> &str {
		match self.namespace_delimiter_idx {
			None => &self.fully_qualified_name,
			Some(delimiter_idx) => &self.fully_qualified_name[delimiter_idx + 1..],
		}
	}

	/// The namespace component of the fully qualified name
	///
	/// e.g. in `a.b.c` it's `a.b`
	pub fn namespace(&self) -> Option<&str> {
		self.namespace_delimiter_idx
			.map(|idx| &self.fully_qualified_name[..idx])
	}

	/// The fully qualified name
	///
	/// e.g. in `a.b.c` it's `a.b.c`
	pub fn fully_qualified_name(&self) -> &str {
		&self.fully_qualified_name
	}

	/// Build a [`Name`] from a fully qualified name
	///
	/// Side note if doing weird stuff: If the only `.` in the fully qualified
	/// name is at the beginning of the string, it will be stripped, that is, we
	/// will parse `namespace: None, name: "anything_behind_the_dot"`. This is
	/// for consistency with the parsing logic, but that would imply that what
	/// would be returned by
	/// [`fully_qualified_name`](Name::fully_qualified_name) is not equal to
	/// what was provided here, because it would not contain the dot.
	pub fn from_fully_qualified_name(fully_qualified_name: impl Into<String>) -> Self {
		fn non_generic_inner(mut fully_qualified_name: String) -> Name {
			Name {
				namespace_delimiter_idx: match fully_qualified_name.rfind('.') {
					Some(0) => {
						// Let's parse ".x" as {namespace: None, name: "x"}
						fully_qualified_name.remove(0);
						None
					}
					other => other,
				},
				fully_qualified_name,
			}
		}
		non_generic_inner(fully_qualified_name.into())
	}
}
