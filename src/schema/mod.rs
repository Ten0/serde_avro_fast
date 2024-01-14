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
		let safe_schema: safe::EditableSchema = s.parse()?;
		safe_schema.try_into()
	}
}

/// Component of a [`SchemaNode`]
#[derive(Clone, Debug)]
pub struct Fixed {
	pub size: usize,
	pub name: Name,
}

/// Schema component for named variants of a [`SchemaNode`]
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
	pub fn name(&self) -> &str {
		match self.namespace_delimiter_idx {
			None => &self.fully_qualified_name,
			Some(delimiter_idx) => &self.fully_qualified_name[delimiter_idx + 1..],
		}
	}

	pub fn namespace(&self) -> Option<&str> {
		self.namespace_delimiter_idx
			.map(|idx| &self.fully_qualified_name[..idx])
	}

	pub fn fully_qualified_name(&self) -> &str {
		&self.fully_qualified_name
	}
}
