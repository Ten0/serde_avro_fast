//! Navigate, modify and initialize the [`Schema`]

pub mod safe;
mod self_referential;

pub use {
	safe::{BuildSchemaFromApacheSchemaError, ParseSchemaError},
	self_referential::*,
};

impl std::str::FromStr for Schema {
	type Err = ParseSchemaError;
	fn from_str(s: &str) -> Result<Self, Self::Err> {
		let safe_schema: safe::Schema = s.parse()?;
		Ok(safe_schema.into())
	}
}

impl Schema {
	/// Attempt to convert a [`Schema`](safe::apache::Schema) from the
	/// `apache-avro` crate into a [`Schema`]
	pub fn from_apache_schema(
		apache_schema: &apache_avro::Schema,
	) -> Result<Self, BuildSchemaFromApacheSchemaError> {
		let safe_schema = safe::Schema::from_apache_schema(apache_schema)?;
		Ok(safe_schema.into())
	}
}

/// Component of a [`SchemaNode`]
#[derive(Clone, Debug)]
pub struct Enum {
	pub symbols: Vec<String>,
	pub name: Name,
}

/// Component of a [`SchemaNode`]
#[derive(Clone, Debug)]
pub struct Fixed {
	pub size: usize,
	pub name: Name,
}

/// Schema component for named variants of a [`SchemaNode`]
#[derive(Debug, Clone)]
pub struct Name {
	fully_qualified_name: String,
	namespace_delimiter_idx: Option<usize>,
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
