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
	pub name: String,
	pub namespace: Option<String>,
}
