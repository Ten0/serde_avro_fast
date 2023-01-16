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
	/// Attempt to convert a [`Schema`](safe::apache::Schema) from the `apache-avro` crate into a [`Schema`]
	pub fn from_apache_schema(apache_schema: &safe::apache::Schema) -> Result<Self, BuildSchemaFromApacheSchemaError> {
		let safe_schema = safe::Schema::from_apache_schema(apache_schema)?;
		Ok(safe_schema.into())
	}
}
