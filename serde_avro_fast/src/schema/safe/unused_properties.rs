pub use serde_json::Value as JsonValue;

#[derive(Default, Clone, Debug)]
pub(crate) struct UnusedProperties {
	pub(crate) unused_properties: Vec<(String, JsonValue)>,
}

impl UnusedProperties {
	/// List of the properties were/should be present in the schema JSON but are
	/// not used by this library.
	pub fn iter(&self) -> impl Iterator<Item = (&str, &JsonValue)> {
		self.unused_properties.iter().map(|(k, v)| (k.as_str(), v))
	}

	/// Add a new unused property to be serialized when the schema is serialized
	/// as JSON
	///
	/// This can be used to add properties such as `default`, `doc`,
	/// `aliases`... that are unused by this library, but that other systems
	/// using this schema might need.
	pub fn add(&mut self, key: impl Into<String>, value: impl Into<JsonValue>) {
		self.unused_properties.push((key.into(), value.into()));
	}
}

/// We don't make the `UnusedProperties` type public in case we need to add
/// structured properties but want to keep them available through the
/// unused_properties iterator as well.
macro_rules! unused_properties {
	($($t: ty)+) => {
		$(
			impl $t {
				/// List of the properties were/should be present in the schema JSON but are
				/// not used by this library.
				pub fn unused_properties(&self) -> impl Iterator<Item = (&str, &JsonValue)> {
					TODO the above is not evolution-safe due to &JsonValue
					self.unused_properties.iter()
				}

				/// Add a new unused property to be serialized when the schema is serialized
				/// as JSON
				///
				/// This can be used to add properties such as `default`, `doc`,
				/// `aliases`... that are unused by this library, but that other systems
				/// using this schema might need.
				pub fn add_unused_property(
					&mut self,
					key: impl Into<String>,
					value: impl Into<JsonValue>,
				) {
					self.unused_properties.add(key, value);
				}
			}
		)*
	};
}
pub(crate) use unused_properties;
