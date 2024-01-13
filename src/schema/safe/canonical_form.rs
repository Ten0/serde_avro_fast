use crate::schema::safe::{Schema, SchemaKey, SchemaNode};

impl Schema {
	/// This function is not public because you shouldn't use that schema
	/// when transmitting the schema to other people, notably because it loses
	/// the logical types information, and additionally because it may not be
	/// valid JSON (there's no escaping in the json generation...)
	/// See https://issues.apache.org/jira/browse/AVRO-1721
	pub(crate) fn parsing_canonical_form(&self) -> Result<String, FingerprintingError> {
		let mut buf = String::new();
		write_canonical_form(self, SchemaKey::from_idx(0), &mut buf)?;
		Ok(buf)
	}

	/// Obtain the Rabin fingerprint of the schema
	pub fn canonical_form_rabin_fingerprint(&self) -> Result<[u8; 8], FingerprintingError> {
		// TODO replace with a local implementation
		Ok(<apache_avro::rabin::Rabin as digest::Digest>::digest(
			self.parsing_canonical_form()?.as_bytes(),
		)
		.into())
	}
}

/// Manual implementation that strictly copies that of the reference
/// implementation in Java. According to the java code, this is not guaranteed
/// to actually be valid JSON (no escaping...)
fn write_canonical_form(
	schema: &Schema,
	key: SchemaKey,
	buf: &mut String,
) -> Result<(), FingerprintingError> {
	use std::fmt::Write;

	let mut first_time = true;
	let node = schema
		.nodes
		.get(key.idx)
		.ok_or_else(|| format!("SchemaKey refers to non-existing node"))?;
	if key.is_ref {
		let name = match node {
			SchemaNode::Enum(enum_) => &enum_.name,
			SchemaNode::Fixed(fixed) => &fixed.name,
			SchemaNode::Record(record) => &record.name,
			_ => {
				return Err("SchemaKey::reference refers to non-named node"
					.to_owned()
					.into())
			}
		};
		buf.push('"');
		buf.push_str(name.fully_qualified_name());
		buf.push('"');
	} else {
		match *node {
			SchemaNode::Null => {
				buf.push_str("\"null\"");
			}
			SchemaNode::Boolean => {
				buf.push_str("\"boolean\"");
			}
			SchemaNode::Bytes => {
				buf.push_str("\"bytes\"");
			}
			SchemaNode::Double => {
				buf.push_str("\"double\"");
			}
			SchemaNode::Float => {
				buf.push_str("\"float\"");
			}
			SchemaNode::Int => {
				buf.push_str("\"int\"");
			}
			SchemaNode::Long => {
				buf.push_str("\"long\"");
			}
			SchemaNode::String => {
				buf.push_str("\"string\"");
			}
			SchemaNode::Union(ref union) => {
				buf.push('[');
				for variant in union.variants {
					if !first_time {
						buf.push(',');
					} else {
						first_time = false;
					}
					write_canonical_form(schema, variant, buf);
				}
				buf.push(']');
			}
			SchemaNode::Array(array_items) => {
				buf.push_str("{\"type\":\"array\",\"items\":");
				write_canonical_form(schema, array_items, buf);
				buf.push('}');
			}
			SchemaNode::Map(map_values) => {
				buf.push_str("{\"type\":\"map\",\"values\":");
				write_canonical_form(schema, map_values, buf);
				buf.push('}');
			}
			SchemaNode::Enum(enum_) => {
				buf.push_str("{\"name\":\"");
				buf.push_str(enum_.name.fully_qualified_name());
				buf.push_str("\",\"type\":\"enum\",\"symbols\":[");
				for enum_symbol in enum_.symbols.iter() {
					if !first_time {
						buf.push(',');
					} else {
						first_time = false;
					}
					buf.push('"');
					buf.push_str(enum_symbol);
					buf.push('"');
				}
				buf.push(']');
				buf.push('}');
			}
			SchemaNode::Fixed(fixed) => {
				buf.push_str("{\"name\":\"");
				buf.push_str(fixed.name.fully_qualified_name());
				buf.push_str("\",\"type\":\"fixed\",\"size\":");
				write!(buf, "{}", fixed.size).unwrap();
				buf.push('}');
			}
			SchemaNode::Record(record) => {
				buf.push_str("{\"name\":\"");
				buf.push_str(record.name.fully_qualified_name());
				buf.push_str("\",\"type\":\"record\",\"fields\":[");
				for field in record.fields.iter() {
					if !first_time {
						buf.push(',');
					} else {
						first_time = false;
					}
					buf.push_str("{\"name\":\"");
					buf.push_str(&field.name);
					buf.push_str("\",\"type\":");
					write_canonical_form(schema, field.schema, buf);
					buf.push('}');
				}
				buf.push_str("]}");
			}
		}
	}
	Ok(())
}

#[derive(thiserror::Error)]
#[error("Fingerprinting error: {inner}")]
pub(crate) struct FingerprintingError {
	inner: String,
}
impl<T: Into<String>> From<T> for FingerprintingError {
	fn from(inner: T) -> Self {
		Self {
			inner: inner.into(),
		}
	}
}
impl std::fmt::Debug for FingerprintingError {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		std::fmt::Display::fmt(self, f)
	}
}
