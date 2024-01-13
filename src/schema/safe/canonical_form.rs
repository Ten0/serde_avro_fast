use crate::schema::{
	safe::{self as s, EditableSchema, SchemaKey, SchemaType},
	SchemaError,
};

impl EditableSchema {
	/// Obtain the
	/// [Parsing Canonical Form](https://avro.apache.org/docs/current/specification/#parsing-canonical-form-for-schemas)
	/// of the schema
	///
	/// This function is not public because you shouldn't use that schema
	/// when transmitting the schema to other people, notably because it loses
	/// the logical types information, and additionally because it may not be
	/// valid JSON (there's no escaping in the json generation...)
	/// See https://issues.apache.org/jira/browse/AVRO-1721
	pub(crate) fn parsing_canonical_form(&self) -> Result<String, SchemaError> {
		let mut buf = String::new();
		write_canonical_form(self, SchemaKey::from_idx(0), &mut buf)?;
		Ok(buf)
	}

	/// Obtain the Rabin fingerprint of the schema
	pub fn canonical_form_rabin_fingerprint(&self) -> Result<[u8; 8], SchemaError> {
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
	schema: &EditableSchema,
	key: SchemaKey,
	buf: &mut String,
) -> Result<(), SchemaError> {
	use std::fmt::Write;

	let mut first_time = true;
	let node = schema
		.nodes
		.get(key.idx)
		.ok_or_else(|| SchemaError::new("SchemaKey refers to non-existing node"))?;
	if key.is_ref {
		let name = match node {
			SchemaType::Enum(enum_) => &enum_.name,
			SchemaType::Fixed(fixed) => &fixed.name,
			SchemaType::Record(record) => &record.name,
			_ => {
				return Err(SchemaError::new(
					"SchemaKey::reference refers to non-named node",
				))
			}
		};
		buf.push('"');
		buf.push_str(name.fully_qualified_name());
		buf.push('"');
	} else {
		match *node {
			SchemaType::Null => {
				buf.push_str("\"null\"");
			}
			SchemaType::Boolean => {
				buf.push_str("\"boolean\"");
			}
			SchemaType::Bytes
			| SchemaType::Decimal(s::Decimal {
				repr: s::DecimalRepr::Bytes,
				..
			}) => {
				buf.push_str("\"bytes\"");
			}
			SchemaType::Double => {
				buf.push_str("\"double\"");
			}
			SchemaType::Float => {
				buf.push_str("\"float\"");
			}
			SchemaType::Int => {
				buf.push_str("\"int\"");
			}
			SchemaType::Long => {
				buf.push_str("\"long\"");
			}
			SchemaType::String => {
				buf.push_str("\"string\"");
			}
			SchemaType::Union(ref union) => {
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
			SchemaType::Array(array_items) => {
				buf.push_str("{\"type\":\"array\",\"items\":");
				write_canonical_form(schema, array_items, buf);
				buf.push('}');
			}
			SchemaType::Map(map_values) => {
				buf.push_str("{\"type\":\"map\",\"values\":");
				write_canonical_form(schema, map_values, buf);
				buf.push('}');
			}
			SchemaType::Enum(enum_) => {
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
			SchemaType::Fixed(fixed)
			| SchemaType::Decimal(s::Decimal {
				repr: s::DecimalRepr::Fixed(fixed),
				..
			}) => {
				buf.push_str("{\"name\":\"");
				buf.push_str(fixed.name.fully_qualified_name());
				buf.push_str("\",\"type\":\"fixed\",\"size\":");
				write!(buf, "{}", fixed.size).unwrap();
				buf.push('}');
			}
			SchemaType::Record(record) => {
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
