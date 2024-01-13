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
		let mut state = WriteCanonicalFormState {
			buf: String::new(),
			named_type_written: vec![false; self.nodes.len()],
		};
		state.write_canonical_form(self, SchemaKey::from_idx(0))?;
		Ok(state.buf)
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

struct WriteCanonicalFormState {
	buf: String,
	named_type_written: Vec<bool>,
}

impl WriteCanonicalFormState {
	/// Manual implementation that strictly copies that of the reference
	/// implementation in Java. According to the java code, this is not
	/// guaranteed to actually be valid JSON (no escaping...)
	fn write_canonical_form(
		&mut self,
		schema: &EditableSchema,
		key: SchemaKey,
	) -> Result<(), SchemaError> {
		use std::fmt::Write;

		let mut first_time = true;
		let node = schema
			.nodes
			.get(key.idx)
			.ok_or_else(|| SchemaError::new("SchemaKey refers to non-existing node"))?;

		let should_not_write_only_name =
			|name: &s::Name, state: &mut WriteCanonicalFormState| -> bool {
				match &mut state.named_type_written[key.idx] {
					b @ false => {
						*b = true;
						true
					}
					true => {
						state.buf.push('"');
						state.buf.push_str(name.fully_qualified_name());
						state.buf.push('"');
						false
					}
				}
			};

		// In PCF, logical types are completely ignored
		// https://issues.apache.org/jira/browse/AVRO-1721
		match node.type_ {
			SchemaType::Null => {
				self.buf.push_str("\"null\"");
			}
			SchemaType::Boolean => {
				self.buf.push_str("\"boolean\"");
			}
			SchemaType::Bytes => {
				self.buf.push_str("\"bytes\"");
			}
			SchemaType::Double => {
				self.buf.push_str("\"double\"");
			}
			SchemaType::Float => {
				self.buf.push_str("\"float\"");
			}
			SchemaType::Int => {
				self.buf.push_str("\"int\"");
			}
			SchemaType::Long => {
				self.buf.push_str("\"long\"");
			}
			SchemaType::String => {
				self.buf.push_str("\"string\"");
			}
			SchemaType::Union(ref union) => {
				self.buf.push('[');
				for &variant in &union.variants {
					if !first_time {
						self.buf.push(',');
					} else {
						first_time = false;
					}
					self.write_canonical_form(schema, variant)?;
				}
				self.buf.push(']');
			}
			SchemaType::Array(array_items) => {
				self.buf.push_str("{\"type\":\"array\",\"items\":");
				self.write_canonical_form(schema, array_items)?;
				self.buf.push('}');
			}
			SchemaType::Map(map_values) => {
				self.buf.push_str("{\"type\":\"map\",\"values\":");
				self.write_canonical_form(schema, map_values)?;
				self.buf.push('}');
			}
			SchemaType::Enum(ref enum_) => {
				if !should_not_write_only_name(&enum_.name, self) {
					self.buf.push_str("{\"name\":\"");
					self.buf.push_str(enum_.name.fully_qualified_name());
					self.buf.push_str("\",\"type\":\"enum\",\"symbols\":[");
					for enum_symbol in enum_.symbols.iter() {
						if !first_time {
							self.buf.push(',');
						} else {
							first_time = false;
						}
						self.buf.push('"');
						self.buf.push_str(enum_symbol);
						self.buf.push('"');
					}
					self.buf.push(']');
					self.buf.push('}');
				}
			}
			SchemaType::Fixed(ref fixed) => {
				if !should_not_write_only_name(&fixed.name, self) {
					self.buf.push_str("{\"name\":\"");
					self.buf.push_str(fixed.name.fully_qualified_name());
					self.buf.push_str("\",\"type\":\"fixed\",\"size\":");
					write!(self.buf, "{}", fixed.size).unwrap();
					self.buf.push('}');
				}
			}
			SchemaType::Record(ref record) => {
				if !should_not_write_only_name(&record.name, self) {
					self.buf.push_str("{\"name\":\"");
					self.buf.push_str(record.name.fully_qualified_name());
					self.buf.push_str("\",\"type\":\"record\",\"fields\":[");
					for field in record.fields.iter() {
						if !first_time {
							self.buf.push(',');
						} else {
							first_time = false;
						}
						self.buf.push_str("{\"name\":\"");
						self.buf.push_str(&field.name);
						self.buf.push_str("\",\"type\":");
						self.write_canonical_form(schema, field.schema)?;
						self.buf.push('}');
					}
					self.buf.push_str("]}");
				}
			}
		}
		Ok(())
	}
}
