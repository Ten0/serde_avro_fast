use crate::schema::{
	safe::{self as s, rabin::Rabin, RegularType, SchemaKey, SchemaMut},
	SchemaError,
};

use std::fmt::Write;

impl SchemaMut {
	/// Obtain the Rabin fingerprint of the schema
	///
	/// This is what is used for avro [single object encoding](https://avro.apache.org/docs/current/specification/#single-object-encoding)
	///
	/// There is no public function to obtain a `String` version of the parsing
	/// canonical form because one shouldn't use that when transmitting the
	/// schema to other people, notably because it loses the logical types
	/// information (see <https://issues.apache.org/jira/browse/AVRO-1721>), additionally
	/// because it may be invalid JSON (there's no escaping in the JSON in the
	/// reference Java implementation), and finally because even if it happens
	/// to be a valid JSON, it may not parse because if a fullname doesn't
	/// contain a dot, it resolves differently than it's written.
	pub fn canonical_form_rabin_fingerprint(&self) -> Result<[u8; 8], SchemaError> {
		let mut state = WriteCanonicalFormState {
			w: ErrorConversionWriter(Rabin::default()),
			named_type_written: vec![false; self.nodes.len()],
		};
		state.write_canonical_form(self, SchemaKey::from_idx(0))?;
		Ok(state.w.0.finish())
	}
}

struct WriteCanonicalFormState<W> {
	w: ErrorConversionWriter<W>,
	named_type_written: Vec<bool>,
}

impl<W: Write> WriteCanonicalFormState<W> {
	/// Manual implementation that strictly copies that of the reference
	/// implementation in Java. According to the java code, this is not
	/// guaranteed to actually be valid JSON (no escaping...)
	fn write_canonical_form(
		&mut self,
		schema: &SchemaMut,
		key: SchemaKey,
	) -> Result<(), SchemaError> {
		let node = schema
			.nodes
			.get(key.idx)
			.ok_or_else(|| SchemaError::new("SchemaKey refers to non-existing node"))?;

		match *node {
			s::SchemaNode::LogicalType {
				inner,
				logical_type: _,
			} => {
				// In PCF, logical types are completely ignored
				// https://issues.apache.org/jira/browse/AVRO-1721
				self.write_canonical_form(schema, inner)
			}
			s::SchemaNode::RegularType(ref type_) => {
				let mut first_time = true;
				let should_not_write_only_name = |name: &s::Name,
				                                  state: &mut WriteCanonicalFormState<W>|
				 -> Result<bool, SchemaError> {
					Ok(match &mut state.named_type_written[key.idx] {
						b @ false => {
							*b = true;
							true
						}
						true => {
							state.w.write_char('"')?;
							state.w.write_str(name.fully_qualified_name())?;
							state.w.write_char('"')?;
							false
						}
					})
				};
				match *type_ {
					RegularType::Null => {
						self.w.write_str("\"null\"")?;
					}
					RegularType::Boolean => {
						self.w.write_str("\"boolean\"")?;
					}
					RegularType::Bytes => {
						self.w.write_str("\"bytes\"")?;
					}
					RegularType::Double => {
						self.w.write_str("\"double\"")?;
					}
					RegularType::Float => {
						self.w.write_str("\"float\"")?;
					}
					RegularType::Int => {
						self.w.write_str("\"int\"")?;
					}
					RegularType::Long => {
						self.w.write_str("\"long\"")?;
					}
					RegularType::String => {
						self.w.write_str("\"string\"")?;
					}
					RegularType::Union(ref union) => {
						self.w.write_char('[')?;
						for &variant in &union.variants {
							if !first_time {
								self.w.write_char(',')?;
							} else {
								first_time = false;
							}
							self.write_canonical_form(schema, variant)?;
						}
						self.w.write_char(']')?;
					}
					RegularType::Array(ref array) => {
						self.w.write_str("{\"type\":\"array\",\"items\":")?;
						self.write_canonical_form(schema, array.items)?;
						self.w.write_char('}')?;
					}
					RegularType::Map(ref map) => {
						self.w.write_str("{\"type\":\"map\",\"values\":")?;
						self.write_canonical_form(schema, map.values)?;
						self.w.write_char('}')?;
					}
					RegularType::Enum(ref enum_) => {
						if should_not_write_only_name(&enum_.name, self)? {
							self.w.write_str("{\"name\":\"")?;
							self.w.write_str(enum_.name.fully_qualified_name())?;
							self.w.write_str("\",\"type\":\"enum\",\"symbols\":[")?;
							for enum_symbol in enum_.symbols.iter() {
								if !first_time {
									self.w.write_char(',')?;
								} else {
									first_time = false;
								}
								self.w.write_char('"')?;
								self.w.write_str(enum_symbol)?;
								self.w.write_char('"')?;
							}
							self.w.write_char(']')?;
							self.w.write_char('}')?;
						}
					}
					RegularType::Fixed(ref fixed) => {
						if should_not_write_only_name(&fixed.name, self)? {
							self.w.write_str("{\"name\":\"")?;
							self.w.write_str(fixed.name.fully_qualified_name())?;
							self.w.write_str("\",\"type\":\"fixed\",\"size\":")?;
							write!(self.w.0, "{}", fixed.size).map_err(convert_error)?;
							self.w.write_char('}')?;
						}
					}
					RegularType::Record(ref record) => {
						if should_not_write_only_name(&record.name, self)? {
							self.w.write_str("{\"name\":\"")?;
							self.w.write_str(record.name.fully_qualified_name())?;
							self.w.write_str("\",\"type\":\"record\",\"fields\":[")?;
							for field in record.fields.iter() {
								if !first_time {
									self.w.write_char(',')?;
								} else {
									first_time = false;
								}
								self.w.write_str("{\"name\":\"")?;
								self.w.write_str(&field.name)?;
								self.w.write_str("\",\"type\":")?;
								self.write_canonical_form(schema, field.type_)?;
								self.w.write_char('}')?;
							}
							self.w.write_str("]}")?;
						}
					}
				}
				Ok(())
			}
		}
	}
}

/// Convert errors from `std::fmt::Write` to `SchemaError`
/// in order to be able to use `?` in `WriteCanonicalFormState`
struct ErrorConversionWriter<W>(W);
impl<W: Write> ErrorConversionWriter<W> {
	#[inline]
	fn write_char(&mut self, c: char) -> Result<(), SchemaError> {
		self.0.write_char(c).map_err(convert_error)
	}
	#[inline]
	fn write_str(&mut self, s: &str) -> Result<(), SchemaError> {
		self.0.write_str(s).map_err(convert_error)
	}
}
fn convert_error(e: std::fmt::Error) -> SchemaError {
	SchemaError::msg(format_args!(
		"Error writing schema parsing canonical form: {}",
		e,
	))
}
