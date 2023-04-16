use crate::schema::Record;

use super::*;

pub struct SerializeStructAsRecordOrMapOrDuration<'r, 's, W> {
	kind: Kind<'r, 's, W>,
}

enum Kind<'r, 's, W> {
	Record {
		serializer_state: &'r mut SerializerState<'s, W>,
		record_state: RecordState<'s>,
	},
	Map {
		block_writer: BlockWriter<'r, 's, W>,
		elements_schema: &'s SchemaNode<'s>,
	},
	Duration {
		serializer_state: &'r mut SerializerState<'s, W>,
		values: [u32; 3],
		gotten_values: u8,
	},
}

struct RecordState<'s> {
	expected_fields: std::slice::Iter<'s, RecordField<'s>>,
	current_idx: usize,
	buffers: Vec<Option<Vec<u8>>>,
	record: &'s Record<'s>,
}

impl<'r, 's, W: Write> SerializeStructAsRecordOrMapOrDuration<'r, 's, W> {
	pub(super) fn record(state: &'r mut SerializerState<'s, W>, record: &'s Record<'s>) -> Self {
		Self {
			kind: Kind::Record {
				serializer_state: state,
				record_state: RecordState {
					expected_fields: record.fields.iter(),
					current_idx: 0,
					buffers: Default::default(),
					record,
				},
			},
		}
	}
	pub(super) fn map(
		state: &'r mut SerializerState<'s, W>,
		elements_schema: &'s SchemaNode<'s>,
		min_len: usize,
	) -> Result<Self, SerError> {
		Ok(Self {
			kind: Kind::Map {
				block_writer: BlockWriter::new(state, min_len)?,
				elements_schema,
			},
		})
	}
	pub(super) fn duration(state: &'r mut SerializerState<'s, W>) -> Result<Self, SerError> {
		Ok(Self {
			kind: Kind::Duration {
				serializer_state: state,
				values: [0; 3],
				gotten_values: 0,
			},
		})
	}

	fn end(self) -> Result<(), SerError> {
		match self.kind {
			Kind::Record {
				serializer_state,
				record_state:
					RecordState {
						expected_fields: _,
						mut current_idx,
						mut buffers,
						record,
					},
			} => {
				loop {
					if current_idx < record.fields.len() {
						let missing_field = || {
							SerError::custom(format_args!(
								"Missing field {:?} in record",
								record.fields[current_idx].name.as_str()
							))
						};
						match record.fields[current_idx].schema {
							SchemaNode::Null => {
								// Always-null fields can be skipped in source
								// without erroring (although providing it with
								// type `()` will result in better perf because
								// we won't need to buffer)
							}
							SchemaNode::Union(union) => {
								match union.per_type_lookup.unnamed(UnionVariantLookupKey::Null) {
									Some((discriminant, SchemaNode::Null)) => {
										// Optional fields can be skipped in source
										// without erroring (although providing it with type `()`
										// will result in better perf because we won't need to
										// buffer)
										serializer_state
											.writer
											.write_varint(discriminant)
											.map_err(SerError::io)?;
									}
									_ => return Err(missing_field()),
								}
							}
							_ => return Err(missing_field()),
						}
						current_idx += 1;
					} else {
						break;
					}
					while let Some(already_serialized) =
						buffers.get(current_idx).and_then(|opt| opt.as_deref())
					{
						serializer_state
							.writer
							.write_all(already_serialized)
							.map_err(SerError::io)?;
						buffers[current_idx] = None;
						current_idx += 1;
					}
				}
				debug_assert!(buffers.iter().all(|opt| opt.is_none()));
			}
			Kind::Map { block_writer, .. } => {
				block_writer.end()?;
			}
			Kind::Duration {
				serializer_state,
				values,
				gotten_values,
			} => {
				if gotten_values != 0b111 {
					return Err(duration_fields_incorrect());
				} else {
					// This section should be noop after optimizer
					let [a, b, c] = values;
					let [a3, a2, a1, a0] = a.to_le_bytes();
					let [b3, b2, b1, b0] = b.to_le_bytes();
					let [c3, c2, c1, c0] = c.to_le_bytes();
					let values = [a3, a2, a1, a0, b3, b2, b1, b0, c3, c2, c1, c0];

					// Now we serialize
					serializer_state
						.writer
						.write_all(&values)
						.map_err(SerError::io)?;
				}
			}
		}
		Ok(())
	}
}

fn field_idx<'s>(
	record_state: &mut RecordState<'s>,
	field_name: &str,
) -> Result<(usize, &'s SchemaNode<'s>), SerError> {
	let key_does_not_exist = || {
		SerError::custom(format_args!(
			"Attempting to serialize field that doesn't exist in record: {field_name}"
		))
	};
	match record_state.expected_fields.as_slice().first() {
		None => Err(match record_state.record.per_name_lookup.get(field_name) {
			Some(_) => serializing_same_field_name_twice(field_name),
			None => key_does_not_exist(),
		}),
		Some(first) => {
			if first.name == field_name {
				// Fast case: fields are ordered so we don't need to buffer nor hash-map lookup
				Ok((record_state.current_idx, first.schema))
			} else {
				let field_idx = *record_state
					.record
					.per_name_lookup
					.get(field_name)
					.ok_or_else(key_does_not_exist)?;
				match field_idx.cmp(&record_state.current_idx) {
					std::cmp::Ordering::Greater => {
						Ok((field_idx, record_state.record.fields[field_idx].schema))
					}
					std::cmp::Ordering::Less => Err(serializing_same_field_name_twice(field_name)),
					std::cmp::Ordering::Equal => panic!(
						"We should have hit the `first.name == field_name` branch - \
								please open an issue at serde_avro_fast"
					),
				}
			}
		}
	}
}

fn serialize_record_value<'r, 's, W: Write, T: ?Sized>(
	serializer_state: &'r mut SerializerState<'s, W>,
	record_state: &mut RecordState<'s>,
	field_idx: usize,
	schema_node: &'s SchemaNode<'s>,
	value: &T,
) -> Result<(), SerError>
where
	T: Serialize,
{
	if field_idx == record_state.current_idx {
		// Fast case: fields are ordered so we don't need to buffer nor
		// hash-map lookup
		value.serialize(DatumSerializer {
			state: serializer_state,
			schema_node,
		})?;
		record_state.expected_fields.next().unwrap();
		record_state.current_idx += 1;
		while let Some(already_serialized) = record_state
			.buffers
			.get(record_state.current_idx)
			.and_then(|opt| opt.as_deref())
		{
			serializer_state
				.writer
				.write_all(already_serialized)
				.map_err(SerError::io)?;
			record_state.buffers[record_state.current_idx] = None;
			record_state.current_idx += 1;
			record_state.expected_fields.next().unwrap();
		}
		Ok(())
	} else {
		if record_state.buffers.len() <= field_idx {
			record_state.buffers.resize(field_idx + 1, None);
		}
		let buf: &mut Vec<u8> = match &mut record_state.buffers[field_idx] {
			Some(_) => {
				return Err(serializing_same_field_name_twice(
					&record_state.record.fields[field_idx].name,
				))
			}
			buf @ None => {
				*buf = Some(Vec::new());
				buf.as_mut().unwrap()
			}
		};
		value.serialize(DatumSerializer {
			state: &mut SerializerState {
				writer: buf,
				config: SerializerConfig {
					schema_root: serializer_state.config.schema_root,
				},
			},
			schema_node,
		})
	}
}

fn serialize_duration_field<T>(
	values: &mut [u32; 3],
	gotten_values: &mut u8,
	duration_field: extract_for_duration::DurationFieldName,
	value: &T,
) -> Result<(), SerError>
where
	T: Serialize + ?Sized,
{
	let bit = 1u8 << (duration_field as u8);
	if *gotten_values & bit != 0 {
		return Err(SerError::custom(format_args!(
			"{duration_field} is getting serialized twice for serialization as Duration",
		)));
	}
	values[duration_field as usize] =
		value.serialize(extract_for_duration::ExtractU32ForDuration)?;
	*gotten_values |= bit;
	Ok(())
}

fn serializing_same_field_name_twice(field_name: &str) -> SerError {
	SerError::custom(format_args!(
		"Attempting to serialize field with same field_name \
			twice in record (field_name: {field_name:?})"
	))
}

pub(super) fn duration_fields_incorrect() -> SerError {
	SerError::new(
		"A struct can indeed be serialized as Duration, but only if its fields are \
			months/days/milliseconds",
	)
}

impl<'r, 's, W: Write> SerializeStruct for SerializeStructAsRecordOrMapOrDuration<'r, 's, W> {
	type Ok = ();

	type Error = SerError;

	fn serialize_field<T: ?Sized>(
		&mut self,
		key: &'static str,
		value: &T,
	) -> Result<(), Self::Error>
	where
		T: Serialize,
	{
		match &mut self.kind {
			Kind::Record {
				serializer_state,
				record_state,
			} => {
				let (field_idx, schema_node) = field_idx(record_state, key)?;
				serialize_record_value(
					serializer_state,
					record_state,
					field_idx,
					schema_node,
					value,
				)
			}
			Kind::Map {
				elements_schema,
				block_writer,
			} => {
				block_writer.signal_next_record()?;
				key.serialize(DatumSerializer {
					state: block_writer.state,
					schema_node: &SchemaNode::String,
				})?;
				value.serialize(DatumSerializer {
					state: block_writer.state,
					schema_node: *elements_schema,
				})
			}
			Kind::Duration {
				values,
				gotten_values,
				..
			} => {
				let duration_field = extract_for_duration::DurationFieldName::from_str(key)?;
				serialize_duration_field(values, gotten_values, duration_field, value)
			}
		}
	}

	fn end(self) -> Result<Self::Ok, Self::Error> {
		self.end()
	}
}

impl<'r, 's, W: Write> SerializeStructVariant
	for SerializeStructAsRecordOrMapOrDuration<'r, 's, W>
{
	type Ok = ();

	type Error = SerError;

	fn serialize_field<T: ?Sized>(
		&mut self,
		key: &'static str,
		value: &T,
	) -> Result<(), Self::Error>
	where
		T: Serialize,
	{
		<Self as SerializeStruct>::serialize_field(self, key, value)
	}

	fn end(self) -> Result<Self::Ok, Self::Error> {
		<Self as SerializeStruct>::end(self)
	}
}

pub struct SerializeMapAsRecordOrMapOrDuration<'r, 's, W> {
	inner: SerializeStructAsRecordOrMapOrDuration<'r, 's, W>,
	key_hint: KeyHint<'s>,
}

enum KeyHint<'s> {
	None,
	KeyLocation {
		field_idx: usize,
		schema_node: &'s SchemaNode<'s>,
	},
	DurationField(extract_for_duration::DurationFieldName),
}

impl<'r, 's, W: Write> SerializeMapAsRecordOrMapOrDuration<'r, 's, W> {
	pub(super) fn record(state: &'r mut SerializerState<'s, W>, record: &'s Record<'s>) -> Self {
		Self {
			inner: SerializeStructAsRecordOrMapOrDuration::record(state, record),
			key_hint: KeyHint::None,
		}
	}
	pub(super) fn map(
		state: &'r mut SerializerState<'s, W>,
		elements_schema: &'s SchemaNode<'s>,
		min_len: usize,
	) -> Result<Self, SerError> {
		Ok(Self {
			inner: SerializeStructAsRecordOrMapOrDuration::map(state, elements_schema, min_len)?,
			key_hint: KeyHint::None,
		})
	}

	pub(super) fn duration(state: &'r mut SerializerState<'s, W>) -> Result<Self, SerError> {
		Ok(Self {
			inner: SerializeStructAsRecordOrMapOrDuration::duration(state)?,
			key_hint: KeyHint::None,
		})
	}
}

impl<'r, 's, W: Write> SerializeMap for SerializeMapAsRecordOrMapOrDuration<'r, 's, W> {
	type Ok = ();
	type Error = SerError;

	fn serialize_key<T: ?Sized>(&mut self, key: &T) -> Result<(), Self::Error>
	where
		T: Serialize,
	{
		match &mut self.inner.kind {
			Kind::Record { record_state, .. } => {
				let (field_idx, schema_node) =
					key.serialize(FindFieldIndexSerializer { record_state })?;
				self.key_hint = KeyHint::KeyLocation {
					field_idx,
					schema_node,
				};
				Ok(())
			}
			Kind::Map { block_writer, .. } => {
				block_writer.signal_next_record()?;
				key.serialize(DatumSerializer {
					state: block_writer.state,
					schema_node: &SchemaNode::String,
				})
			}
			Kind::Duration { .. } => {
				self.key_hint = KeyHint::DurationField(
					key.serialize(extract_for_duration::ExtractFieldNameForDuration)?,
				);
				Ok(())
			}
		}
	}

	fn serialize_value<T: ?Sized>(&mut self, value: &T) -> Result<(), Self::Error>
	where
		T: Serialize,
	{
		match &mut self.inner.kind {
			Kind::Record {
				serializer_state,
				record_state,
			} => match std::mem::replace(&mut self.key_hint, KeyHint::None) {
				KeyHint::KeyLocation {
					field_idx,
					schema_node,
				} => serialize_record_value(
					serializer_state,
					record_state,
					field_idx,
					schema_node,
					value,
				),
				_ => panic!("serialize_key should have been called before serialize_value"),
			},
			Kind::Map {
				elements_schema,
				block_writer,
			} => value.serialize(DatumSerializer {
				state: block_writer.state,
				schema_node: *elements_schema,
			}),
			Kind::Duration {
				values,
				gotten_values,
				..
			} => match std::mem::replace(&mut self.key_hint, KeyHint::None) {
				KeyHint::DurationField(duration_field) => {
					serialize_duration_field(values, gotten_values, duration_field, value)
				}
				_ => panic!("serialize_key should have been called before serialize_value"),
			},
		}
	}

	fn serialize_entry<K: ?Sized, V: ?Sized>(
		&mut self,
		key: &K,
		value: &V,
	) -> Result<(), Self::Error>
	where
		K: Serialize,
		V: Serialize,
	{
		match &mut self.inner.kind {
			Kind::Record {
				serializer_state,
				record_state,
			} => {
				let (field_idx, schema_node) =
					key.serialize(FindFieldIndexSerializer { record_state })?;
				serialize_record_value(
					serializer_state,
					record_state,
					field_idx,
					schema_node,
					value,
				)
			}
			Kind::Map {
				elements_schema,
				block_writer,
			} => {
				block_writer.signal_next_record()?;
				key.serialize(DatumSerializer {
					state: block_writer.state,
					schema_node: &SchemaNode::String,
				})?;
				value.serialize(DatumSerializer {
					state: block_writer.state,
					schema_node: *elements_schema,
				})
			}
			Kind::Duration {
				values,
				gotten_values,
				..
			} => {
				let duration_field =
					key.serialize(extract_for_duration::ExtractFieldNameForDuration)?;
				serialize_duration_field(values, gotten_values, duration_field, value)
			}
		}
	}

	fn end(self) -> Result<Self::Ok, Self::Error> {
		self.inner.end()
	}
}

struct FindFieldIndexSerializer<'record_state, 's> {
	record_state: &'record_state mut RecordState<'s>,
}
impl<'s> serde::Serializer for FindFieldIndexSerializer<'_, 's> {
	type Ok = (usize, &'s SchemaNode<'s>);
	type Error = SerError;

	fn serialize_str(self, v: &str) -> Result<Self::Ok, Self::Error> {
		field_idx(self.record_state, v)
	}

	serde_serializer_quick_unsupported::serializer_unsupported! {
		err = (SerError::new("Key of map being serialized as record should have been an str"));
		bool i8 i16 i32 i64 u8 u16 u32 u64 f32 f64 char bytes none some unit unit_struct
		unit_variant newtype_struct newtype_variant seq tuple tuple_struct tuple_variant map struct
		struct_variant i128 u128
	}
}
