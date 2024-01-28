use super::*;

pub struct SerializeStructAsRecordOrMapOrDuration<'r, 'c, 's, W> {
	kind: Kind<'r, 'c, 's, W>,
}

enum Kind<'r, 'c, 's, W> {
	Record(KindRecord<'r, 'c, 's, W>),
	Map {
		block_writer: BlockWriter<'r, 'c, 's, W>,
		elements_schema: &'s SchemaNode<'s>,
	},
	Duration {
		serializer_state: &'r mut SerializerState<'c, 's, W>,
		values: [u32; 3],
		gotten_values: u8,
	},
}

struct KindRecord<'r, 'c, 's, W> {
	serializer_state: &'r mut SerializerState<'c, 's, W>,
	record_state: RecordState<'s>,
}

struct RecordState<'s> {
	expected_fields: std::slice::Iter<'s, RecordField<'s>>,
	current_idx: usize,
	buffers: Vec<Option<Vec<u8>>>,
	record: &'s Record<'s>,
}

impl<'r, 'c, 's, W> Drop for KindRecord<'r, 'c, 's, W> {
	fn drop(&mut self) {
		// In order to avoid allocating even when field reordering is necessary we can
		// preserve the necessary allocations from one record to another (even across
		// serializations).
		// This brings ~40% perf improvement
		if self.record_state.buffers.capacity() > 0 {
			self.serializer_state
				.config
				.buffers
				.field_reordering_buffers
				.extend(
					self.record_state
						.buffers
						.drain(..)
						.filter_map(std::convert::identity)
						.map(|mut v| {
							v.clear();
							v
						}),
				);
			self.serializer_state
				.config
				.buffers
				.field_reordering_super_buffers
				.push(std::mem::replace(
					&mut self.record_state.buffers,
					Vec::new(),
				));
		}
	}
}

impl<'r, 'c, 's, W: Write> SerializeStructAsRecordOrMapOrDuration<'r, 'c, 's, W> {
	pub(super) fn record(
		state: &'r mut SerializerState<'c, 's, W>,
		record: &'s Record<'s>,
	) -> Self {
		Self {
			kind: Kind::Record(KindRecord {
				record_state: RecordState {
					expected_fields: record.fields.iter(),
					current_idx: 0,
					buffers: state
						.config
						.buffers
						.field_reordering_super_buffers
						.pop()
						.map(|v| {
							// To be replaced with `Option::inspect` once that is stabilized
							assert!(v.is_empty());
							v
						})
						.unwrap_or_else(Vec::new),
					record,
				},
				serializer_state: state,
			}),
		}
	}
	pub(super) fn map(
		state: &'r mut SerializerState<'c, 's, W>,
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
	pub(super) fn duration(state: &'r mut SerializerState<'c, 's, W>) -> Result<Self, SerError> {
		Ok(Self {
			kind: Kind::Duration {
				serializer_state: state,
				values: [0; 3],
				gotten_values: 0,
			},
		})
	}

	fn end(mut self) -> Result<(), SerError> {
		match self.kind {
			Kind::Record(KindRecord {
				ref mut serializer_state,
				record_state:
					RecordState {
						expected_fields: _,
						mut current_idx,
						ref mut buffers,
						record,
					},
			}) => {
				let serializer_state = &mut **serializer_state;
				loop {
					if current_idx < record.fields.len() {
						let missing_field = || {
							SerError::custom(format_args!(
								"Missing field {:?} in record",
								record.fields[current_idx].name.as_str()
							))
						};
						match *record.fields[current_idx].schema {
							SchemaNode::Null => {
								// Always-null fields can be skipped in source
								// without erroring (although providing it with
								// type `()` will result in better perf because
								// we won't need to buffer)
							}
							SchemaNode::Union(ref union) => {
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
					while let Some(mut already_serialized) =
						buffers.get_mut(current_idx).and_then(|opt| opt.take())
					{
						serializer_state
							.writer
							.write_all(&already_serialized)
							.map_err(SerError::io)?;

						already_serialized.clear();
						serializer_state
							.config
							.buffers
							.field_reordering_buffers
							.push(already_serialized);

						current_idx += 1;
					}
				}
				debug_assert!(buffers.iter().all(|opt| opt.is_none()));
				// We have emptied them all so no need for the drop impl to re-check that
				buffers.clear();
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
				Ok((record_state.current_idx, first.schema.as_ref()))
			} else {
				let field_idx = *record_state
					.record
					.per_name_lookup
					.get(field_name)
					.ok_or_else(key_does_not_exist)?;
				match field_idx.cmp(&record_state.current_idx) {
					std::cmp::Ordering::Greater => Ok((
						field_idx,
						record_state.record.fields[field_idx].schema.as_ref(),
					)),
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

fn serialize_record_value<'r, 'c, 's, W: Write, T: ?Sized>(
	serializer_state: &'r mut SerializerState<'c, 's, W>,
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
		while let Some(mut already_serialized) = record_state
			.buffers
			.get_mut(record_state.current_idx)
			.and_then(|opt| opt.take())
		{
			serializer_state
				.writer
				.write_all(&already_serialized)
				.map_err(SerError::io)?;

			already_serialized.clear();
			serializer_state
				.config
				.buffers
				.field_reordering_buffers
				.push(already_serialized);

			record_state.current_idx += 1;
			record_state.expected_fields.next().unwrap();
		}
		Ok(())
	} else {
		if record_state.buffers.len() <= field_idx {
			record_state.buffers.resize(field_idx + 1, None);
		}
		let field_buf: &mut Option<Vec<u8>> = match &mut record_state.buffers[field_idx] {
			Some(_) => {
				return Err(serializing_same_field_name_twice(
					&record_state.record.fields[field_idx].name,
				))
			}
			field_buf @ None => field_buf,
		};
		let mut buf_serializer_state = SerializerState {
			writer: serializer_state
				.config
				.buffers
				.field_reordering_buffers
				.pop()
				.map(|v| {
					// To be replaced with `Option::inspect` once that is stabilized
					assert!(v.is_empty());
					v
				})
				.unwrap_or_else(Vec::new),
			config: serializer_state.config,
		};
		value.serialize(DatumSerializer {
			state: &mut buf_serializer_state,
			schema_node,
		})?;
		// Put buffer in place after serialization
		// (after instead of before gives one less deref level during inner
		// serialization, and avoids extra monomorphizations if serializing to Vec)
		*field_buf = Some(buf_serializer_state.into_writer());
		Ok(())
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

impl<'r, 'c, 's, W: Write> SerializeStruct
	for SerializeStructAsRecordOrMapOrDuration<'r, 'c, 's, W>
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
		match &mut self.kind {
			Kind::Record(KindRecord {
				serializer_state,
				record_state,
			}) => {
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

impl<'r, 'c, 's, W: Write> SerializeStructVariant
	for SerializeStructAsRecordOrMapOrDuration<'r, 'c, 's, W>
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

pub struct SerializeMapAsRecordOrMapOrDuration<'r, 'c, 's, W> {
	inner: SerializeStructAsRecordOrMapOrDuration<'r, 'c, 's, W>,
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

impl<'r, 'c, 's, W: Write> SerializeMapAsRecordOrMapOrDuration<'r, 'c, 's, W> {
	pub(super) fn record(
		state: &'r mut SerializerState<'c, 's, W>,
		record: &'s Record<'s>,
	) -> Self {
		Self {
			inner: SerializeStructAsRecordOrMapOrDuration::record(state, record),
			key_hint: KeyHint::None,
		}
	}
	pub(super) fn map(
		state: &'r mut SerializerState<'c, 's, W>,
		elements_schema: &'s SchemaNode<'s>,
		min_len: usize,
	) -> Result<Self, SerError> {
		Ok(Self {
			inner: SerializeStructAsRecordOrMapOrDuration::map(state, elements_schema, min_len)?,
			key_hint: KeyHint::None,
		})
	}

	pub(super) fn duration(state: &'r mut SerializerState<'c, 's, W>) -> Result<Self, SerError> {
		Ok(Self {
			inner: SerializeStructAsRecordOrMapOrDuration::duration(state)?,
			key_hint: KeyHint::None,
		})
	}
}

impl<'r, 'c, 's, W: Write> SerializeMap for SerializeMapAsRecordOrMapOrDuration<'r, 'c, 's, W> {
	type Ok = ();
	type Error = SerError;

	fn serialize_key<T: ?Sized>(&mut self, key: &T) -> Result<(), Self::Error>
	where
		T: Serialize,
	{
		match &mut self.inner.kind {
			Kind::Record(KindRecord { record_state, .. }) => {
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
			Kind::Record(KindRecord {
				serializer_state,
				record_state,
			}) => match std::mem::replace(&mut self.key_hint, KeyHint::None) {
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
			Kind::Record(KindRecord {
				serializer_state,
				record_state,
			}) => {
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
