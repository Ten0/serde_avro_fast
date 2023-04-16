use crate::schema::Record;

use super::*;

pub struct SerializeStructAsRecordOrMap<'r, 's, W> {
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
}

struct RecordState<'s> {
	expected_fields: std::slice::Iter<'s, RecordField<'s>>,
	current_idx: usize,
	buffers: Vec<Option<Vec<u8>>>,
	record: &'s Record<'s>,
}

impl<'r, 's, W: Write> SerializeStructAsRecordOrMap<'r, 's, W> {
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
	(field_idx, schema_node): (usize, &'s SchemaNode<'s>),
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

fn serializing_same_field_name_twice(field_name: &str) -> SerError {
	SerError::custom(format_args!(
		"Attempting to serialize field with same field_name \
			twice in record (field_name: {field_name:?})"
	))
}

impl<'r, 's, W: Write> SerializeStruct for SerializeStructAsRecordOrMap<'r, 's, W> {
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
				let field_idx = field_idx(record_state, key)?;
				serialize_record_value(serializer_state, record_state, field_idx, value)
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
		}
	}

	fn end(self) -> Result<Self::Ok, Self::Error> {
		self.end()
	}
}

impl<'r, 's, W: Write> SerializeStructVariant for SerializeStructAsRecordOrMap<'r, 's, W> {
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

pub struct SerializeMapAsRecordOrMap<'r, 's, W> {
	inner: SerializeStructAsRecordOrMap<'r, 's, W>,
	key_location: Option<(usize, &'s SchemaNode<'s>)>,
}

impl<'r, 's, W: Write> SerializeMapAsRecordOrMap<'r, 's, W> {
	pub(super) fn record(state: &'r mut SerializerState<'s, W>, record: &'s Record<'s>) -> Self {
		Self {
			inner: SerializeStructAsRecordOrMap::record(state, record),
			key_location: None,
		}
	}
	pub(super) fn map(
		state: &'r mut SerializerState<'s, W>,
		elements_schema: &'s SchemaNode<'s>,
		min_len: usize,
	) -> Result<Self, SerError> {
		Ok(Self {
			inner: SerializeStructAsRecordOrMap::map(state, elements_schema, min_len)?,
			key_location: None,
		})
	}
}

impl<'r, 's, W: Write> SerializeMap for SerializeMapAsRecordOrMap<'r, 's, W> {
	type Ok = ();
	type Error = SerError;

	fn serialize_key<T: ?Sized>(&mut self, key: &T) -> Result<(), Self::Error>
	where
		T: Serialize,
	{
		match &mut self.inner.kind {
			Kind::Record { record_state, .. } => {
				let field_idx = key.serialize(FindFieldIndexSerializer { record_state })?;
				self.key_location = Some(field_idx);
				Ok(())
			}
			Kind::Map { block_writer, .. } => {
				block_writer.signal_next_record()?;
				key.serialize(DatumSerializer {
					state: block_writer.state,
					schema_node: &SchemaNode::String,
				})
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
			} => {
				let field_idx = self
					.key_location
					.take()
					.expect("serialize_key should have been called before serialize_value");
				serialize_record_value(serializer_state, record_state, field_idx, value)
			}
			Kind::Map {
				elements_schema,
				block_writer,
			} => value.serialize(DatumSerializer {
				state: block_writer.state,
				schema_node: *elements_schema,
			}),
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
				let field_idx = key.serialize(FindFieldIndexSerializer { record_state })?;
				serialize_record_value(serializer_state, record_state, field_idx, value)
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
