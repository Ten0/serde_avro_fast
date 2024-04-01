use super::*;

pub struct SerializeSeqOrTupleOrTupleStruct<'r, 'c, 's, W> {
	kind: Kind<'r, 'c, 's, W>,
}

enum Kind<'r, 'c, 's, W> {
	Array {
		block_writer: BlockWriter<'r, 'c, 's, W>,
		elements_schema: &'s SchemaNode<'s>,
	},
	Duration {
		serializer_state: &'r mut SerializerState<'c, 's, W>,
		n_values: u8,
	},
	BufferedBytes {
		serializer_state: &'r mut SerializerState<'c, 's, W>,
		buffer: Vec<u8>,
	},
	Fixed {
		serializer_state: &'r mut SerializerState<'c, 's, W>,
		expected_len: usize,
	},
	Finished,
}

impl<'r, 'c, 's, W: Write> SerializeSeqOrTupleOrTupleStruct<'r, 'c, 's, W> {
	pub(super) fn array(
		block_writer: BlockWriter<'r, 'c, 's, W>,
		elements_schema: &'s SchemaNode<'s>,
	) -> Self {
		Self {
			kind: Kind::Array {
				block_writer,
				elements_schema,
			},
		}
	}

	pub(super) fn duration(serializer_state: &'r mut SerializerState<'c, 's, W>) -> Self {
		Self {
			kind: Kind::Duration {
				serializer_state,
				n_values: 0,
			},
		}
	}

	pub(crate) fn buffered_bytes(state: &'r mut SerializerState<'c, 's, W>) -> Self {
		Self {
			kind: Kind::BufferedBytes {
				buffer: state
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
				serializer_state: state,
			},
		}
	}

	pub(crate) fn bytes(
		state: &'r mut SerializerState<'c, 's, W>,
		len: usize,
	) -> Result<Self, SerError> {
		state
			.writer
			.write_varint::<i64>(len.try_into().map_err(|_| {
				SerError::new(
					"Buffer len does not fit i64 for encoding as length-delimited field size",
				)
			})?)
			.map_err(SerError::io)?;
		Ok(Self::fixed(state, len))
	}

	pub(crate) fn fixed(state: &'r mut SerializerState<'c, 's, W>, expected_len: usize) -> Self {
		Self {
			kind: Kind::Fixed {
				serializer_state: state,
				expected_len,
			},
		}
	}

	fn serialize_element<T: ?Sized>(&mut self, value: &T) -> Result<(), SerError>
	where
		T: Serialize,
	{
		match self.kind {
			Kind::Array {
				ref mut block_writer,
				elements_schema,
			} => {
				block_writer.signal_next_record()?;
				value.serialize(DatumSerializer {
					state: block_writer.state,
					schema_node: elements_schema,
				})
			}
			Kind::Duration {
				ref mut serializer_state,
				ref mut n_values,
			} => {
				if *n_values >= 3 {
					Err(duration_seq_len_incorrect())
				} else {
					let val =
						value.serialize(super::extract_for_duration::ExtractU32ForDuration)?;
					serializer_state
						.writer
						.write_all(&val.to_le_bytes())
						.map_err(SerError::io)?;
					*n_values += 1;
					Ok(())
				}
			}
			Kind::BufferedBytes { ref mut buffer, .. } => {
				buffer.push(value.serialize(ExtractU8Serializer)?);
				Ok(())
			}
			Kind::Fixed {
				ref mut serializer_state,
				ref mut expected_len,
			} => {
				match (*expected_len).checked_sub(1) {
					Some(n) => *expected_len = n,
					None => {
						return Err(SerError::new(
							"Advertised/Fixed length exceeded for serialization as Fixed/Bytes",
						))
					}
				}
				serializer_state
					.writer
					.write_all(&[value.serialize(ExtractU8Serializer)?])
					.map_err(SerError::io)?;

				Ok(())
			}
			Kind::Finished => Err(should_not_be_finished()),
		}
	}

	fn end(mut self) -> Result<(), SerError> {
		match self.kind {
			Kind::Array { .. } => match std::mem::replace(&mut self.kind, Kind::Finished) {
				Kind::Array { block_writer, .. } => block_writer.end(),
				_ => unreachable!(),
			},
			Kind::Duration { n_values, .. } => {
				if n_values != 3 {
					Err(duration_seq_len_incorrect())
				} else {
					Ok(())
				}
			}
			Kind::BufferedBytes {
				ref mut serializer_state,
				ref buffer,
			} => serializer_state.write_length_delimited(buffer),
			Kind::Fixed { expected_len, .. } => {
				if expected_len != 0 {
					Err(SerError::new(
						"Advertised/Fixed length not reached for serialization as Fixed/Bytes",
					))
				} else {
					Ok(())
				}
			}
			Kind::Finished => Err(should_not_be_finished()),
		}
	}
}

impl<W> Drop for SerializeSeqOrTupleOrTupleStruct<'_, '_, '_, W> {
	fn drop(&mut self) {
		if let Kind::BufferedBytes {
			serializer_state,
			mut buffer,
		} = std::mem::replace(&mut self.kind, Kind::Finished)
		{
			if buffer.capacity() > 0 {
				buffer.clear();
				serializer_state
					.config
					.buffers
					.field_reordering_buffers
					.push(buffer);
			}
		}
	}
}

pub(super) fn duration_seq_len_incorrect() -> SerError {
	SerError::new("seq/tuple can indeed be serialized as Duration, but only if it's of length 3")
}

fn should_not_be_finished() -> SerError {
	SerError::new(
		"Internal serializer error: should not have state \
				Finished before `end()` is called",
	)
}

macro_rules! impl_serialize_seq_or_tuple {
	($($trait_: ident $f: ident,)+) => {
		$(
			impl<'r, 'c, 's, W: Write> $trait_ for SerializeSeqOrTupleOrTupleStruct<'r, 'c, 's, W> {
				type Ok = ();
				type Error = SerError;

				fn $f<T: ?Sized>(&mut self, value: &T) -> Result<(), Self::Error>
				where
					T: Serialize,
				{
					self.serialize_element(value)
				}

				fn end(self) -> Result<Self::Ok, Self::Error> {
					self.end()
				}
			}
		)*
	};
}
impl_serialize_seq_or_tuple! {
	SerializeSeq serialize_element,
	SerializeTuple serialize_element,
	SerializeTupleStruct serialize_field,
	SerializeTupleVariant serialize_field,
}

struct ExtractU8Serializer;
impl Serializer for ExtractU8Serializer {
	type Ok = u8;
	type Error = SerError;

	fn serialize_i8(self, v: i8) -> Result<Self::Ok, Self::Error> {
		v.try_into().map_err(|_| {
			SerError::new(
				"Out of bounds i8->u8 element for sequence\
					serialization as Fixed/Bytes",
			)
		})
	}

	fn serialize_i16(self, v: i16) -> Result<Self::Ok, Self::Error> {
		v.try_into().map_err(|_| {
			SerError::new(
				"Out of bounds i16->u8 element for sequence\
					serialization as Fixed/Bytes",
			)
		})
	}

	fn serialize_i32(self, v: i32) -> Result<Self::Ok, Self::Error> {
		v.try_into().map_err(|_| {
			SerError::new(
				"Out of bounds i32->u8 element for sequence\
					serialization as Fixed/Bytes",
			)
		})
	}

	fn serialize_i64(self, v: i64) -> Result<Self::Ok, Self::Error> {
		v.try_into().map_err(|_| {
			SerError::new(
				"Out of bounds i64->u8 element for sequence\
					serialization as Fixed/Bytes",
			)
		})
	}

	fn serialize_u8(self, v: u8) -> Result<Self::Ok, Self::Error> {
		Ok(v)
	}

	fn serialize_u16(self, v: u16) -> Result<Self::Ok, Self::Error> {
		v.try_into().map_err(|_| {
			SerError::new(
				"Out of bounds u16->u8 element for sequence\
					serialization as Fixed/Bytes",
			)
		})
	}

	fn serialize_u32(self, v: u32) -> Result<Self::Ok, Self::Error> {
		v.try_into().map_err(|_| {
			SerError::new(
				"Out of bounds u32->u8 element for sequence\
					serialization as Fixed/Bytes",
			)
		})
	}

	fn serialize_u64(self, v: u64) -> Result<Self::Ok, Self::Error> {
		v.try_into().map_err(|_| {
			SerError::new(
				"Out of bounds u64->u8 element for sequence\
					serialization as Fixed/Bytes",
			)
		})
	}

	serde::serde_if_integer128! {
		fn serialize_i128(self, v: i128) -> Result<Self::Ok, Self::Error> {
			v.try_into().map_err(|_| {
				SerError::new(
					"Out of bounds i128->u8 element for sequence\
						serialization as Fixed/Bytes",
				)
			})
		}

		fn serialize_u128(self, v: u128) -> Result<Self::Ok, Self::Error> {
			v.try_into().map_err(|_| {
				SerError::new(
					"Out of bounds u128->u8 element for sequence\
						serialization as Fixed/Bytes",
				)
			})
		}
	}

	serde_serializer_quick_unsupported::serializer_unsupported! {
		err = (SerError::new("Elements should be u8-like for serialization as Fixed/Bytes"));
		bool f32 f64 char str bytes none some unit unit_struct
		unit_variant newtype_struct newtype_variant seq tuple tuple_struct tuple_variant map struct
		struct_variant
	}
}
