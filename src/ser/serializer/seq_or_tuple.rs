use super::*;

pub struct SerializeAsArrayOrDuration<'r, 's, W> {
	kind: Kind<'r, 's, W>,
}

enum Kind<'r, 's, W> {
	Array {
		block_writer: BlockWriter<'r, 's, W>,
		elements_schema: &'s SchemaNode<'s>,
	},
	Duration {
		serializer_state: &'r mut SerializerState<'s, W>,
		n_values: u8,
	},
}

impl<'r, 's, W: Write> SerializeAsArrayOrDuration<'r, 's, W> {
	pub(super) fn array(
		block_writer: BlockWriter<'r, 's, W>,
		elements_schema: &'s SchemaNode<'s>,
	) -> Self {
		Self {
			kind: Kind::Array {
				block_writer,
				elements_schema,
			},
		}
	}

	pub(super) fn duration(serializer_state: &'r mut SerializerState<'s, W>) -> Self {
		Self {
			kind: Kind::Duration {
				serializer_state,
				n_values: 0,
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
		}
	}

	fn end(self) -> Result<(), SerError> {
		match self.kind {
			Kind::Array { block_writer, .. } => block_writer.end(),
			Kind::Duration { n_values, .. } => {
				if n_values != 3 {
					Err(duration_seq_len_incorrect())
				} else {
					Ok(())
				}
			}
		}
	}
}

pub(super) fn duration_seq_len_incorrect() -> SerError {
	SerError::new("seq/tuple can indeed be serialized as Duration, but only if it's of length 3")
}

macro_rules! impl_serialize_seq_or_tuple {
	($($trait_: ident $f: ident,)+) => {
		$(
			impl<'r, 's, W: Write> $trait_ for SerializeAsArrayOrDuration<'r, 's, W> {
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
