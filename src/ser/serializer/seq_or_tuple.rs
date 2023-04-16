use super::*;

pub struct SerializeAsArray<'r, 's, W> {
	pub(super) block_writer: BlockWriter<'r, 's, W>,
	pub(super) elements_schema: &'s SchemaNode<'s>,
}

impl<'r, 's, W: Write> SerializeAsArray<'r, 's, W> {
	fn serialize_element<T: ?Sized>(&mut self, value: &T) -> Result<(), SerError>
	where
		T: Serialize,
	{
		self.block_writer.signal_next_record()?;
		value.serialize(DatumSerializer {
			state: self.block_writer.state,
			schema_node: self.elements_schema,
		})
	}
}

macro_rules! impl_serialize_seq_or_tuple {
	($($trait_: ident $f: ident,)+) => {
		$(
			impl<'r, 's, W: Write> $trait_ for SerializeAsArray<'r, 's, W> {
				type Ok = ();
				type Error = SerError;

				fn $f<T: ?Sized>(&mut self, value: &T) -> Result<(), Self::Error>
				where
					T: Serialize,
				{
					self.serialize_element(value)
				}

				fn end(self) -> Result<Self::Ok, Self::Error> {
					self.block_writer.end()
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
