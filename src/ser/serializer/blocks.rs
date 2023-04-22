use super::*;

pub(super) struct BlockWriter<'r, 'c, 's, W> {
	pub(super) state: &'r mut SerializerState<'c, 's, W>,
	current_block_len: usize,
}

impl<'r, 'c, 's, W> BlockWriter<'r, 'c, 's, W>
where
	W: std::io::Write,
{
	pub(super) fn new(
		state: &'r mut SerializerState<'c, 's, W>,
		min_len: usize,
	) -> Result<Self, SerError> {
		if min_len > 0 {
			let len: i64 = min_len
				.try_into()
				.map_err(|_| SerError::new("Array or map len overflows i64"))?;
			state.writer.write_varint(len).map_err(SerError::io)?;
		}
		Ok(BlockWriter {
			state,
			current_block_len: min_len,
		})
	}
	pub(super) fn signal_next_record(&mut self) -> Result<(), SerError> {
		match self.current_block_len.checked_sub(1) {
			None => {
				self.state.writer.write_varint(1i32).map_err(SerError::io)?;
			}
			Some(new_block_len) => {
				self.current_block_len = new_block_len;
			}
		}
		Ok(())
	}
	/// Check that last block is complete and advertise end (zero-sized block)
	pub(super) fn end(self) -> Result<(), SerError> {
		// We advertise block len based on size provided by Serialize type
		// so when we are given less elements than that (which happens only if
		// the impl of `Serialize` for a given type does not respect the Serde
		// contract) the generated output has been invalid so we need to error
		if self.current_block_len != 0 {
			Err(SerError::new(
				"Got less fields in Map or Array than initially \
					advertised by `Serialize` implementor (check your `impl Serialize` types)",
			))
		} else {
			self.state.writer.write_varint(0i32).map_err(SerError::io)?;
			Ok(())
		}
	}
}
