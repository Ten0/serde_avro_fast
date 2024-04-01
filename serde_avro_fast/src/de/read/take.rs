//! Largely internal traits and structs for `object_container_file`
//! implementation (probably don't use this directly - but you may need what's
//! in there to write trait bounds)

use super::*;

/// Largely internal trait for `object_container_file` implementation
/// (probably don't use this directly - but you may need it to write trait
/// bounds)
///
/// Represents the ability to turn `Self` into a reader that we can only read
/// `block_size` bytes from, then turn the resulting reader back into `Self`.
pub trait Take {
	/// The reader that we can only read `block_size` bytes from, then can also
	/// turn back into `Self`
	type Take: IntoLeftAfterTake<Original = Self> + std::io::BufRead;
	/// Take `block_size` bytes from `self`, returning a reader that can only
	/// read those bytes.
	fn take(self, block_size: usize) -> Result<Self::Take, DeError>;
}
/// Largely internal trait for `object_container_file` implementation
/// (probably don't use this directly - but you may need it to write trait
/// bounds)
pub trait IntoLeftAfterTake {
	/// The original reader that we took `block_size` bytes from
	type Original;
	/// Check that we have consumed everything we should have (`block_size`)
	/// from the `take`n reader, then turn it back into the original reader
	fn into_left_after_take(self) -> Result<Self::Original, DeError>;
}

impl<'de> Take for SliceRead<'de> {
	type Take = SliceReadTake<'de>;
	fn take(self, block_size: usize) -> Result<Self::Take, DeError> {
		if block_size > self.slice.len() {
			return Err(DeError::new("Read block size larger than original slice"));
		}
		let (start, end) = self.slice.split_at(block_size);
		Ok(SliceReadTake {
			inner_slice_read: SliceRead::new(start),
			left_after_take: SliceRead::new(end),
		})
	}
}
/// Largely internal struct for `object_container_file` implementation
/// (probably don't use this directly)
///
/// This is `<SliceRead as Take>::Output`
pub struct SliceReadTake<'de> {
	inner_slice_read: SliceRead<'de>,
	left_after_take: SliceRead<'de>,
}
impl private::Sealed for SliceReadTake<'_> {}
impl Read for SliceReadTake<'_> {
	fn read_varint<I>(&mut self) -> Result<I, DeError>
	where
		I: VarInt,
	{
		<SliceRead<'_> as Read>::read_varint(&mut self.inner_slice_read)
	}

	fn read_const_size_buf<const N: usize>(&mut self) -> Result<[u8; N], DeError> {
		self.inner_slice_read.read_const_size_buf()
	}
}
impl<'de> ReadSlice<'de> for SliceReadTake<'de> {
	fn read_slice<V>(&mut self, n: usize, read_visitor: V) -> Result<V::Value, DeError>
	where
		V: ReadVisitor<'de>,
	{
		self.inner_slice_read.read_slice(n, read_visitor)
	}
}
impl std::io::Read for SliceReadTake<'_> {
	fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
		self.inner_slice_read.read(buf)
	}
	fn read_vectored(&mut self, bufs: &mut [std::io::IoSliceMut<'_>]) -> std::io::Result<usize> {
		self.inner_slice_read.read_vectored(bufs)
	}
}
impl std::io::BufRead for SliceReadTake<'_> {
	fn fill_buf(&mut self) -> std::io::Result<&[u8]> {
		self.inner_slice_read.fill_buf()
	}

	fn consume(&mut self, amt: usize) {
		self.inner_slice_read.consume(amt)
	}
}

impl<'de> IntoLeftAfterTake for SliceReadTake<'de> {
	type Original = SliceRead<'de>;
	fn into_left_after_take(self) -> Result<Self::Original, DeError> {
		if !self.inner_slice_read.slice.is_empty() {
			return Err(DeError::new(
				"There's data left in the block after deserializing it entirely",
			));
		}
		Ok(self.left_after_take)
	}
}

impl<R: std::io::BufRead> Take for ReaderRead<R> {
	type Take = ReaderRead<std::io::Take<R>>;
	fn take(self, block_size: usize) -> Result<Self::Take, DeError> {
		let block_size: u64 = block_size
			.try_into()
			.map_err(|_| DeError::new("Invalid container file block size in bytes"))?;
		Ok(ReaderRead {
			reader: self.reader.take(block_size),
			scratch: self.scratch,
			max_alloc_size: self.max_alloc_size,
		})
	}
}
impl<R: std::io::BufRead> IntoLeftAfterTake for ReaderRead<std::io::Take<R>> {
	type Original = ReaderRead<R>;
	fn into_left_after_take(self) -> Result<Self::Original, DeError> {
		let left_to_consume = self.reader.limit();
		if left_to_consume > 0 {
			return Err(DeError::new(
				"There's data left in the block after deserializing it entirely",
			));
		}
		let reader = self.reader.into_inner();
		// The following would consume everything that hasn't been read in this block
		// but given the current interface it shouldn't ever happen
		/*
		while left_to_consume != 0 {
			let s = reader.fill_buf().map_err(DeError::io)?;
			let to_consume_this_time = left_to_consume.min(s.len().try_into().expect("Buffer len does not fit in u64"));
			reader.consume(to_consume_this_time as usize);
			left_to_consume -= to_consume_this_time;
		}
		*/
		Ok(ReaderRead {
			reader,
			scratch: self.scratch,
			max_alloc_size: self.max_alloc_size,
		})
	}
}
