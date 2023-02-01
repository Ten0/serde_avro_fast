//! Abstract reading from slices (propagating lifetime) or any other `impl Read` behind the same interface
//!
//! The deserializer is implemented generically on this.

use super::{DeError, Error};

use integer_encoding::{VarInt, VarIntReader};

/// Abstracts reading from slices (propagating lifetime) or any other `impl Read` behind the same interface
///
/// The deserializer is implemented generically on this.
pub trait Read: std::io::Read + Sized + private::Sealed {
	fn read_varint<I>(&mut self) -> Result<I, DeError>
	where
		I: VarInt,
	{
		<Self as VarIntReader>::read_varint(self).map_err(DeError::io)
	}
	fn read_const_size_buf<const N: usize>(&mut self) -> Result<[u8; N], DeError> {
		let mut buf = [0u8; N];
		self.read_exact(&mut buf).map_err(DeError::io)?;
		Ok(buf)
	}
}

/// Abstracts reading from slices (propagating lifetime) or any other `impl Read` behind the same interface
///
/// The deserializer is implemented generically on this.
pub trait ReadSlice<'de>: Read {
	fn read_slice<V>(&mut self, n: usize, read_visitor: V) -> Result<V::Value, DeError>
	where
		V: ReadVisitor<'de>;
}

mod private {
	pub trait Sealed {}
}

/// Implements `Read<'de>` reading from `&'de [u8]`
pub struct SliceRead<'de> {
	slice: &'de [u8],
}
impl<'de> SliceRead<'de> {
	pub fn new(slice: &'de [u8]) -> Self {
		Self { slice }
	}
}
impl private::Sealed for SliceRead<'_> {}
impl<'de> Read for SliceRead<'de> {
	fn read_varint<I>(&mut self) -> Result<I, DeError>
	where
		I: VarInt,
	{
		match I::decode_var(self.slice) {
			None => Err(DeError::new(
				"All bytes have MSB set when decoding varint (Reached EOF)",
			)),
			Some((val, read)) => {
				self.slice = &self.slice[read..];
				Ok(val)
			}
		}
	}
}
impl<'de> ReadSlice<'de> for SliceRead<'de> {
	fn read_slice<V>(&mut self, n: usize, visitor: V) -> Result<V::Value, DeError>
	where
		V: ReadVisitor<'de>,
	{
		if n > self.slice.len() {
			Err(DeError::unexpected_eof())
		} else {
			let (just_read, end) = self.slice.split_at(n);
			self.slice = end;
			visitor.visit_borrowed(just_read)
		}
	}
}
impl std::io::Read for SliceRead<'_> {
	fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
		self.slice.read(buf)
	}
	fn read_vectored(&mut self, bufs: &mut [std::io::IoSliceMut<'_>]) -> std::io::Result<usize> {
		self.slice.read_vectored(bufs)
	}
}
impl std::io::BufRead for SliceRead<'_> {
	fn fill_buf(&mut self) -> std::io::Result<&[u8]> {
		self.slice.fill_buf()
	}

	fn consume(&mut self, amt: usize) {
		self.slice.consume(amt)
	}
}

/// Implements `Read<'de>` reading from any `impl Read`
pub struct ReaderRead<R> {
	reader: R,
	scratch: Vec<u8>,
	/// This is a safeguard for malformed data
	max_alloc_size: usize,
}
impl<R: std::io::Read> private::Sealed for ReaderRead<R> {}
impl<R: std::io::Read> ReaderRead<R> {
	pub fn new(reader: R) -> Self {
		Self {
			reader,
			scratch: Vec::new(),
			max_alloc_size: 512 * 512 * 1024 * 1024,
		}
	}
}
impl<R> ReaderRead<R> {
	pub fn into_inner(self) -> R {
		self.reader
	}
}
impl<R: std::io::Read> Read for ReaderRead<R> {}
impl<'de, R: std::io::Read> ReadSlice<'de> for ReaderRead<R> {
	fn read_slice<V>(&mut self, n: usize, read_visitor: V) -> Result<V::Value, DeError>
	where
		V: ReadVisitor<'de>,
	{
		if n > self.max_alloc_size {
			return Err(DeError::custom(format_args!(
				"Allocation size that would be required ({n}) is larger than allowed for this \
					deserializer from reader ({}) - this is probably due to malformed data",
				self.max_alloc_size
			)));
		}
		if n > self.scratch.len() {
			self.scratch.resize(n, 0);
		}
		let scratch = &mut self.scratch[..n];
		self.reader.read_exact(scratch).map_err(DeError::io)?;
		read_visitor.visit(scratch)
	}
}
impl<R: std::io::Read> std::io::Read for ReaderRead<R> {
	fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
		self.reader.read(buf)
	}
	fn read_vectored(&mut self, bufs: &mut [std::io::IoSliceMut<'_>]) -> std::io::Result<usize> {
		self.reader.read_vectored(bufs)
	}
}
impl<R: std::io::BufRead> std::io::BufRead for ReaderRead<R> {
	fn fill_buf(&mut self) -> std::io::Result<&[u8]> {
		self.reader.fill_buf()
	}

	fn consume(&mut self, amt: usize) {
		self.reader.consume(amt)
	}
}

/// Largely internal trait for `Read` usage (probably don't use this directly)
pub trait ReadVisitor<'de>: Sized {
	type Value;
	fn visit(self, bytes: &[u8]) -> Result<Self::Value, DeError>;
	fn visit_borrowed(self, bytes: &'de [u8]) -> Result<Self::Value, DeError> {
		self.visit(bytes)
	}
}

impl<'de, F, V> ReadVisitor<'de> for F
where
	F: FnOnce(&[u8]) -> Result<V, DeError>,
{
	type Value = V;
	fn visit(self, bytes: &[u8]) -> Result<Self::Value, DeError> {
		self(bytes)
	}
}

/// Useful for reading object container file
pub trait Take {
	type Take: IntoLeftAfterTake<Original = Self>;
	fn take(self, block_size: usize) -> Result<Self::Take, DeError>;
}
pub trait IntoLeftAfterTake {
	type Original;
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
