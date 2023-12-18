//! Abstract reading from slices (propagating lifetime) or any other
//! `impl BufRead`/`impl Read` behind the same interface
//!
//! The deserializer is implemented generically on this.

pub mod take;

use super::{DeError, Error};

use integer_encoding::{VarInt, VarIntReader};

/// Abstracts reading from slices or any other `impl BufRead` behind the same
/// interface
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

/// Abstracts reading from slices (propagating lifetime) or any other `impl
/// Read` behind the same interface
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

/// Implements `Read<'de>` reading from any `impl BufRead`
pub struct ReaderRead<R> {
	reader: R,
	scratch: Vec<u8>,
	/// This is a safeguard for malformed data
	///
	/// See [`de`](crate::de) module documentation for an example
	pub max_alloc_size: usize,
}
impl<R: std::io::Read> private::Sealed for ReaderRead<R> {}
impl<R: std::io::BufRead> ReaderRead<R> {
	/// Construct a `ReaderRead` from an `impl BufRead`
	///
	/// If you only have an `impl Read`, wrap it in a
	/// [`BufReader`](std::io::BufReader) first.
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
impl<'de, R: std::io::BufRead> ReadSlice<'de> for ReaderRead<R> {
	fn read_slice<V>(&mut self, n: usize, read_visitor: V) -> Result<V::Value, DeError>
	where
		V: ReadVisitor<'de>,
	{
		let buffer = self.reader.fill_buf().map_err(DeError::io)?;
		match buffer.get(0..n) {
			Some(slice) => {
				let produced = read_visitor.visit(slice)?;
				self.reader.consume(n);
				Ok(produced)
			}
			None => {
				if n > self.max_alloc_size {
					return Err(DeError::custom(format_args!(
						"Allocation size that would be required ({n}) is larger than \
							allowed for this deserializer from reader ({}) - \
							this is probably due to malformed data",
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
///
/// This is what can be passed to [`ReadSlice::read_slice`] to obtain either
/// owned or borrowed values depending on whether we're reading from a slice or
/// an arbitrary impl `Read`.
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
