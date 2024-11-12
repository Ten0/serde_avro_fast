use std::io::{Error, ErrorKind, IoSlice, Result, Write};

pub(super) fn write_all_vectored<'a, W: Write, const N: usize>(
	writer: &mut W,
	slices: [&'a [u8]; N],
) -> Result<()> {
	let mut bufs = slices.map(IoSlice::new);
	write_all_vectored_inner(writer, &mut bufs)
}

/// Taken from std before stabilization
/// https://github.com/rust-lang/rust/issues/70436
///
/// One less level of generics compared to the function above
fn write_all_vectored_inner<'a, W: Write>(
	writer: &mut W,
	mut bufs: &mut [IoSlice<'a>],
) -> Result<()> {
	// Guarantee that bufs is empty if it contains no data,
	// to avoid calling write_vectored if there is no data to be written.
	IoSlice::advance_slices(&mut bufs, 0);
	while !bufs.is_empty() {
		match writer.write_vectored(bufs) {
			Ok(0) => {
				return Err(Error::new(
					ErrorKind::WriteZero,
					"failed to write whole buffer",
				));
			}
			Ok(n) => IoSlice::advance_slices(&mut bufs, n),
			Err(ref e) if e.kind() == ErrorKind::Interrupted => {}
			Err(e) => return Err(e),
		}
	}
	Ok(())
}
