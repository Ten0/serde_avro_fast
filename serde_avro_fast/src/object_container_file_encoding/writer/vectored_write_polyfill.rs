use std::io::{Error, ErrorKind, IoSlice, Result, Write};

pub(super) fn write_all_vectored<'a, W: Write, const N: usize>(
	writer: &mut W,
	mut slices: [&'a [u8]; N],
) -> Result<()> {
	let mut bufs = slices.map(IoSlice::new);
	write_all_vectored_inner(writer, &mut slices, &mut bufs)
}

/// Taken from std before stabilization
/// https://github.com/rust-lang/rust/issues/70436
fn write_all_vectored_inner<'a, W: Write>(
	writer: &mut W,
	mut slices: &mut [&'a [u8]],
	mut bufs: &mut [IoSlice<'a>],
) -> Result<()> {
	// Guarantee that bufs is empty if it contains no data,
	// to avoid calling write_vectored if there is no data to be written.
	advance_slices(&mut slices, &mut bufs, 0);
	while !bufs.is_empty() {
		match writer.write_vectored(bufs) {
			Ok(0) => {
				return Err(Error::new(
					ErrorKind::WriteZero,
					"failed to write whole buffer",
				));
			}
			Ok(n) => advance_slices(&mut slices, &mut bufs, n),
			Err(ref e) if e.kind() == ErrorKind::Interrupted => {}
			Err(e) => return Err(e),
		}
	}
	Ok(())
}

/// ~Taken from std before stabilization
/// https://github.com/rust-lang/rust/issues/62726
///
/// We need to pass both slices and bufs because otherwise we have no way to
/// advance an IoSlice, so we just re-construct it from the original slice
/// https://github.com/rust-lang/rust/issues/62726#issuecomment-542826827
///
/// This has been FCPd recently though so it shouldn't take long to stabilize.
fn advance_slices<'a>(slices: &mut &mut [&'a [u8]], bufs: &mut &mut [IoSlice<'a>], n: usize) {
	assert_eq!(slices.len(), bufs.len());
	// Number of buffers to remove.
	let mut remove = 0;
	// Remaining length before reaching n. This prevents overflow
	// that could happen if the length of slices in `bufs` were instead
	// accumulated. Those slice may be aliased and, if they are large
	// enough, their added length may overflow a `usize`.
	let mut left = n;
	for slice in slices.iter() {
		if let Some(remainder) = left.checked_sub(slice.len()) {
			left = remainder;
			remove += 1;
		} else {
			break;
		}
	}

	*slices = &mut std::mem::take(slices)[remove..];
	*bufs = &mut std::mem::take(bufs)[remove..];
	if slices.is_empty() {
		assert!(left == 0, "advancing io slices beyond their length");
	} else {
		// Edited from std's implementation because they don't make `IoSlice::advance`
		// available
		let first = &mut slices[0];
		let new_slice = &(*first)[left..];
		*first = new_slice;
		bufs[0] = IoSlice::new(new_slice);
	}
}
