use super::*;

pub(super) fn serialize<'r, 'c, 's, W>(
	state: &'r mut SerializerState<'c, 's, W>,
	decimal: &'s Decimal,
	mut rust_decimal: rust_decimal::Decimal,
) -> Result<(), SerError>
where
	W: Write,
{
	// Try to scale it appropriately
	rust_decimal.rescale(decimal.scale);
	if rust_decimal.scale() != decimal.scale {
		return Err(SerError::new(
			"Decimal number cannot be scaled to fit in schema scale \
				with a 96 bit mantissa (number or scale too large)",
		));
	}
	let buf: [u8; 16] = rust_decimal.mantissa().to_be_bytes();
	let start = match decimal.repr {
		DecimalRepr::Bytes => {
			// If it's a negative number we can ignore all 0xff followed by MSB
			// at 1 If it's a positive number we can ignore all 0x00
			let mut start = 0;
			if buf[0] & 0x80 == 0 {
				// Positive number
				while buf.get(start).map_or(false, |&v| v == 0x00) {
					start += 1;
				}
				// In case some other deserializers explode when giving empty bytes to
				// represent zero we'll play it safe and still serialize it as a
				// single byte with zeroes
				if start == buf.len() {
					start -= 1;
				}
			} else {
				// Negative number
				while buf.get(start).map_or(false, |&v| v == 0xFF) {
					start += 1;
				}
				if start != 0 && buf.get(start).map_or(true, |&v| v & 0x80 == 0) {
					start -= 1;
				}
			}
			let len = (buf.len() - start) as i32;
			state
				.writer
				.write_varint::<i32>(len)
				.map_err(SerError::io)?;
			start
		}
		DecimalRepr::Fixed(fixed) => {
			let size = fixed.size;
			match buf.len().checked_sub(size) {
				Some(start) => start,
				None => {
					let byte: u8 = if buf[0] & 0x80 == 0 { 0x00 } else { 0xFF };
					for _ in buf.len()..size {
						state.writer.write_all(&[byte]).map_err(SerError::io)?;
					}
					0
				}
			}
		}
	};
	state.writer.write_all(&buf[start..]).map_err(SerError::io)
}
