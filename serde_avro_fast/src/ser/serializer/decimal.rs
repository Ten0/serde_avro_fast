use super::*;

pub(in super::super) enum DecimalMode<'a> {
	Big,
	Regular(&'a Decimal),
}

pub(super) fn serialize<'r, 'c, 's, W>(
	state: &'r mut SerializerState<'c, 's, W>,
	decimal_mode: DecimalMode<'s>,
	mut rust_decimal: rust_decimal::Decimal,
) -> Result<(), SerError>
where
	W: Write,
{
	let mut scale_buf = [0; 10];
	let scale_to_write = match decimal_mode {
		DecimalMode::Regular(decimal) => {
			// Try to scale it appropriately
			rust_decimal.rescale(decimal.scale);
			if rust_decimal.scale() != decimal.scale {
				return Err(SerError::new(
					"Decimal number cannot be scaled to fit in schema scale \
				with a 96 bit mantissa (number or scale too large)",
				));
			}
			&[]
		}
		DecimalMode::Big => {
			let scale: i64 = rust_decimal.scale().into();
			let n = <i64 as integer_encoding::VarInt>::encode_var(scale, &mut scale_buf);
			&scale_buf[0..n]
		}
	};
	let buf: [u8; 16] = rust_decimal.mantissa().to_be_bytes();
	#[inline]
	fn can_truncate_without_altering_number(buf: &[u8]) -> usize {
		// If it's a negative number we can ignore all 0xff followed by MSB
		// at 1 If it's a positive number we can ignore all 0x00 followed by MSB at 0
		let mut can_truncate = 0;
		if buf[0] & 0x80 == 0 {
			// Positive number
			while buf.get(can_truncate).map_or(false, |&v| v == 0x00) {
				can_truncate += 1;
			}
			// In case some other deserializers explode when giving empty bytes to
			// represent zero we'll play it safe and still serialize it as a
			// single byte with zeroes
			if can_truncate != 0 && buf.get(can_truncate).map_or(true, |&v| v & 0x80 != 0) {
				can_truncate -= 1;
			}
		} else {
			// Negative number
			while buf.get(can_truncate).map_or(false, |&v| v == 0xFF) {
				can_truncate += 1;
			}
			if can_truncate != 0 && buf.get(can_truncate).map_or(true, |&v| v & 0x80 == 0) {
				can_truncate -= 1;
			}
		}
		can_truncate
	}
	let start = match decimal_mode {
		DecimalMode::Big
		| DecimalMode::Regular(Decimal {
			repr: DecimalRepr::Bytes,
			..
		}) => {
			// If it's a negative number we can ignore all 0xff followed by MSB
			// at 1 If it's a positive number we can ignore all 0x00 followed by MSB at 0
			let start = can_truncate_without_altering_number(&buf);
			let len = (buf.len() - start) as i32;
			match decimal_mode {
				DecimalMode::Big => {
					// We need to write the length of the full bytes, then write
					// the length of the unscaled
					assert!(!scale_to_write.is_empty());
					let mut len_buf = [0; 10];
					let len_len = <i32 as integer_encoding::VarInt>::encode_var(len, &mut len_buf);
					state
						.writer
						.write_varint::<i32>(len_len as i32 + len + scale_to_write.len() as i32)
						.map_err(SerError::io)?;
					state
						.writer
						.write_all(&len_buf[0..len_len])
						.map_err(SerError::io)?;
				}
				DecimalMode::Regular(Decimal {
					repr: DecimalRepr::Bytes,
					..
				}) => {
					// We need to write the length of the bytes
					state
						.writer
						.write_varint::<i32>(len)
						.map_err(SerError::io)?;
				}
				DecimalMode::Regular(Decimal {
					repr: DecimalRepr::Fixed(_),
					..
				}) => unreachable!(),
			}
			start
		}
		DecimalMode::Regular(Decimal {
			repr: DecimalRepr::Fixed(fixed),
			..
		}) => {
			let size = fixed.size;
			match buf.len().checked_sub(size) {
				Some(start) => {
					// We are going to truncate the number - make sure that doesn't alter it
					match buf.get(0..start + 1) {
						Some(relevant_buf_for_check) => {
							let can_truncate =
								can_truncate_without_altering_number(relevant_buf_for_check);
							if can_truncate < start {
								return Err(SerError::custom(format_args!(
									"Decimal number does not fit in `fixed` field size \
										(fixed size: {size}, required: {})",
									size + (start - can_truncate)
								)));
							}
						}
						None => {
							assert!(size == 0);
							// We only know how to represent 0 in this case (empty bytes)
							if !rust_decimal.is_zero() {
								return Err(SerError::new(
									"Non-zero decimal number can not be serialized \
										as a fixed size decimal with size 0",
								));
							}
						}
					}
					start
				}
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
	state
		.writer
		.write_all(&buf[start..])
		.map_err(SerError::io)?;
	if !scale_to_write.is_empty() {
		state
			.writer
			.write_all(&scale_to_write)
			.map_err(SerError::io)?;
	}
	Ok(())
}
