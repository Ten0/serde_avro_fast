use super::*;

use {core::marker::PhantomData, rust_decimal::prelude::ToPrimitive as _};

pub(in super::super) enum DecimalMode<'a> {
	Big,
	Regular(&'a Decimal),
}

pub(in super::super) fn read_decimal<'de, R, V>(
	state: &mut DeserializerState<'_, R>,
	decimal_mode: DecimalMode<'_>,
	hint: VisitorHint,
	visitor: V,
) -> Result<V::Value, DeError>
where
	R: ReadSlice<'de>,
	V: Visitor<'de>,
{
	let (size, scale_reader) = match decimal_mode {
		DecimalMode::Big => {
			// BigDecimal are represented as bytes, and inside the bytes contain a length
			// marker followed by the actual bytes, followed by another Long that represents
			// the scale.

			let bytes_len: usize = state
				.read_varint::<i64>()?
				.try_into()
				.map_err(|e| DeError::custom(format_args!("Invalid BigDecimal bytes length: {e}")))?;

			// Read the unsized repr len
			let unsized_len: i64 = state.read_varint()?;
			let unsized_len: usize = unsized_len.try_into().map_err(|e| {
				DeError::custom(format_args!("Invalid BigDecimal length in bytes: {e}"))
			})?;

			// Calculate the number of bytes read for varint encoding
			let varint_bytes_used = {
				let mut buf = [0u8; 10];
				integer_encoding::VarInt::encode_var(unsized_len as i64, &mut buf)
			};

			// The scale will need to be read after reading the unscaled value
			let remaining_for_scale = bytes_len
				.checked_sub(varint_bytes_used)
				.and_then(|v| v.checked_sub(unsized_len))
				.ok_or_else(|| DeError::new("Invalid BigDecimal structure"))?;

			(unsized_len, ScaleReader::Big { remaining_for_scale })
		}
		DecimalMode::Regular(Decimal {
			repr: DecimalRepr::Bytes,
			scale,
			..
		}) => (read_len(state)?, ScaleReader::Regular(*scale)),
		DecimalMode::Regular(Decimal {
			repr: DecimalRepr::Fixed(fixed),
			scale,
			..
		}) => (fixed.size, ScaleReader::Regular(*scale)),
	};
	let mut buf = [0u8; 16];
	let start = buf.len().checked_sub(size).ok_or_else(|| {
		DeError::custom(format_args!(
			"Decimals of size larger than 16 are not supported (got size {size})"
		))
	})?;

	// Read the decimal bytes
	state.read_slice(size, |bytes: &[u8]| {
		buf[start..].copy_from_slice(bytes);
		Ok(())
	})?;

	if buf.get(start).map_or(false, |&v| v & 0x80 != 0) {
		// This is a negative number in CA2 repr, we need to maintain that for the
		// larger number
		for v in &mut buf[0..start] {
			*v = 0xFF;
		}
	}
	let unscaled = i128::from_be_bytes(buf);
	let scale = match scale_reader {
		ScaleReader::Big { remaining_for_scale } => {
			let scale: i64 = state.read_varint()?;
			// Calculate bytes used for scale varint
			let scale_varint_bytes = {
				let mut buf = [0u8; 10];
				integer_encoding::VarInt::encode_var(scale, &mut buf)
			};
			if scale_varint_bytes != remaining_for_scale {
				return Err(DeError::new(
					"BigDecimal scale is not at the end of the bytes",
				));
			}
			scale.try_into().map_err(|e| {
				DeError::custom(format_args!("Invalid BigDecimal scale in stream: {e}"))
			})?
		}
		ScaleReader::Regular(scale) => scale,
	};

	if scale == 0 {
		match hint {
			VisitorHint::U64 => {
				if let Ok(v) = unscaled.try_into() {
					return visitor.visit_u64(v);
				} else if unscaled < 0 {
					return visitor.visit_i128(unscaled);
				}
			}
			VisitorHint::I64 => {
				return if let Ok(v) = unscaled.try_into() {
					visitor.visit_i64(v)
				} else {
					visitor.visit_i128(unscaled)
				};
			}
			VisitorHint::U128 => {
				return if let Ok(v) = unscaled.try_into() {
					visitor.visit_u128(v)
				} else {
					visitor.visit_i128(unscaled)
				};
			}
			VisitorHint::I128 => {
				return visitor.visit_i128(unscaled);
			}
			VisitorHint::Str | VisitorHint::F64 => {}
		}
	}
	let decimal = rust_decimal::Decimal::try_from_i128_with_scale(unscaled, scale)
		.map_err(|e| DeError::custom(format_args!("Could not parse decimal from i128: {e}")))?;
	if hint == VisitorHint::F64 {
		if let Some(float) = decimal.to_f64() {
			return visitor.visit_f64(float);
		}
	}
	serde::Serialize::serialize(
		&decimal,
		SerializeToVisitorStr {
			visitor,
			_lifetime: PhantomData,
		},
	)
}

enum ScaleReader {
	Big { remaining_for_scale: usize },
	Regular(u32),
}

#[derive(PartialEq, Eq)]
pub(in super::super) enum VisitorHint {
	Str,
	U64,
	I64,
	U128,
	I128,
	F64,
}

/// We're using this struct because when serializing decimal uses a private API
/// that does not allocate so we can benefit from that by providing it with a
/// serializer that actually just visits the `Visitor` provided by the the
/// original `Deserialize` impl
struct SerializeToVisitorStr<'de, V> {
	visitor: V,
	_lifetime: PhantomData<&'de ()>,
}

impl<'de, V: Visitor<'de>> serde::Serializer for SerializeToVisitorStr<'de, V> {
	type Ok = V::Value;
	type Error = DeError;

	fn serialize_str(self, v: &str) -> Result<Self::Ok, Self::Error> {
		self.visitor.visit_str(v)
	}

	serde_serializer_quick_unsupported::serializer_unsupported! {
		err = (DeError::new("rust_decimal::Decimal should only serialize as str"));
		bool i8 i16 i32 i64 u8 u16 u32 u64 f32 f64 char bytes none some unit unit_struct
		unit_variant newtype_struct newtype_variant seq tuple tuple_struct tuple_variant map struct
		struct_variant i128 u128
	}
}
