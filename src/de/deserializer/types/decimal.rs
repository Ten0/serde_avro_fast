use crate::schema::{Decimal, DecimalRepr};

use super::*;

use {
	rust_decimal::prelude::ToPrimitive as _,
	std::{io::Read, marker::PhantomData},
};

pub(in super::super) enum DecimalMode<'a> {
	Big,
	Regular(&'a Decimal),
}

pub(in super::super) fn read_decimal<'de, R, V>(
	state: &mut DeserializerState<R>,
	decimal_mode: DecimalMode<'_>,
	hint: VisitorHint,
	visitor: V,
) -> Result<V::Value, DeError>
where
	R: ReadSlice<'de>,
	V: Visitor<'de>,
{
	let (size, mut reader) = match decimal_mode {
		DecimalMode::Big => {
			// BigDecimal are represented as bytes, and inside the bytes contain a length
			// marker followed by the actual bytes, followed by another Long that represents
			// the scale.

			let bytes_len = state.read_varint::<i64>()?.try_into().map_err(|e| {
				DeError::custom(format_args!(
					"Invalid BigDecimal bytes length in stream: {e}"
				))
			})?;

			let mut reader = (&mut state.reader).take(bytes_len);

			// Read the unsized repr len
			let unsized_len = integer_encoding::VarIntReader::read_varint::<i64>(&mut reader)
				.map_err(DeError::io)?
				.try_into()
				.map_err(|e| {
					DeError::custom(format_args!("Invalid BigDecimal length in bytes: {e}"))
				})?;

			(unsized_len, ReaderEither::Take(reader))
		}
		DecimalMode::Regular(Decimal {
			repr: DecimalRepr::Bytes,
			..
		}) => (read_len(state)?, ReaderEither::Reader(&mut state.reader)),
		DecimalMode::Regular(Decimal {
			repr: DecimalRepr::Fixed(fixed),
			..
		}) => (fixed.size, ReaderEither::Reader(&mut state.reader)),
	};
	let mut buf = [0u8; 16];
	let start = buf.len().checked_sub(size).ok_or_else(|| {
		DeError::custom(format_args!(
			"Decimals of size larger than 16 are not supported (got size {size})"
		))
	})?;
	reader.read_exact(&mut buf[start..]).map_err(DeError::io)?;
	if buf.get(start).map_or(false, |&v| v & 0x80 != 0) {
		// This is a negative number in CA2 repr, we need to maintain that for the
		// larger number
		for i in 0..start {
			buf[i] = 0xFF;
		}
	}
	let unscaled = i128::from_be_bytes(buf);
	let scale = match decimal_mode {
		DecimalMode::Big => integer_encoding::VarIntReader::read_varint::<i64>(&mut reader)
			.map_err(DeError::io)?
			.try_into()
			.map_err(|e| {
				DeError::custom(format_args!("Invalid BigDecimal scale in stream: {e}"))
			})?,
		DecimalMode::Regular(Decimal { scale, .. }) => *scale,
	};
	match reader {
		ReaderEither::Take(take) => {
			if take.limit() > 0 {
				// This would be incorrect if we don't skip the extra bytes
				// in the original reader.
				// Arguably we could just ignore the extra bytes, but until proven
				// that this is a real use-case we'll just do the conservative thing
				// and encourage people to use the appropriate number of bytes.
				return Err(DeError::new(
					"BigDecimal scale is not at the end of the bytes",
				));
			}
		}
		ReaderEither::Reader(_) => {}
	}
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

enum ReaderEither<'a, R> {
	Reader(&'a mut R),
	Take(std::io::Take<&'a mut R>),
}
impl<R: Read> Read for ReaderEither<'_, R> {
	fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
		match self {
			ReaderEither::Reader(reader) => reader.read(buf),
			ReaderEither::Take(reader) => reader.read(buf),
		}
	}
}
