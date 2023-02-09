use crate::schema::{Decimal, DecimalRepr};

use super::*;

use {
	rust_decimal::prelude::ToPrimitive as _,
	serde_serializer_quick_unsupported::serializer_unsupported, std::marker::PhantomData,
};

pub(in super::super) fn read_decimal<'de, R, V>(
	state: &mut DeserializerState<R>,
	decimal: &Decimal,
	hint: VisitorHint,
	visitor: V,
) -> Result<V::Value, DeError>
where
	R: ReadSlice<'de>,
	V: Visitor<'de>,
{
	let size = match decimal.repr {
		DecimalRepr::Bytes => read_len(state)?,
		DecimalRepr::Fixed(ref fixed) => fixed.size,
	};
	let mut buf = [0u8; 16];
	let start = buf.len().checked_sub(size).ok_or_else(|| {
		DeError::custom(format_args!(
			"Decimals of size larger than 16 are not supported (got size {size})"
		))
	})?;
	state.read_exact(&mut buf[start..]).map_err(DeError::io)?;
	let unscaled = i128::from_be_bytes(buf);
	let scale = decimal.scale;
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
			_error: PhantomData,
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
struct SerializeToVisitorStr<'de, V, E> {
	visitor: V,
	_lifetime: PhantomData<&'de ()>,
	_error: PhantomData<E>,
}

impl<'de, V: Visitor<'de>, E> serde::Serializer for SerializeToVisitorStr<'de, V, E>
where
	E: serde::ser::Error + serde::de::Error,
{
	type Ok = V::Value;
	type Error = E;

	fn serialize_str(self, v: &str) -> Result<Self::Ok, Self::Error> {
		self.visitor.visit_str(v)
	}

	serializer_unsupported! {
		err = (<Self::Error as serde::ser::Error>::custom("rust_decimal::Decimal should only serialize as str"));
		bool i8 i16 i32 i64 u8 u16 u32 u64 f32 f64 char bytes none some unit unit_struct
		unit_variant newtype_struct newtype_variant seq tuple tuple_struct tuple_variant map struct
		struct_variant i128 u128
	}
}
