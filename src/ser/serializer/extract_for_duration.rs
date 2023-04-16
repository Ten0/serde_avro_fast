use super::*;

pub(super) struct ExtractU32ForDuration;
impl serde::Serializer for ExtractU32ForDuration {
	type Ok = u32;
	type Error = SerError;

	fn serialize_u32(self, v: u32) -> Result<Self::Ok, Self::Error> {
		Ok(v)
	}

	serde_serializer_quick_unsupported::serializer_unsupported! {
		err = (SerError::new("Elements should be u32s for serialization as Duration"));
		bool i8 i16 i32 i64 u8 u16 u64 f32 f64 char str bytes none some unit unit_struct
		unit_variant newtype_struct newtype_variant seq tuple tuple_struct tuple_variant map struct
		struct_variant i128 u128
	}
}
