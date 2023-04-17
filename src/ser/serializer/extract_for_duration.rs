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

pub(super) struct ExtractFieldNameForDuration;
#[derive(Copy, Clone)]
/// Order of the enum variants matters because repr is used for indexing
pub(super) enum DurationFieldName {
	Months,
	Days,
	Milliseconds,
}
impl std::fmt::Display for DurationFieldName {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		std::fmt::Display::fmt(
			match self {
				DurationFieldName::Months => "months",
				DurationFieldName::Days => "days",
				DurationFieldName::Milliseconds => "milliseconds",
			},
			f,
		)
	}
}
impl DurationFieldName {
	// hoping that this will result in some nice const propagation and make the str
	// comparisons disappear
	#[inline(always)]
	pub(super) fn from_str(s: &str) -> Result<Self, SerError> {
		Ok(match s {
			"months" => DurationFieldName::Months,
			"days" => DurationFieldName::Days,
			"milliseconds" => DurationFieldName::Milliseconds,
			_ => {
				return Err(SerError::new(
					"Map field names should be one of months/days/milliseconds \
						for serialization as Duration",
				))
			}
		})
	}
}
impl serde::Serializer for ExtractFieldNameForDuration {
	type Ok = DurationFieldName;
	type Error = SerError;

	fn serialize_str(self, v: &str) -> Result<Self::Ok, Self::Error> {
		DurationFieldName::from_str(v)
	}

	serde_serializer_quick_unsupported::serializer_unsupported! {
		err = (SerError::new("Map field names should be str for serialization as Duration"));
		bool i8 i16 i32 i64 u8 u16 u32 u64 f32 f64 char bytes none some unit unit_struct
		unit_variant newtype_struct newtype_variant seq tuple tuple_struct tuple_variant map struct
		struct_variant i128 u128
	}
}
