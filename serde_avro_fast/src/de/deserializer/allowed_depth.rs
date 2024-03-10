use super::DeError;

/// Allowed additional depth of the deserialization
///
/// This is decremented as we advance in depth to prevent stack overflow
#[derive(Clone, Copy)]
pub(crate) struct AllowedDepth {
	allowed_additional_depth: usize,
}

impl AllowedDepth {
	pub(crate) fn new(allowed_depth: usize) -> Self {
		Self {
			allowed_additional_depth: allowed_depth,
		}
	}

	pub(crate) fn dec(self) -> Result<Self, DeError> {
		match self.allowed_additional_depth.checked_sub(1) {
			Some(allowed_additional_depth) => Ok(Self {
				allowed_additional_depth,
			}),
			None => Err(DeError::new(
				"Deserialization recursivity limit reached (stack overflow prevention)",
			)),
		}
	}
}
