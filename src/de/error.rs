use {serde::de::Error, std::borrow::Cow};

#[derive(thiserror::Error)]
#[error("{}", inner.value)]
pub struct DeError {
	inner: Box<ErrorInner>,
}

impl std::fmt::Debug for DeError {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		std::fmt::Debug::fmt(&*self.inner.value, f)
	}
}

struct ErrorInner {
	value: Cow<'static, str>,
}

impl DeError {
	/// If you need a dynamic string use `DeError::custom(format_args!(...))`
	pub(crate) fn new(s: &'static str) -> Self {
		Self {
			inner: Box::new(ErrorInner {
				value: Cow::Borrowed(s),
			}),
		}
	}
	pub(crate) fn unexpected_eof() -> Self {
		Self::new("Unexpected end of slice while deserializing")
	}
	pub(crate) fn io(io_error: std::io::Error) -> Self {
		Self::custom(format_args!(
			"Encountered IO error when attempting to read for deserialization: {io_error}"
		))
	}
}

impl serde::de::Error for DeError {
	fn custom<T>(msg: T) -> Self
	where
		T: std::fmt::Display,
	{
		Self {
			inner: Box::new(ErrorInner {
				value: Cow::Owned(msg.to_string()),
			}),
		}
	}
}
