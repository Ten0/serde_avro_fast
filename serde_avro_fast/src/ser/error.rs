use {serde::ser::Error, std::borrow::Cow};

/// Any error that may happen during serialization
#[derive(thiserror::Error)]
#[error("{}", inner.value)]
pub struct SerError {
	inner: Box<ErrorInner>,
}

impl std::fmt::Debug for SerError {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		std::fmt::Debug::fmt(&*self.inner.value, f)
	}
}

struct ErrorInner {
	value: Cow<'static, str>,
}

impl SerError {
	/// If you need a dynamic string use `SerError::custom(format_args!(...))`
	pub(crate) fn new(s: &'static str) -> Self {
		Self {
			inner: Box::new(ErrorInner {
				value: Cow::Borrowed(s),
			}),
		}
	}
	pub(crate) fn io(io_error: std::io::Error) -> Self {
		Self::custom(format_args!(
			"Encountered IO error when attempting to write for serialization: {io_error}"
		))
	}
}

impl serde::ser::Error for SerError {
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
