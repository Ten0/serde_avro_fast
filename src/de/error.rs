use std::borrow::Cow;

/// Any error that may happen during deserialization
#[derive(thiserror::Error)]
#[error("{}", inner.value)]
pub struct DeError {
	inner: Box<ErrorInner>,
}

impl std::fmt::Debug for DeError {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match self.inner.io_error.as_ref() {
			Some(io_error) => write!(f, "{}: {}", self.inner.value, io_error),
			None => std::fmt::Debug::fmt(&*self.inner.value, f),
		}
	}
}

struct ErrorInner {
	value: Cow<'static, str>,
	io_error: Option<std::io::Error>,
}

impl DeError {
	/// If you need a dynamic string use `DeError::custom(format_args!(...))`
	pub(crate) fn new(s: &'static str) -> Self {
		Self {
			inner: Box::new(ErrorInner {
				value: Cow::Borrowed(s),
				io_error: None,
			}),
		}
	}
	pub(crate) fn unexpected_eof() -> Self {
		Self::new("Unexpected end of slice while deserializing")
	}
	pub(crate) fn io(io_error: std::io::Error) -> Self {
		Self::custom_io(
			"Encountered IO error when attempting to read for deserialization",
			io_error,
		)
	}
	pub(crate) fn custom_io(msg: &'static str, io_error: std::io::Error) -> Self {
		Self {
			inner: Box::new(ErrorInner {
				value: Cow::Borrowed(msg),
				io_error: Some(io_error),
			}),
		}
	}
	/// If this error was caused by an IO error, return it
	pub fn io_error(&self) -> Option<&std::io::Error> {
		self.inner.io_error.as_ref()
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
				io_error: None,
			}),
		}
	}
}

/// This is implemented just to support the trick with decimal where we use
/// their serialize implementation to enable zero-alloc deserialization
impl serde::ser::Error for DeError {
	fn custom<T>(msg: T) -> Self
	where
		T: std::fmt::Display,
	{
		<Self as serde::de::Error>::custom(msg)
	}
}
