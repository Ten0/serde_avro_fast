use alloc::borrow::Cow;
use alloc::boxed::Box;
use alloc::string::ToString;

/// Any error that may happen during serialization
#[derive(thiserror::Error)]
pub struct SchemaError {
	inner: Box<ErrorInner>,
}

enum ErrorInner {
	SerdeJson(serde_json::Error),
	Other(Cow<'static, str>),
}

impl SchemaError {
	/// If you need a dynamic string use `SerError::custom(format_args!(...))`
	pub(crate) fn new(s: &'static str) -> Self {
		Self {
			inner: Box::new(ErrorInner::Other(Cow::Borrowed(s))),
		}
	}

	pub(crate) fn msg(s: core::fmt::Arguments<'_>) -> Self {
		Self::display(s)
	}

	pub(crate) fn display(s: impl core::fmt::Display) -> Self {
		Self {
			inner: Box::new(ErrorInner::Other(Cow::Owned(s.to_string()))),
		}
	}

	pub(crate) fn serde_json(serde_json_error: serde_json::Error) -> Self {
		Self {
			inner: Box::new(ErrorInner::SerdeJson(serde_json_error)),
		}
	}
}

impl core::fmt::Debug for SchemaError {
	fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
		match *self.inner {
			ErrorInner::SerdeJson(ref e) => core::fmt::Debug::fmt(e, f),
			ErrorInner::Other(ref s) => core::fmt::Debug::fmt(&**s, f),
		}
	}
}

impl core::fmt::Display for SchemaError {
	fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
		match *self.inner {
			ErrorInner::SerdeJson(ref e) => core::fmt::Display::fmt(e, f),
			ErrorInner::Other(ref s) => core::fmt::Display::fmt(&**s, f),
		}
	}
}
