use std::borrow::Cow;

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

	pub(crate) fn msg(s: std::fmt::Arguments<'_>) -> Self {
		Self::display(s)
	}

	pub(crate) fn display(s: impl std::fmt::Display) -> Self {
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

impl std::fmt::Debug for SchemaError {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match *self.inner {
			ErrorInner::SerdeJson(ref e) => std::fmt::Debug::fmt(e, f),
			ErrorInner::Other(ref s) => std::fmt::Debug::fmt(&**s, f),
		}
	}
}

impl std::fmt::Display for SchemaError {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match *self.inner {
			ErrorInner::SerdeJson(ref e) => std::fmt::Display::fmt(e, f),
			ErrorInner::Other(ref s) => std::fmt::Display::fmt(&**s, f),
		}
	}
}
