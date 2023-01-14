mod deserializer;
mod error;
mod read;
mod types;

use {deserializer::*, error::DeError, read::*, types::*};

use crate::Schema;

use serde::de::*;

pub struct ReaderAndConfig<R> {
	reader: R,
	max_seq_size: usize,
}
impl<'de, R: Read<'de>> ReaderAndConfig<R> {
	pub fn new(r: R) -> Self {
		ReaderAndConfig {
			reader: r,
			max_seq_size: 1_000_000_000,
		}
	}

	pub fn deserializer<'r, 's>(&'r mut self, schema: &'s Schema) -> DatumDeserializer<'r, 's, R> {
		DatumDeserializer { reader: self, schema }
	}
}
impl<'a> ReaderAndConfig<read::SliceRead<'a>> {
	pub fn from_slice(s: &'a [u8]) -> Self {
		Self::new(read::SliceRead::new(s))
	}
}

impl<R: std::io::Read> ReaderAndConfig<read::ReaderRead<R>> {
	pub fn from_reader(r: R) -> Self {
		Self::new(read::ReaderRead::new(r))
	}
}

impl<R> std::ops::Deref for ReaderAndConfig<R> {
	type Target = R;
	fn deref(&self) -> &Self::Target {
		&self.reader
	}
}

impl<R> std::ops::DerefMut for ReaderAndConfig<R> {
	fn deref_mut(&mut self) -> &mut Self::Target {
		&mut self.reader
	}
}
