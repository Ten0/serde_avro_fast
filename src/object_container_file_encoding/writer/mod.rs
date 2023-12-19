mod compression;
mod vectored_write_polyfill;

use compression::CompressionCodecState;

use crate::{
	object_container_file_encoding::{Metadata, METADATA_SCHEMA},
	ser::{SerError, SerializerConfig, SerializerState},
	Schema,
};

use super::{Compression, HEADER_CONST};

use {
	serde::Serialize,
	std::{io::Write, num::NonZeroUsize},
};

/// Write all the elements of the provided sequence in an [object container file](https://avro.apache.org/docs/current/specification/#object-container-files)
pub fn write_all<W, IT>(
	schema: &Schema,
	compression: Compression,
	writer: W,
	iterator: IT,
) -> Result<W, SerError>
where
	W: Write,
	IT: IntoIterator,
	IT::Item: Serialize,
{
	let mut serializer_config = SerializerConfig::new(schema);
	let mut writer = WriterBuilder::new(&mut serializer_config)
		.compression(compression)
		.build(writer)?;
	writer.serialize_all(iterator.into_iter())?;
	writer.into_inner()
}

/// [`Writer`] builder for [object container files](https://avro.apache.org/docs/current/specification/#object-container-files)
pub struct WriterBuilder<'c, 's> {
	compression: Compression,
	aprox_block_size: u32,
	serializer_config: &'c mut SerializerConfig<'s>,
}

impl<'c, 's> WriterBuilder<'c, 's> {
	/// Construct a writer from a [`SerializerConfig`].
	///
	/// [`SerializerConfig`] holds the schema, as well as buffers that may
	/// be reused across serializations for performance, and other
	/// serialization configuration.
	pub fn new(serializer_config: &'c mut SerializerConfig<'s>) -> Self {
		Self {
			serializer_config,
			compression: Compression::Null,
			aprox_block_size: 64 * 1024,
		}
	}

	/// Specify the compression codec that each block will be compressed with
	pub fn compression(mut self, compression: Compression) -> Self {
		self.compression = compression;
		self
	}

	/// Approximate uncompressed block size in bytes
	///
	/// If after serializing a value, the total size of the uncompressed block
	/// is greater than this value, the block will be compressed and flushed.
	///
	/// Default value is 64KiB.
	pub fn aprox_block_size(mut self, aprox_block_size: u32) -> Self {
		self.aprox_block_size = aprox_block_size;
		self
	}

	/// Build the [`Writer`]
	///
	/// After this method is called, it is guaranteed that the full object
	/// container file encoding header is already written to the `writer`.
	pub fn build<W: Write>(self, writer: W) -> Result<Writer<'c, 's, W>, SerError> {
		self.build_with_user_metadata(writer, ())
	}

	/// Build the [`Writer`], also encoding [user-specified metadata](https://avro.apache.org/docs/current/specification/#object-container-files)
	///
	/// After this method is called, it is guaranteed that the full object
	/// container file encoding header is already written to the `writer`.
	pub fn build_with_user_metadata<W: Write, M: Serialize>(
		self,
		mut writer: W,
		metadata: M,
	) -> Result<Writer<'c, 's, W>, SerError> {
		// We'll use this both for serializing the header and as a buffer when
		// serializing blocks
		let mut sync_marker = [0; 16];
		rand::Rng::fill(&mut rand::thread_rng(), &mut sync_marker);

		let mut buf = Vec::with_capacity(self.aprox_block_size as usize * 5 / 4);

		// Construct the header into the buf
		buf.write_all(&HEADER_CONST).map_err(SerError::io)?;

		{
			// Serialize metadata
			let mut header_serializer_config = SerializerConfig::new_with_optional_schema(None);
			let mut header_serializer_state =
				SerializerState::from_writer(buf, &mut header_serializer_config);
			(Metadata::<&str, M> {
				schema: self.serializer_config.schema().json(),
				codec: self.compression.codec(),
				user_metadata: metadata,
			})
			.serialize(header_serializer_state.serializer_overriding_schema_root(METADATA_SCHEMA))
			.map_err(|ser_error| {
				<SerError as serde::ser::Error>::custom(format_args!(
					"Failed to serialize object container file header metadata: {ser_error}"
				))
			})?;
			buf = header_serializer_state.into_writer();
		}

		buf.write_all(&sync_marker).map_err(SerError::io)?;

		writer.write_all(&buf).map_err(SerError::io)?;
		buf.clear();

		Ok(Writer {
			inner: WriterInner {
				serializer_state: SerializerState::from_writer(buf, self.serializer_config),
				sync_marker,
				compression_codec_state: CompressionCodecState::new(self.compression),
				n_elements_in_block: 0,
				aprox_block_size: self.aprox_block_size,
				block_header_buffer: [0; 20],
				block_header_size: None,
			},
			writer: Some(writer),
		})
	}
}

/// Writer for [object container files](https://avro.apache.org/docs/current/specification/#object-container-files)
///
/// To be constructed via [`WriterBuilder`].
pub struct Writer<'c, 's, W: Write> {
	inner: WriterInner<'c, 's>,
	writer: Option<W>,
}

impl<'c, 's, W: Write> Writer<'c, 's, W> {
	/// Serialize each value of the provided sequence in the object container
	/// file
	pub fn serialize_all<IT: IntoIterator>(&mut self, iterator: IT) -> Result<(), SerError>
	where
		IT::Item: Serialize,
	{
		iterator.into_iter().try_for_each(|i| self.serialize(i))
	}

	/// Serialize one value as an object in the object container file
	pub fn serialize<T: Serialize>(&mut self, value: T) -> Result<(), SerError> {
		self.flush_finished_block()?;
		if self.inner.serializer_state.writer.len() >= self.inner.aprox_block_size as usize {
			self.finish_block()?;
		}
		self.inner.serialize(value)?;
		self.flush_finished_block()?;
		Ok(())
	}

	/// Flushe the final block (if a block was started) then return the
	/// underlying writer.
	pub fn into_inner(mut self) -> Result<W, SerError> {
		self.finish_block()?;
		Ok(self
			.writer
			.take()
			.expect("Only called by this function, which takes ownership"))
	}

	/// Flush the current block (if a block was started)
	///
	/// After this function is called, if it returned no error, it is guaranteed
	/// that the full block is written to the writer.
	/// This implies that all bytes written so far amount to a valid object
	/// container file.
	pub fn finish_block(&mut self) -> Result<(), SerError> {
		self.inner.finish_block()?;
		self.flush_finished_block()?;
		Ok(())
	}

	fn flush_finished_block(&mut self) -> Result<(), SerError> {
		match self.inner.block_header_size {
			None => {
				// there's no block to flush
			}
			Some(block_header_size) => {
				let writer = self.writer.as_mut().expect(
					"This is only unset by into_inner, which guarantees that \
						flush_finished_block is called, which guarantees that block_header_size \
						is None",
				);
				// To be replaced with std's write_all_vectored once that is stabilized
				// https://github.com/rust-lang/rust/issues/70436
				vectored_write_polyfill::write_all_vectored(
					writer,
					[
						&self.inner.block_header_buffer[..block_header_size.get()],
						self.inner.compressed_block(),
						&self.inner.sync_marker,
					],
				)
				.map_err(SerError::io)?;
				self.inner.block_header_size = None; // Mark that we have flushed
				self.inner.serializer_state.writer.clear();
			}
		}

		Ok(())
	}

	/// Get a mutable reference to the inner writer
	///
	/// If you haven't received any error while manipulating the [`Writer`] or
	/// its builder, you may expect that:
	/// - The object container file encoding header has been written
	/// - All blocks written so far are complete
	///
	/// This implies that everything written so far amounts to a valid object
	/// container file.
	///
	/// It is however not guaranteed that all `serialize`d data has been written
	/// as a block: there may still be an incomplete block in the writer's own
	/// buffer.
	///
	/// You may use this if you want to e.g. write the headers/blocks to
	/// separate files, free up the memory...
	pub fn inner_mut(&mut self) -> &mut W {
		self.writer.as_mut().expect(
			"This is only unset by into_inner, which guarantees we \
				couldn't call this function after",
		)
	}

	/// Get a reference to the inner writer
	///
	/// If you haven't received any error while manipulating the [`Writer`] or
	/// its builder, you may expect that:
	/// - The object container file encoding header has been written
	/// - All blocks written so far are complete
	///
	/// This implies that everything written so far amounts to a valid object
	/// container file.
	///
	/// It is however not guaranteed that all `serialize`d data has been written
	/// as a block: there may still be an incomplete block in the writer's own
	/// buffer.
	///
	/// You may use this to e.g. read the header, check the length of what was
	/// serialized so far...
	pub fn inner(&self) -> &W {
		self.writer.as_ref().expect(
			"This is only unset by into_inner, which guarantees we \
				couldn't call this function after",
		)
	}
}

impl<'c, 's, W: Write> Drop for Writer<'c, 's, W> {
	fn drop(&mut self) {
		let panicking = std::thread::panicking();
		let res = match panicking {
			false => self.finish_block(),
			true => {
				// We are already panicking so even if finish_block panics we just want to let
				// the current panic propagate.
				// There is no exception safety concern within `self` because everything
				// in there will be dropped as soon as we return anyway.
				std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| self.finish_block()))
					.unwrap_or(Ok(()))
			}
		};
		if cfg!(debug_assertions) && !panicking {
			res.expect(
				"Failed to flush Writer on Drop. \
					Please favor flushing manually before dropping the Writer.",
			);
		}
	}
}

struct WriterInner<'c, 's> {
	serializer_state: SerializerState<'c, 's, Vec<u8>>,
	n_elements_in_block: u64,
	aprox_block_size: u32,
	sync_marker: [u8; 16],
	compression_codec_state: CompressionCodecState,
	block_header_buffer: [u8; 20],
	block_header_size: Option<NonZeroUsize>,
}

impl<'c, 's> WriterInner<'c, 's> {
	fn serialize<T: Serialize>(&mut self, value: T) -> Result<(), SerError> {
		let buf_len_before_attempt = self.serializer_state.writer.len();
		value
			.serialize(self.serializer_state.serializer())
			.map_err(|e| {
				// If the flush is going wrong though there's nothing we can do
				self.serializer_state
					.writer
					.truncate(buf_len_before_attempt);
				e
			})?;
		self.n_elements_in_block += 1;
		if self.serializer_state.writer.len() >= self.aprox_block_size as usize {
			self.finish_block()?;
		}
		Ok(())
	}

	fn finish_block(&mut self) -> Result<(), SerError> {
		if self.n_elements_in_block > 0 {
			assert!(
				self.block_header_size.is_none(),
				"Previous block should always be flushed before starting to serialize a new one"
			);

			self.compression_codec_state
				.encode(self.serializer_state.writer.as_slice())?;

			let n = <i64 as integer_encoding::VarInt>::encode_var(
				self.n_elements_in_block as i64,
				&mut self.block_header_buffer,
			);
			let n2 = <i64 as integer_encoding::VarInt>::encode_var(
				self.compressed_block().len() as i64,
				&mut self.block_header_buffer[n..],
			);
			self.block_header_size = Some(
				NonZeroUsize::new(n + n2).expect("Encoding VarInts should never write zero bytes"),
			);
			self.n_elements_in_block = 0;
		}

		Ok(())
	}

	fn compressed_block(&self) -> &[u8] {
		self.compression_codec_state
			.compressed_buffer()
			.unwrap_or_else(|| {
				// No compression codec, use the serializer's buffer directly
				self.serializer_state.writer.as_slice()
			})
	}
}
