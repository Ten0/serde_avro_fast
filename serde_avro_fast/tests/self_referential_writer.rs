use {
	serde::Serialize,
	serde_avro_derive::BuildSchema,
	serde_avro_fast::{ser::SerializerConfig, Schema},
};

#[derive(Serialize, BuildSchema)]
struct Bar {
	a: i32,
	b: String,
}

#[ouroboros::self_referencing]
struct Writer<'schema> {
	serializer_config: SerializerConfig<'schema>,
	#[borrows(mut serializer_config)]
	#[covariant]
	writer: serde_avro_fast::object_container_file_encoding::Writer<'this, 'schema, Vec<u8>>,
}

fn build_writer() -> Writer<'static> {
	lazy_static::lazy_static! {
		static ref SCHEMA: Schema = Bar::schema().unwrap();
	}
	Writer::new(SerializerConfig::new(&*SCHEMA), |serializer_config| {
		serde_avro_fast::object_container_file_encoding::WriterBuilder::new(serializer_config)
			.sync_marker({
				// make test deterministic
				[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16]
			})
			.build(Vec::new())
			.unwrap()
	})
}

#[test]
fn test_owned_writer() {
	let values = [
		&Bar {
			a: 1,
			b: "foo".to_string(),
		},
		&Bar {
			a: 2,
			b: "bar".to_string(),
		},
	];

	let mut writer = build_writer();
	for b in values {
		writer.with_writer_mut(|w| w.serialize(b).unwrap());
	}

	let finished_written_buffer: Vec<u8> = writer
		.with_writer_mut(|w| -> Result<_, serde_avro_fast::ser::SerError> {
			w.finish_block()?;
			Ok(std::mem::replace(w.inner_mut(), Vec::new()))
		})
		.unwrap();

	// In case we want to re-use it
	let _source_serializer_config: SerializerConfig = writer.into_heads().serializer_config;

	assert_eq!(
		finished_written_buffer,
		&[
			79, 98, 106, 1, 2, 22, 97, 118, 114, 111, 46, 115, 99, 104, 101, 109, 97, 240, 1, 123,
			34, 116, 121, 112, 101, 34, 58, 34, 114, 101, 99, 111, 114, 100, 34, 44, 34, 110, 97,
			109, 101, 34, 58, 34, 115, 101, 108, 102, 95, 114, 101, 102, 101, 114, 101, 110, 116,
			105, 97, 108, 95, 119, 114, 105, 116, 101, 114, 46, 66, 97, 114, 34, 44, 34, 102, 105,
			101, 108, 100, 115, 34, 58, 91, 123, 34, 110, 97, 109, 101, 34, 58, 34, 97, 34, 44, 34,
			116, 121, 112, 101, 34, 58, 34, 105, 110, 116, 34, 125, 44, 123, 34, 110, 97, 109, 101,
			34, 58, 34, 98, 34, 44, 34, 116, 121, 112, 101, 34, 58, 34, 115, 116, 114, 105, 110,
			103, 34, 125, 93, 125, 2, 20, 97, 118, 114, 111, 46, 99, 111, 100, 101, 99, 8, 110,
			117, 108, 108, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 4, 20, 2, 6,
			102, 111, 111, 4, 6, 98, 97, 114, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15,
			16
		]
	)
}
