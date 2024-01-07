use std::io::Read;

use serde_avro_fast::Schema;

#[test]
fn prevents_stack_overflow() {
	let schema: Schema = r#"{
      "type": "record",
      "name": "test",
      "fields": [
        {
            "name": "b",
            "type": {"type": ["null", "test"]}
        }
      ]
    }"#
	.parse()
	.unwrap();

	struct LongReader;
	impl Read for LongReader {
		fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
			buf.fill(2);
			Ok(buf.len())
		}
	}

	#[derive(serde_derive::Deserialize, Debug)]
	struct S {
		#[allow(unused)]
		b: Option<Box<S>>,
	}

	let res: Result<S, serde_avro_fast::de::DeError> =
		serde_avro_fast::from_datum_reader(std::io::BufReader::new(LongReader), &schema);

	assert_eq!(
		res.unwrap_err().to_string(),
		"Deserialization recursivity limit reached (stack overflow prevention)"
	)
}
