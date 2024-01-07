use serde_avro_fast::{
	schema::{Record, SchemaNode},
	Schema,
};

#[test]
fn test_no_cyclic_debug_on_schema() {
	let schema: Schema = r#"{
      "type": "record",
      "name": "test",
      "fields": [
        {
          "name": "a",
          "type": "long",
          "default": 42
        },
        {
            "name": "b",
            "type": {"type": ["null", "test"]}
        }
      ]
    }"#
	.parse()
	.unwrap();
	let root = schema.root();
	dbg!(&root);
	let sub_root = match root {
		SchemaNode::Record(Record { fields, .. }) => fields[1].schema,
		_ => panic!(),
	};
	let sub_root_some = match sub_root {
		SchemaNode::Union(union) => union.variants[1],
		_ => panic!(),
	};
	assert_eq!(root as *const _, sub_root_some as *const _); // This is a case where we have to pay attention

	use std::fmt::Write;
	struct CheckCycle {
		len: usize,
	}
	impl Write for CheckCycle {
		fn write_str(&mut self, s: &str) -> std::fmt::Result {
			self.len += s.len();
			if self.len > 10_000 {
				panic!("This seems to be writing forever!");
			}
			Ok(())
		}
	}

	write!(&mut CheckCycle { len: 0 }, "{root:?}").unwrap();

	// Now we know that this can render without crashing, let's ensure it's a
	// reasonable value...
	assert_eq!(
		format!("{root:#?}"),
		r#"Record(
    Record {
        fields: [
            RecordField {
                name: "a",
                schema: Long,
            },
            RecordField {
                name: "b",
                schema: Union(
                    Union {
                        variants: [
                            Null,
                            Record,
                        ],
                    },
                ),
            },
        ],
        name: "test",
    },
)"#
	);
}
