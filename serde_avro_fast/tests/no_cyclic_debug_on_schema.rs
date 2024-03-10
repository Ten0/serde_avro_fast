use serde_avro_fast::schema::*;

#[test]
fn test_no_cyclic_debug_on_schema() {
	let schema: SchemaMut = r#"{
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
            "type": ["null", "test"]
        }
      ]
    }"#
	.parse()
	.unwrap();
	let root = schema.root();
	let sub_root = match &root {
		SchemaNode::RegularType(RegularType::Record(Record { fields, .. })) => fields[1].type_,
		_ => panic!(),
	};
	let sub_root_some = match &schema[sub_root] {
		SchemaNode::RegularType(RegularType::Union(union)) => union.variants[1],
		_ => panic!(),
	};
	assert_eq!(sub_root_some, SchemaKey::from_idx(0)); // This is a case where we have to pay attention
	dbg!(&root);
	let schema: Schema = schema.try_into().unwrap();

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

	write!(&mut CheckCycle { len: 0 }, "{schema:?}").unwrap();

	// Now we know that this can render without crashing, let's ensure it's a
	// reasonable value...
	assert_eq!(
		format!("{schema:#?}"),
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
