use {pretty_assertions::assert_eq, serde_avro_fast::schema::*};

#[test]
fn schema_construction() {
	// Let's simulate what would happen if we associated schemas to Rust structs
	let nodes: Vec<SchemaNode> = vec![
		Union::new(vec![SchemaKey::from_idx(1), SchemaKey::from_idx(2)]).into(),
		RegularType::Null.into(),
		Record::new(
			Name::from_fully_qualified_name("a.b"),
			vec![RecordField::new("c", SchemaKey::from_idx(0))],
		)
		.into(),
	];
	let schema = SchemaMut::from_nodes(nodes);

	// The following schema should parse to exactly what's above
	let schema_str = prettify_json(
		r#"
			[
				"null",
				{
					"type": "record",
					"name": "a.b",
					"fields": [{
						"name": "c",
						"type": ["null", "b"]
					}]
				}
			]
		"#,
	);
	let parsed_schema: SchemaMut = schema_str.parse().unwrap();

	// Make sure we can export that. It's hard because we need to allow the union
	// to cycle once, because on the second run the record will already be in the
	// schema, so the union will not cycle again.
	assert_eq!(serde_json::to_string_pretty(&schema).unwrap(), schema_str);
	// Make sure both serialize to the same thing
	assert_eq!(
		serde_json::to_string_pretty(&parsed_schema).unwrap(),
		schema_str
	);
}

fn prettify_json(s: &str) -> String {
	String::from_utf8({
		// Sanitize & minify json, preserving all keys.
		let mut serializer = serde_json::Serializer::pretty(Vec::new());
		serde_transcode::transcode(&mut serde_json::Deserializer::from_str(s), &mut serializer)
			.unwrap();
		serializer.into_inner()
	})
	.unwrap()
}

#[test]
fn impossible_schema_construction() {
	// Contains a cycle that would lead to infinite recursion when serializing
	let nodes: Vec<SchemaNode> = vec![
		Union::new(vec![SchemaKey::from_idx(1), SchemaKey::from_idx(2)]).into(),
		RegularType::Null.into(),
		Array::new(SchemaKey::from_idx(0)).into(),
	];
	let schema = SchemaMut::from_nodes(nodes);
	assert_eq!(
		serde_json::to_string(&schema).unwrap_err().to_string(),
		"Schema contains a cycle that can't be avoided using named references"
	);
}
