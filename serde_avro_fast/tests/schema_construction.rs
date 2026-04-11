#![allow(missing_docs)]

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

#[test]
fn schema_mut_from_schemata_denies_cycles() {
	let main = r#"{
		"type": "record",
		"name": "Main",
		"fields": [{ "name": "v", "type": "int" }]
	}"#;
	let dep_cyclic = r#"{
		"type": "record",
		"name": "CyclicA",
		"fields": [{
			"name": "b",
			"type": {
				"type": "record",
				"name": "CyclicB",
				"fields": [{ "name": "a", "type": "CyclicA" }]
			}
		}]
	}"#;

	let schema_mut_res = SchemaMut::from_schemata(main, [dep_cyclic]);
	assert_eq!(
		schema_mut_res.unwrap_err().to_string(),
		"The schema contains a record that ends up always containing itself"
	);
}

#[test]
fn schema_mut_from_schemata_allows_unreferenced_nodes() {
	let main = r#"{
		"type": "record",
		"name": "Main",
		"fields": [{ "name": "f", "type": "Used" }]
	}"#;
	let dep_used = r#"{
		"type": "record",
		"name": "Used",
		"fields": [{ "name": "v", "type": "int" }]
	}"#;
	let dep_unused = r#"{
		"type": "record",
		"name": "Unused",
		"fields": [{ "name": "w", "type": "string" }]
	}"#;

	let schema_mut = SchemaMut::from_schemata(main, [dep_used, dep_unused]).unwrap();
	let minimal_schema_mut = SchemaMut::from_schemata(main, [dep_used]).unwrap();
	assert_eq!(schema_mut.nodes().len(), 5);
	assert_eq!(minimal_schema_mut.nodes().len(), 3);
}
