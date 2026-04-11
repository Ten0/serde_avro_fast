#![allow(missing_docs)]

use serde_avro_fast::Schema;

#[test]
fn forbids_zero_sized_cycles() {
	let schema = r#"
	{
		"type": "record",
		"name": "A",
		"fields": [
			{
				"name": "a",
				"type": {
					"type": "record",
					"name": "B",
					"fields": [
						{
							"name": "b",
							"type": "A"
						}
					]
				}
			}
		]
	}"#;
	assert_eq!(
		schema.parse::<Schema>().unwrap_err().to_string(),
		"The schema contains a record that ends up always containing itself"
	);
}

#[test]
fn from_schemata_resolves_named_dependency() {
	let main = r#"{
		"type": "record",
		"name": "Bar",
		"fields": [{ "name": "b", "type": "Foo" }]
	}"#;
	let dep = r#"{
		"type": "record",
		"name": "Foo",
		"fields": [{ "name": "a", "type": "int" }]
	}"#;
	let schema = Schema::from_schemata(main, [dep]).unwrap();
	assert_eq!(
		schema.json(),
		r#"{"type":"record","name":"Bar","fields":[{"name":"b","type":{"type":"record","name":"Foo","fields":[{"name":"a","type":"int"}]}}]}"#
	);
}

#[test]
fn from_schemata_dependency_order_does_not_matter() {
	let main = r#"{
		"type": "record",
		"name": "Root",
		"fields": [{ "name": "x", "type": "Alpha" }]
	}"#;
	let dep_alpha = r#"{
		"type": "record",
		"name": "Alpha",
		"fields": [{ "name": "b", "type": "Beta" }]
	}"#;
	let dep_beta = r#"{
		"type": "record",
		"name": "Beta",
		"fields": [{ "name": "v", "type": "int" }]
	}"#;
	let normalized = r#"{
		"type": "record",
		"name": "Root",
		"fields": [{
			"name": "x",
			"type": {
				"type": "record",
				"name": "Alpha",
				"fields": [{
					"name": "b",
					"type": {
						"type": "record",
						"name": "Beta",
						"fields": [{ "name": "v", "type": "int" }]
					}
				}]
			}
		}]
	}"#;

	let s1 = Schema::from_schemata(main, [dep_alpha, dep_beta]).unwrap();
	let s2 = Schema::from_schemata(main, [dep_beta, dep_alpha]).unwrap();
	let s_normalized: Schema = normalized.parse().unwrap();

	assert_eq!(s1.rabin_fingerprint(), s2.rabin_fingerprint());
	assert_eq!(s1.rabin_fingerprint(), s_normalized.rabin_fingerprint());
	assert_eq!(s1.json(), s2.json());
}

#[test]
fn from_schemata_allows_forward_references_across_documents() {
	let main = r#"{
		"type": "record",
		"name": "Root",
		"fields": [{ "name": "f", "type": "Foo" }]
	}"#;
	let dep_foo = r#"{
		"type": "record",
		"name": "Foo",
		"fields": [{ "name": "b", "type": "Bar" }]
	}"#;
	let dep_bar = r#"{
		"type": "record",
		"name": "Bar",
		"fields": [{ "name": "v", "type": "string" }]
	}"#;

	let schema = Schema::from_schemata(main, [dep_foo, dep_bar]).unwrap();
	assert_eq!(
		schema.json(),
		r#"{"type":"record","name":"Root","fields":[{"name":"f","type":{"type":"record","name":"Foo","fields":[{"name":"b","type":{"type":"record","name":"Bar","fields":[{"name":"v","type":"string"}]}}]}}]}"#
	);
}

#[test]
fn from_schemata_rejects_duplicate_across_dependencies() {
	let main = r#"{
		"type": "record",
		"name": "Main",
		"fields": [{ "name": "f", "type": "Foo" }]
	}"#;
	let dep1 = r#"{
		"type": "record",
		"name": "Foo",
		"fields": [{ "name": "a", "type": "int" }]
	}"#;
	let dep2 = r#"{
		"type": "record",
		"name": "Foo",
		"fields": [{ "name": "b", "type": "long" }]
	}"#;

	let err = Schema::from_schemata(main, [dep1, dep2]).unwrap_err();
	let msg = err.to_string();
	assert!(
		msg.contains("duplicate") || msg.contains("Duplicate"),
		"expected duplicate error, got: {msg}"
	);
}

#[test]
fn from_schemata_rejects_duplicate_between_main_and_dependency() {
	let main = r#"{
		"type": "record",
		"name": "Foo",
		"fields": [{ "name": "a", "type": "int" }]
	}"#;
	let dep = r#"{
		"type": "record",
		"name": "Foo",
		"fields": [{ "name": "b", "type": "long" }]
	}"#;

	let err = Schema::from_schemata(main, [dep]).unwrap_err();
	let msg = err.to_string();
	assert!(
		msg.contains("duplicate") || msg.contains("Duplicate"),
		"expected duplicate error between main and dep, got: {msg}"
	);
}

#[test]
fn from_schemata_rejects_unknown_reference_from_main() {
	let main = r#"{
		"type": "record",
		"name": "Main",
		"fields": [{ "name": "f", "type": "Missing" }]
	}"#;

	let err = Schema::from_schemata(main, Vec::<&str>::new()).unwrap_err();
	let msg = err.to_string();
	assert!(
		msg.contains("unknown reference") || msg.contains("Unknown"),
		"expected unknown reference error, got: {msg}"
	);
}

#[test]
fn from_schemata_rejects_unknown_reference_from_dependency() {
	let main = r#"{
		"type": "record",
		"name": "Main",
		"fields": [{ "name": "f", "type": "Dep" }]
	}"#;
	let dep = r#"{
		"type": "record",
		"name": "Dep",
		"fields": [{ "name": "x", "type": "Nonexistent" }]
	}"#;

	let err = Schema::from_schemata(main, [dep]).unwrap_err();
	let msg = err.to_string();
	assert!(
		msg.contains("unknown reference") || msg.contains("Unknown"),
		"expected unknown reference error from dependency, got: {msg}"
	);
}

#[test]
fn from_schemata_prunes_unreferenced_nodes() {
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

	let schema = Schema::from_schemata(main, [dep_used, dep_unused]).unwrap();
	assert_eq!(
		schema.json(),
		r#"{"type":"record","name":"Main","fields":[{"name":"f","type":{"type":"record","name":"Used","fields":[{"name":"v","type":"int"}]}}]}"#
	);
}
