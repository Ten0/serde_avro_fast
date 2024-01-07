use serde_avro_fast::{
	schema::{BuildSchemaFromApacheSchemaError, ParseSchemaError},
	Schema,
};

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
	assert!(matches!(
		schema.parse::<Schema>(),
		Err(ParseSchemaError::ApacheToFast(
			BuildSchemaFromApacheSchemaError::UnconditionalCycle
		))
	));
}
