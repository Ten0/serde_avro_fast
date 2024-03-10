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
