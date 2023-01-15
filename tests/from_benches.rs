const RAW_BIG_SCHEMA: &str = r#"
{
	"namespace": "my.example",
	"type": "record",
	"name": "userInfo",
	"fields": [
		{
			"default": "NONE",
			"type": "string",
			"name": "username"
		},
		{
			"default": -1,
			"type": "int",
			"name": "age"
		},
		{
			"default": "NONE",
			"type": "string",
			"name": "phone"
		},
		{
			"default": "NONE",
			"type": "string",
			"name": "housenum"
		},
		{
			"default": {},
			"type": {
				"fields": [
					{
						"default": "NONE",
						"type": "string",
						"name": "street"
					},
					{
						"default": "NONE",
						"type": "string",
						"name": "city"
					},
					{
						"default": "NONE",
						"type": "string",
						"name": "state_prov"
					},
					{
						"default": "NONE",
						"type": "string",
						"name": "country"
					},
					{
						"default": "NONE",
						"type": "string",
						"name": "zip"
					}
				],
				"type": "record",
				"name": "mailing_address"
			},
			"name": "address"
		}
	]
}
"#;

const RAW_ADDRESS_SCHEMA: &str = r#"
{
	"fields": [
		{
			"default": "NONE",
			"type": "string",
			"name": "street"
		},
		{
			"default": "NONE",
			"type": "string",
			"name": "city"
		},
		{
			"default": "NONE",
			"type": "string",
			"name": "state_prov"
		},
		{
			"default": "NONE",
			"type": "string",
			"name": "country"
		},
		{
			"default": "NONE",
			"type": "string",
			"name": "zip"
		}
	],
	"type": "record",
	"name": "mailing_address"
}
"#;

fn make_big_record() -> anyhow::Result<(apache_avro::Schema, apache_avro::types::Value)> {
	let big_schema = apache_avro::Schema::parse_str(RAW_BIG_SCHEMA)?;
	let address_schema = apache_avro::Schema::parse_str(RAW_ADDRESS_SCHEMA)?;
	let mut address = apache_avro::types::Record::new(&address_schema).unwrap();
	address.put("street", "street");
	address.put("city", "city");
	address.put("state_prov", "state_prov");
	address.put("country", "country");
	address.put("zip", "zip");

	let big_record = {
		let mut big_record = apache_avro::types::Record::new(&big_schema).unwrap();
		big_record.put("username", "username");
		big_record.put("age", 10i32);
		big_record.put("phone", "000000000");
		big_record.put("housenum", "0000");
		big_record.put("address", address);
		big_record.into()
	};

	Ok((big_schema, big_record))
}

#[derive(serde::Deserialize)]
#[allow(unused)]
struct BigStruct<'a> {
	username: &'a str,
	age: i32,
	phone: &'a str,
	housenum: &'a str,
	address: Address<'a>,
}

#[derive(serde::Deserialize)]
#[allow(unused)]
struct Address<'a> {
	street: &'a str,
	city: &'a str,
	state_prov: &'a str,
	country: &'a str,
	zip: &'a str,
}

#[derive(serde::Deserialize)]
#[allow(unused)]
struct BigStructApache {
	username: String,
	age: i32,
	phone: String,
	housenum: String,
	address: AddressApache,
}

#[derive(serde::Deserialize)]
#[allow(unused)]
struct AddressApache {
	street: String,
	city: String,
	state_prov: String,
	country: String,
	zip: String,
}

#[test]
fn big_record() {
	let (schema, record) = make_big_record().unwrap();
	let datum = apache_avro::to_avro_datum(&schema, record).unwrap();
	let value = apache_avro::from_avro_datum(&schema, &mut &*datum, None).unwrap();
	let _deserialized: BigStructApache = apache_avro::from_value(&value).unwrap();
	let fast_schema = serde_avro_fast::Schema::from_apache_schema(&schema).unwrap();
	let my_big: BigStruct = serde_avro_fast::from_datum_slice(&datum, &fast_schema).unwrap();
	assert_eq!(my_big.address.state_prov, "state_prov");
}
