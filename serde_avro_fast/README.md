# serde_avro_fast

**An idiomatic implementation of serde/avro (de)serialization**

[![Crates.io](https://img.shields.io/crates/v/serde_avro_fast.svg)](https://crates.io/crates/serde_avro_fast)
[![License](https://img.shields.io/github/license/Ten0/serde_avro_fast)](LICENSE)

# Getting started

```rust
let schema: serde_avro_fast::Schema = r#"
{
	"namespace": "test",
	"type": "record",
	"name": "Test",
	"fields": [
		{
			"type": {
				"type": "string"
			},
			"name": "field"
		}
	]
}
"#
.parse()
.expect("Failed to parse schema");

#[derive(serde_derive::Deserialize, Debug, PartialEq)]
struct Test<'a> {
	field: &'a str,
}

let avro_datum = &[6, 102, 111, 111];
assert_eq!(
	serde_avro_fast::from_datum_slice::<Test>(avro_datum, &schema)
		.expect("Failed to deserialize"),
	Test { field: "foo" }
);
```

# An idiomatic (re)implementation of serde/avro (de)serialization

At the time of writing, the other existing libraries for [Avro](https://avro.apache.org/docs/current/specification/)
(de)serialization do tons of unnecessary allocations, `HashMap` lookups,
etc... for every record they encounter.

This version is a more idiomatic implementation, both with regards to Rust
and to [`serde`](https://crates.io/crates/serde).

It is consequently >10x more performant (cf benchmarks):

```txt
apache_avro/small       time:   [386.57 ns 387.04 ns 387.52 ns]
serde_avro_fast/small   time:   [19.367 ns 19.388 ns 19.413 ns] <- x20 improvement

apache_avro/big         time:   [1.8618 µs 1.8652 µs 1.8701 µs]
serde_avro_fast/big     time:   [165.87 ns 166.92 ns 168.09 ns] <- x11 improvement
```

It supports any schema/deserialization target type combination that came to mind, including advanced union usage with (or without) enums, as well as proper Option support. If you have an exotic use-case in mind, it typically should be supported (if it isn't, feel free to open an issue).

# Comparison with apache-avro

Aside from the already mentionned performance improvements, there are a couple major design differences:

- `Value` is removed. Deserialization is a one-step process, which is fully serde-integrated, and leverages its zero-copy features. The output struct can now borrow from the source slice.
  - Having an intermediate `Value` representation appears to be unnecessary in Rust, as the two use-cases for `Value` would seem to be:
    - Somewhat-known structure of data but still some amount of dynamic processing -> You can deserialize to somewhat-dynamic rust types, e.g. HashMap, Vec...
    - Transcoding to a different serialization format (e.g. JSON) with basically zero structural information -> This can still be achieved in a much more performant and idiomatic manner using `serde_transcode`.
  - The `Value` representation hurts performance compared to deserializing right away to the correct struct (especially when said representation involves as many allocations as that of apache-avro does).
- Reader schema concept is removed. It appeared to be unnecessary in Rust, as it is a fully statically typed language, and the deserialization hints provided by the struct through the Serde framework combined with the writer schema information give all that is necessary to construct the correct types directly, without the need for a separate schema.
  - I expect that any code that currently uses a reader schema could work out of the box with this new deserializer without the need to specify a reader schema at all.
  - If needing to convert Avro byte streams from one schema to another, this could likely be achieved simply by plugging the deserializer to the serializer through `serde_transcode`, as such serializer would combine the types provided from the original struct (or in this case, deserializer) with the schema variant to remap the values in a correct way, while preserving zero-alloc.
- Schema representation is reworked to be a pre-computed self-referential graph structure.
  - This is what allows for maximum performance when traveling it during (de)serialization operations.
