use super::safe::SchemaNode as SafeSchemaNode;

/// The most performant and easiest to navigate version of the schema.
///
/// It is however built using `unsafe`, so it can only be built from [its safe counterpart](crate::schema::safe::Schema)
/// because it makes the conversion code simple enough that we can reasonably guarantee its correctness despite the
/// usage of `unsafe`.
#[derive(Debug)]
pub struct Schema {
	// First node in the array is considered to be the root
	//
	// This lifetime is fake, but since all elements have to be accessed by the root
	// which will downcast it and we never push anything more in there (which would cause
	// reallocation and invalidate all nodes) this is correct.
	nodes: Vec<SchemaNode<'static>>,
}

impl Schema {
	// this downgrades the fake lifetime in a way that makes it correct
	pub fn root<'a>(&'a self) -> &'a SchemaNode<'a> {
		&self.nodes[0]
	}
}

/// Represents any valid Avro schema
/// More information about Avro schemas can be found in the
/// [Avro Specification](https://avro.apache.org/docs/current/spec.html#schemas)
#[derive(Debug)]
pub enum SchemaNode<'a> {
	/// A `null` Avro schema.
	Null,
	/// A `boolean` Avro schema.
	Boolean,
	/// An `int` Avro schema.
	Int,
	/// A `long` Avro schema.
	Long,
	/// A `float` Avro schema.
	Float,
	/// A `double` Avro schema.
	Double,
	/// A `bytes` Avro schema.
	/// `Bytes` represents a sequence of 8-bit unsigned bytes.
	Bytes,
	/// A `string` Avro schema.
	/// `String` represents a unicode character sequence.
	String,
	/// A `array` Avro schema. Avro arrays are required to have the same type for each element.
	/// This variant holds the `Schema` for the array element type.
	Array(&'a SchemaNode<'a>),
	/// A `map` Avro schema.
	/// `Map` holds a pointer to the `Schema` of its values, which must all be the same schema.
	/// `Map` keys are assumed to be `string`.
	Map(&'a SchemaNode<'a>),
	/// A `union` Avro schema.
	Union(UnionSchema<'a>),
	/// A `record` Avro schema.
	///
	/// The `lookup` table maps field names to their position in the `Vec`
	/// of `fields`.
	Record(RecordSchema<'a>),
	/// An `enum` Avro schema.
	Enum { symbols: Vec<String> },
	/// A `fixed` Avro schema.
	Fixed { size: usize },
	/// Logical type which represents `Decimal` values. The underlying type is serialized and
	/// deserialized as `Schema::Bytes` or `Schema::Fixed`.
	///
	/// `scale` defaults to 0 and is an integer greater than or equal to 0 and `precision` is an
	/// integer greater than 0.
	Decimal {
		precision: usize,
		scale: usize,
		inner: &'a SchemaNode<'a>,
	},
	/// A universally unique identifier, annotating a string.
	Uuid,
	/// Logical type which represents the number of days since the unix epoch.
	/// Serialization format is `Schema::Int`.
	Date,
	/// The time of day in number of milliseconds after midnight with no reference any calendar,
	/// time zone or date in particular.
	TimeMillis,
	/// The time of day in number of microseconds after midnight with no reference any calendar,
	/// time zone or date in particular.
	TimeMicros,
	/// An instant in time represented as the number of milliseconds after the UNIX epoch.
	TimestampMillis,
	/// An instant in time represented as the number of microseconds after the UNIX epoch.
	TimestampMicros,
	/// An amount of time defined by a number of months, days and milliseconds.
	Duration,
}

#[derive(Clone, Debug)]
pub struct UnionSchema<'a> {
	pub variants: Vec<&'a SchemaNode<'a>>,
}

#[derive(Clone, Debug)]
pub struct RecordSchema<'a> {
	pub fields: Vec<RecordField<'a>>,
}

#[derive(Clone, Debug)]
pub struct RecordField<'a> {
	pub name: String,
	pub schema: &'a SchemaNode<'a>,
}

impl From<super::safe::Schema> for Schema {
	fn from(safe: super::safe::Schema) -> Self {
		// This allocation should never be moved otherwise references will become invalid
		let mut ret = Self {
			nodes: (0..safe.nodes.len()).map(|_| SchemaNode::Null).collect(),
		};
		let len = ret.nodes.len();
		// Let's be extra-sure (second condition is for calls to add)
		assert!(len > 0 && len == safe.nodes.len() && len <= (isize::MAX as usize));
		let storage_start_ptr = ret.nodes.as_mut_ptr();
		// unsafe closure used below in unsafe block
		let key_to_node = |schema_key: super::safe::SchemaKey| -> &'static SchemaNode {
			let idx = schema_key.idx;
			assert!(idx < len);
			unsafe { &*(storage_start_ptr.add(schema_key.idx)) }
		};
		let mut curr_storage_node_ptr = storage_start_ptr;
		for safe_node in safe.nodes {
			// Safety:
			// - The nodes we create here are never moving in memory since the entire vec is preallocated, and even when
			//   moving a vec, the pointed space doesn't move.
			// - The fake `'static` lifetimes are always downgraded before being made available.
			// - We only use pointers from the point at which we call `as_mut_ptr` so the compiler will not have
			//   aliasing constraints.
			// - We don't dereference the references we create in key_to_node until the original &mut is released and
			//   well out of scope (we don't dereference them at all in this function).
			unsafe {
				*curr_storage_node_ptr = match safe_node {
					SafeSchemaNode::Null => SchemaNode::Null,
					SafeSchemaNode::Boolean => SchemaNode::Boolean,
					SafeSchemaNode::Int => SchemaNode::Int,
					SafeSchemaNode::Long => SchemaNode::Long,
					SafeSchemaNode::Float => SchemaNode::Float,
					SafeSchemaNode::Double => SchemaNode::Double,
					SafeSchemaNode::Bytes => SchemaNode::Bytes,
					SafeSchemaNode::String => SchemaNode::String,
					SafeSchemaNode::Array(schema_key) => SchemaNode::Array(key_to_node(schema_key)),
					SafeSchemaNode::Map(schema_key) => SchemaNode::Map(key_to_node(schema_key)),
					SafeSchemaNode::Union(union_schema) => SchemaNode::Union(UnionSchema {
						variants: union_schema
							.variants
							.into_iter()
							.map(|schema_key| key_to_node(schema_key))
							.collect(),
					}),
					SafeSchemaNode::Record(record_schema) => SchemaNode::Record(RecordSchema {
						fields: record_schema
							.fields
							.into_iter()
							.map(|f| RecordField {
								name: f.name,
								schema: key_to_node(f.schema),
							})
							.collect(),
					}),
					SafeSchemaNode::Enum { symbols } => SchemaNode::Enum { symbols },
					SafeSchemaNode::Fixed { size } => SchemaNode::Fixed { size },
					SafeSchemaNode::Decimal {
						precision,
						scale,
						inner,
					} => SchemaNode::Decimal {
						precision,
						scale,
						inner: key_to_node(inner),
					},
					SafeSchemaNode::Uuid => SchemaNode::Uuid,
					SafeSchemaNode::Date => SchemaNode::Date,
					SafeSchemaNode::TimeMillis => SchemaNode::TimeMillis,
					SafeSchemaNode::TimeMicros => SchemaNode::TimeMicros,
					SafeSchemaNode::TimestampMillis => SchemaNode::TimestampMillis,
					SafeSchemaNode::TimestampMicros => SchemaNode::TimestampMicros,
					SafeSchemaNode::Duration => SchemaNode::Duration,
				};
				curr_storage_node_ptr = curr_storage_node_ptr.add(1);
			};
		}
		ret
	}
}
