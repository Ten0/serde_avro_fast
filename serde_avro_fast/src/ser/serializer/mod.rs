mod blocks;
mod decimal;
mod extract_for_duration;
mod seq_or_tuple;
mod struct_or_map;

use super::*;

use {
	blocks::BlockWriter,
	seq_or_tuple::SerializeSeqOrTupleOrTupleStruct,
	struct_or_map::{SerializeMapAsRecordOrMapOrDuration, SerializeStructAsRecordOrMapOrDuration},
};

/// Can't be instantiated directly - has to be constructed from a
/// [`SerializerState`]
pub struct DatumSerializer<'r, 'c, 's, W> {
	pub(super) state: &'r mut SerializerState<'c, 's, W>,
	pub(super) schema_node: &'s SchemaNode<'s>,
}

impl<'r, 'c, 's, W: Write> Serializer for DatumSerializer<'r, 'c, 's, W> {
	type Ok = ();
	type Error = SerError;

	type SerializeSeq = SerializeSeqOrTupleOrTupleStruct<'r, 'c, 's, W>;
	type SerializeTuple = SerializeSeqOrTupleOrTupleStruct<'r, 'c, 's, W>;
	type SerializeTupleStruct = SerializeSeqOrTupleOrTupleStruct<'r, 'c, 's, W>;
	type SerializeTupleVariant = SerializeSeqOrTupleOrTupleStruct<'r, 'c, 's, W>;
	type SerializeMap = SerializeMapAsRecordOrMapOrDuration<'r, 'c, 's, W>;
	type SerializeStruct = SerializeStructAsRecordOrMapOrDuration<'r, 'c, 's, W>;
	type SerializeStructVariant = SerializeStructAsRecordOrMapOrDuration<'r, 'c, 's, W>;

	fn serialize_bool(self, v: bool) -> Result<Self::Ok, Self::Error> {
		match self.schema_node {
			SchemaNode::Boolean => self
				.state
				.writer
				.write_all(&[v as u8])
				.map_err(SerError::io),
			SchemaNode::Union(union) => {
				self.serialize_union_unnamed(union, UnionVariantLookupKey::Boolean, |ser| {
					ser.serialize_bool(v)
				})
			}
			_ => Err(SerError::custom(format_args!(
				"Could not serialize bool to {:?}",
				self.schema_node
			))),
		}
	}

	fn serialize_i8(self, v: i8) -> Result<Self::Ok, Self::Error> {
		self.serialize_integer(v)
	}

	fn serialize_i16(self, v: i16) -> Result<Self::Ok, Self::Error> {
		self.serialize_integer(v)
	}

	fn serialize_i32(self, v: i32) -> Result<Self::Ok, Self::Error> {
		self.serialize_integer(v)
	}

	fn serialize_i64(self, v: i64) -> Result<Self::Ok, Self::Error> {
		self.serialize_integer(v)
	}

	fn serialize_i128(self, v: i128) -> Result<Self::Ok, Self::Error> {
		self.serialize_integer(v)
	}

	fn serialize_u8(self, v: u8) -> Result<Self::Ok, Self::Error> {
		self.serialize_integer(v)
	}

	fn serialize_u16(self, v: u16) -> Result<Self::Ok, Self::Error> {
		self.serialize_integer(v)
	}

	fn serialize_u32(self, v: u32) -> Result<Self::Ok, Self::Error> {
		self.serialize_integer(v)
	}

	fn serialize_u64(self, v: u64) -> Result<Self::Ok, Self::Error> {
		self.serialize_integer(v)
	}

	fn serialize_u128(self, v: u128) -> Result<Self::Ok, Self::Error> {
		self.serialize_integer(v)
	}

	fn serialize_f32(self, v: f32) -> Result<Self::Ok, Self::Error> {
		match self.schema_node {
			SchemaNode::Float => self
				.state
				.writer
				.write_all(&v.to_le_bytes())
				.map_err(SerError::io),
			SchemaNode::Double => Err(SerError::custom(
				"Attempting to serialize a f32 as Avro Double - \
					the receiver seems to be expecting higher precision, please use f64",
			)),
			SchemaNode::Union(union) => {
				self.serialize_union_unnamed(union, UnionVariantLookupKey::Float4, |ser| {
					ser.serialize_f32(v)
				})
			}
			_ => Err(SerError::custom(format_args!(
				"Could not serialize f32 to {:?}",
				self.schema_node
			))),
		}
	}

	fn serialize_f64(self, v: f64) -> Result<Self::Ok, Self::Error> {
		match self.schema_node {
			SchemaNode::Double => self
				.state
				.writer
				.write_all(&v.to_le_bytes())
				.map_err(SerError::io),
			SchemaNode::Float => self
				.state
				.writer
				.write_all(&(v as f32).to_le_bytes())
				.map_err(SerError::io),
			SchemaNode::Decimal(decimal) => {
				let rust_decimal: rust_decimal::Decimal = num_traits::FromPrimitive::from_f64(v)
					.ok_or_else(|| {
						SerError::new(
							"f64 cannot be converted to decimal for serialization as Decimal",
						)
					})?;
				decimal::serialize(self.state, decimal, rust_decimal)
			}
			SchemaNode::Union(union) => {
				self.serialize_union_unnamed(union, UnionVariantLookupKey::Float8, |ser| {
					ser.serialize_f64(v)
				})
			}
			_ => Err(SerError::custom(format_args!(
				"Could not serialize f64 to {:?}",
				self.schema_node
			))),
		}
	}

	fn serialize_char(self, v: char) -> Result<Self::Ok, Self::Error> {
		self.serialize_str(&*v.encode_utf8(&mut [0u8; 4]))
	}

	fn serialize_str(self, v: &str) -> Result<Self::Ok, Self::Error> {
		match self.schema_node {
			SchemaNode::String | SchemaNode::Bytes | SchemaNode::Uuid => {
				self.state.write_length_delimited(v.as_bytes())
			}
			SchemaNode::Enum(
				e @ Enum {
					per_name_lookup, ..
				},
			) => {
				let discriminant = per_name_lookup.get(v).copied().ok_or_else(|| {
					SerError::custom(format_args!(
						"Failed to find matching enum variant for {v:?} in {e:?}"
					))
				})?;
				self.state
					.writer
					.write_varint::<i64>(discriminant.try_into().map_err(|_| {
						SerError::new("Number does not fit i64 for encoding as Enum discriminant")
					})?)
					.map_err(SerError::io)?;
				Ok(())
			}
			SchemaNode::Fixed(Fixed { size, .. }) => {
				if *size != v.len() {
					Err(SerError::new(
						"Can't serialize str as Fixed: str's len does not match Fixed's size",
					))
				} else {
					self.state
						.writer
						.write_all(v.as_bytes())
						.map_err(SerError::io)
				}
			}
			SchemaNode::Decimal(decimal) => {
				let rust_decimal: rust_decimal::Decimal = v.parse().map_err(|parse_err| {
					SerError::custom(format_args!(
						"str cannot be converted to decimal for serialization as Decimal: {}",
						parse_err
					))
				})?;
				decimal::serialize(self.state, decimal, rust_decimal)
			}
			SchemaNode::Union(union) => {
				self.serialize_union_unnamed(union, UnionVariantLookupKey::Str, |ser| {
					ser.serialize_str(v)
				})
			}
			_ => Err(SerError::custom(format_args!(
				"Could not serialize str to {:?}",
				self.schema_node
			))),
		}
	}

	fn serialize_bytes(self, v: &[u8]) -> Result<Self::Ok, Self::Error> {
		match self.schema_node {
			SchemaNode::Bytes | SchemaNode::String => self.state.write_length_delimited(v),
			SchemaNode::Fixed(Fixed { size, .. }) => {
				if *size != v.len() {
					Err(SerError::new(
						"Can't serialize &[u8] as Fixed: slice's len does not match Fixed's size",
					))
				} else {
					self.state.writer.write_all(v).map_err(SerError::io)
				}
			}
			SchemaNode::Duration => {
				// In that case we assume that it's the raw value.
				// This is the most efficient way to deserialize it then
				// re-serialize it if you're not doing anything else with it
				if v.len() != 12 {
					Err(SerError::new(
						"&[u8] can be serialized as Duration, but only if it's of length 12. \
							We got a too long slice here.",
					))
				} else {
					self.state.writer.write_all(v).map_err(SerError::io)
				}
			}
			SchemaNode::Union(union) => {
				self.serialize_union_unnamed(union, UnionVariantLookupKey::SliceU8, |ser| {
					ser.serialize_bytes(v)
				})
			}
			_ => Err(SerError::custom(format_args!(
				"Could not serialize bytes to {:?}",
				self.schema_node
			))),
		}
	}

	fn serialize_none(self) -> Result<Self::Ok, Self::Error> {
		self.serialize_unit()
	}

	fn serialize_some<T: ?Sized>(self, value: &T) -> Result<Self::Ok, Self::Error>
	where
		T: Serialize,
	{
		// If there are union lookups to do, they can be performed
		// directly by the functions that serialize the value
		value.serialize(self)
	}

	fn serialize_unit(self) -> Result<Self::Ok, Self::Error> {
		match self.schema_node {
			SchemaNode::Null => Ok(()),
			SchemaNode::Union(union) => {
				self.serialize_union_unnamed(union, UnionVariantLookupKey::Null, |_| Ok(()))
			}
			_ => Err(SerError::custom(format_args!(
				"Could not serialize unit to {:?}",
				self.schema_node
			))),
		}
	}

	fn serialize_unit_struct(self, name: &'static str) -> Result<Self::Ok, Self::Error> {
		match self.schema_node {
			SchemaNode::Null => Ok(()),
			SchemaNode::String | SchemaNode::Bytes | SchemaNode::Enum(_) => {
				self.serialize_str(name)
			}
			SchemaNode::Union(union) => {
				self.serialize_union_unnamed(union, UnionVariantLookupKey::UnitStruct, |ser| {
					ser.serialize_unit_struct(name)
				})
			}
			_ => Err(SerError::custom(format_args!(
				"Could not serialize unit struct to {:?}",
				self.schema_node
			))),
		}
	}

	fn serialize_unit_variant(
		self,
		name: &'static str,
		variant_index: u32,
		variant: &'static str,
	) -> Result<Self::Ok, Self::Error> {
		match self.schema_node {
			SchemaNode::Null if variant == "Null" => Ok(()),
			SchemaNode::String | SchemaNode::Bytes | SchemaNode::Enum(_) => {
				self.serialize_str(variant)
			}
			SchemaNode::Union(union) => {
				self.serialize_union_unnamed(union, UnionVariantLookupKey::UnitVariant, |ser| {
					ser.serialize_unit_variant(name, variant_index, variant)
				})
			}
			_ => Err(SerError::custom(format_args!(
				"Could not serialize unit variant to {:?}",
				self.schema_node
			))),
		}
	}

	fn serialize_newtype_struct<T: ?Sized>(
		self,
		name: &'static str,
		value: &T,
	) -> Result<Self::Ok, Self::Error>
	where
		T: Serialize,
	{
		self.serialize_lookup_union_variant_by_name(name, |serializer| value.serialize(serializer))
	}

	fn serialize_newtype_variant<T: ?Sized>(
		self,
		_name: &'static str,
		_variant_index: u32,
		variant: &'static str,
		value: &T,
	) -> Result<Self::Ok, Self::Error>
	where
		T: Serialize,
	{
		self.serialize_lookup_union_variant_by_name(variant, |serializer| {
			value.serialize(serializer)
		})
	}

	fn serialize_seq(self, len: Option<usize>) -> Result<Self::SerializeSeq, Self::Error> {
		match self.schema_node {
			SchemaNode::Array(elements_schema) => Ok(SerializeSeqOrTupleOrTupleStruct::array(
				BlockWriter::new(self.state, len.unwrap_or(0))?,
				elements_schema,
			)),
			SchemaNode::Duration => {
				if len.map_or(false, |l| l != 3) {
					Err(seq_or_tuple::duration_seq_len_incorrect())
				} else {
					Ok(SerializeSeqOrTupleOrTupleStruct::duration(self.state))
				}
			}
			SchemaNode::Bytes => {
				self.state.check_allowed_slow_sequence_to_bytes()?;
				match len {
					None => Ok(SerializeSeqOrTupleOrTupleStruct::buffered_bytes(self.state)),
					Some(len) => SerializeSeqOrTupleOrTupleStruct::bytes(self.state, len),
				}
			}
			SchemaNode::Fixed(fixed) => {
				self.state.check_allowed_slow_sequence_to_bytes()?;
				if len.map_or(false, |l| l != fixed.size) {
					Err(SerError::new(
						"Could not serialize sequence, tuple or tuple struct to fixed: \
							advertised size mismatch",
					))
				} else {
					Ok(SerializeSeqOrTupleOrTupleStruct::fixed(
						self.state, fixed.size,
					))
				}
			}
			SchemaNode::Union(union) => self.serialize_union_unnamed(
				union,
				UnionVariantLookupKey::SeqOrTupleOrTupleStruct,
				|ser| ser.serialize_seq(len),
			),
			_ => Err(SerError::custom(format_args!(
				"Could not serialize sequence, tuple or tuple struct to {:?}",
				self.schema_node
			))),
		}
	}

	fn serialize_tuple(self, len: usize) -> Result<Self::SerializeTuple, Self::Error> {
		self.serialize_seq(Some(len))
	}

	fn serialize_tuple_struct(
		self,
		_name: &'static str,
		len: usize,
	) -> Result<Self::SerializeTupleStruct, Self::Error> {
		self.serialize_seq(Some(len))
	}

	fn serialize_tuple_variant(
		self,
		_name: &'static str,
		_variant_index: u32,
		variant: &'static str,
		len: usize,
	) -> Result<Self::SerializeTupleVariant, Self::Error> {
		self.serialize_lookup_union_variant_by_name(variant, |serializer| {
			serializer.serialize_seq(Some(len))
		})
	}

	fn serialize_map(self, len: Option<usize>) -> Result<Self::SerializeMap, Self::Error> {
		match *self.schema_node {
			SchemaNode::Record(ref record) => Ok(SerializeMapAsRecordOrMapOrDuration::record(
				self.state, record,
			)),
			SchemaNode::Map(map) => {
				SerializeMapAsRecordOrMapOrDuration::map(self.state, map.as_ref(), len.unwrap_or(0))
			}
			SchemaNode::Duration => {
				if len.map_or(false, |l| l != 3) {
					return Err(struct_or_map::duration_fields_incorrect());
				}
				SerializeMapAsRecordOrMapOrDuration::duration(self.state)
			}
			SchemaNode::Union(ref union) => {
				self.serialize_union_unnamed(union, UnionVariantLookupKey::StructOrMap, |ser| {
					ser.serialize_map(len)
				})
			}
			_ => Err(SerError::custom(format_args!(
				"Could not serialize map to {:?}",
				self.schema_node
			))),
		}
	}

	fn serialize_struct(
		self,
		name: &'static str,
		len: usize,
	) -> Result<Self::SerializeStruct, Self::Error> {
		self.serialize_struct_or_struct_variant(name, len)
	}

	fn serialize_struct_variant(
		self,
		_name: &'static str,
		_variant_index: u32,
		variant: &'static str,
		len: usize,
	) -> Result<Self::SerializeStructVariant, Self::Error> {
		self.serialize_struct_or_struct_variant(variant, len)
	}
}

impl<'c, 's, W: std::io::Write> SerializerState<'c, 's, W> {
	fn write_length_delimited(&mut self, data: &[u8]) -> Result<(), SerError> {
		self.writer
			.write_varint::<i64>(data.len().try_into().map_err(|_| {
				SerError::new(
					"Buffer len does not fit i64 for encoding as length-delimited field size",
				)
			})?)
			.map_err(SerError::io)?;
		self.writer.write_all(data).map_err(SerError::io)
	}

	fn check_allowed_slow_sequence_to_bytes(&self) -> Result<(), SerError> {
		if self.config.allow_slow_sequence_to_bytes {
			Ok(())
		} else {
			Err(SerError::new(
				"Sequence to bytes conversion is not allowed by default because it is much \
					slower than going through `serialize_bytes`, which can be achieved via \
					the `serde_bytes` crate. If this is not an option because e.g. you are \
					transcoding, you can enable the slow sequence-to-bytes conversion by calling \
					`allow_slow_sequence_to_bytes` on the `SerializerConfig`.",
			))
		}
	}
}

impl<'r, 'c, 's, W: Write> DatumSerializer<'r, 'c, 's, W> {
	fn serialize_union_unnamed<O>(
		self,
		union: &'s Union<'s>,
		variant_lookup: UnionVariantLookupKey,
		with_serializer: impl FnOnce(Self) -> Result<O, SerError>,
	) -> Result<O, SerError> {
		match union.per_type_lookup.unnamed(variant_lookup) {
			None => Err(SerError::custom(format_args!(
				"Could not serialize {:?} to {:?} - \
					if you need to explicit a variant because it can't be figured out \
					automatically, consider using an enum or newtype struct to \
					serialize this field",
				variant_lookup, self.schema_node
			))),
			Some((discriminant, union_node)) => {
				self.state
					.writer
					.write_varint(discriminant)
					.map_err(SerError::io)?;
				with_serializer(Self {
					state: self.state,
					schema_node: union_node,
				})
			}
		}
	}

	fn serialize_integer<N>(self, num: N) -> Result<(), SerError>
	where
		N: TryInto<i64> + TryInto<i32> + TryInto<i128>,
	{
		match self.schema_node {
			SchemaNode::Int | SchemaNode::Date | SchemaNode::TimeMillis => {
				self.state
					.writer
					.write_varint::<i32>(num.try_into().map_err(|_| {
						SerError::new("Number does not fit i32 for encoding as Int")
					})?)
					.map_err(SerError::io)?;
				Ok(())
			}
			SchemaNode::Long
			| SchemaNode::TimestampMillis
			| SchemaNode::TimestampMicros
			| SchemaNode::TimeMicros => {
				self.state
					.writer
					.write_varint::<i64>(num.try_into().map_err(|_| {
						SerError::new("Number does not fit i64 for encoding as Long")
					})?)
					.map_err(SerError::io)?;
				Ok(())
			}
			SchemaNode::Decimal(decimal) => {
				let mut n: i128 = num.try_into().map_err(|_| {
					SerError::new("Number does not fit i128 for encoding as Decimal")
				})?;
				n = 10i128
					.checked_pow(decimal.scale)
					.and_then(|pow| n.checked_mul(pow))
					.ok_or_else(|| {
						SerError::new(
							"Integer to be encoded as decimal, multiplied by 10^scale \
								is too large to fit in an i128. This is unsupported.",
						)
					})?;
				let bytes = n.to_be_bytes();
				let buf = match decimal.repr {
					DecimalRepr::Bytes => {
						let mut start = 0;
						while start < bytes.len() - 1 && bytes[start] == 0 {
							start += 1;
						}
						let buf = &bytes[start..];
						self.state
							.writer
							.write_varint::<i64>(buf.len().try_into().map_err(|_| {
								SerError::new(
									"Buffer len does not fit i64 for encoding as bytes size",
								)
							})?)
							.map_err(SerError::io)?;
						buf
					}
					DecimalRepr::Fixed(fixed) => {
						let start = bytes.len().checked_sub(fixed.size).ok_or_else(|| {
							SerError::custom("Decimals of size larger than 16 are not supported")
						})?;
						&bytes[start..]
					}
				};
				self.state.writer.write_all(buf).map_err(SerError::io)
			}
			SchemaNode::Enum(_) => {
				self.state
					.writer
					.write_varint::<i64>(num.try_into().map_err(|_| {
						SerError::new("Number does not fit i64 for encoding as Enum discriminant")
					})?)
					.map_err(SerError::io)?;
				Ok(())
			}
			SchemaNode::Union(union) => self.serialize_union_unnamed(
				union,
				match std::mem::size_of::<N>() {
					4 => UnionVariantLookupKey::Integer4,
					8 => UnionVariantLookupKey::Integer8,
					_ => UnionVariantLookupKey::Integer,
				},
				|ser| ser.serialize_integer(num),
			),
			_ => Err(SerError::custom(format_args!(
				"Could not serialize integer to {:?}",
				self.schema_node
			))),
		}
	}

	fn serialize_lookup_union_variant_by_name<O>(
		self,
		variant_name: &str,
		f: impl FnOnce(DatumSerializer<'r, 'c, 's, W>) -> Result<O, SerError>,
	) -> Result<O, SerError> {
		match self.schema_node {
			SchemaNode::Union(union) => match union.per_type_lookup.named(variant_name) {
				None => {
					// Variant name doesn't hint us, fallback to trying to deduce from serialized
					// type
					f(self)
				}
				Some((discriminant, schema_node)) => {
					self.state
						.writer
						.write_varint(discriminant)
						.map_err(SerError::io)?;
					f(DatumSerializer {
						state: self.state,
						schema_node,
					})
				}
			},
			_ => f(self),
		}
	}

	fn serialize_struct_or_struct_variant(
		self,
		variant_or_struct_name: &str,
		len: usize,
	) -> Result<SerializeStructAsRecordOrMapOrDuration<'r, 'c, 's, W>, SerError> {
		self.serialize_lookup_union_variant_by_name(variant_or_struct_name, |serializer| {
			match *serializer.schema_node {
				SchemaNode::Record(ref record) => Ok(
					SerializeStructAsRecordOrMapOrDuration::record(serializer.state, record),
				),
				SchemaNode::Map(map) => Ok(SerializeStructAsRecordOrMapOrDuration::map(
					serializer.state,
					map.as_ref(),
					len,
				)?),
				SchemaNode::Duration => {
					if len != 3 {
						return Err(struct_or_map::duration_fields_incorrect());
					}
					SerializeStructAsRecordOrMapOrDuration::duration(serializer.state)
				}
				SchemaNode::Union(ref union) => serializer.serialize_union_unnamed(
					union,
					UnionVariantLookupKey::StructOrMap,
					|ser| ser.serialize_struct_or_struct_variant(variant_or_struct_name, len),
				),
				_ => Err(SerError::custom(format_args!(
					"Could not serialize struct to {:?}",
					serializer.schema_node
				))),
			}
		})
	}
}
