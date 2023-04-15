use super::*;

pub struct DatumSerializer<'r, 's, W> {
	pub(super) state: &'r mut SerializerState<'s, W>,
	pub(super) schema_node: &'s SchemaNode<'s>,
}

impl<'r, 's, W: Write> Serializer for DatumSerializer<'r, 's, W> {
	type Ok = ();
	type Error = SerError;

	//type SerializeSeq;
	//type SerializeTuple;
	//type SerializeTupleStruct;
	//type SerializeTupleVariant;
	//type SerializeMap;
	//type SerializeStruct;
	//type SerializeStructVariant;

	serde_serializer_quick_unsupported::serializer_unsupported! {
		err = (<Self::Error as serde::ser::Error>::custom("Unexpected input"));
		newtype_variant seq tuple tuple_struct tuple_variant map struct
		struct_variant
	}

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
				.write_all(&(v as f64).to_le_bytes())
				.map_err(SerError::io),
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
				self.write_length_delimited(v.as_bytes())
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
			SchemaNode::Bytes | SchemaNode::String => self.write_length_delimited(v),
			SchemaNode::Fixed(Fixed { size, .. }) => {
				if *size != v.len() {
					Err(SerError::new(
						"Can't serialize &[u8] as Fixed: slice's len does not match Fixed's size",
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
			SchemaNode::String | SchemaNode::Enum(_) => self.serialize_str(name),
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
			SchemaNode::String | SchemaNode::Enum(_) => self.serialize_str(variant),
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
		_name: &'static str,
		value: &T,
	) -> Result<Self::Ok, Self::Error>
	where
		T: Serialize,
	{
		value.serialize(self)
	}

	/*fn serialize_newtype_variant<T: ?Sized>(
		self,
		name: &'static str,
		variant_index: u32,
		variant: &'static str,
		value: &T,
	) -> Result<Self::Ok, Self::Error>
	where
		T: Serialize,
	{
		todo!()
	}

	fn serialize_seq(self, len: Option<usize>) -> Result<Self::SerializeSeq, Self::Error> {
		todo!()
	}

	fn serialize_tuple(self, len: usize) -> Result<Self::SerializeTuple, Self::Error> {
		todo!()
	}

	fn serialize_tuple_struct(
		self,
		name: &'static str,
		len: usize,
	) -> Result<Self::SerializeTupleStruct, Self::Error> {
		todo!()
	}

	fn serialize_tuple_variant(
		self,
		name: &'static str,
		variant_index: u32,
		variant: &'static str,
		len: usize,
	) -> Result<Self::SerializeTupleVariant, Self::Error> {
		todo!()
	}

	fn serialize_map(self, len: Option<usize>) -> Result<Self::SerializeMap, Self::Error> {
		todo!()
	}

	fn serialize_struct(
		self,
		name: &'static str,
		len: usize,
	) -> Result<Self::SerializeStruct, Self::Error> {
		todo!()
	}

	fn serialize_struct_variant(
		self,
		name: &'static str,
		variant_index: u32,
		variant: &'static str,
		len: usize,
	) -> Result<Self::SerializeStructVariant, Self::Error> {
		todo!()
	}*/
}

impl<'r, 's, W: Write> DatumSerializer<'r, 's, W> {
	fn serialize_union_unnamed(
		self,
		union: &'s Union<'s>,
		variant_lookup: UnionVariantLookupKey,
		with_serializer: impl FnOnce(Self) -> Result<(), SerError>,
	) -> Result<(), SerError> {
		match union.per_type_lookup.unnamed(variant_lookup) {
			None => Err(SerError::custom(format_args!(
				"Could not serialize {:?} to {:?} - \
					if you need to explicit a variant because it can't be figured out \
					automatically, consider using a (maybe single-variant) enum to \
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
					DecimalRepr::Fixed(Fixed { size, .. }) => {
						let start = bytes.len().checked_sub(size).ok_or_else(|| {
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

	fn write_length_delimited(self, data: &[u8]) -> Result<(), SerError> {
		self.state
			.writer
			.write_varint::<i64>(data.len().try_into().map_err(|_| {
				SerError::new(
					"Buffer len does not fit i64 for encoding as length-delimited field size",
				)
			})?)
			.map_err(SerError::io)?;
		self.state.writer.write_all(data).map_err(SerError::io)
	}
}
