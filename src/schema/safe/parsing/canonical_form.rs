use super::*;

impl raw::SchemaNode<'_> {
	/// This function is not public because you shouldn't use that schema
	/// when transmitting the schema to other people, notably because it loses
	/// the logical types information
	/// See https://issues.apache.org/jira/browse/AVRO-1721
	fn parsing_canonical_form(&self) -> String {
		todo!()
	}
	pub(crate) fn rabin_fingerprint(&self) -> [u8; 8] {
		// TODO replace with a local implementation
		<apache_avro::rabin::Rabin as digest::Digest>::digest(&self.parsing_canonical_form()).into()
	}
}
