/// Implementation of the Rabin fingerprint algorithm using the Digest trait as described in [schema_fingerprints](https://avro.apache.org/docs/current/specification/#schema_fingerprints).
///
/// The digest is returned as the 8-byte little-endian encoding of the Rabin
/// hash. This is what is used for avro [single object encoding](https://avro.apache.org/docs/current/specification/#single-object-encoding)
#[derive(Clone)]
pub struct Rabin {
	result: i64,
}

impl Default for Rabin {
	fn default() -> Self {
		Rabin { result: EMPTY64 }
	}
}

impl Rabin {
	pub(crate) fn write(&mut self, data: &[u8]) {
		for b in data {
			self.result =
				(self.result >> 8) ^ fp_table()[((self.result ^ *b as i64) & 0xFF) as usize];
		}
	}

	pub(crate) fn finish(self) -> [u8; 8] {
		self.result.to_le_bytes()
	}
}

impl std::fmt::Write for Rabin {
	fn write_str(&mut self, s: &str) -> std::fmt::Result {
		self.write(s.as_bytes());
		Ok(())
	}
}

const EMPTY64: i64 = -4513414715797952619;

fn fp_table() -> &'static [i64; 256] {
	static FP_TABLE: std::sync::OnceLock<[i64; 256]> = std::sync::OnceLock::new();
	FP_TABLE.get_or_init(|| {
		let mut fp_table: [i64; 256] = [0; 256];
		for i in 0..256 {
			let mut fp: i64 = i;
			for _ in 0..8 {
				fp = (fp >> 1) ^ (EMPTY64 & -(fp & 1));
			}
			fp_table[i as usize] = fp;
		}
		fp_table
	})
}

#[cfg(test)]
mod tests {
	use {super::Rabin, pretty_assertions::assert_eq};

	#[test]
	fn test() {
		let data: &[(&str, i64)] = &[
			(r#""null""#, 7195948357588979594),
			(r#""boolean""#, -6970731678124411036),
			(
				r#"{"name":"foo","type":"fixed","size":15}"#,
				1756455273707447556,
			),
			(
				r#"{"name":"PigValue","type":"record","fields":[{"name":"value","type":["null","int","long","PigValue"]}]}"#,
				-1759257747318642341,
			),
			("hello world", 2906301498937520992),
		];

		let mut hasher = Rabin::default();

		for (s, fp) in data {
			hasher.write(s.as_bytes());
			let res: &[u8] = &hasher.finish();
			let result = i64::from_le_bytes(res.try_into().unwrap());
			assert_eq!(*fp, result);
		}
	}
}
