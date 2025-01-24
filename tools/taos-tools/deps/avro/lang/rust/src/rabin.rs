// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Implementation of the Rabin fingerprint algorithm
use byteorder::{ByteOrder, LittleEndian};
use digest::{consts::U8, generic_array::GenericArray, FixedOutput, Reset, Update};
use lazy_static::lazy_static;

const EMPTY: i64 = -4513414715797952619;

lazy_static! {
    static ref FPTABLE: [i64; 256] = {
        let mut fp_table: [i64; 256] = [0; 256];
        for i in 0..256 {
            let mut fp = i;
            for _ in 0..8 {
                fp = (fp as u64 >> 1) as i64 ^ (EMPTY & -(fp & 1));
            }
            fp_table[i as usize] = fp
        }
        fp_table
    };
}

/// Implementation of the Rabin fingerprint algorithm using the Digest trait as described in [schema_fingerprints](https://avro.apache.org/docs/current/spec.html#schema_fingerprints).
///
/// The digest is returned as the 8-byte little-endian encoding of the Rabin hash.
/// This is what is used for avro [single object encoding](https://avro.apache.org/docs/current/spec.html#single_object_encoding)
///
/// ```rust
/// use avro_rs::rabin::Rabin;
/// use digest::Digest;
/// use hex_literal::hex;
///
/// // create the Rabin hasher
/// let mut hasher = Rabin::new();
///
/// // add the data
/// hasher.update(b"hello world");
///
/// // read hash digest and consume hasher
/// let result = hasher.finalize();
///
/// assert_eq!(result[..], hex!("60335ba6d0415528"));
/// ```
///
/// To convert the digest to the commonly used 64-bit integer value, you can use the byteorder crate:
///
/// ```rust
/// # use avro_rs::rabin::Rabin;
/// # use digest::Digest;
/// # use hex_literal::hex;
///
/// # let mut hasher = Rabin::new();
///
/// # hasher.update(b"hello world");
///
/// # let result = hasher.finalize();
///
/// # assert_eq!(result[..], hex!("60335ba6d0415528"));
/// use byteorder::{ByteOrder, LittleEndian};
///
/// let i = LittleEndian::read_i64(&result.to_vec());
///
/// assert_eq!(i, 2906301498937520992)
/// ```
#[derive(Clone)]
pub struct Rabin {
    result: i64,
}

impl Default for Rabin {
    fn default() -> Self {
        Rabin { result: EMPTY }
    }
}

impl Update for Rabin {
    fn update(&mut self, input: impl AsRef<[u8]>) {
        for b in input.as_ref() {
            self.result = (self.result as u64 >> 8) as i64
                ^ FPTABLE[((self.result ^ *b as i64) & 0xff) as usize];
        }
    }
}

impl FixedOutput for Rabin {
    // 8-byte little-endian form of the i64
    // See: https://avro.apache.org/docs/current/spec.html#single_object_encoding
    type OutputSize = U8;

    fn finalize_into(self, out: &mut GenericArray<u8, Self::OutputSize>) {
        LittleEndian::write_i64(out, self.result);
    }

    fn finalize_into_reset(&mut self, out: &mut GenericArray<u8, Self::OutputSize>) {
        LittleEndian::write_i64(out, self.result);
        self.result = EMPTY;
    }
}

impl Reset for Rabin {
    fn reset(&mut self) {
        self.result = EMPTY;
    }
}

digest::impl_write!(Rabin);

#[cfg(test)]
mod tests {
    use super::Rabin;
    use byteorder::{ByteOrder, LittleEndian};
    use digest::Digest;

    // See: https://github.com/apache/avro/blob/master/share/test/data/schema-tests.txt
    #[test]
    fn test1() {
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
        ];

        let mut hasher = Rabin::new();

        for (s, fp) in data {
            hasher.update(s.as_bytes());
            let result = LittleEndian::read_i64(&hasher.finalize_reset().to_vec());
            assert_eq!(*fp, result);
        }
    }
}
