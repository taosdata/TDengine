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

use crate::{AvroResult, Error};
use num_bigint::{BigInt, Sign};

#[derive(Debug, Clone)]
pub struct Decimal {
    value: BigInt,
    len: usize,
}

// We only care about value equality, not byte length. Can two equal `BigInt`s have two different
// byte lengths?
impl PartialEq for Decimal {
    fn eq(&self, other: &Self) -> bool {
        self.value == other.value
    }
}

impl Decimal {
    pub(crate) fn len(&self) -> usize {
        self.len
    }

    fn to_vec(&self) -> AvroResult<Vec<u8>> {
        self.to_sign_extended_bytes_with_len(self.len)
    }

    pub(crate) fn to_sign_extended_bytes_with_len(&self, len: usize) -> AvroResult<Vec<u8>> {
        let sign_byte = 0xFF * u8::from(self.value.sign() == Sign::Minus);
        let mut decimal_bytes = vec![sign_byte; len];
        let raw_bytes = self.value.to_signed_bytes_be();
        let num_raw_bytes = raw_bytes.len();
        let start_byte_index = len.checked_sub(num_raw_bytes).ok_or(Error::SignExtend {
            requested: len,
            needed: num_raw_bytes,
        })?;
        decimal_bytes[start_byte_index..].copy_from_slice(&raw_bytes);
        Ok(decimal_bytes)
    }
}

impl std::convert::TryFrom<&Decimal> for Vec<u8> {
    type Error = Error;

    fn try_from(decimal: &Decimal) -> Result<Self, Self::Error> {
        decimal.to_vec()
    }
}

impl<T: AsRef<[u8]>> From<T> for Decimal {
    fn from(bytes: T) -> Self {
        let bytes_ref = bytes.as_ref();
        Self {
            value: BigInt::from_signed_bytes_be(bytes_ref),
            len: bytes_ref.len(),
        }
    }
}
