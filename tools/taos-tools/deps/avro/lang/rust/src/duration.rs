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

use byteorder::LittleEndian;
use zerocopy::U32;

/// A struct representing duration that hides the details of endianness and conversion between
/// platform-native u32 and byte arrays.
#[derive(Debug, Copy, Clone, PartialEq)]
pub struct Duration {
    months: Months,
    days: Days,
    millis: Millis,
}

#[derive(Debug, Copy, Clone, PartialEq)]
pub struct Months(U32<LittleEndian>);

impl Months {
    pub fn new(months: u32) -> Self {
        Self(U32::new(months))
    }
}

impl From<Months> for u32 {
    fn from(days: Months) -> Self {
        days.0.get()
    }
}

impl From<[u8; 4]> for Months {
    fn from(bytes: [u8; 4]) -> Self {
        Self(U32::from(bytes))
    }
}

impl AsRef<[u8; 4]> for Months {
    fn as_ref(&self) -> &[u8; 4] {
        self.0.as_ref()
    }
}

#[derive(Debug, Copy, Clone, PartialEq)]
pub struct Days(U32<LittleEndian>);

impl Days {
    pub fn new(days: u32) -> Self {
        Self(U32::new(days))
    }
}

impl From<Days> for u32 {
    fn from(days: Days) -> Self {
        days.0.get()
    }
}

impl From<[u8; 4]> for Days {
    fn from(bytes: [u8; 4]) -> Self {
        Self(U32::from(bytes))
    }
}

impl AsRef<[u8; 4]> for Days {
    fn as_ref(&self) -> &[u8; 4] {
        self.0.as_ref()
    }
}

#[derive(Debug, Copy, Clone, PartialEq)]
pub struct Millis(U32<LittleEndian>);

impl Millis {
    pub fn new(millis: u32) -> Self {
        Self(U32::new(millis))
    }
}

impl From<Millis> for u32 {
    fn from(days: Millis) -> Self {
        days.0.get()
    }
}

impl From<[u8; 4]> for Millis {
    fn from(bytes: [u8; 4]) -> Self {
        Self(U32::from(bytes))
    }
}

impl AsRef<[u8; 4]> for Millis {
    fn as_ref(&self) -> &[u8; 4] {
        self.0.as_ref()
    }
}

impl Duration {
    /// Construct a new `Duration`.
    pub fn new(months: Months, days: Days, millis: Millis) -> Self {
        Self {
            months,
            days,
            millis,
        }
    }

    /// Return the number of months in this duration.
    pub fn months(&self) -> Months {
        self.months
    }

    /// Return the number of days in this duration.
    pub fn days(&self) -> Days {
        self.days
    }

    /// Return the number of milliseconds in this duration.
    pub fn millis(&self) -> Millis {
        self.millis
    }
}

impl From<Duration> for [u8; 12] {
    fn from(duration: Duration) -> Self {
        let mut bytes = [0u8; 12];
        bytes[0..4].copy_from_slice(duration.months.as_ref());
        bytes[4..8].copy_from_slice(duration.days.as_ref());
        bytes[8..12].copy_from_slice(duration.millis.as_ref());
        bytes
    }
}

impl From<[u8; 12]> for Duration {
    fn from(bytes: [u8; 12]) -> Self {
        Self {
            months: Months::from([bytes[0], bytes[1], bytes[2], bytes[3]]),
            days: Days::from([bytes[4], bytes[5], bytes[6], bytes[7]]),
            millis: Millis::from([bytes[8], bytes[9], bytes[10], bytes[11]]),
        }
    }
}
