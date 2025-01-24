<!---
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->

# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](http://keepachangelog.com/en/1.0.0/)
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## Unreleased

## [0.13.0] - 2021-01-29
### Added
- Support for parsing a list of schemas which may have cross dependencies (#173)

### Changed
- Allow Value::Bytes to be assigned to Schema::Fixed (#171)

### Fixed
- Allow resolution of union schemas with logical types (#176)


## [0.12.0] - 2020-11-27
### Added
- Added support for the Rabin fingerprint (#157)

### Fixed
- Strip more fields in PCF and fix panic (#164)

## [0.11.0] - 2020-08-13
### Changed
- Introduce custom Error enum to replace all existing errors (backward-incompatible) (#135)
- Swapped failure for thiserror (backward-incompatible) (#135)
- Update digest crate and digest::Digest trait to 0.9 (backward-incompatible with digest::Digest 0.8) (#133)
- Replace some manual from_str implementations with strum (#136)
- Handle logical types in canonical form schemas (#144)
- Move to specific error variants for errors (#146)

### Added
- Support to convert avro value to json value (#155)
- Implement deserialize for Uuid (#153)

## Deprecated
- Deprecate ToAvro in favor of From<T> for Value implementations (#137)

## [0.10.0] - 2020-05-31
### Changed
- Writer::into_inner() now calls flush() and returns a Result (backward-incompatible)

### Added
- Add utility for schema compatibility check

## [0.9.1] - 2020-05-02
### Changed
- Port benchmarks to criterion

### Fixed
- Fix bug in the reader buffer length

## [0.9.0] - 2020-04-24
### Added
- Add support for logical types
- Make writer block size configurable via builder pattern

## [0.8.0] - 2020-04-15
### Added
- Partial rust enum serialization/deserialization support

## [0.7.0] - 2020-02-16
### Added
- Export de::Error and ser::Error as DeError and SerError

### Fixed
- Fix union resolution of default values

## [0.6.6] - 2019-12-22
### Fixed
- Negative block lengths are not handled

## [0.6.5] - 2019-03-09
### Fixed
- Allow Array(Int) to be converted to Bytes
- Fix enum type deserialization bug

## [0.6.4] - 2018-12-24
### Fixed
- Variable-length encoding for big i64 numbers

## [0.6.3]- 2018-12-19
### Added
- Schema fingerprint (md5, sha256) generation

## [0.6.2]- 2018-12-04
### Fixed
- Snappy codec

## [0.6.1]- 2018-10-07
### Fixed
- Encoding of i32/i64

## [0.6.0]- 2018-08-11
### Added
- impl Send+Sync for Schema (backwards-incompatible)

## [0.5.0] - 2018-08-06
### Added
- A maximum allocation size when decoding
- Support for Parsing Canonical Form
- `to_value` to serialize anything that implements Serialize into a Value
- Full support for union types (non-backwards compatible)
### Fixed
- Encoding of empty containers (array/map)

## [0.4.1] - 2018-06-17
### Changed
- Implemented clippy suggestions

## [0.4.0] - 2018-06-17
### Changed
- Many performance improvements to both encoding and decoding
### Added
- New public method extend_from_slice for Writer
- serde_json benchmark for comparison
- bench_from_file function and a file from the goavro repository for comparison

## [0.3.2] - 2018-06-07
### Added
- Some missing serialization fields for Schema::Record

## [0.3.1] - 2018-06-02
### Fixed
- Encode/decode Union values with a leading zig-zag long

## [0.3.0] - 2018-05-29
### Changed
- Move from string as errors to custom fail types

### Fixed
- Avoid reading the first item over and over in Reader

## [0.2.0] - 2018-05-22
### Added
- `from_avro_datum` to decode Avro-encoded bytes into a `Value`
- Documentation for `from_value`

## [0.1.1] - 2018-05-16
- Initial release
