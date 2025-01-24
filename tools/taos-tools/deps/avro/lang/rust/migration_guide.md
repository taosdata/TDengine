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

# Migration Guide
## Unreleased
All changes are backward-compatible so far.

# 0.13.0
All changes are backward compatible.

# 0.12.0
All changes are backward compatible.

## 0.11.0
- A custom `Error` enum has been introduced to replace all existing errors and
  the `failure` crate has been replaced by `thiserror`.

  This means that all public functions returning `Result<T, failure::Error>`
  will now return `Result<T, avro::Error>` and that you can pattern match on
  `Error` variants if you want to gather more information about the error.

  For example, code that used to be like this:
  ```rust
  match decoded {
      Ok(msg) => Ok(msg.to_string()),
      // assuming you were reading a Duration
      Err(ref e) => match e.downcast_ref::<SchemaResolutionError>() {
          Some(_) => Ok("default".to_string()),
          None => Err(format!("Unexpected error: {}", e)),
      },
  }
  ```
  now becomes:
  ```rust
  match decoded {
      Ok(msg) => Ok(msg.to_string()),
      Err(Error::ReadDuration(_)) => Ok("default".to_string()),
      Err(e) => Err(format!("Unexpected error: {}", e)),
  }
  ```

  Please note that all instances of:
  - `DecodeError`
  - `ValidationError`
  - `DeError`
  - `SerError`
  - `ParseSchemaError`
  - `SchemaResolutionError`

  must be replaced by `Error`.

- The `ToAvro` trait has been deprecated in favor of `From<T>` for `Value` implementations.

  Code like the following:
  ```rust
  use crate::types::{Record, ToAvro, Value};

  let expected: Value = record.avro();
  ```

  should be updated to:

  ```rust
  use crate::types::{Record, Value};

  let expected: Value = record.into();
  ```

  Using the `ToAvro` trait will result in a deprecation warning. The trait will
  be removed in future versions.

- The `digest` crate has been updated to version `0.9`. If you were using the
  `digest::Digest` trait from version `0.8`, you must update to the one defined
  in `0.9`.

## 0.10.0
- `Writer::into_inner()` now calls `flush()` and returns a `Result`.

  This means that code like
  ```rust
  writer.append_ser(test)?;
  writer.flush()?;
  let input = writer.into_inner();
  ```

  can be simplified into
  ```rust
  writer.append_ser(test)?;
  let input = writer.into_inner()?;
  ```
  There is no harm in leaving old calls to `flush()` around.
