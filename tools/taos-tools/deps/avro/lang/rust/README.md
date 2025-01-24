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

# avro-rs

[![Latest Version](https://img.shields.io/crates/v/avro-rs.svg)](https://crates.io/crates/avro-rs)
[![Continuous Integration](https://github.com/flavray/avro-rs/workflows/Continuous%20Integration/badge.svg)](https://github.com/flavray/avro-rs/actions)
[![Latest Documentation](https://docs.rs/avro-rs/badge.svg)](https://docs.rs/avro-rs)
[![MIT licensed](https://img.shields.io/badge/license-MIT-blue.svg)](https://github.com/flavray/avro-rs/blob/main/LICENSE)

A library for working with [Apache Avro](https://avro.apache.org/) in Rust.

Please check our [documentation](https://docs.rs/avro-rs) for examples, tutorials and API reference.

**[Apache Avro](https://avro.apache.org/)** is a data serialization system which provides rich
data structures and a compact, fast, binary data format.

All data in Avro is schematized, as in the following example:

```
{
    "type": "record",
    "name": "test",
    "fields": [
        {"name": "a", "type": "long", "default": 42},
        {"name": "b", "type": "string"}
    ]
}
```

There are basically two ways of handling Avro data in Rust:

* **as Avro-specialized data types** based on an Avro schema;
* **as generic Rust serde-compatible types** implementing/deriving `Serialize` and
`Deserialize`;

**avro-rs** provides a way to read and write both these data representations easily and
efficiently.

## Installing the library


Add to your `Cargo.toml`:

```toml
[dependencies]
avro-rs = "x.y"
```

Or in case you want to leverage the **Snappy** codec:

```toml
[dependencies.avro-rs]
version = "x.y"
features = ["snappy"]
```

## Upgrading to a newer minor version

The library is still in beta, so there might be backward-incompatible changes between minor
versions. If you have troubles upgrading, check the [version upgrade guide](migration_guide.md).

## Defining a schema

An Avro data cannot exist without an Avro schema. Schemas **must** be used while writing and
**can** be used while reading and they carry the information regarding the type of data we are
handling. Avro schemas are used for both schema validation and resolution of Avro data.

Avro schemas are defined in **JSON** format and can just be parsed out of a raw string:

```rust
use avro_rs::Schema;

let raw_schema = r#"
    {
        "type": "record",
        "name": "test",
        "fields": [
            {"name": "a", "type": "long", "default": 42},
            {"name": "b", "type": "string"}
        ]
    }
"#;

// if the schema is not valid, this function will return an error
let schema = Schema::parse_str(raw_schema).unwrap();

// schemas can be printed for debugging
println!("{:?}", schema);
```

Additionally, a list of of definitions (which may depend on each other) can be given and all of
them will be parsed into the corresponding schemas.

```rust
use avro_rs::Schema;

let raw_schema_1 = r#"{
        "name": "A",
        "type": "record",
        "fields": [
            {"name": "field_one", "type": "float"}
        ]
    }"#;

// This definition depends on the definition of A above
let raw_schema_2 = r#"{
        "name": "B",
        "type": "record",
        "fields": [
            {"name": "field_one", "type": "A"}
        ]
    }"#;

// if the schemas are not valid, this function will return an error
let schemas = Schema::parse_list(&[raw_schema_1, raw_schema_2]).unwrap();

// schemas can be printed for debugging
println!("{:?}", schemas);
```
*N.B.* It is important to note that the composition of schema definitions requires schemas with names.
For this reason, only schemas of type Record, Enum, and Fixed should be input into this function.

The library provides also a programmatic interface to define schemas without encoding them in
JSON (for advanced use), but we highly recommend the JSON interface. Please read the API
reference in case you are interested.

For more information about schemas and what kind of information you can encapsulate in them,
please refer to the appropriate section of the
[Avro Specification](https://avro.apache.org/docs/current/spec.html#schemas).

## Writing data

Once we have defined a schema, we are ready to serialize data in Avro, validating them against
the provided schema in the process. As mentioned before, there are two ways of handling Avro
data in Rust.

**NOTE:** The library also provides a low-level interface for encoding a single datum in Avro
bytecode without generating markers and headers (for advanced use), but we highly recommend the
`Writer` interface to be totally Avro-compatible. Please read the API reference in case you are
interested.

### The avro way

Given that the schema we defined above is that of an Avro *Record*, we are going to use the
associated type provided by the library to specify the data we want to serialize:

```rust
use avro_rs::types::Record;
use avro_rs::Writer;
#
// a writer needs a schema and something to write to
let mut writer = Writer::new(&schema, Vec::new());

// the Record type models our Record schema
let mut record = Record::new(writer.schema()).unwrap();
record.put("a", 27i64);
record.put("b", "foo");

// schema validation happens here
writer.append(record).unwrap();

// this is how to get back the resulting avro bytecode
// this performs a flush operation to make sure data has been written, so it can fail
// you can also call `writer.flush()` yourself without consuming the writer
let encoded = writer.into_inner().unwrap();
```

The vast majority of the times, schemas tend to define a record as a top-level container
encapsulating all the values to convert as fields and providing documentation for them, but in
case we want to directly define an Avro value, the library offers that capability via the
`Value` interface.

```rust
use avro_rs::types::Value;

let mut value = Value::String("foo".to_string());
```

### The serde way

Given that the schema we defined above is an Avro *Record*, we can directly use a Rust struct
deriving `Serialize` to model our data:

```rust
use avro_rs::Writer;

#[derive(Debug, Serialize)]
struct Test {
    a: i64,
    b: String,
}

// a writer needs a schema and something to write to
let mut writer = Writer::new(&schema, Vec::new());

// the structure models our Record schema
let test = Test {
    a: 27,
    b: "foo".to_owned(),
};

// schema validation happens here
writer.append_ser(test).unwrap();

// this is how to get back the resulting avro bytecode
// this performs a flush operation to make sure data is written, so it can fail
// you can also call `writer.flush()` yourself without consuming the writer
let encoded = writer.into_inner();
```

The vast majority of the times, schemas tend to define a record as a top-level container
encapsulating all the values to convert as fields and providing documentation for them, but in
case we want to directly define an Avro value, any type implementing `Serialize` should work.

```rust
let mut value = "foo".to_string();
```

### Using codecs to compress data

Avro supports three different compression codecs when encoding data:

* **Null**: leaves data uncompressed;
* **Deflate**: writes the data block using the deflate algorithm as specified in RFC 1951, and
typically implemented using the zlib library. Note that this format (unlike the "zlib format" in
RFC 1950) does not have a checksum.
* **Snappy**: uses Google's [Snappy](http://google.github.io/snappy/) compression library. Each
compressed block is followed by the 4-byte, big-endianCRC32 checksum of the uncompressed data in
the block. You must enable the `snappy` feature to use this codec.

To specify a codec to use to compress data, just specify it while creating a `Writer`:
```rust
use avro_rs::Writer;
use avro_rs::Codec;
#
let mut writer = Writer::with_codec(&schema, Vec::new(), Codec::Deflate);
```

## Reading data

As far as reading Avro encoded data goes, we can just use the schema encoded with the data to
read them. The library will do it automatically for us, as it already does for the compression
codec:

```rust
use avro_rs::Reader;
#
// reader creation can fail in case the input to read from is not Avro-compatible or malformed
let reader = Reader::new(&input[..]).unwrap();
```

In case, instead, we want to specify a different (but compatible) reader schema from the schema
the data has been written with, we can just do as the following:
```rust
use avro_rs::Schema;
use avro_rs::Reader;
#

let reader_raw_schema = r#"
    {
        "type": "record",
        "name": "test",
        "fields": [
            {"name": "a", "type": "long", "default": 42},
            {"name": "b", "type": "string"},
            {"name": "c", "type": "long", "default": 43}
        ]
    }
"#;

let reader_schema = Schema::parse_str(reader_raw_schema).unwrap();

// reader creation can fail in case the input to read from is not Avro-compatible or malformed
let reader = Reader::with_schema(&reader_schema, &input[..]).unwrap();
```

The library will also automatically perform schema resolution while reading the data.

For more information about schema compatibility and resolution, please refer to the
[Avro Specification](https://avro.apache.org/docs/current/spec.html#schemas).

As usual, there are two ways to handle Avro data in Rust, as you can see below.

**NOTE:** The library also provides a low-level interface for decoding a single datum in Avro
bytecode without markers and header (for advanced use), but we highly recommend the `Reader`
interface to leverage all Avro features. Please read the API reference in case you are
interested.


### The avro way

We can just read directly instances of `Value` out of the `Reader` iterator:

```rust
use avro_rs::Reader;
#
let reader = Reader::new(&input[..]).unwrap();

// value is a Result  of an Avro Value in case the read operation fails
for value in reader {
    println!("{:?}", value.unwrap());
}

```

### The serde way

Alternatively, we can use a Rust type implementing `Deserialize` and representing our schema to
read the data into:

```rust
use avro_rs::Reader;
use avro_rs::from_value;

#[derive(Debug, Deserialize)]
struct Test {
    a: i64,
    b: String,
}

let reader = Reader::new(&input[..]).unwrap();

// value is a Result in case the read operation fails
for value in reader {
    println!("{:?}", from_value::<Test>(&value.unwrap()));
}
```

## Putting everything together

The following is an example of how to combine everything showed so far and it is meant to be a
quick reference of the library interface:

```rust
use avro_rs::{Codec, Reader, Schema, Writer, from_value, types::Record, Error};
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize)]
struct Test {
    a: i64,
    b: String,
}

fn main() -> Result<(), Error> {
    let raw_schema = r#"
        {
            "type": "record",
            "name": "test",
            "fields": [
                {"name": "a", "type": "long", "default": 42},
                {"name": "b", "type": "string"}
            ]
        }
    "#;

    let schema = Schema::parse_str(raw_schema)?;

    println!("{:?}", schema);

    let mut writer = Writer::with_codec(&schema, Vec::new(), Codec::Deflate);

    let mut record = Record::new(writer.schema()).unwrap();
    record.put("a", 27i64);
    record.put("b", "foo");

    writer.append(record)?;

    let test = Test {
        a: 27,
        b: "foo".to_owned(),
    };

    writer.append_ser(test)?;

    let input = writer.into_inner()?;
    let reader = Reader::with_schema(&schema, &input[..])?;

    for record in reader {
        println!("{:?}", from_value::<Test>(&record?));
    }
    Ok(())
}
```

`avro-rs` also supports the logical types listed in the [Avro specification](https://avro.apache.org/docs/current/spec.html#Logical+Types):

1. `Decimal` using the [`num_bigint`](https://docs.rs/num-bigint/0.2.6/num_bigint) crate
1. UUID using the [`uuid`](https://docs.rs/uuid/0.8.1/uuid) crate
1. Date, Time (milli) as `i32` and Time (micro) as `i64`
1. Timestamp (milli and micro) as `i64`
1. Duration as a custom type with `months`, `days` and `millis` accessor methods each of which returns an `i32`

Note that the on-disk representation is identical to the underlying primitive/complex type.

#### Read and write logical types

```rust
use avro_rs::{
    types::Record, types::Value, Codec, Days, Decimal, Duration, Millis, Months, Reader, Schema,
    Writer, Error,
};
use num_bigint::ToBigInt;

fn main() -> Result<(), Error> {
    let raw_schema = r#"
    {
      "type": "record",
      "name": "test",
      "fields": [
        {
          "name": "decimal_fixed",
          "type": {
            "type": "fixed",
            "size": 2,
            "name": "decimal"
          },
          "logicalType": "decimal",
          "precision": 4,
          "scale": 2
        },
        {
          "name": "decimal_var",
          "type": "bytes",
          "logicalType": "decimal",
          "precision": 10,
          "scale": 3
        },
        {
          "name": "uuid",
          "type": "string",
          "logicalType": "uuid"
        },
        {
          "name": "date",
          "type": "int",
          "logicalType": "date"
        },
        {
          "name": "time_millis",
          "type": "int",
          "logicalType": "time-millis"
        },
        {
          "name": "time_micros",
          "type": "long",
          "logicalType": "time-micros"
        },
        {
          "name": "timestamp_millis",
          "type": "long",
          "logicalType": "timestamp-millis"
        },
        {
          "name": "timestamp_micros",
          "type": "long",
          "logicalType": "timestamp-micros"
        },
        {
          "name": "duration",
          "type": {
            "type": "fixed",
            "size": 12,
            "name": "duration"
          },
          "logicalType": "duration"
        }
      ]
    }
    "#;

    let schema = Schema::parse_str(raw_schema)?;

    println!("{:?}", schema);

    let mut writer = Writer::with_codec(&schema, Vec::new(), Codec::Deflate);

    let mut record = Record::new(writer.schema()).unwrap();
    record.put("decimal_fixed", Decimal::from(9936.to_bigint().unwrap().to_signed_bytes_be()));
    record.put("decimal_var", Decimal::from((-32442.to_bigint().unwrap()).to_signed_bytes_be()));
    record.put("uuid", uuid::Uuid::new_v4());
    record.put("date", Value::Date(1));
    record.put("time_millis", Value::TimeMillis(2));
    record.put("time_micros", Value::TimeMicros(3));
    record.put("timestamp_millis", Value::TimestampMillis(4));
    record.put("timestamp_micros", Value::TimestampMicros(5));
    record.put("duration", Duration::new(Months::new(6), Days::new(7), Millis::new(8)));

    writer.append(record)?;

    let input = writer.into_inner()?;
    let reader = Reader::with_schema(&schema, &input[..])?;

    for record in reader {
        println!("{:?}", record?);
    }
    Ok(())
}
```

### Calculate Avro schema fingerprint

This library supports calculating the following fingerprints:

 - SHA-256
 - MD5
 - Rabin

An example of fingerprinting for the supported fingerprints:

```rust
use avro_rs::rabin::Rabin;
use avro_rs::{Schema, Error};
use md5::Md5;
use sha2::Sha256;

fn main() -> Result<(), Error> {
    let raw_schema = r#"
        {
            "type": "record",
            "name": "test",
            "fields": [
                {"name": "a", "type": "long", "default": 42},
                {"name": "b", "type": "string"}
            ]
        }
    "#;
    let schema = Schema::parse_str(raw_schema)?;
    println!("{}", schema.fingerprint::<Sha256>());
    println!("{}", schema.fingerprint::<Md5>());
    println!("{}", schema.fingerprint::<Rabin>());
    Ok(())
}
```

### Ill-formed data

In order to ease decoding, the Binary Encoding specification of Avro data
requires some fields to have their length encoded alongside the data.

If encoded data passed to a `Reader` has been ill-formed, it can happen that
the bytes meant to contain the length of data are bogus and could result
in extravagant memory allocation.

To shield users from ill-formed data, `avro-rs` sets a limit (default: 512MB)
to any allocation it will perform when decoding data.

If you expect some of your data fields to be larger than this limit, be sure
to make use of the `max_allocation_bytes` function before reading **any** data
(we leverage Rust's [`std::sync::Once`](https://doc.rust-lang.org/std/sync/struct.Once.html)
mechanism to initialize this value, if
any call to decode is made before a call to `max_allocation_bytes`, the limit
will be 512MB throughout the lifetime of the program).


```rust
use avro_rs::max_allocation_bytes;

max_allocation_bytes(2 * 1024 * 1024 * 1024);  // 2GB

// ... happily decode large data

```

### Check schemas compatibility

This library supports checking for schemas compatibility.

Note: It does not yet support named schemas (more on
https://github.com/flavray/avro-rs/pull/76).

Examples of checking for compatibility:

1. Compatible schemas

Explanation: an int array schema can be read by a long array schema- an int
(32bit signed integer) fits into a long (64bit signed integer)

```rust
use avro_rs::{Schema, schema_compatibility::SchemaCompatibility};

let writers_schema = Schema::parse_str(r#"{"type": "array", "items":"int"}"#).unwrap();
let readers_schema = Schema::parse_str(r#"{"type": "array", "items":"long"}"#).unwrap();
assert_eq!(true, SchemaCompatibility::can_read(&writers_schema, &readers_schema));
```

2. Incompatible schemas (a long array schema cannot be read by an int array schema)

Explanation: a long array schema cannot be read by an int array schema- a
long (64bit signed integer) does not fit into an int (32bit signed integer)

```rust
use avro_rs::{Schema, schema_compatibility::SchemaCompatibility};

let writers_schema = Schema::parse_str(r#"{"type": "array", "items":"long"}"#).unwrap();
let readers_schema = Schema::parse_str(r#"{"type": "array", "items":"int"}"#).unwrap();
assert_eq!(false, SchemaCompatibility::can_read(&writers_schema, &readers_schema));
```

## License
This project is licensed under [MIT License](https://github.com/flavray/avro-rs/blob/main/LICENSE).
Please note that this is not an official project maintained by [Apache Avro](https://avro.apache.org/).

## Contributing
Everyone is encouraged to contribute! You can contribute by forking the GitHub repo and making a pull request or opening an issue.
All contributions will be licensed under [MIT License](https://github.com/flavray/avro-rs/blob/main/LICENSE).

Please consider adding documentation, tests and a line for your change under the Unreleased section in the [CHANGELOG](https://github.com/flavray/avro-rs/blob/main/CHANGELOG.md).
If you introduce a backward-incompatible change, please consider adding instruction to migrate in the [Migration Guide](migration_guide.md)
If you modify the crate documentation in `lib.rs`, run `make readme` to sync the README file.
