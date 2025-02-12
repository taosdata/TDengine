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

use avro_rs::{
    schema::Schema,
    types::{Record, Value},
    Reader, Writer,
};
use criterion::{criterion_group, criterion_main, Criterion};
use std::time::Duration;

const RAW_SMALL_SCHEMA: &str = r#"
{
  "namespace": "test",
  "type": "record",
  "name": "Test",
  "fields": [
    {
      "type": {
        "type": "string"
      },
      "name": "field"
    }
  ]
}
"#;

const RAW_BIG_SCHEMA: &str = r#"
{
  "namespace": "my.example",
  "type": "record",
  "name": "userInfo",
  "fields": [
    {
      "default": "NONE",
      "type": "string",
      "name": "username"
    },
    {
      "default": -1,
      "type": "int",
      "name": "age"
    },
    {
      "default": "NONE",
      "type": "string",
      "name": "phone"
    },
    {
      "default": "NONE",
      "type": "string",
      "name": "housenum"
    },
    {
      "default": {},
      "type": {
        "fields": [
          {
            "default": "NONE",
            "type": "string",
            "name": "street"
          },
          {
            "default": "NONE",
            "type": "string",
            "name": "city"
          },
          {
            "default": "NONE",
            "type": "string",
            "name": "state_prov"
          },
          {
            "default": "NONE",
            "type": "string",
            "name": "country"
          },
          {
            "default": "NONE",
            "type": "string",
            "name": "zip"
          }
        ],
        "type": "record",
        "name": "mailing_address"
      },
      "name": "address"
    }
  ]
}
"#;

const RAW_ADDRESS_SCHEMA: &str = r#"
{
  "fields": [
    {
      "default": "NONE",
      "type": "string",
      "name": "street"
    },
    {
      "default": "NONE",
      "type": "string",
      "name": "city"
    },
    {
      "default": "NONE",
      "type": "string",
      "name": "state_prov"
    },
    {
      "default": "NONE",
      "type": "string",
      "name": "country"
    },
    {
      "default": "NONE",
      "type": "string",
      "name": "zip"
    }
  ],
  "type": "record",
  "name": "mailing_address"
}
"#;

fn make_small_record() -> (Schema, Value) {
    let small_schema = Schema::parse_str(RAW_SMALL_SCHEMA).unwrap();
    let small_record = {
        let mut small_record = Record::new(&small_schema).unwrap();
        small_record.put("field", "foo");
        small_record.into()
    };
    (small_schema, small_record)
}

fn make_big_record() -> (Schema, Value) {
    let big_schema = Schema::parse_str(RAW_BIG_SCHEMA).unwrap();
    let address_schema = Schema::parse_str(RAW_ADDRESS_SCHEMA).unwrap();
    let mut address = Record::new(&address_schema).unwrap();
    address.put("street", "street");
    address.put("city", "city");
    address.put("state_prov", "state_prov");
    address.put("country", "country");
    address.put("zip", "zip");

    let big_record = {
        let mut big_record = Record::new(&big_schema).unwrap();
        big_record.put("username", "username");
        big_record.put("age", 10i32);
        big_record.put("phone", "000000000");
        big_record.put("housenum", "0000");
        big_record.put("address", address);
        big_record.into()
    };

    (big_schema, big_record)
}

fn make_records(record: Value, count: usize) -> Vec<Value> {
    std::iter::repeat(record).take(count).collect()
}

fn write(schema: &Schema, records: &[Value]) -> Vec<u8> {
    let mut writer = Writer::new(schema, Vec::new());
    writer.extend_from_slice(records).unwrap();
    writer.into_inner().unwrap()
}

fn read(schema: &Schema, bytes: &[u8]) {
    let reader = Reader::with_schema(schema, bytes).unwrap();

    for record in reader {
        let _ = record.unwrap();
    }
}

fn read_schemaless(bytes: &[u8]) {
    let reader = Reader::new(bytes).unwrap();

    for record in reader {
        let _ = record.unwrap();
    }
}

fn bench_write(
    c: &mut Criterion,
    make_record: impl Fn() -> (Schema, Value),
    n_records: usize,
    name: &str,
) {
    let (schema, record) = make_record();
    let records = make_records(record, n_records);
    c.bench_function(name, |b| b.iter(|| write(&schema, &records)));
}

fn bench_read(
    c: &mut Criterion,
    make_record: impl Fn() -> (Schema, Value),
    n_records: usize,
    name: &str,
) {
    let (schema, record) = make_record();
    let records = make_records(record, n_records);
    let bytes = write(&schema, &records);
    c.bench_function(name, |b| b.iter(|| read(&schema, &bytes)));
}

fn bench_from_file(c: &mut Criterion, file_path: &str, name: &str) {
    let bytes = std::fs::read(file_path).unwrap();
    c.bench_function(name, |b| b.iter(|| read_schemaless(&bytes)));
}

fn bench_small_schema_write_1_record(c: &mut Criterion) {
    bench_write(c, &make_small_record, 1, "small schema, write 1 record");
}

fn bench_small_schema_write_100_record(c: &mut Criterion) {
    bench_write(
        c,
        &make_small_record,
        100,
        "small schema, write 100 records",
    );
}

fn bench_small_schema_write_10_000_record(c: &mut Criterion) {
    bench_write(
        c,
        &make_small_record,
        10_000,
        "small schema, write 10k records",
    );
}

fn bench_small_schema_read_1_record(c: &mut Criterion) {
    bench_read(c, &make_small_record, 1, "small schema, read 1 record");
}

fn bench_small_schema_read_100_record(c: &mut Criterion) {
    bench_read(c, &make_small_record, 100, "small schema, read 100 records");
}

fn bench_small_schema_read_10_000_record(c: &mut Criterion) {
    bench_read(
        c,
        &make_small_record,
        10_000,
        "small schema, read 10k records",
    );
}

fn bench_big_schema_write_1_record(c: &mut Criterion) {
    bench_write(c, &make_big_record, 1, "big schema, write 1 record");
}

fn bench_big_schema_write_100_record(c: &mut Criterion) {
    bench_write(c, &make_big_record, 100, "big schema, write 100 records");
}

fn bench_big_schema_write_10_000_record(c: &mut Criterion) {
    bench_write(c, &make_big_record, 10_000, "big schema, write 10k records");
}

fn bench_big_schema_read_1_record(c: &mut Criterion) {
    bench_read(c, &make_big_record, 1, "big schema, read 1 record");
}

fn bench_big_schema_read_100_record(c: &mut Criterion) {
    bench_read(c, &make_big_record, 100, "big schema, read 100 records");
}

fn bench_big_schema_read_10_000_record(c: &mut Criterion) {
    bench_read(c, &make_big_record, 10_000, "big schema, read 10k records");
}

fn bench_big_schema_read_100_000_record(c: &mut Criterion) {
    bench_read(
        c,
        &make_big_record,
        100_000,
        "big schema, read 100k records",
    );
}

// This benchmark reads from the `benches/quickstop-null.avro` file, which was pulled from
// the `goavro` project benchmarks:
// https://github.com/linkedin/goavro/blob/master/fixtures/quickstop-null.avro
// This was done for the sake of comparing this crate against the `goavro` implementation.
fn bench_file_quickstop_null(c: &mut Criterion) {
    bench_from_file(c, "benches/quickstop-null.avro", "quickstop null file");
}

criterion_group!(
    benches,
    bench_small_schema_write_1_record,
    bench_small_schema_write_100_record,
    bench_small_schema_read_1_record,
    bench_small_schema_read_100_record,
    bench_big_schema_write_1_record,
    bench_big_schema_write_100_record,
    bench_big_schema_read_1_record,
    bench_big_schema_read_100_record,
);

criterion_group!(
    name = long_benches;
    config = Criterion::default().sample_size(20).measurement_time(Duration::from_secs(10));
    targets =
        bench_file_quickstop_null,
        bench_small_schema_write_10_000_record,
        bench_small_schema_read_10_000_record,
        bench_big_schema_read_10_000_record,
        bench_big_schema_write_10_000_record
);

criterion_group!(
    name = very_long_benches;
    config = Criterion::default().sample_size(10).measurement_time(Duration::from_secs(20));
    targets =
        bench_big_schema_read_100_000_record,
);

criterion_main!(benches, long_benches, very_long_benches);
