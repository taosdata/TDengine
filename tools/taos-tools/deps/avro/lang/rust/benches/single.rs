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
    to_avro_datum,
    types::{Record, Value},
};
use criterion::{criterion_group, criterion_main, Criterion};

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

fn bench_small_schema_write_record(c: &mut Criterion) {
    let (schema, record) = make_small_record();
    c.bench_function("small record", |b| {
        b.iter(|| to_avro_datum(&schema, record.clone()))
    });
}

fn bench_big_schema_write_record(c: &mut Criterion) {
    let (schema, record) = make_big_record();
    c.bench_function("big record", |b| {
        b.iter(|| to_avro_datum(&schema, record.clone()))
    });
}

criterion_group!(
    benches,
    bench_small_schema_write_record,
    bench_big_schema_write_record
);
criterion_main!(benches);
