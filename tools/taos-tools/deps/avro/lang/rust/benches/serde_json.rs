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

use criterion::{criterion_group, criterion_main, Criterion};
use serde_json::Value;
use std::{collections::HashMap, iter::FromIterator};

fn make_big_json_record() -> Value {
    let address = HashMap::<_, _>::from_iter(vec![
        ("street", "street"),
        ("city", "city"),
        ("state_prov", "state_prov"),
        ("country", "country"),
        ("zip", "zip"),
    ]);
    let address_json = serde_json::to_value(address).unwrap();
    let big_record = HashMap::<_, _>::from_iter(vec![
        ("username", serde_json::to_value("username").unwrap()),
        ("age", serde_json::to_value(10i32).unwrap()),
        ("phone", serde_json::to_value("000000000").unwrap()),
        ("housenum", serde_json::to_value("0000").unwrap()),
        ("address", address_json),
    ]);
    serde_json::to_value(big_record).unwrap()
}

fn write_json(records: &[Value]) -> Vec<u8> {
    serde_json::to_vec(records).unwrap()
}

fn read_json(bytes: &[u8]) {
    let reader: serde_json::Value = serde_json::from_slice(bytes).unwrap();
    for record in reader.as_array().unwrap() {
        let _ = record;
    }
}

fn bench_read_json(
    c: &mut Criterion,
    make_record: impl Fn() -> Value,
    n_records: usize,
    name: &str,
) {
    let records = std::iter::repeat(make_record())
        .take(n_records)
        .collect::<Vec<_>>();
    let bytes = write_json(&records);
    c.bench_function(name, |b| b.iter(|| read_json(&bytes)));
}

fn bench_big_schema_json_read_10_000_record(c: &mut Criterion) {
    bench_read_json(
        c,
        &make_big_json_record,
        10_000,
        "big schema, read 10k JSON records",
    );
}

criterion_group!(
    name = benches;
    config = Criterion::default().sample_size(10);
    targets = bench_big_schema_json_read_10_000_record,
);
criterion_main!(benches);
