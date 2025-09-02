use std::sync::Arc;

use arrow::array::{ArrayRef, StringArray};
use arrow_schema::{DataType, Field};
use criterion::{criterion_group, criterion_main, Criterion};
use serde_json::json;
use taosx_core::plugins::transform::parse::{json::Json, Parse};

fn json_parse_normal(c: &mut Criterion) {
    let parser: Json = serde_json::from_value(json!({
        "json": ""
    }))
    .unwrap();

    let field = Field::new("payload", DataType::Utf8, true);
    let array: ArrayRef = Arc::new(StringArray::from_iter_values(std::iter::repeat_n(
        r#"
        {
            "a": 123,
            "b": "123",
            "c": true,
            "d": 123.4,
            "e": 123,
            "f": "123",
            "g": true,
            "h": 123.4
        }
        "#,
        10000,
    )));
    c.bench_function("json_parse_normal", |b| {
        b.iter(|| {
            parser.parse_array(&field, &array).unwrap();
        })
    });
}

fn json_parse_depth(c: &mut Criterion) {
    let parser: Json = serde_json::from_value(json!({
        "json": "",
        "depth": 4
    }))
    .unwrap();

    let field = Field::new("payload", DataType::Utf8, true);
    let array: ArrayRef = Arc::new(StringArray::from_iter_values(std::iter::repeat_n(
        r#"
        {
            "a": 123,
            "b": {
                "b1": "123",
                "b2": {
                    "b3": false
                }
            }, 
            "c": {
                "c1": "12345",
                "c2": {
                    "c3": {
                        "c4": 456,
                        "c5": 5676.8
                    }
                }
            },
            "d": 123.4
        }
        "#,
        10000,
    )));
    c.bench_function("json_parse_depth", |b| {
        b.iter(|| {
            parser.parse_array(&field, &array).unwrap();
        })
    });
}

fn json_parse_path(c: &mut Criterion) {
    let parser: Json = serde_json::from_value(json!({
        "json": ["$['a']=a::double", "$['b']['b1']=b1", "$.b.b2.b3=b3", "$.c.c1=c1", "$.c.c2.c3.c4=c4","$.c.c2.c3.c5=c5", "d"],
        "depth": 4
    }))
    .unwrap();

    let field = Field::new("payload", DataType::Utf8, true);
    let array: ArrayRef = Arc::new(StringArray::from_iter_values(std::iter::repeat_n(
        r#"
        {
            "a": 123,
            "b": {
                "b1": "123",
                "b2": {
                    "b3": false
                }
            }, 
            "c": {
                "c1": "12345",
                "c2": {
                    "c3": {
                        "c4": 456,
                        "c5": 5676.8
                    }
                }
            },
            "d": 123.4
        }
        "#,
        10000,
    )));
    c.bench_function("json_parse_path", |b| {
        b.iter(|| {
            parser.parse_array(&field, &array).unwrap();
        })
    });
}

criterion_group!(
    benches,
    json_parse_normal,
    json_parse_depth,
    json_parse_path
);
criterion_main!(benches);
