use std::collections::HashMap;
use std::sync::Arc;

use arrow::array;
use arrow::array::{ArrayBuilder, ArrayRef};
use arrow::datatypes::{Field, Schema};
use arrow::record_batch::RecordBatch;
use arrow_schema::DataType;
use chrono::DateTime;
use itertools::Itertools;
use linked_hash_map::LinkedHashMap;
use mongodb::bson::{Bson, Document};
use serde_json::json;

pub fn to_schema() -> anyhow::Result<Schema> {
    let fields = vec![Field::new("value", DataType::Utf8, false)];
    let schema = build_schema(fields)?;
    Ok(schema)
}

pub fn to_record_batches(
    documents: &[Document],
    batch_size: usize,
) -> anyhow::Result<Vec<RecordBatch>> {
    let fields = vec![Field::new("value", DataType::Utf8, false)];
    let mut builders = vec![array::make_builder(&DataType::Utf8, 10)];
    let mut batches = Vec::new();

    let mut row_count = 0;

    for document in documents {
        let mut payload: LinkedHashMap<String, serde_json::Value> = LinkedHashMap::new();
        let keys = document.keys();
        for key in keys {
            let val = document.get(key);
            match val {
                Some(val) => match val {
                    Bson::Double(v) => {
                        payload.insert(key.clone(), json!(v));
                    }
                    Bson::String(v) => {
                        payload.insert(key.clone(), json!(v));
                    }
                    Bson::Array(v) => {
                        payload.insert(key.clone(), json!(v));
                    }
                    Bson::Document(v) => {
                        payload.insert(key.clone(), json!(v));
                    }
                    Bson::Boolean(v) => {
                        payload.insert(key.clone(), json!(v));
                    }
                    Bson::Null => {
                        payload.insert(key.clone(), json!(null));
                    }
                    Bson::RegularExpression(v) => {
                        payload.insert(key.clone(), json!(serde_json::to_string(v).unwrap()));
                    }
                    Bson::JavaScriptCode(v) => {
                        payload.insert(key.clone(), json!(v.to_string()));
                    }
                    Bson::JavaScriptCodeWithScope(v) => {
                        payload.insert(key.clone(), json!(serde_json::to_string(v).unwrap()));
                    }
                    Bson::Int32(v) => {
                        payload.insert(key.clone(), json!(v));
                    }
                    Bson::Int64(v) => {
                        payload.insert(key.clone(), json!(v));
                    }
                    Bson::Timestamp(v) => {
                        let v = DateTime::from_timestamp(v.time as i64, v.increment).unwrap();
                        payload.insert(key.clone(), json!(v));
                    }
                    Bson::Binary(v) => {
                        let value: String = v.bytes.iter().map(|b| format!("{:02x}", b)).collect();
                        payload.insert(key.clone(), json!(format!("\\x{}", value)));
                    }
                    Bson::ObjectId(v) => {
                        payload.insert(key.clone(), json!(v.to_string()));
                    }
                    Bson::DateTime(v) => {
                        let v = DateTime::from_timestamp_millis(v.timestamp_millis()).unwrap();
                        payload.insert(key.clone(), json!(v));
                    }
                    Bson::Symbol(v) => {
                        payload.insert(key.clone(), json!(v));
                    }
                    Bson::Decimal128(v) => {
                        payload.insert(key.clone(), json!(v.to_string()));
                    }
                    Bson::Undefined => {
                        payload.insert(key.clone(), json!(null));
                    }
                    Bson::MaxKey => {
                        payload.insert(key.clone(), json!("MaxKey"));
                    }
                    Bson::MinKey => {
                        payload.insert(key.clone(), json!("MinKey"));
                    }
                    _ => {
                        payload.insert(key.clone(), json!(null));
                    }
                },
                None => {
                    payload.insert(key.clone(), json!(null));
                }
            }
        }
        builders[0]
            .as_any_mut()
            .downcast_mut::<array::StringBuilder>()
            .unwrap()
            .append_value(serde_json::to_string(&payload).unwrap());
        // increase row count
        row_count += 1;
        // check batch size
        if row_count == batch_size {
            // build record batch
            let batch = build_record_batch(fields.clone(), builders)?;
            batches.push(batch);
            // reset builders
            builders = vec![array::make_builder(&DataType::Utf8, 10)];
            // reset row count
            row_count = 0;
        }
    }
    let batch = build_record_batch(fields, builders)?;
    batches.push(batch);

    Ok(batches)
}

fn build_schema(fields: Vec<Field>) -> anyhow::Result<Schema> {
    // metadata
    let mut metadata = HashMap::new();
    metadata.insert(String::from("version"), String::from("1.0"));
    metadata.insert(String::from("stream"), String::from("flat"));
    metadata.insert(String::from("ack"), String::from("lush"));
    // schema
    let schema = Schema::new(fields).with_metadata(metadata);
    Ok(schema)
}

fn build_record_batch(
    fields: Vec<Field>,
    mut builders: Vec<Box<dyn ArrayBuilder>>,
) -> anyhow::Result<RecordBatch> {
    // schema
    let schema = build_schema(fields)?;
    // data array
    let array_refs = builders
        .iter_mut()
        .map(|builder| Arc::new(builder.finish()) as ArrayRef)
        .collect_vec();
    // record batch
    let batch = RecordBatch::try_new(Arc::new(schema), array_refs)?;
    Ok(batch)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runners::mongodb::{config::connect::ConnectConfig, query::MongoDBQuery};
    use mongodb::bson::{doc, oid::ObjectId, Decimal128};
    use std::str::FromStr;
    use taos::Dsn;

    async fn test_create_table() {
        let dsn =
            Dsn::from_str("mongodb://admin:tbase125!@192.168.1.40:27017?source=admin").unwrap();
        let config = ConnectConfig::from_dsn(&dsn).unwrap();

        let result = MongoDBQuery::try_new(config).await;
        match result {
            Ok(query) => {
                let database = query.client.database("test_taosx");
                let x = database.create_collection("metrics").await;
                println!("create table: {:?}", x);
            }
            Err(e) => {
                println!("error: {:?}", e);
            }
        }
    }

    async fn test_insert_data(len: usize) {
        let _ = test_create_table().await;

        let dsn =
            Dsn::from_str("mongodb://admin:tbase125!@192.168.1.40:27017?source=admin").unwrap();
        let config = ConnectConfig::from_dsn(&dsn).unwrap();

        let result = MongoDBQuery::try_new(config).await;
        match result {
            Ok(query) => {
                for _ in 0..len {
                    let database = query.client.database("test_taosx");
                    let collection = database.collection("metrics");
                    let doc_all = doc! {
                        "double": Bson::Double(3.141592653),
                        "string": Bson::String("abc".to_string()),
                        "array": Bson::Array(vec![Bson::Int32(1), Bson::Int32(2), Bson::Int32(3)]),
                        "document": Bson::Document(doc! {
                            "int32": Bson::Int32(123),
                            "int64": Bson::Int64(123)
                        }),
                        "bool": Bson::Boolean(true),
                        "null": Bson::Null,
                        "regex": Bson::RegularExpression(mongodb::bson::Regex {
                            pattern: "abc".to_string(),
                            options: "i".to_string()
                        }),
                        "javascript": Bson::JavaScriptCode("function() { return 1; }".to_string()),
                        "javascript_with_scope": Bson::JavaScriptCodeWithScope(mongodb::bson::JavaScriptCodeWithScope {
                            code: "function() { return n; }".to_string(),
                            scope: doc! { "n": 1 }
                        }),
                        "int32": Bson::Int32(123),
                        "int64": Bson::Int64(123),
                        "timestamp": Bson::Timestamp(mongodb::bson::Timestamp {
                            time: 123,
                            increment: 456
                        }),
                        "binary": Bson::Binary(mongodb::bson::Binary {
                            subtype: mongodb::bson::spec::BinarySubtype::Generic,
                            bytes: vec![1, 2, 3]
                        }),
                        "object_id": Bson::ObjectId(ObjectId::new()),
                        "datetime": Bson::DateTime(mongodb::bson::DateTime::now()),
                        "symbol": Bson::Symbol("abc".to_string()),
                        "decimal128": Bson::Decimal128(Decimal128::from_str("3.141592653").unwrap()),
                        "undefined": Bson::Undefined,
                        "max_key": Bson::MaxKey,
                        "min_key": Bson::MinKey,
                        // Deprecated
                        // "db_pointer": Bson::DbPointer(DbPointer {
                        //     db: "test".to_string(),
                        //     oid: ObjectId::new()
                        // }),
                    };
                    let _ = collection.insert_one(doc_all).await;
                }
            }
            Err(e) => {
                println!("error: {:?}", e);
            }
        }
    }

    async fn test_clear_data() {
        let _ = test_create_table().await;

        let dsn =
            Dsn::from_str("mongodb://admin:tbase125!@192.168.1.40:27017?source=admin").unwrap();
        let config = ConnectConfig::from_dsn(&dsn).unwrap();

        let result = MongoDBQuery::try_new(config).await;
        match result {
            Ok(query) => {
                let database = query.client.database("test_taosx");
                let collection: mongodb::Collection<Document> = database.collection("metrics");

                let _ = collection.delete_many(doc! {}).await;
            }
            Err(e) => {
                println!("error: {:?}", e);
            }
        }
    }

    #[tokio::test]
    async fn test_to_schema() {
        let schema = to_schema().unwrap();
        dbg!(&schema);
        assert_eq!(schema.fields().len(), 1);
    }

    #[tokio::test]
    async fn test_to_record_batches() {
        // prepare data
        let _ = test_clear_data().await;
        let _ = test_insert_data(7).await;

        let dsn = Dsn::from_str(
            "mongodb://admin:tbase125!@192.168.1.40:27017?source=admin&database=test_taosx&collection=metrics&sql={\"datetime\":{\"$gte\":${start_datetime},\"$lt\":${end_datetime}}}&start=2024-07-01T00:00:00+00:00&end=2024-08-01T00:00:00+00:00&interval=12h&delay=0&sample_data_limit=4",
        )
        .unwrap();
        let config = ConnectConfig::from_dsn(&dsn).unwrap();

        let result = MongoDBQuery::try_new(config).await;
        match result {
            Ok(mut query) => {
                let query_result = query
                    .top_n("test_taosx", "metrics", doc! {}, doc! {}, 7)
                    .await;
                match query_result {
                    Ok(documents) => {
                        let batches = to_record_batches(&documents, 3).unwrap();
                        dbg!(&batches);
                        assert_eq!(batches.len(), 3);
                    }
                    Err(e) => {
                        println!("error: {:?}", e);
                    }
                }
            }
            Err(e) => {
                println!("error: {:?}", e);
            }
        }
        // clear data
        let _ = test_clear_data().await;
    }

    #[test]
    fn test_build_schema() {
        let fields = vec![Field::new(
            "value".to_string(),
            arrow::datatypes::DataType::Utf8,
            true,
        )];
        let schema = build_schema(fields).unwrap();
        dbg!(schema);
    }

    #[test]
    fn test_build_record_batch() {
        let fields = vec![Field::new(
            "value".to_string(),
            arrow::datatypes::DataType::Utf8,
            true,
        )];
        let mut builders = vec![array::make_builder(&arrow_schema::DataType::Utf8, 10)];
        builders[0]
            .as_any_mut()
            .downcast_mut::<array::StringBuilder>()
            .unwrap()
            .append_value("Alice");
        // build record batch
        let batch = build_record_batch(fields, builders).unwrap();
        dbg!(batch);
    }
}
