use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    sync::Arc,
};

use arrow_schema::{DataType, Field, Schema, TimeUnit};

fn schema(data_type: DataType) -> Arc<Schema> {
    let fields = vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("name", DataType::Utf8, false),
        Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            false,
        ),
        Field::new(
            "received",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            false,
        ),
        Field::new("value", data_type, true),
        Field::new("status", DataType::Int64, false),
        Field::new(
            "request",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            false,
        ),
    ];
    let meta = HashMap::<_, _, _>::from_iter(
        [("version", "1.0"), ("stream", "point"), ("ack", "lush")]
            .map(|(k, v)| (k.to_string(), v.to_string())),
    );
    Arc::new(Schema::new_with_metadata(fields, meta))
}

pub fn get_schema_path(dir: impl AsRef<Path>) -> HashMap<Arc<Schema>, PathBuf> {
    let dir = dir.as_ref();
    HashMap::from_iter(
        [
            (schema(DataType::Boolean), "_bool"),
            (
                schema(DataType::Timestamp(TimeUnit::Nanosecond, None)),
                "_timestamp",
            ),
            (schema(DataType::Int8), "_int8"),
            (schema(DataType::UInt8), "_uint8"),
            (schema(DataType::Int16), "_int16"),
            (schema(DataType::UInt16), "_uint16"),
            (schema(DataType::Int32), "_int32"),
            (schema(DataType::UInt32), "_uint32"),
            (schema(DataType::Int64), "_int64"),
            (schema(DataType::UInt64), "_uint64"),
            (schema(DataType::Float32), "_float"),
            (schema(DataType::Float64), "_double"),
            (schema(DataType::Utf8), "_str"),
        ]
        .map(|(k, v)| (k, dir.join(v))),
    )
}
