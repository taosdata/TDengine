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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn schema_has_expected_fields_and_metadata_for_value_type() {
        let schema = schema(DataType::Float64);

        assert_eq!(schema.fields().len(), 7);
        assert_eq!(schema.field(0).name(), "id");
        assert_eq!(schema.field(0).data_type(), &DataType::Utf8);
        assert!(!schema.field(0).is_nullable());
        assert_eq!(
            schema.field(2).data_type(),
            &DataType::Timestamp(TimeUnit::Millisecond, None)
        );
        assert_eq!(schema.field(4).name(), "value");
        assert_eq!(schema.field(4).data_type(), &DataType::Float64);
        assert!(schema.field(4).is_nullable());
        assert_eq!(schema.metadata().get("version").unwrap(), "1.0");
        assert_eq!(schema.metadata().get("stream").unwrap(), "point");
        assert_eq!(schema.metadata().get("ack").unwrap(), "lush");
    }

    #[test]
    fn get_schema_path_returns_all_supported_value_type_directories() {
        let paths = get_schema_path("/tmp/opc");

        assert_eq!(paths.len(), 13);
        assert!(
            paths
                .values()
                .any(|path| path == Path::new("/tmp/opc/_bool"))
        );
        assert!(
            paths
                .values()
                .any(|path| path == Path::new("/tmp/opc/_timestamp"))
        );
        assert!(
            paths
                .values()
                .any(|path| path == Path::new("/tmp/opc/_int8"))
        );
        assert!(
            paths
                .values()
                .any(|path| path == Path::new("/tmp/opc/_uint8"))
        );
        assert!(
            paths
                .values()
                .any(|path| path == Path::new("/tmp/opc/_int16"))
        );
        assert!(
            paths
                .values()
                .any(|path| path == Path::new("/tmp/opc/_uint16"))
        );
        assert!(
            paths
                .values()
                .any(|path| path == Path::new("/tmp/opc/_int32"))
        );
        assert!(
            paths
                .values()
                .any(|path| path == Path::new("/tmp/opc/_uint32"))
        );
        assert!(
            paths
                .values()
                .any(|path| path == Path::new("/tmp/opc/_int64"))
        );
        assert!(
            paths
                .values()
                .any(|path| path == Path::new("/tmp/opc/_uint64"))
        );
        assert!(
            paths
                .values()
                .any(|path| path == Path::new("/tmp/opc/_float"))
        );
        assert!(
            paths
                .values()
                .any(|path| path == Path::new("/tmp/opc/_double"))
        );
        assert!(
            paths
                .values()
                .any(|path| path == Path::new("/tmp/opc/_str"))
        );
    }
}
