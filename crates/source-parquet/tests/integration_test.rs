use arrow::array::{Int64Array, StringArray, TimestampMillisecondArray};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use std::fs;
use std::path::PathBuf;
use std::sync::Arc;
use tempfile::TempDir;

/// Helper function to create a test parquet file
fn create_test_parquet_file(
    dir: &TempDir,
    filename: &str,
    prefix: &str,
    num_rows: usize,
) -> PathBuf {
    let path = dir.path().join(filename);
    let file = fs::File::create(&path).unwrap();

    // Create schema
    let schema = Arc::new(Schema::new(vec![
        Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            false,
        ),
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("value", DataType::Int64, false),
    ]));

    let ms = chrono::Utc::now().timestamp_millis();
    // Create data
    let ts_data: Vec<i64> = (0..num_rows).map(|i| ms + i as i64 * 1000).collect();
    let id_data: Vec<i64> = (0..num_rows).map(|i| i as i64).collect();
    let name_data: Vec<String> = (0..num_rows)
        .map(|i| {
            if i % 2 == 0 {
                format!("{}_test_a", prefix)
            } else {
                format!("{}_test_b", prefix)
            }
        })
        .collect();
    let value_data: Vec<i64> = (0..num_rows).map(|i| (i * 10) as i64).collect();

    let ts_array = TimestampMillisecondArray::from(ts_data);
    let id_array = Int64Array::from(id_data);
    let name_array = StringArray::from(name_data);
    let value_array = Int64Array::from(value_data);

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(ts_array),
            Arc::new(id_array),
            Arc::new(name_array),
            Arc::new(value_array),
        ],
    )
    .unwrap();

    // Write parquet file
    let mut writer = ArrowWriter::try_new(file, schema, None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();

    path
}

#[cfg(test)]
mod tests {
    use super::*;
    use source_parquet::parquet_to_taos;
    use taos::{AsyncQueryable, AsyncTBuilder, Dsn, TaosBuilder};
    use taosx_core::Parser;

    #[tokio::test(flavor = "multi_thread")]
    async fn test_simple_parquet_file_with_taos() {
        let temp_dir = TempDir::new().unwrap();
        let path = create_test_parquet_file(&temp_dir, "test.parquet", "t0", 100);

        assert!(path.exists());
        assert!(path.is_file());

        // Verify file is readable
        let file = fs::File::open(&path).unwrap();
        let reader = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(file)
            .unwrap()
            .build()
            .unwrap();

        let mut total_rows = 0;
        for batch in reader {
            let batch = batch.unwrap();
            total_rows += batch.num_rows();
        }

        assert_eq!(total_rows, 100);

        let parser: Parser = serde_json::from_str(
            r#"{
              "parse": {
                "ts": { "as": "timestamp(ms)" },
                "value": { "as": "bigint" },
                "id": { "as": "bigint" },
                "name": { "as": "varchar(64)" }
              },
              "model": {
                "name": "t_{name}",
                "using": "metrics",
                "tags": ["id","name"],
                "columns": ["ts", "value"]
              }
            }"#,
        )
        .unwrap();
        let (rx, _tx) = flume::unbounded();

        let dsn: Dsn = "taos://".parse().unwrap();
        let pool = TaosBuilder::from_dsn(&dsn).unwrap().pool().unwrap();
        let conn = pool.get().await.unwrap();
        conn.exec_many([
            "drop database if exists parquet_source_test_1",
            "create database if not exists parquet_source_test_1",
        ])
        .await
        .unwrap();
        let cancel = tokio_util::sync::CancellationToken::new();
        let _guard = cancel.clone().drop_guard();
        parquet_to_taos(
            format!("parquet:{}", path.display()).parse().unwrap(),
            Some(parser),
            "taos:///parquet_source_test_1".parse().unwrap(),
            None,
            cancel,
            rx,
        )
        .await
        .unwrap();

        conn.exec("use parquet_source_test_1").await.unwrap();
        let table = conn.describe("metrics").await.unwrap();
        assert_eq!(
            table.iter().map(|i| i.field()).collect::<Vec<_>>(),
            vec!["ts", "value", "id", "name"]
        );

        conn.exec("drop database parquet_source_test_1 force")
            .await
            .unwrap();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_read_parquet_with_projection() {
        let temp_dir = TempDir::new().unwrap();
        let path = create_test_parquet_file(&temp_dir, "test_projection.parquet", "t1", 50);

        // Test basic projection reading
        let file = fs::File::open(&path).unwrap();
        let builder =
            parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(file).unwrap();

        // Project only columns 0 and 1 (ts and id)
        let mask = parquet::arrow::ProjectionMask::roots(builder.parquet_schema(), vec![0, 1]);
        let reader = builder.with_projection(mask).build().unwrap();

        for batch in reader {
            let batch = batch.unwrap();
            assert_eq!(batch.num_columns(), 2);
            assert_eq!(batch.schema().field(0).name(), "ts");
            assert_eq!(batch.schema().field(1).name(), "id");
        }

        // Test with TDengine - only import projected columns
        let parser: Parser = serde_json::from_str(
            r#"{
              "parse": {
                "ts": { "as": "timestamp(ms)" },
                "id": { "as": "bigint" }
              },
              "model": {
                "name": "metrics_proj",
                "tags": ["id"],
                "columns": ["ts", "id"]
              }
            }"#,
        )
        .unwrap();

        let (rx, _tx) = flume::unbounded();
        let dsn: Dsn = "taos://".parse().unwrap();
        let pool = TaosBuilder::from_dsn(&dsn).unwrap().pool().unwrap();
        let conn = pool.get().await.unwrap();
        conn.exec_many([
            "drop database if exists parquet_source_test_2",
            "create database if not exists parquet_source_test_2",
        ])
        .await
        .unwrap();

        let cancel = tokio_util::sync::CancellationToken::new();
        let _guard = cancel.clone().drop_guard();
        parquet_to_taos(
            format!("parquet:{}?projection=0,1", path.display())
                .parse()
                .unwrap(),
            Some(parser),
            "taos:///parquet_source_test_2".parse().unwrap(),
            None,
            cancel,
            rx,
        )
        .await
        .unwrap();

        conn.exec("use parquet_source_test_2").await.unwrap();
        let table = conn.describe("metrics_proj").await.unwrap();
        assert_eq!(
            table.iter().map(|i| i.field()).collect::<Vec<_>>(),
            vec!["ts", "id"]
        );

        conn.exec("drop database parquet_source_test_2 force")
            .await
            .unwrap();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_read_parquet_with_named_projection_with_taos() {
        let temp_dir = TempDir::new().unwrap();
        let path = create_test_parquet_file(&temp_dir, "test_projection.parquet", "t1", 50);

        // Test basic projection reading
        let file = fs::File::open(&path).unwrap();
        let builder =
            parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(file).unwrap();

        // Project only columns 0 and 1 (ts and id)
        let mask = parquet::arrow::ProjectionMask::roots(builder.parquet_schema(), vec![0, 1]);
        let reader = builder.with_projection(mask).build().unwrap();

        for batch in reader {
            let batch = batch.unwrap();
            assert_eq!(batch.num_columns(), 2);
            assert_eq!(batch.schema().field(0).name(), "ts");
            assert_eq!(batch.schema().field(1).name(), "id");
        }

        // Test with TDengine - only import projected columns
        let parser: Parser = serde_json::from_str(
            r#"{
              "parse": {
                "ts": { "as": "timestamp(ms)" },
                "id": { "as": "bigint" }
              },
              "model": {
                "name": "metrics_proj",
                "tags": ["id"],
                "columns": ["ts", "id"]
              }
            }"#,
        )
        .unwrap();

        let (rx, _tx) = flume::unbounded();
        let dsn: Dsn = "taos://".parse().unwrap();
        let pool = TaosBuilder::from_dsn(&dsn).unwrap().pool().unwrap();
        let conn = pool.get().await.unwrap();
        conn.exec_many([
            "drop database if exists parquet_source_test_2_2",
            "create database if not exists parquet_source_test_2_2",
        ])
        .await
        .unwrap();

        let cancel = tokio_util::sync::CancellationToken::new();
        let _guard = cancel.clone().drop_guard();
        parquet_to_taos(
            format!("parquet:{}?projection=ts,id", path.display())
                .parse()
                .unwrap(),
            Some(parser),
            "taos:///parquet_source_test_2_2".parse().unwrap(),
            None,
            cancel,
            rx,
        )
        .await
        .unwrap();

        conn.exec("use parquet_source_test_2_2").await.unwrap();
        let table = conn.describe("metrics_proj").await.unwrap();
        assert_eq!(
            table.iter().map(|i| i.field()).collect::<Vec<_>>(),
            vec!["ts", "id"]
        );

        conn.exec("drop database parquet_source_test_2_2 force")
            .await
            .unwrap();
    }
    #[tokio::test(flavor = "multi_thread")]
    async fn test_read_multiple_parquet_files() {
        let temp_dir = TempDir::new().unwrap();
        let path1 = create_test_parquet_file(&temp_dir, "test1.parquet", "t1", 30);
        let path2 = create_test_parquet_file(&temp_dir, "test2.parquet", "t2", 40);
        let path3 = create_test_parquet_file(&temp_dir, "test3.parquet", "t3", 50);

        // Verify basic file reading
        let paths = vec![path1.clone(), path2.clone(), path3.clone()];
        let mut total_rows = 0;

        for path in &paths {
            let file = fs::File::open(path).unwrap();
            let reader =
                parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(file)
                    .unwrap()
                    .build()
                    .unwrap();

            for batch in reader {
                let batch = batch.unwrap();
                total_rows += batch.num_rows();
            }
        }

        assert_eq!(total_rows, 120);

        // Test importing multiple files to TDengine
        let parser: Parser = serde_json::from_str(
            r#"{
              "parse": {
                "ts": { "as": "timestamp(ms)" },
                "value": { "as": "bigint" },
                "id": { "as": "bigint" },
                "name": { "as": "varchar(64)" }
              },
              "model": {
                "name": "t_multi_{name}",
                "using": "metrics_multi",
                "tags": ["id","name"],
                "columns": ["ts", "value"]
              }
            }"#,
        )
        .unwrap();

        let (rx, _tx) = flume::unbounded();
        let dsn: Dsn = "taos://".parse().unwrap();
        let pool = TaosBuilder::from_dsn(&dsn).unwrap().pool().unwrap();
        let conn = pool.get().await.unwrap();
        conn.exec_many([
            "drop database if exists parquet_source_test_3",
            "create database if not exists parquet_source_test_3",
        ])
        .await
        .unwrap();

        // Import multiple files using comma-separated paths
        let multi_path = format!(
            "{},{},{}",
            path1.display(),
            path2.display(),
            path3.display()
        );
        let cancel = tokio_util::sync::CancellationToken::new();
        let _guard = cancel.clone().drop_guard();
        parquet_to_taos(
            format!("parquet:{}", multi_path).parse().unwrap(),
            Some(parser),
            "taos:///parquet_source_test_3".parse().unwrap(),
            None,
            cancel,
            rx,
        )
        .await
        .unwrap();

        conn.exec("use parquet_source_test_3").await.unwrap();
        let count: i64 = conn
            .query_one("select count(*) from metrics_multi")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(count, 120); // 30 + 40 + 50

        conn.exec("drop database parquet_source_test_3 force")
            .await
            .unwrap();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_parquet_batch_size() {
        let temp_dir = TempDir::new().unwrap();
        let path = create_test_parquet_file(&temp_dir, "test_batch.parquet", "t0", 1000);

        // Verify batch reading works
        let file = fs::File::open(&path).unwrap();
        let reader = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(file)
            .unwrap()
            .with_batch_size(100)
            .build()
            .unwrap();

        let batches: Vec<_> = reader.collect::<Result<Vec<_>, _>>().unwrap();

        // Should have multiple batches with batch_size=100
        assert!(batches.len() >= 10);
        for batch in &batches[..batches.len() - 1] {
            assert_eq!(batch.num_rows(), 100);
        }

        // Test importing with custom batch size to TDengine
        let parser: Parser = serde_json::from_str(
            r#"{
              "parse": {
                "ts": { "as": "timestamp(ms)" },
                "value": { "as": "bigint" },
                "id": { "as": "bigint" },
                "name": { "as": "varchar(64)" }
              },
              "model": {
                "name": "t_batch_{name}",
                "using": "metrics_batch",
                "tags": ["id","name"],
                "columns": ["ts", "value"]
              }
            }"#,
        )
        .unwrap();

        let (rx, _tx) = flume::unbounded();
        let dsn: Dsn = "taos://".parse().unwrap();
        let pool = TaosBuilder::from_dsn(&dsn).unwrap().pool().unwrap();
        let conn = pool.get().await.unwrap();
        conn.exec_many([
            "drop database if exists parquet_source_test_4",
            "create database if not exists parquet_source_test_4",
        ])
        .await
        .unwrap();

        let cancel = tokio_util::sync::CancellationToken::new();
        let _guard = cancel.clone().drop_guard();
        parquet_to_taos(
            format!("parquet:{}?batch_size=100", path.display())
                .parse()
                .unwrap(),
            Some(parser),
            "taos:///parquet_source_test_4".parse().unwrap(),
            None,
            cancel,
            rx,
        )
        .await
        .unwrap();

        conn.exec("use parquet_source_test_4").await.unwrap();
        let count: i64 = conn
            .query_one("select count(*) from metrics_batch")
            .await
            .unwrap()
            .unwrap();

        assert_eq!(count, 1000);

        conn.exec("drop database parquet_source_test_4 force")
            .await
            .unwrap();
    }
}
