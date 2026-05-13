# source-parquet

A data source module for reading Apache Parquet files and importing data into TDengine.

## Features

- Read one or multiple Parquet files
- Support column projection (by index or name)
- Configurable batch size for processing
- Asynchronous data processing with backpressure control

## DSN Format

```
parquet:path/to/file1.parquet,path/to/file2.parquet?batch_size=1000&projection=col1,col2&unprocessed_batches=64
```

## Parameters

- `path` (required): Comma-separated list of Parquet file paths
- `batch_size` (optional, default: 1000): Number of rows to read per batch
- `projection` (optional): Column projection, can be:
  - Column names: `projection=col1,col2,col3`
  - Column indices (0-based): `projection=0,1,2`
- `unprocessed_batches` (optional, default: 64): Maximum number of unprocessed batches in the pipeline

## Example

```rust
use source_parquet::parquet_to_taos;

// Read from Parquet files and write to TDengine
parquet_to_taos(
    from_dsn,      // parquet:data.parquet?batch_size=1000
    parser,        // Optional data parser
    to_dsn,        // taos://localhost:6030
    task_id,       // Optional task ID
    cancel_token,  // Cancellation token
    notifier,      // Task notification sender
).await?;
```
