---
sidebar_label: Parquet
title: Parquet
toc_max_heading_level: 4
---

import { Enterprise } from '../../assets/resources/_resources.mdx';

<Enterprise />

Parquet Data Out exports the result of one read-only TDengine SQL query to a single local Parquet file on the taosX server node. The output path is interpreted by the taosX server, not by the browser.

## Create a Parquet Data Out Task

In Explorer, open the Data Publisher page, choose Parquet as the target type, and configure:

- `TDengine DSN`: TDengine connection address, for example `taos+ws://root:taosdata@localhost:6041/db`.
- `SQL Query`: one read-only `SELECT` query. `WITH ... SELECT` queries are supported.
- `Output File`: output file path on the taosX server node. It must end with `.parquet`.
- `Overwrite Existing File`: whether an existing final file can be replaced after the new temporary file is closed successfully.
- `Compression`: `uncompressed`, `zstd`, `snappy`, `gzip`, `brotli`, or `lz4_raw`.
- `Compression Level`: optional for `zstd`, `gzip`, and `brotli`.
- `Row Group Size`: maximum rows per Parquet row group, default 131072. The Parquet writer only flushes to disk when a row group is full or the file is closed; a smaller value makes the `.part` file grow more frequently but reduces compression ratio and increases file metadata overhead. Valid range: 1024 to 10000000.

## Output Path

The output file is created on the taosX server node. It is not a browser-local path.

- Relative paths are written under `$DATA_DIR/tasks/<task_id>/<job_id>/`.
- Absolute paths are used as absolute paths on the taosX server node.
- The writer first creates `<final_name>.part` in the same directory, closes the Parquet writer, checks metadata, and then renames the temporary file to the final path.

Parquet Data Out does not support agent or via execution in the first version.

## DSN Example

```text
FROM 'taos+ws://root:taosdata@localhost:6041/db?query=select%20*%20from%20meters'
TO 'parquet:/tmp/meters.parquet?overwrite=false&compression=zstd&row_group_size=131072'
```

## Limits

- Only one read-only `SELECT` query is allowed.
- `SHOW`, `DESCRIBE`, `DESC`, and statements that change data, schema, session, or permissions are not allowed.
- Directory output and multi-file splitting are not supported.
- Failed exports restart from the beginning.
- Appending to an existing Parquet file is not supported.
- The task page does not provide a Parquet download action in the first version.
- The task configuration page does not estimate row count, file size, or remaining time.

## Performance and Observability

Large exports can run for a long time and consume disk, CPU, and network resources on the taosX server. TDengine result blocks are streamed and written as Arrow record batches to the Parquet writer.

The task metrics include Parquet output rows, batches, blocks, bytes, current file size, elapsed time, query time, write time, close time, and failed batches. Activity logs record query start, first result block, progress, writer close, completion, cancellation, and failures.
