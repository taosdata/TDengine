# boosttools

High-performance cluster-to-cluster data synchronization tool for TDengine 3.x, using native `taos_fetch_raw_block()` + `taos_write_raw_block()` APIs for zero-copy block transfer.

**~8x faster than `taosdump`** in head-to-head benchmarks, with zero intermediate disk I/O.

---

## Table of Contents

- [Why boosttools](#why-boosttools)
- [Architecture](#architecture)
- [Build & Install](#build--install)
  - [Prerequisites](#prerequisites)
  - [Build from source](#build-from-source)
  - [Verify installation](#verify-installation)
- [Quick Start](#quick-start)
- [CLI Reference](#cli-reference)
- [Usage Scenarios](#usage-scenarios)
  - [Full cluster migration](#1-full-cluster-migration)
  - [Single database sync](#2-single-database-sync)
  - [Schema-only / Data-only sync](#3-schema-only--data-only-sync)
  - [Resume interrupted sync](#4-resume-interrupted-sync)
  - [Incremental sync by time range](#5-incremental-sync-by-time-range)
  - [Dry run preview](#6-dry-run-preview)
- [Performance Tuning](#performance-tuning)
- [Progress & Checkpointing](#progress--checkpointing)
- [Error Handling](#error-handling)
- [FAQ](#faq)
- [Limitations](#limitations)
- [Troubleshooting](#troubleshooting)
- [Benchmark Results](#benchmark-results)

---

## Why boosttools

`taosdump` exports data through SQL queries → Avro serialization → disk dump → re-import via SQL INSERT. On large datasets with many tables, this leads to:

- **Hangs / crashes** when exporting 600K+ child tables.
- **Slow throughput** (~50K rows/s) due to row-level SQL parsing.
- **3x disk I/O** (read source → write Avro → read Avro → write dest).
- **Large temp files** (hundreds of MB to GB of Avro intermediates).

`boosttools` solves these by using TDengine's internal raw block API: data moves directly from source memory to destination memory as columnar blocks, with no serialization and no disk intermediate.

| Aspect | taosdump | boosttools |
|--------|----------|------------|
| Protocol | SQL INSERT strings | Raw columnar blocks |
| Serialization | Avro | None (zero-copy) |
| Disk I/O | 3x (read → Avro → read) | 0 (in-memory) |
| Parallelism | Per-file thread | Per-table worker with pool |
| Throughput (typical) | ~50K rows/s | **~400K rows/s** |
| 100GB / HDD | ~5.8 hours (often hangs) | **~44 min** |

---

## Architecture

```
Source Cluster A                       Destination Cluster B
┌──────────────┐                       ┌──────────────┐
│   taosd      │                       │   taosd      │
└──────┬───────┘                       └──────▲───────┘
       │ taos_fetch_raw_block()               │ taos_write_raw_block()
       ▼                                      │
  ┌────────────────────────────────────────────┘
  │       boosttools (N parallel workers)
  │  ┌─────────┐  ┌─────────┐  ┌─────────┐
  │  │ Worker0 │  │ Worker1 │  │ WorkerN │
  │  └─────────┘  └─────────┘  └─────────┘
  │       │            │            │
  │  ┌────▼────────────▼────────────▼────┐
  │  │  Connection Pool (src + dst)      │
  │  └────────────────────────────────────┘
  │
  └─ Work Queue: [child_table_1, child_table_2, ..., child_table_N]
```

**Two-phase pipeline:**

1. **Schema sync** — replicates database config, supertable schema, and child tables (with tags) via batch `CREATE TABLE` (1000 tables per batch).
2. **Data sync** — N parallel workers pick tables from a shared queue; each worker uses `taos_fetch_raw_block()` on source and `taos_write_raw_block()` on destination.

---

## Build & Install

### Prerequisites

- Linux (Ubuntu 18.04+, CentOS 7+) or macOS
- TDengine client library (`libtaos.so`) installed — either from package or source build
- GCC 7+ or Clang, pthread support
- GNU Make

### Build from source

**Option A: Installed TDengine client (common case)**

```bash
cd tools/boosttools
make TDENGINE_DIR=/usr/local/taos
```

This automatically finds `taos.h` under `${TDENGINE_DIR}/include` and links against `${TDENGINE_DIR}/driver/libtaos.so`. The build embeds an rpath so the binary doesn't need `LD_LIBRARY_PATH`.

**Option B: Inside the TDengine source tree**

```bash
cd <TDengine-source>/tools/boosttools
make
```

The Makefile detects the source tree and uses `../../include/client/taos.h` plus the library from `../../build/build/lib/`.

**Option C: Debug build (with AddressSanitizer)**

```bash
make DEBUG=1
```

**Option D: CMake**

```bash
cd tools/boosttools
mkdir build && cd build
cmake .. -DCMAKE_BUILD_TYPE=Release -DTDENGINE_DIR=/usr/local/taos
make -j
```

### Verify installation

```bash
./boosttools --help
```

You should see the CLI help screen. Confirm the library link:

```bash
ldd boosttools | grep taos
# Expected: libtaos.so => /usr/local/taos/driver/libtaos.so (0x00007f...)
```

### Install system-wide (optional)

```bash
sudo make install
# Installs to /usr/local/bin/boosttools
```

---

## Quick Start

Sync database `mydb` from cluster A (10.0.1.1:6030) to cluster B (10.0.2.1:6030):

```bash
./boosttools \
    --src-host 10.0.1.1 --src-port 6030 \
    --dst-host 10.0.2.1 --dst-port 6030 \
    --database mydb \
    --workers 16
```

`boosttools` will:

1. Create connection pools to both clusters (18 connections each).
2. Replicate database / supertable / child table schemas.
3. Launch 16 worker threads that stream data in parallel using raw blocks.
4. Print progress every 5 seconds, save a checkpoint every 30 seconds.

Example output:

```
[2026-04-14 03:28:15] ========== Schema Sync Start ==========
[2026-04-14 03:28:15] --- Syncing child tables: mydb.meters ---
[2026-04-14 03:28:21] Synced 10000 child tables for mydb.meters
[2026-04-14 03:28:21] ========== Data Sync Start ==========
[2026-04-14 03:28:21] Work queue: 10000 tables to sync
[2026-04-14 03:28:26] [Progress] 1794/10000 tables (17.9%) | 285K rows/s | 19.6 MB/s
[2026-04-14 03:28:41] [Progress] 6364/10000 tables (63.6%) | 428K rows/s | 29.4 MB/s
[2026-04-14 03:28:56] [Progress] 10000/10000 tables (100%) | 426K rows/s | errors: 0
[2026-04-14 03:28:56] ========== Data Sync Complete ==========
```

---

## CLI Reference

```
boosttools [options]
```

### Source cluster

| Option | Default | Description |
|--------|---------|-------------|
| `--src-host <ip>` | `localhost` | Source TDengine host (FQDN or IP) |
| `--src-port <port>` | `6030` | Source TDengine port |
| `--src-user <user>` | `root` | Source username |
| `--src-pass <pass>` | `taosdata` | Source password |

### Destination cluster

| Option | Default | Description |
|--------|---------|-------------|
| `--dst-host <ip>` | `localhost` | Destination host |
| `--dst-port <port>` | `6030` | Destination port |
| `--dst-user <user>` | `root` | Destination username |
| `--dst-pass <pass>` | `taosdata` | Destination password |

### Scope

| Option | Default | Description |
|--------|---------|-------------|
| `--database <db>` | *(all)* | Sync only this database. Leave empty to sync all user databases (excludes `information_schema`, `performance_schema`, `log`). |
| `--stable <name>` | *(all)* | Sync only this supertable within the selected database. |
| `--time-start <ts>` | — | ISO timestamp filter (e.g., `'2024-01-01 00:00:00'`) — only sync rows with `ts >= time-start`. |
| `--time-end <ts>` | — | ISO timestamp — only sync rows with `ts < time-end`. |

### Performance

| Option | Default | Description |
|--------|---------|-------------|
| `--workers <n>` | `16` | Number of parallel worker threads (1-64). |
| `--pool-size <n>` | `workers + 2` | Connection pool capacity for each cluster. Max 128. |

### Mode

| Option | Description |
|--------|-------------|
| `--schema-only` | Replicate schema (DB/stable/tables) but don't transfer row data. |
| `--data-only` | Skip schema sync — destination must already have matching schemas. |
| `--resume` | Resume from last checkpoint (`boost_progress.json` in the working directory). |
| `--dry-run` | Print what would be executed without making changes on the destination. |
| `--verbose` | Enable DEBUG-level logging. |
| `--help` | Show help and exit. |

---

## Usage Scenarios

### 1. Full cluster migration

Sync all user databases from A to B:

```bash
./boosttools \
    --src-host A.example.com --src-port 6030 --src-user root --src-pass your_pass \
    --dst-host B.example.com --dst-port 6030 --dst-user root --dst-pass your_pass \
    --workers 32
```

### 2. Single database sync

```bash
./boosttools \
    --src-host A.example.com --dst-host B.example.com \
    --database mydb --workers 16
```

### 3. Schema-only / Data-only sync

Replicate the empty schema structure first, then populate data later (useful for staged migrations):

```bash
# Step 1: sync schema only
./boosttools --src-host A --dst-host B --database mydb --schema-only

# Step 2: sync data separately (possibly on a different machine)
./boosttools --src-host A --dst-host B --database mydb --data-only
```

### 4. Resume interrupted sync

If the sync was interrupted (network issue, Ctrl+C, OOM, etc.), `boost_progress.json` holds completed tables. Resume where you left off:

```bash
./boosttools --src-host A --dst-host B --database mydb --data-only --resume
```

The progress file is written in the working directory. Keep it alongside the binary for resume to work.

### 5. Incremental sync by time range

Sync only rows newer than a given timestamp (e.g., daily delta):

```bash
./boosttools \
    --src-host A --dst-host B --database mydb --data-only \
    --time-start '2024-12-01 00:00:00' \
    --time-end '2024-12-02 00:00:00'
```

Bounded time ranges also help split very large tables into digestible chunks.

### 6. Dry run preview

Preview what would be synced without modifying the destination:

```bash
./boosttools --src-host A --dst-host B --database mydb --dry-run --verbose
```

---

## Performance Tuning

| Situation | Recommendation |
|-----------|---------------|
| Many small tables (<1MB each, 100K+ tables) | `--workers 32` (more parallelism pays off) |
| Few large tables (GBs each) | `--workers 8-16` + split by `--time-start` / `--time-end` |
| Slow network (<100 Mbps) | `--workers 8` (more parallelism yields diminishing returns when network-bound) |
| Fast LAN + plenty of CPU | `--workers 32-64` |
| Destination under heavy write load | `--workers 8` + `--pool-size 16` to reduce contention |

**Destination tuning (on `taosd`):**

```ini
# /etc/taos/taos.cfg on the destination
numOfVnodeQueryThreads    32
numOfVnodeFetchThreads    16
numOfCommitThreads        8
```

After editing, restart `taosd` or use `ALTER DNODE` for dynamic parameters.

**Client-side tuning:** No special configuration needed. `boosttools` auto-adjusts based on `--workers` / `--pool-size`.

---

## Progress & Checkpointing

While running, `boosttools` writes progress snapshots to `boost_progress.json` every 30 seconds:

```json
{
  "tables_done": 6364,
  "tables_total": 10000,
  "rows_transferred": 11137000,
  "bytes_transferred": 801837056,
  "errors": 0,
  "completed": [
    "mydb.meters.d_0",
    "mydb.meters.d_1",
    ...
  ]
}
```

The `completed` array lists fully-synced tables in `db.stable.table` format. When `--resume` is used, `boosttools` marks these tables as already synced and skips them.

### Progress output format

Live progress lines (stdout):

```
[Progress] 6364/10000 tables (63.6%) | 11137000 rows | 764.7 MB | 428346 rows/s | 29.4 MB/s | elapsed: 00:00:26 | errors: 0
```

Fields:
- **tables done / total** and percentage
- **rows transferred** (monotonic counter)
- **bytes transferred** (approximate, based on row * field count * 8 bytes)
- **rows/s** and **MB/s** (averaged since start)
- **elapsed** wall-clock time
- **errors** count (see [Error Handling](#error-handling))

---

## Error Handling

`boosttools` distinguishes between fatal errors (abort) and transient errors (retry + continue).

| Error | Behavior |
|-------|----------|
| Source / destination connection fails | Retry 3 times with 1s delay, then abort if unsuccessful |
| Connection pool timeout (5s) | Worker logs error, skips table, continues |
| `SELECT * FROM table` fails | Skip this table, increment error counter, move on |
| `write_raw_block` fails | Retry once; on second failure, skip block and continue |
| Destination database does not exist | Auto-created by schema sync unless `--data-only` |
| "Table does not exist" on destination | Can happen if `--data-only` was used without prior schema sync — run schema sync first |

Each error is logged to stderr with timestamp. Errors do not halt the overall sync — `boosttools` always attempts to continue and finishes with a summary of error count.

**Graceful shutdown:** `Ctrl+C` (SIGINT) or `SIGTERM` triggers a graceful exit — workers finish their current table before terminating. Progress is saved to `boost_progress.json` before exit so `--resume` can pick up.

---

## FAQ

### Q1: Does boosttools support TDengine 2.x?

No. `boosttools` relies on `taos_write_raw_block()` which is only available in TDengine 3.x. Tested against 3.3.x and 3.4.x.

### Q2: Does schema need to pre-exist on the destination?

No (default). `boosttools` will replicate the database, supertables, and child tables automatically. If you want to skip schema sync (e.g., the destination already matches), use `--data-only`.

### Q3: What happens if the destination table has a different schema?

`taos_write_raw_block_with_fields()` is used as a fallback when the simple write fails, carrying field metadata. However, **schema drift is not automatically reconciled** — if the destination schema is incompatible (e.g., column types differ), the write will fail and the error will be logged. Best practice: let `boosttools` create the schema, or verify schemas match manually.

### Q4: Is the transfer transactional?

No. `boosttools` is **eventually consistent** within each table (all rows for a table are written in order). Across tables, writes are independent. If you need point-in-time consistency, pause writes to the source before running `boosttools`.

### Q5: Can I run multiple boosttools instances in parallel?

Yes, as long as each instance targets a different database or uses non-overlapping `--stable` / `--time-start`/`--time-end` ranges. Running parallel instances on the same database/table set causes duplicate writes (TDengine will overwrite based on timestamp, so no corruption, but wasteful).

### Q6: Does it handle TDengine cloud / TDengine Enterprise?

`boosttools` uses the native protocol on port 6030 — it works with any TDengine 3.x deployment that accepts native connections, including TDengine Enterprise. For TDengine Cloud, use the native port exposed by the cloud endpoint.

### Q7: Does it support TTL, compression, replica settings?

Schema sync uses `SHOW CREATE DATABASE` / `SHOW CREATE STABLE` on the source and replays the DDL on the destination. This preserves most settings including TTL, compression, and keep duration. **Replica count on the destination is not auto-adjusted** — if you want different replicas on B, modify the database config after sync.

### Q8: Is it safe to run against a production source?

`boosttools` is **read-only on the source** — it only runs `SHOW`, `DESCRIBE`, and `SELECT` statements. Query load equals `--workers` concurrent `SELECT * FROM table` queries. Adjust `--workers` based on source capacity.

---

## Limitations

- **TDengine 3.x only** (requires `taos_write_raw_block` API).
- **No schema migration** — if source and destination schemas differ, the sync will fail. Use matching schemas or let `boosttools` create them fresh.
- **No replica count migration** — destination inherits its own cluster's default.
- **No TopicDB / TMQ consumer offset sync** — only table data is transferred.
- **Progress file format is not versioned** — don't mix progress files across tool versions.
- **Tested up to 1.05 billion rows / 600K child tables** in benchmarks; larger scales should work but haven't been exhaustively tested.
- **No built-in data validation** — for strict correctness, run `SELECT COUNT(*)` comparisons after sync.

---

## Troubleshooting

### `Error: libtaos.so not found`

Set `LD_LIBRARY_PATH` explicitly:

```bash
export LD_LIBRARY_PATH=/usr/local/taos/driver:$LD_LIBRARY_PATH
./boosttools --help
```

Or rebuild with explicit rpath:

```bash
make clean
make TDENGINE_DIR=/usr/local/taos
```

### `failed to connect to server: Mnode not found`

The target TDengine instance hasn't fully initialized. Check:

```bash
taos -h <host> -P <port> -s 'SHOW DNODES' < /dev/null
```

If this fails, wait for `taosd` to fully start (can take ~10s on first boot) and retry.

### `boost_pool_get: timed out after 5 seconds`

The connection pool is exhausted because a worker is holding a connection for too long. This usually indicates:

- A very slow query on a large table — consider splitting with `--time-start`/`--time-end`.
- Network issues between source and destination — check latency.
- Destination `taosd` is overloaded — reduce `--workers`.

Increase pool size:

```bash
./boosttools --workers 16 --pool-size 32 ...
```

### `Table does not exist` errors during data sync

Schema sync didn't complete before data sync started, or schema sync was skipped with `--data-only`. Solutions:

1. Run schema sync first: `./boosttools ... --schema-only`
2. Or run full sync in one go: `./boosttools ...` (default does both phases)

### High error count but sync "completes"

Check `/tmp/boost_progress.json` to see which tables are marked complete. Run a count comparison:

```bash
# On source
taos -h A -s 'SELECT COUNT(*) FROM mydb.meters' < /dev/null

# On destination
taos -h B -s 'SELECT COUNT(*) FROM mydb.meters' < /dev/null
```

If counts differ, resume with: `./boosttools ... --data-only --resume`

### Compilation: `taos.h: No such file or directory`

Override the include path:

```bash
make TDENGINE_DIR=/custom/path/to/taos
# Or manually:
make CFLAGS="-I/custom/path/include" LDFLAGS="-L/custom/path/lib -ltaos"
```

### Windows support

Not supported. The tool uses pthreads, POSIX `getopt_long`, and other Unix APIs. WSL2 works fine.

---

## Benchmark Results

**Test environment:**

| Item | Detail |
|------|--------|
| OS | Ubuntu 22.04.5 LTS |
| CPU | 32 cores (x86_64) |
| Memory | 64GB |
| Disk | HDD (mechanical) |
| Network | localhost loopback |
| TDengine | 3.4.1.0 (built from source) |

**Test dataset:** 10,000 child tables × 1,750 rows = 17.5M rows total (~1.6GB raw), 8 data columns + 3 tag columns.

| Metric | taosdump | boosttools | Delta |
|--------|----------|------------|-------|
| Total time | 336s | **42s** | **8.0x faster** |
| Throughput | 52,083 rows/s | **416,666 rows/s** | **8.0x** |
| Peak throughput | — | **428,346 rows/s** | — |
| Tables synced | 10,000 | 10,000 | ✅ 100% |
| Rows synced | 17,500,000 | 17,500,000 | ✅ 100% |
| Disk temp files | 704MB (Avro) | **0 bytes** | No temp files |
| Errors | 0 | 0 | — |

**Projections for 100GB workloads:**

| Scenario | taosdump | boosttools |
|----------|----------|------------|
| 100GB / localhost HDD | ~5.8 hours (often hangs) | **~44 min** |
| 100GB / 1Gbps LAN | ~8+ hours | **~1 hour** |
| 1.3GB / 600K tables | 10-30 min (frequently crashes) | **< 2 min** |

---

## License

MIT — see project root LICENSE file.

## Contributing

This tool lives in the TDengine source tree under `tools/boosttools/`. Issues and PRs are welcome at the upstream repository.
