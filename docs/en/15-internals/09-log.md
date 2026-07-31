---
sidebar_label: Logging System
title: Logging System
description: TDengine normal logs and slow logs
toc_max_heading_level: 4
---

TDengine records runtime status and diagnostic information in two categories: normal logs and slow logs. Normal logs describe engine operation, while slow logs record operations whose execution time exceeds the configured threshold.

## Normal Logs

### Implementation

- Normal logs can be synchronous or asynchronous. Synchronous logging writes immediately; asynchronous logging first writes to a circular buffer and flushes periodically.
- The asynchronous circular buffer is 20 MB. If one log write is larger than the available buffer space, that record is discarded and the log records `...Lost N lines here...`.
- The asynchronous thread refreshes disk information every second to determine whether enough space remains.
- The thread flushes according to a dynamically adjusted interval:
  - If buffered data is less than one tenth of the buffer, it is not flushed unless one second has elapsed.
  - If buffered data exceeds one tenth of the buffer, all buffered data is flushed.
  - The interval defaults to 25 ms. For small volumes it increases by 5 ms up to 25 ms; above one third of the buffer it is 5 ms; between one quarter and one third it decreases by 5 ms to a minimum of 5 ms; between one tenth and one quarter it remains unchanged.

### File Behavior

- Client processes use names such as `taoslogX.Y`, where `X` is empty or 0 through 9 and `Y` is 0 or 1. On Windows, only the `taoslog.Y` form is used.
- Server processes use `taosdlog.Y`, where `Y` is 0 or 1.
- For a default log directory of `/var/log/taos/`, a client checks `taoslog0.Y` through `taoslog9.Y` and uses the first sequence number not already held by another process. If all ten are in use, it writes to `taoslog.Y`.
- After choosing a sequence number, the process chooses suffix 0 or 1. If neither file exists, it starts with 0; if only one exists, it uses that suffix; if both exist, it uses the more recently modified file.
- When a file reaches `numOfLogLines`, logging switches to the other suffix. The completed file is renamed with a timestamp and compressed asynchronously. For example, `taoslog3.0` may become `taoslog.1735616543.gz` while writes continue in `taoslog3.1`.
- `logKeepDays` controls retention of archived logs. Retention is measured as elapsed time, not calendar days.
- `numOfLogLines` defaults to ten million lines and supports values from 1,000 to two billion.

## Slow Logs

Operations that exceed the configured duration are written to slow logs for performance analysis and troubleshooting.

### Buffering

- Slow SQL records are uploaded in batches.
- To survive process crashes, pending records are cached in temporary files.
- Each generated record enters a queue. The slow-log thread groups records by `clusterId` and writes each group to a separate file.

The queued data uses the following structure:

```c
typedef struct {
    int64_t clusterId;
    char   *value;
} MonitorSlowLogData;
```

A client process may contain connections to multiple clusters, so temporary files group records by cluster. Their names follow this pattern:

```text
{tmp dir}/tdengine_slow_log/tdengine-{clusterId}-{processId}-{rand}
```

`processId` distinguishes client processes.

### Upload

The uploader reads a temporary file line by line, constructs a JSON array, and uploads batches close to 1 MB. After a successful upload it records the read position, and the asynchronous callback continues from that position until the file is complete. The file is cleared after all data is uploaded. A failed callback records the error and continues processing subsequent data.

Slow logs are uploaded at the following times:

- Periodically, once per `monitorInterval`
- When a client exits normally, after which successfully uploaded files are deleted
- After an abnormal exit, when a later connection to the same cluster scans and locks matching temporary files, uploads them, and deletes them

Because uploading and removing uploaded records cannot form one atomic operation, a crash between those steps can cause duplicate upload. Duplicate records overwrite existing data rather than being lost. For performance, the slow-log thread flushes records to the operating-system buffer but does not call `fsync` for every record, so a machine power failure can still lose a small amount of slow-log data.

### File Behavior

- Slow logs are written locally and, when monitoring is enabled, sent through taosAdapter to taosKeeper for structured storage.
- A file is created only on a day that has slow records and is named `taosSlowLog.yyyy-mm-dd`.
- `logDir` controls its location. Multiple clients use the same daily file in that directory.
- Slow-log files are not deleted or compressed automatically.
- Slow logs use `logDir`, `minimalLogDirGB`, and `asyncLog`. `numOfLogLines` and `logKeepDays` do not apply.

## Log Levels

TDengine defines the following bit-based log levels:

```c
typedef enum {
    DEBUG_FATAL = 1,
    DEBUG_ERROR = 1,
    DEBUG_WARN = 2,
    DEBUG_INFO = 2,
    DEBUG_DEBUG = 4,
    DEBUG_TRACE = 8,
    DEBUG_DUMP = 16,
    DEBUG_SCREEN = 64,
    DEBUG_FILE = 128
} ELogLevel;
```

Common combinations include:

- `131 = 128 + 2 + 1`: file, info, and error
- `135 = 128 + 4 + 2 + 1`: file, debug, info, and error
- `143 = 128 + 8 + 4 + 2 + 1`: file, trace, debug, info, and error

Set the corresponding logging parameter to enable the required combination.
