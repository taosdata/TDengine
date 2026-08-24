---
sidebar_label: Observability and Troubleshooting
title: Observability and Troubleshooting
description: Use system views to monitor stream latency, throughput, historical processing, and recalculation progress
---

TDengine exposes stream-processing status through `information_schema.ins_streams`, `information_schema.ins_stream_tasks`, and `information_schema.ins_stream_recalculates`. Start with the stream-level view to identify an issue, drill down into the task-level view to locate a node or task, and inspect recalculation jobs when needed.

## View Overall Stream Status

The following query displays stream status, error information, and the primary runtime metrics:

```sql
SELECT stream_name,
       status,
       message,
       realtime_lag_ms,
       input_rows_per_sec_1m,
       output_rows_per_sec_1m,
       runner_result_latency_avg_1m_ms,
       history_progress_pct
FROM information_schema.ins_streams
ORDER BY stream_name;
```

| Metric | Unit | Description |
| --- | --- | --- |
| `realtime_lag_ms` | Milliseconds | Real-time processing lag of the slowest valid entry Reader |
| `input_rows_per_sec_1m` | Rows/second | Logical input rate accepted by the Trigger during the latest complete 60-second window |
| `output_rows_per_sec_1m` | Rows/second | Result rate successfully delivered by all final-result Runners during the latest complete 60-second window |
| `runner_result_latency_avg_1m_ms` | Milliseconds | Weighted average time from when a Runner starts processing a calculation request until it forms a logical result during the latest complete 60-second window |
| `history_progress_pct` | Percentage | Completion progress for historical-data processing when the stream is created, from 0 to 100 |

`realtime_lag_ms` uses the slowest progress among all valid entry Readers. A Reader that has caught up but temporarily has no new data does not cause this value to grow indefinitely. A stream that references an external data source has no WAL progress, so this field is `NULL`.

The input rate counts logical rows accepted by the stream after filtering and routing. The output rate counts only final result rows that were delivered successfully. These metrics represent different layers of data and must not be used directly to calculate a loss rate. Window aggregation, filtering, and producing multiple results from one input can all cause the values to differ.

Result latency excludes queueing and network time before the request reaches a Runner, as well as result-table writes or notifications after the result has formed.

## Drill Down into Tasks

When stream status is abnormal, a stream-level metric is `NULL`, or you need to locate a specific node, query the task view:

```sql
SELECT stream_name,
       task_id,
       type,
       deploy_id,
       node_type,
       node_id,
       status,
       last_update,
       message,
       input_rows_per_sec_1m,
       output_rows_per_sec_1m,
       runner_result_latency_avg_1m_ms
FROM information_schema.ins_stream_tasks
WHERE stream_name = 'your_stream_name'
ORDER BY type, deploy_id, task_id;
```

Task-level metrics depend on task responsibility:

- An entry `Reader` provides the physical input rate, which counts rows actually read and processed by that Reader.
- A `Runner` responsible for final result delivery provides the output rate and result formation latency.
- These columns are `NULL` for `Trigger` tasks, calculation-data Readers, and non-final-result Runners.

Use `status` to check whether a task is healthy and `last_update` to determine whether its status and metrics are still fresh. The stream-level view does not have a `last_update` column.

## View Historical-Data Processing Progress

For a stream created with `STREAM_OPTIONS(FILL_HISTORY)` or `STREAM_OPTIONS(FILL_HISTORY_FIRST)`, `ins_streams.history_progress_pct` reports progress over the initial historical range:

- `0` through `99`: historical-data processing is incomplete.
- `100`: historical-data processing is complete.
- `NULL`: historical-data processing is not enabled or no valid progress is currently available.

This percentage represents coverage of the original historical time range, not the percentage of result rows already produced.

## View Manual Recalculations

The following query displays the range, progress, and status of each manual recalculation job:

```sql
SELECT stream_name,
       recalc_id,
       start,
       end,
       progress,
       status,
       request_time,
       message
FROM information_schema.ins_stream_recalculates
WHERE stream_name = 'your_stream_name'
ORDER BY start, recalc_id;
```

| Status | Description |
| --- | --- |
| `Pending` | The request has been accepted, but recalculation has not started |
| `Running` | Recalculation has started but is not yet complete |
| `Finished` | Recalculation is complete and progress is `100%` |
| `Failed` | An unrecoverable error prevents the recalculation from completing |

During a rolling upgrade, `status` may be `NULL` when an older task can report recalculation progress but not the typed status. The `progress` field remains available.

A successful `RECALCULATE STREAM` response means that the request was accepted, not that execution finished. Recalculation runs in the background. Unfinished requests are restored after a service or stream-task restart or redeployment, and transient execution failures are retried automatically. Use `recalc_id` to track one request and avoid submitting duplicates while it is `Pending` or `Running`. `request_time` is the time the mnode accepted the request; `message` contains the status or error text when available.

Terminal recalculation records are retained for one hour from the time the mnode first observes the terminal state, with a maximum of 100 terminal records per stream. `Pending` and `Running` records do not count toward this limit. Records are held only in memory and may disappear after a process restart.

This retention policy applies to terminal records; it is separate from persistence of unfinished requests.

## Understand `NULL` and Zero Values

The one-minute metrics cover the latest 60 complete seconds and exclude the current second. After a task starts, restarts, or is redeployed, it must first produce a complete window; until then, the corresponding metrics are `NULL`.

- If there is no input or output during a complete window, the corresponding rate is `0`.
- If a Runner forms no result sample during a complete window, result latency is `NULL`.
- The stream-level output rate requires a valid complete window from every final-result Runner. If any required Runner is not ready, this field is `NULL`.
- Stream-level result latency also requires at least one result sample. Otherwise, it is `NULL`.
- A metric that does not apply to a task type is `NULL`.
- A `NULL` field does not invalidate unrelated fields. For example, input rate can remain available when there is no result-latency sample.
- During a short heartbeat interruption, the management node may retain the last successful metric snapshot. Use task `status` and `last_update` to evaluate freshness.
- During a rolling upgrade, tasks that have not yet been upgraded cannot provide the new metrics, so the corresponding columns may temporarily be `NULL`.

## Recommended Troubleshooting Sequence

1. Query `status` and `message` in `ins_streams` to confirm that the stream is healthy.
2. Check whether `realtime_lag_ms` continues to increase. If the stream uses an external data source, `NULL` is expected.
3. Check input, output, and result latency. A nonzero input rate with a zero output rate is not necessarily a failure; it can also occur before a window closes or when filtering produces no result.
4. Query `ins_stream_tasks` and use `status`, `last_update`, `node_id`, and task-level metrics to locate the affected task.
5. For historical-data processing or manual recalculation issues, inspect `history_progress_pct` and `ins_stream_recalculates`, respectively.

For complete column definitions, see [System Information](../05-tdengine-sql/09-system-info/01-meta.md#ins_streams).
