---
sidebar_label: Workload Recording and Replay
title: Workload Recording and Replay
toc_max_heading_level: 3
---

Workload recording captures API calls in a Native client process. Use `taosreplay` to inspect the recording and reissue those calls against a test cluster to reproduce an issue. Recording covers Native connections, not WebSocket or REST connections. Each client process creates one recording session; a directory can contain multiple sessions.

:::warning
Recordings contain unmasked SQL and bound parameter values. Restrict access to the recording directory and files, and handle copies as sensitive data. Replay can execute writes and DDL. Always specify the address of an isolated test cluster, and verify the target database and account permissions before replaying. Never replay against a production cluster.
:::

## Start Recording

On the machine running the application, set these options in the **client's** `taos.cfg`:

```ini
workloadRecord 1
workloadRecordDir /var/log/taos/workload
workloadRecordBufferSize 256
```

Restart the client application, then run the workload you want to reproduce. Recordings are written to `workloadRecordDir`. If it is unset, the default is the `workload` directory under the client's `logDir`. The directory contains segment files whose names start with `wlr-` and end with `.wlr`, along with session manifests. After the workload finishes, exit the client normally before copying the recording for analysis. To stop recording, set `workloadRecord` to `0` and restart the application.

These settings cannot be changed dynamically. See the [taosc configuration parameters](../../03-components/02-taosc.md#debugging-related) for their values and ranges. If the directory is unavailable or a recording write fails, client API calls continue, but the recording may be incomplete. Check the client logs for `WLR` messages to confirm the recording status.

## Inspect the Recording

`taosreplay` is a development tool built from source on Linux; it is not included in the product installation package. Find it in `build/bin` under the source build directory. Inspect the recording before replaying it:

```bash
taosreplay inspect --input /var/log/taos/workload
```

`--input` accepts a directory containing `.wlr` files or an individual `.wlr` file. You can provide it more than once. Add `--verbose` to inspect individual events; its output may contain SQL text, so protect terminal output as well.

## Export the Recording

To compute a call distribution or analyse a recording offline, export it as text:

```bash
taosreplay export --input /var/log/taos/workload --format csv --output /tmp/recording.csv
```

`--format csv` writes one event per line and ends each line with a `session` column identifying the recording session; `--format jsonl` writes one JSON object per line with the same fields. Omitting `--output` writes to standard output so the data can be piped through other tools. The export contains SQL and bind data, so treat it as sensitive.

## Replay on a Test Cluster

Prepare the required databases and accounts on an isolated test cluster. Use `--target-host` to redirect every recorded connection to that cluster. The default port is `6030` if `--target-port` is omitted. This example supplies credentials through an environment variable so the password does not appear in the command arguments or shell history:

```bash
export TAOSREPLAY_USER_PASSWORD='root=test-cluster-password'
taosreplay replay --input /var/log/taos/workload \
  --target-host test-taos.example.com --target-port 6030
unset TAOSREPLAY_USER_PASSWORD
```

For recordings with multiple users, repeat `--user-password user=password` for each account. This exposes passwords in the process list and shell history; use it only in a controlled environment. To redirect distinct recorded addresses separately, repeat `--endpoint-map old-host:port=new-host:port` instead. Endpoint mappings cannot be combined with `--target-host` or `--target-port`. Map every recorded address so no connection reaches the original cluster by accident.

By default, replay refuses a recording with a gap in its events. Use `--allow-incomplete` only if replaying the intact prefix is acceptable. A call with a BEGIN event but no END event does not, by itself, count as an event gap. `--unfinished-timeout` bounds the wait for unfinished calls; it defaults to 300 seconds, and `0` removes the limit. `--report-dir` writes `replay-report.json` to an existing directory; replay also prints a report to the terminal.

Replay reproduces the recorded pacing by default. `--time-scale` compresses or stretches it: `--time-scale 10` turns a recorded 10-second gap into one second. It changes only the send timing — neither the number nor the order of calls changes.

Replay by default also refuses the shape where an asynchronous call has not reached DONE before the same recorded thread issues another call: the lane's synchronous wait would then queue a recorded kill or stop behind the operation it was meant to cancel. Use `--allow-async-overlap` when replaying such calls serially in recorded begin order is acceptable. The flag does not reproduce the overlap itself — asynchronous APIs are still reissued synchronously, so each call finishes before the next begins — but it lets recordings such as taosAdapter's asynchronous STMT2 writes replay at all. If a recorded `connect` names a database the target does not have yet, replay reconnects without the default database and lets the recording's own `CREATE DATABASE` and `use` establish it, so a session recorded from an empty instance bootstraps itself. That retry happens only when the failure is the missing database; replay then reports that the connection has no default database, because statements relying on it fail until the recording issues `use`. Other connection failures (credentials, network) are reported as they are and are not retried.

## taosreplay Options

| No. | Option | Command | Description |
| --- | ------ | ------- | ----------- |
| 1 | `--input <path>` | `inspect`, `export`, `replay` | Required and repeatable. Accepts a recording directory or an individual `.wlr` segment. |
| 2 | `--verbose` | `inspect` | Prints individual event details, which may include SQL; the default output is a summary. |
| 3 | `--format <csv\|jsonl>` | `export` | Export format: `csv` for tabular text, `jsonl` for one JSON object per line. |
| 4 | `--output <path>` | `export` | Writes to the given file; without it the export goes to standard output. |
| 5 | `--user-password <user=password>` | `replay` | Supplies a recorded user's password for the target cluster; repeat for multiple users. At least one credential is required. If this option is omitted, `TAOSREPLAY_USER_PASSWORD=user=password` can supply one credential. Command-line passwords may appear in process lists and shell history. |
| 6 | `--target-host <host>` | `replay` | Redirects all recorded connections to this host. Cannot be combined with `--endpoint-map`. Explicitly specify an isolated test cluster. |
| 7 | `--target-port <port>` | `replay` | Target port, from `1` to `65535`. Requires `--target-host`; defaults to `6030` when omitted. |
| 8 | `--endpoint-map <old-host[:port]=new-host[:port]>` | `replay` | Maps one recorded endpoint to a target endpoint; repeat as needed. Cannot be combined with `--target-host` or `--target-port`. |
| 9 | `--allow-incomplete` | `replay` | Replays the verifiable prefix of a recording with an event gap; such input is refused by default. |
| 10 | `--unfinished-timeout <seconds>` | `replay` | Wait limit for unfinished calls. Defaults to `300` seconds; `0` disables the limit. |
| 11 | `--allow-async-overlap` | `replay` | Replays recordings where an asynchronous call has not reached DONE before the same thread issues another call, in recorded begin order; such input is refused by default. The overlap itself is not reproduced. |
| 12 | `--report-dir <path>` | `replay` | Writes `replay-report.json` to an existing directory. Without it, the report is printed only to the terminal. |
| 13 | `--time-scale <factor>` | `replay` | Divides the recorded pacing by `factor`: default `1`, `10` compresses the gaps to one tenth. Only the timing changes; no call is dropped. |
| 14 | `--help`, `-h` | `inspect`, `export`, `replay` | Displays command usage. |

Exit code `0` means success; `2` means invalid input or arguments; `3` means a credentials or connection error; `4` means replay finished with errors, skipped calls, or incomplete input; and `5` means an internal error. Exit code `1` is reserved.
