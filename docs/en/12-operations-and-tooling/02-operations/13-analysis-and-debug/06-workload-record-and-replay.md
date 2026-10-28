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

## taosreplay Options

| No. | Option | Command | Description |
| --- | ------ | ------- | ----------- |
| 1 | `--input <path>` | `inspect`, `replay` | Required and repeatable. Accepts a recording directory or an individual `.wlr` segment. |
| 2 | `--verbose` | `inspect` | Prints individual event details, which may include SQL; the default output is a summary. |
| 3 | `--user-password <user=password>` | `replay` | Supplies a recorded user's password for the target cluster; repeat for multiple users. At least one credential is required. If this option is omitted, `TAOSREPLAY_USER_PASSWORD=user=password` can supply one credential. Command-line passwords may appear in process lists and shell history. |
| 4 | `--target-host <host>` | `replay` | Redirects all recorded connections to this host. Cannot be combined with `--endpoint-map`. Explicitly specify an isolated test cluster. |
| 5 | `--target-port <port>` | `replay` | Target port, from `1` to `65535`. Requires `--target-host`; defaults to `6030` when omitted. |
| 6 | `--endpoint-map <old-host[:port]=new-host[:port]>` | `replay` | Maps one recorded endpoint to a target endpoint; repeat as needed. Cannot be combined with `--target-host` or `--target-port`. |
| 7 | `--allow-incomplete` | `replay` | Replays the verifiable prefix of a recording with an event gap; such input is refused by default. |
| 8 | `--unfinished-timeout <seconds>` | `replay` | Wait limit for unfinished calls. Defaults to `300` seconds; `0` disables the limit. |
| 9 | `--report-dir <path>` | `replay` | Writes `replay-report.json` to an existing directory. Without it, the report is printed only to the terminal. |
| 10 | `--help`, `-h` | `inspect`, `replay` | Displays command usage. |

Exit code `0` means success; `2` means invalid input or arguments; `3` means a credentials or connection error; `4` means replay finished with errors, skipped calls, or incomplete input; and `5` means an internal error. Exit code `1` is reserved.
