---
sidebar_label: Inspection Tool
title: Inspection Tool
toc_max_heading_level: 4
---

`taosinspect` performs periodic checks of a TDengine deployment and its operating environment.

## Usage

```text
usage: taosinspect [-h] [--model {local,ssh}] [--basic] [--config CONFIG]
                   [--result RESULT] [--lookback LOOKBACK] [--backend]
                   [--check-nginx] [--log-level {debug,info}]
                   [--skip {all,dd,stc,none}] [--version]
```

| Option | Description |
| --- | --- |
| `-m`, `--model` | Inspect locally or connect to all configured hosts over SSH; default: `local` |
| `-b`, `--basic` | Run the reduced basic inspection |
| `-f`, `--config` | Configuration file; default: `/etc/taos/inspect.cfg` |
| `-r`, `--result` | Output directory; default: the `logDir` configured in `taos.cfg` |
| `-l`, `--lookback` | Load-statistics period in days; default: 30 |
| `-cn`, `--check-nginx` | Also validate Nginx configuration |
| `-s`, `--skip` | Skip data distribution (`dd`), subtable counts (`stc`), or both (`all`) |
| `-L`, `--log-level` | Log level: `debug` or `info`; default: `info` |

The configuration can either let the tool discover cluster nodes from TDengine or explicitly list SSH endpoints. It also defines database credentials, optional Nginx access, expected system parameters, required packages, inspected services, and error strings to ignore.

## Inspection Scope

Basic inspection collects operating system, CPU, network adapter, disk usage, `/etc/hosts`, dnode and mnode status, database definitions, and supertable summaries.

Full inspection additionally checks:

- Disk, CPU, memory, swap, firewall, SELinux, core dumps, kernel errors, and system limits.
- TDengine component versions, process status, configuration, logs, users, privileges, licenses, and slow queries.
- Vnode and vgroup distribution, replica count, table counts and schemas, and local storage usage.
- Stream, topic, consumer, and subscription definitions.
- Optional Nginx host and address mappings.

Resource-load statistics require TDengine v3.3.6.25 or later, or v3.3.7.8 or later.

## Output

The result directory can contain:

- `inspect_report.md`: human-readable inspection report.
- `inspect.json`: structured inspection data.
- `table_schemas.md`: long supertable schemas.
- Component error logs and configuration snapshots.
- `results.zip`: packaged results other than the collected error logs.

## Examples

```bash
# Inspect the current host
./taosinspect

# Inspect all cluster nodes
./taosinspect -m ssh

# Basic inspection
./taosinspect -m ssh -b

# Skip data distribution and subtable-count collection
./taosinspect -m ssh -s all

# Use a configuration file and check Nginx
./taosinspect -m ssh -f /path/to/inspect.cfg -cn
```
