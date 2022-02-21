# taosX User Manual

taosX is an easy-to-use, high-performance, feature-rich TDengine data integration tool. It works like a streaming data platform that supports offline data import/export and real-time data synchronization from or to TDengine. It's built for performance, reliability, productivity, observability and ergonomics. If you have a problem with this tool, please file a issue in GitHub, we'll do our best to solve it.

## Features

- Easy to use command line interface.
- Simple but flexible configuration(s).
- High-performance with best effort.
- High-throughout with massive data.
- Modular and plugin system easy to extend, for different data sources/sinks.
- Streaming data aggregation.
- Fearless service running for long term.
- Metrics-rich monitoring.

## Use scenarios

1st, for TDengine logical backup and restore.

- TDengine database/tables full backup.
- TDengine database/tables incremental backup from specific full backup.
- TDengine database/tables automatically(scheduled) backup.
- TDengine database/tables restore from specific full backup(with incremental backup or not).

2nd, for offline data integration.

- Export/import from TDengine(to files or directories, which support local fs or s3).
- Offline data import to TDengine with single file, such as csv/parquet etc.
- Offline data import to TDengine with structured files/directories(by configuration).

3rd, for streaming data integration.

- Subscription (with aggregations) from TDengine.
- Synchronization from different data sources to TDengine.

4th, for TDengine data migration(this feature will be delayed to later taosX release plan).

- TDengine data migration from 2.x to 3.x
- TDengine data migration from 3.x to 3.x

## Installation

You can download prebuilt binaries along with TDengine release package, or just install from crates.io with cargo install taosx. We also provides prebuilt binaries on GitHub release page for different platforms or OSes.

## Quick start

taosX provides a bunch of command line interfaces for convenient use.

### Full backup

Full backup of specific table(s) is easy with taosX.

```bash
taosx backup -d <db> -t <table> [-o <backup-place>]
```

Restore table from a full backup place.

```bash
taosx restore -d <db> -t <table> -i <backup-place>
```

### Incremental backup

Incremental backup could be performed on an existing full backup and generate the incremental backup files after the current backup version.

```bash
taosx backup -d <db> -t <table> -o <backup-place>
```

Or generate incremental backups regularly.

```bash
taosx backup -d <db> -t <table> -o <backup-place> --interval <interval>
```

### Backup schedule

With full backup and incremental backup strategies, you can easily setup an automatically backup schedule by the following command:

```bash
taosx backup schedule <schedule.conf>
```

### Streaming synchronization

Synchronize an streaming-like data source to the sink(currently TDengine only).

```bash
taosx sync -i <source.conf> -o <sink.conf>
```

The source/sink strategy is supported by a plugin system, which will be open for the community to extend the functionality.

### Data import/export

Export TDengine data to a specific file format.

```bash
taosx export -s <sql> -o <out>
```

Or import different file types to TDengine.

```bash
taosx import -i <input> [<tdengine-sink-options>]
```

## Advanced usage

All the supported operations for taosX could be configured in a configuration file. taosX will read configuration files from specific directory and do a batch of tasks.

```bash
taosx run <batch.conf>
taosx run -c <conf-dir>
```

### Configurations

Configuration is a bit more complex part and we'll use another document for the details. A simple configuration file could be:

```json
[
  {
    "backup": {
      "uri": "taos://root:taosdata@localhost:6030",
      "database": "test",
      "tables": ["tb1", "stb1"],
      "output": "/path/to/backup/dir/",
      "schedule": "daily",
      "with-incremental": {
        "interval": "1h",
        "max-size": "1G"
      }
    }
  },
  {
    "stream": [
      {
        "name": "meters",
        "source": {
          "type": "tdengine",
          "options": {
              "uri": "taos://root:taosdata@source:6030/test",
              "sql": "select avg(value) from meters interval(1m)"
            }
        },
        "sink": [
          {
            "type": "tdengine",
            "options": {
              "uri": "taos://root:taosdata@sink:6030/target",
              "table": "meters-stats"
            }
          }
        ]
      }
    ]
  },
  {
    "stream": [
      {
        "name": "socket demo",
        "source": {
          "type": "socket",
          "options": {
            "uri": "sock:6543",
            "parser": "(?P<ts>\\d+),(?P<value>\\S+),(?P<t1>\\d+)\n?"
          }
        },
        "sink": [
          {
            "type": "tdengine",
            "options": {
              "uri": "taos://root:taosdata@target:6030/test",
              "table": "demo"
            }
          }
        ]
      }
    ]
  }
]
```

## Monitoring

taosx will output performance and status metrics to standard output by default in batch mode. For long term service, you can configure to listen on a http port to export the metrics as OpenMetrics format, and then monitor it with Prometheus or other tools, or push the metrics to a Prometheus push gateway.

```bash
taosx run <batch.conf> --metrics-exporter 0.0.0.0:6061/metrics
taosx run <batch.conf> --metrics-push-gateway http://prom:9090
```

## Service mode

taosx provides a builtin service mode, to automatically monitor a configuration directory, expose an OpenAPI with workflow control support and enable OpenMetrics exporter by default. We have a schedule to add more useful functionalities, include a monitor web dashboard to manage  configurations and display the status and metrics view in later release channel.

In 1.0 release, you could configure the service mode with:

```bash
taosx -d -l 0.0.0.0:6061 -c /path/to/conf/dir/
```

## Build from source

taosX use Rust to benefit from the awesome Rust community. You need to install Rust first to build from source. Better start it from [rustup](https://rustup.rs/)(the installer for Rust).

```bash
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
```

Then you can build it:

```bash
cargo build
```
