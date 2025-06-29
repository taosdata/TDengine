---
title: taosX Reference
sidebar_label: taosX
slug: /tdengine-reference/components/taosx
---

import Image from '@theme/IdealImage';
import imgTdx from '../../assets/taosx-01.png';

import Enterprise from '../../assets/resources/_enterprise.mdx';

<Enterprise/>

taosX is a core component of TDengine Enterprise, providing the capability of zero-code data access. taosX supports two modes of operation: service mode and command line mode. This section discusses how to use taosX in these two ways. To use taosX, you must first install the TDengine Enterprise package.

## Command Line Mode

### Command Line Format

The command line argument format for taosX is as follows:

```shell
taosx -f <from-DSN> -t <to-DSN> <other parameters>
```

The command line arguments for taosX are divided into three main parts:

- `-f` specifies the data source, i.e., Source DSN
- `-t` specifies the write target, i.e., Sink DSN
- Other parameters

The following parameter descriptions and examples use `<content>` as a placeholder format, which should be replaced with actual parameters when used.

### DSN (Data Source Name)

In command line mode, taosX uses DSN to represent a data source (source or destination), a typical DSN is as follows:

```shell
# url-like
<driver>[+<protocol>]://[[<username>:<password>@]<host>:<port>][/<object>][?<p1>=<v1>[&<p2>=<v2>]]
|------|------------|---|-----------|-----------|------|------|----------|-----------------------|
|driver|   protocol |   | username  | password  | host | port |  object  |  params               |

// url example
tmq+ws://root:taosdata@localhost:6030/db1?timeout=never
```

Data in [] is optional.

1. Different drivers have different parameters. The driver includes the following options:

- taos: Use the query interface to get data from TDengine
- tmq: Enable data subscription to get data from TDengine
- local: Data backup or recovery
- pi: Enable pi-connector to get data from pi database
- opc: Enable opc-connector to get data from opc-server
- mqtt: Enable mqtt-connector to get data from mqtt-broker
- kafka: Enable Kafka connector to subscribe to messages from Kafka Topics
- influxdb: Enable influxdb connector to get data from InfluxDB
- csv: Parse data from CSV files

1. +protocol includes the following options:

- +ws: Used when the driver is taos or tmq, indicating that data is obtained using rest. If +ws is not used, it indicates that data is obtained using a native connection, in which case taosx must be installed on the server.
- +ua: Used when the driver is opc, indicating that the data's opc-server is opc-ua
- +da: Used when the driver is opc, indicating that the data's opc-server is opc-da

1. host:port represents the address and port of the data source.
1. object represents the specific data source, which can be a TDengine database, supertable, table, or a local backup file path, or a database in the corresponding data source server.
1. username and password represent the username and password of that data source.
1. params represent the parameters of the dsn.

### Other Parameters

1. --jobs `<number>` specifies the number of concurrent tasks, only supports tmq tasks
1. -v is used to specify the log level of taosx, -v enables info level logs, -vv corresponds to debug, -vvv corresponds to trace

### Usage Examples

#### Import and Export of User and Permission Information

Export username, password, permissions, and whitelist information from Cluster A to Cluster B:

```shell
taosx privileges -f "taos://root:taosdata@hostA:6030" \
  -t "taos+ws://root:password@hostB:6041"
```

Export username, password, permissions, and whitelist information from Cluster A to a JSON file:

```shell
taosx privileges -f "taos+ws://root:taosdata@localhost:6041" \
  -o ./user-pass-privileges-backup.json
```

Restore from the exported JSON file to the local machine:

```shell
taosx privileges -i ./user-pass-privileges-backup.json -t "taos:///"
```

List of available parameters:

| Parameter | Description                             |
| --------- | --------------------------------------- |
| -u        | Includes user basic information (password, whether enabled, etc.) |
| -p        | Includes permission information         |
| -w        | Includes whitelist information          |

When the `-u`/`-p` parameters are applied, only the specified information will be included. Without parameters, it means all information (username, password, permissions, and whitelist).

The `-w` parameter cannot be used alone; it is only effective when used together with `-u` (using `-u` alone will not include the whitelist).

#### Migrating Data from Older Versions

1. Synchronize historical data

Synchronize the entire database:

```shell
taosx run -f 'taos://root:taosdata@localhost:6030/db1' -t 'taos:///db2' -v
```

Synchronize specified supertables:

```shell
taosx run \
  -f 'taos://root:taosdata@localhost:6030/db1?stables=meters' \
  -t 'taos:///db2' -v
```

Synchronize subtables or regular tables, support specifying subtables of supertables with `{stable}.{table}`, or directly specify the table name `{table}`

```shell
taosx run \
  -f 'taos://root:taosdata@localhost:6030/db1?tables=meters.d0,d1,table1' \
  -t 'taos:///db2' -v
```

1. Synchronize data for a specified time interval (using RFC3339 time format, note the timezone):

```shell
taosx run -f 'taos:///db1?start=2022-10-10T00:00:00Z' -t 'taos:///db2' -v
```

1. Continuous synchronization, `restro` specifies syncing data from the last 5 minutes and syncing new data, in the example it checks every 1s, `excursion` allows for 500ms of delay or out-of-order data

```shell
taosx run \
  -f 'taos:///db1?mode=realtime&restro=5m&interval=1s&excursion=500ms' \
  -t 'taos:///db2' -v
```

1. Synchronize historical data + real-time data:

```shell
taosx run -f 'taos:///db1?mode=all' -t 'taos:///db2' -v
```

1. Configure data synchronization through --transform or -T (only supports synchronization between 2.6 to 3.0 and within 3.0) for operations on table names and table fields during the process. It cannot be set through Explorer yet. Configuration instructions are as follows:

  ```shell
  1.AddTag, add tag to a table. Setting example: -T add-tag:<tag1>=<value1>.
  2.Table renaming:
      2.1 Renaming scope
          2.1.1 RenameTable: Rename all tables that meet the conditions.
          2.1.2 RenameChildTable: Rename all child tables that meet the conditions.
          2.1.3 RenameSuperTable: Rename all supertables that meet the conditions.
      2.2 Renaming method
          2.2.1 Prefix: Add a prefix.
          2.2.2 Suffix: Add a suffix.
          2.2.3 Template: Template method.
          2.2.4 ReplaceWithRegex: Regular expression replacement. New in taosx 1.1.0.
  Renaming configuration method:
      <Table scope>:<Renaming method>:<Renaming value>
  Usage example:
      1.Add a prefix <prefix> to all tables
      --transform rename-table:prefix:<prefix>
      2.Replace the prefix for tables that meet the conditions: replace prefix1 with prefix2, in the following example <> is no longer a placeholder in regular expressions.
      -T rename-child-table:replace_with_regex:^prefix1(?<old>)::prefix2_$old
  ```

Example explanation: `^prefix1(?<old>)` is a regular expression that matches table names starting with `prefix1` and records the suffix as `old`. `prefix2$old` will then replace it using `prefix2` and `old`. Note: The two parts are separated by the key character `::`, so ensure that the regular expression does not contain this character.
For more complex replacement needs, please refer to: [https://docs.rs/regex/latest/regex/#example-replacement-with-named-capture-groups](https://docs.rs/regex/latest/regex/#example-replacement-with-named-capture-groups) or consult taosx developers.

1. Using a CSV mapping file to rename tables: The following example uses the `map.csv` file to rename tables

`-T rename-child-table:map:@./map.csv`

The format of the CSV file `./map.csv` is as follows:

```csv
name1,newname1
name2,newname2
```

It is important to note: When migrating between versions that are not the same, and using a native connection, you need to specify `libraryPath` in the DSN, such as: `taos:///db1?libraryPath=./libtaos.so`

#### Importing CSV File Data

Basic usage is as follows:

```shell
taosx run -f csv:./meters/meters.csv.gz \
  --parser '@./meters/meters.json' \
  -t taos:///csv1 -qq
```

Taking electricity meter data as an example, the CSV file is as follows:

```csv
tbname,ts,current,voltage,phase,groupid,location
d4,2017-07-14T10:40:00+08:00,-2.598076,16,-0.866025,7,California.LosAngles
d4,2017-07-14T10:40:00.001+08:00,-2.623859,6,-0.87462,7,California.LosAngles
d4,2017-07-14T10:40:00.002+08:00,-2.648843,2,-0.862948,7,California.LosAngles
d4,2017-07-14T10:40:00.003+08:00,-2.673019,16,-0.891006,7,California.LosAngles
d4,2017-07-14T10:40:00.004+08:00,-2.696382,10,-0.898794,7,California.LosAngles
d4,2017-07-14T10:40:00.005+08:00,-2.718924,6,-0.886308,7,California.LosAngles
d4,2017-07-14T10:40:00.006+08:00,-2.740636,10,-0.893545,7,California.LosAngles
```

`--parser` is used to set the import parameters, as shown below:

```json
{
  "parse": {
    "ts": { "as": "TIMESTAMP(ms)" },
    "current": { "as": "FLOAT" },
    "voltage": { "as": "INT" },
    "phase": { "as": "FLOAT" },
    "groupid": { "as": "INT" },
    "location": { "as": "VARCHAR(24)" }
  },
  "model": {
    "name": "${tbname}",
    "using": "meters",
    "tags": ["groupid", "location"],
    "columns": ["ts", "current", "voltage", "phase"]
  }
}
```

It will import data from `./meters/meters.csv.gz` (a gzip-compressed CSV file) into the supertable `meters`, inserting each row into the specified table name - `${tbname}` using the `tbname` column from the CSV content as the table name (i.e., in the JSON parser's `.model.name`).

## Service Mode

This section discusses how to deploy `taosX` in service mode. When running taosX in service mode, its functions need to be used through the graphical interface on taosExplorer.

### Configuration

`taosX` supports configuration through a configuration file. On Linux, the default configuration file path is `/etc/taos/taosx.toml`, and on Windows, it is `C:\\TDengine\\cfg\\taosx.toml`, which includes the following configuration items:

- `plugins_home`: Directory where external data source connectors are located.
- `data_dir`: Directory where data files are stored.
- `instanceId`: Instance ID of the current taosX service. If multiple taosX instances are started on the same machine, it is necessary to ensure that the instance IDs of each instance are unique.
- `logs_home`: Directory where log files are stored, the prefix for `taosX` log files is `taosx.log`, external data sources have their own log file name prefixes. Deprecated, please use `log.path` instead.
- `log_level`: Log level, available levels include `error`, `warn`, `info`, `debug`, `trace`, default value is `info`. Deprecated, please use `log.level` instead.
- `log_keep_days`: Maximum storage days for logs, `taosX` logs will be divided into different files by day. Deprecated, please use `log.keepDays` instead.
- `jobs`: Maximum number of threads per runtime. In service mode, the total number of threads is `jobs*2`, default number of threads is `current server cores*2`.
- `serve.listen`: `taosX` REST API listening address, default value is `0.0.0.0:6050`. Supports IPv6 and multiple comma-separated addresses with the same port.
- `serve.ssl_cert`: SSL/TLS certificate file.
- `serve.ssl_key`: SSL/TLS server's private key.
- `serve.ssl_ca`: SSL/TLS certificate authority (CA) certificates.
- `serve.database_url`: Address of the `taosX` database, format is `sqlite:<path>`.
- `serve.request_timeout`: Global interface API timeout.
- `serve.grpc`:`taosX` gRPC listening address, default value is `0.0.0.0:6055`. Supports IPv6 and multiple comma-separated addresses with the same port.
- `monitor.fqdn`: FQDN of the `taosKeeper` service, no default value, leave blank to disable monitoring.
- `monitor.port`: Port of the `taosKeeper` service, default `6043`.
- `monitor.interval`: Frequency of sending metrics to `taosKeeper`, default is every 10 seconds, only values between 1 and 10 are valid.
- `log.path`: Directory where log files are stored.
- `log.level`: Log level, available values are "error", "warn", "info", "debug", "trace".
- `log.compress`: Whether to compress log files after rolling.
- `log.rotationCount`: Maximum number of files to retain in the log file directory, older files exceeding this number are deleted.
- `log.rotationSize`: File size that triggers log file rolling (in bytes), when log files exceed this size a new file is generated, and new logs are written to the new file.
- `log.reservedDiskSize`: Threshold for stopping log writing on the disk where logs are stored (in bytes), when the remaining disk space reaches this size, log writing is stopped.
- `log.keepDays`: Number of days to keep log files, old log files exceeding this number of days are deleted.
- `log.watching`: Whether to monitor changes in the `log.loggers` configuration content in the log files and attempt to reload.
- `log.loggers`: Specifies the log output level of modules, format is `"modname" = "level"`, also compatible with tracing library syntax, can be specified as `modname[span{field=value}]=level`, where `level` is the log level.

As shown below:

```toml
# data dir
#data_dir = "/var/lib/taos/taosx" # on linux/macOS
#data_dir = "C:\\TDengine\\data\\taosx" # on windows

# number of threads used for tokio workers, default to 0 (means cores * 2)
#jobs = 0

# enable OpenTelemetry tracing and metrics exporter
#otel = false

# server instance id
#
# The instanceId of each instance is unique on the host
# instanceId = 16

[serve]
# listen to ip:port address
#listen = "0.0.0.0:6050"

# TLS/SSL certificate
#ssl_cert = "/path/to/tls/server.pem"
# TLS/SSL certificate key
#ssl_key = "/path/to/tls/server.key"
# TLS/SSL CA certificate
#ssl_ca = "/path/to/tls/ca.pem"

# database url
#database_url = "sqlite:taosx.db"

# default global request timeout which unit is second. This parameter takes effect for certain interfaces that require a timeout setting
#request_timeout = 30

# GRPC listen address，use ip:port like `0.0.0.0:6055`.
#
# When use this in explorer, please set explorer grpc configuration to **Public** IP or
# FQDN with correct port, which might be changed exposing to Public network.
#
# - Example 1: "http://192.168.111.111:6055" 
# - Example 2: "http://node1.company.domain:6055" 
#
# Please also make sure the above address is not blocked if firewall is enabled.
#
#grpc = "0.0.0.0:6055"

[monitor]
# FQDN of taosKeeper service, no default value
#fqdn = "localhost"

# Port of taosKeeper service, default 6043
#port = 6043

# How often to send metrics to taosKeeper, default every 10 seconds. Only value from 1 to 10 is valid.
#interval = 10


# log configuration
[log]
# All log files are stored in this directory
#
#path = "/var/log/taos" # on linux/macOS
#path = "C:\\TDengine\\log" # on windows

# log filter level
#
#level = "info"

# Compress archived log files or not
#
#compress = false

# The number of log files retained by the current explorer server instance in the `path` directory
#
#rotationCount = 30

# Rotate when the log file reaches this size
#
#rotationSize = "1GB"

# Log downgrade when the remaining disk space reaches this size, only logging `ERROR` level logs
#
#reservedDiskSize = "1GB"

# The number of days log files are retained
#
#keepDays = 30

# Watching the configuration file for log.loggers changes, default to true.
#
#watching = true

# Customize the log output level of modules, and changes will be applied after modifying the file when log.watching is enabled
#
# ## Examples:
#
# crate = "error"
# crate::mod1::mod2 = "info"
# crate::span[field=value] = "warn"
#
[log.loggers]
#"actix_server::accept" = "warn"
#"taos::query" = "warn"
```

### Start

On Linux systems, `taosX` can be started using the Systemd command:

```shell
systemctl start taosx
```

On Windows systems, find the `taosX` service through the system management tool "Services" and start it, or execute the following command in the command line tool (cmd.exe or PowerShell) to start:

```shell
sc.exe start taosx
```

### Troubleshooting

1. Modify `taosX` log level

The default log level of `taosX` is `info`. To specify a different level, please modify the configuration file, or use the following command line parameters:

- `error`: `taosx serve -qq`
- `debug`: `taosx serve -q`
- `info`: `taosx serve -v`
- `debug`: `taosx serve -vv`
- `trace`: `taosx serve -vvv`

To specify command line parameters when `taosX` is running as a service, please refer to the configuration.

1. View `taosX` logs

You can view the log files or use the `journalctl` command to view the logs of `taosX`.

The command to view logs under Linux using `journalctl` is as follows:

```shell
journalctl -u taosx [-f]
```

## taosX Monitoring Metrics

taosX reports monitoring metrics to taosKeeper, which are written into the monitoring database by taosKeeper, defaulting to the `log` database, which can be modified in the taoskeeper configuration file. Here is a detailed introduction to these monitoring metrics.

### taosX Service

| Field                       | Description                                                                   |
| -------------------------- | ----------------------------------------------------------------------------- |
| sys_cpu_cores              | Number of system CPU cores                                                    |
| sys_total_memory           | Total system memory, in bytes                                                 |
| sys_used_memory            | System memory used, in bytes                                                  |
| sys_available_memory       | System memory available, in bytes                                             |
| process_uptime             | taosX runtime, in seconds                                                     |
| process_id                 | taosX process ID                                                              |
| running_tasks              | Number of current tasks being executed by taosX                               |
| completed_tasks            | Number of tasks completed by taosX in a monitoring period (e.g., 10s)         |
| failed_tasks               | Number of tasks failed by taosX in a monitoring period (e.g., 10s)            |
| process_cpu_percent        | CPU percentage used by the taosX process, in %                                |
| process_memory_percent     | Memory percentage used by the taosX process, in %                             |
| process_disk_read_bytes    | Average number of bytes read from disk by the taosX process in a monitoring period (e.g., 10s), in bytes/s |
| process_disk_written_bytes | Average number of bytes written to disk by the taosX process in a monitoring period (e.g., 10s), in bytes/s |

### Agent

| Field                        | Description                                                                 |
| ---------------------------- | --------------------------------------------------------------------------- |
| sys_cpu_cores                | Number of system CPU cores                                                  |
| sys_total_memory             | Total system memory, in bytes                                               |
| sys_used_memory              | System memory used, in bytes                                                |
| sys_available_memory         | System memory available, in bytes                                           |
| process_uptime               | Agent runtime, in seconds                                                   |
| process_id                   | Agent process id                                                            |
| process_cpu_percent          | CPU percentage used by the agent process                                    |
| process_memory_percent       | Memory percentage used by the agent process                                 |
| process_uptime               | Process uptime, in seconds                                                  |
| process_disk_read_bytes      | Average number of bytes read from disk by the agent process in a monitoring period (e.g., 10s), in bytes/s |
| process_disk_written_bytes   | Average number of bytes written to disk by the agent process in a monitoring period (e.g., 10s), in bytes/s |

### Connector

| Field                        | Description                                                                              |
| ---------------------------- | ---------------------------------------------------------------------------------------- |
| process_id                   | Connector process id                                                                     |
| process_uptime               | Process uptime, in seconds                                                               |
| process_cpu_percent          | CPU percentage used by the process, in %                                                 |
| process_memory_percent       | Memory percentage used by the process, in %                                              |
| process_disk_read_bytes      | Average number of bytes read from disk by the connector process in a monitoring period (e.g., 10s), in bytes/s |
| process_disk_written_bytes   | Average number of bytes written to disk by the connector process in a monitoring period (e.g., 10s), in bytes/s |

### taosX General Data Source Task

| Field                | Description                                                         |
| -------------------- | ------------------------------------------------------------------- |
| total_execute_time   | Total cumulative runtime of the task, in milliseconds               |
| total_written_rowsls | Total number of rows successfully written to TDengine (including duplicates) |
| total_written_points | Total number of successful write points (equal to the number of rows in the data block multiplied by the number of columns in the data block) |
| start_time           | Task start time (reset each time the task is restarted)             |
| written_rows         | Total number of rows successfully written to TDengine in this run (including duplicates) |
| written_points       | Number of successful write points in this run (equal to the number of rows in the data block multiplied by the number of columns in the data block) |
| execute_time         | Runtime of the task in this instance, in seconds                    |

### taosX TDengine V2 Task

| Field                 | Description                                                          |
| --------------------- | -------------------------------------------------------------------- |
| read_concurrency      | Number of data worker threads reading concurrently from the data source, also equals the number of worker threads writing concurrently to TDengine |
| total_stables         | Number of supertables to be migrated                                |
| total_updated_tags    | Total number of tags updated                                         |
| total_created_tables  | Total number of subtables created                                    |
| total_tables          | Number of subtables to be migrated                                   |
| total_finished_tables | Number of subtables that have completed data migration (may be greater than actual value if the task is interrupted and restarted) |
| total_success_blocks  | Total number of successful data blocks written                       |
| finished_tables       | Number of subtables that have completed migration in this run        |
| success_blocks        | Number of successful data blocks written in this run                 |
| created_tables        | Number of subtables created in this run                              |
| updated_tags          | Number of tags updated in this run                                   |

### taosX TDengine V3 Task

| Field                   | Description                                                    |
| ---------------------- | -------------------------------------------------------------- |
| total_messages         | Total number of messages received through TMQ                  |
| total_messages_of_meta | Total number of Meta type messages received through TMQ        |
| total_messages_of_data | Total number of Data and MetaData type messages received through TMQ |
| total_write_raw_fails  | Total number of failures in writing raw meta                    |
| total_success_blocks   | Total number of successful data blocks written                  |
| topics                 | Number of topics subscribed through TMQ                         |
| consumers              | Number of TMQ consumers                                         |
| messages               | Total number of messages received through TMQ in this run       |
| messages_of_meta       | Number of Meta type messages received through TMQ in this run   |
| messages_of_data       | Number of Data and MetaData type messages received through TMQ in this run |
| write_raw_fails        | Number of failures in writing raw meta in this run              |
| success_blocks         | Number of successful data blocks written in this run            |

### taosX Other Data Sources Task

These data sources include: InfluxDB, OpenTSDB, OPC UA, OPC DA, PI, CSV, MQTT, AVEVA Historian, and Kafka.

| Field                    | Description                                                  |
| ----------------------- | ----------------------------------------------------------- |
| total_received_batches  | Total number of data batches received through IPC Stream    |
| total_processed_batches | Number of batches processed                                  |
| total_processed_rows    | Total number of rows processed (equals the sum of rows in each batch) |
| total_inserted_sqls     | Total number of INSERT SQLs executed                         |
| total_failed_sqls       | Total number of failed INSERT SQLs                           |
| total_created_stables   | Total number of supertables created (may be greater than actual) |
| total_created_tables    | Total number of child tables attempted to create (may be greater than actual) |
| total_failed_rows       | Total number of rows failed to write                         |
| total_failed_point      | Total number of points failed to write                       |
| total_written_blocks    | Total number of raw blocks successfully written              |
| total_failed_blocks     | Total number of raw blocks failed to write                   |
| received_batches        | Total number of data batches received through IPC Stream in this task run |
| processed_batches       | Number of batches processed in this task run                 |
| processed_rows          | Total number of rows processed in this task run (equals the sum of rows in data-containing batches) |
| received_records        | Total number of data rows received through IPC Stream in this task run |
| inserted_sqls           | Total number of INSERT SQLs executed in this task run        |
| failed_sqls             | Total number of failed INSERT SQLs in this task run          |
| created_stables         | Number of supertables attempted to create in this task run (may be greater than actual) |
| created_tables          | Number of child tables attempted to create in this task run (may be greater than actual) |
| failed_rows             | Number of rows failed to write in this task run              |
| failed_points           | Number of points failed to write in this task run            |
| written_blocks          | Number of raw blocks successfully written in this task run   |
| failed_blocks           | Number of raw blocks failed to write in this task run        |

### Kafka Data Source Metrics

| Field                          | Description                         |
| ----------------------------- | ---------------------------- |
| kafka_consumers               | Number of Kafka consumers in this run  |
| kafka_total_partitions        | Total number of Kafka topic partitions           |
| kafka_consuming_partitions    | Number of partitions being consumed in this run |
| kafka_consumed_messages       | Number of messages consumed in this run |
| total_kafka_consumed_messages | Total number of messages consumed           |

## taosX Data Parsing Plugin

When integrating with kafka/mqtt message middleware, it is necessary to parse the original data. If json/regex and other pattern parsers cannot meet the parsing requirements, and UDT (custom parsing script) also cannot meet the performance requirements, you can customize a data parsing plugin.

### Plugin Overview

The taosX Parser plugin is a dynamic library compatible with the C ABI, developed in C/Rust language. This dynamic library must implement the agreed API and be compiled into a dynamic library that can run correctly in the taosX operating environment. It is then copied to the specified location to be loaded by taosX at runtime and called during the data Parsing phase.

### Plugin Deployment

After the plugin development is completed, the compilation environment needs to be compatible with the target operating environment. Copy the compiled plugin dynamic library to the plugin directory. After taosX starts, the system initializes and loads the plugin the first time it is used. You can check whether it is loaded successfully on the kafka or mqtt data access configuration page in explorer.

The plugin directory reuses the plugins configuration in the `taosx.toml` configuration file, appending `/parsers` as the plugin installation path. The default value in UNIX environment is `/usr/local/taos/plugins/parsers`, and in Windows it is `C:\TDengine\plugins\parsers`.

### Plugin API Description

#### 1. Get Plugin Name

Get the plugin name for display on the frontend.

**Function Signature**: const char* parser_name()

**Return Value**: String.

#### 2. Get Plugin Version

Plugin version, useful for troubleshooting.

**Function Signature**: const char* parser_version()

**Return Value**: String.

#### 3. Configure Parser

Parse a string argument into a configuration object, for internal use only.

**Function Signature**: parser_resp_t parser_new(char* ctx, uint32_t len);

char* ctx: User-defined configuration string.

uint32_t len: The binary length of this string (excluding `\0`).

**Return Value**:

```c
struct parser_resp_t {
  int e;    // 0 if success.
  void* p;  // Success if contains.
}
```

When object creation fails, e is not 0.

When creation is successful, e = 0, p is the parser object.

#### 4. Parse Data

**Function Signature**:

Parse the input payload and return the result in JSON format [u8]. The returned JSON will be fully decoded using the default JSON parser (expanding the root array and all objects).

```c
const char* parser_mutate(
  void* parser,
  const uint8_t* in_ptr, uint32_t in_len,
  const void* uint8_t* out_ptr, uint32_t* out_len
); 
```

`void* parser`: Object pointer generated by parser_new;

`const uint8_t* in_ptr`: Pointer to the input Payload;

`uint32_t in_len`: Byte length of the input Payload (excluding `\0`);

`const void* uint8_t* out_ptr`: Pointer to the output JSON string (excluding \0). When out_ptr points to null, it indicates that the output is null.

`uint32_t * out_len`: Output length of the JSON string.

**Return Value**: When the call is successful, the return value is NULL.

#### 5. Release the Parser

Release the memory of the parser object.

**Function Signature**: void parser_free(void* parser);

void* parser: Pointer to the object generated by parser_new.
