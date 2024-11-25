---
title: taosX Reference
sidebar_label: taosX
slug: /tdengine-reference/components/taosx
---

taosX is a core component in TDengine Enterprise that provides zero-code data access capabilities. taosX supports two running modes: service mode and command-line mode. This section describes how to use taosX in both ways. To use taosX, you must first install the TDengine Enterprise installation package.

## Command-Line Mode

### Command-Line Format

The command-line parameter format for taosX is as follows:

```shell
taosx -f <from-DSN> -t <to-DSN> <other parameters>
```

The command-line parameters for taosX are divided into three main parts:

- `-f` specifies the data source, i.e., Source DSN.
- `-t` specifies the write target, i.e., Sink DSN.
- Other parameters.

For the following parameter descriptions and examples, if no special instructions are provided, the format of `<content>` is a placeholder, which needs to be replaced with actual parameters when used.

### DSN (Data Source Name)

In the command-line mode, taosX uses DSN to represent a data source (source or destination). A typical DSN is as follows:

```bash
# url-like
<driver>[+<protocol>]://[[<username>:<password>@]<host>:<port>][/<object>][?<p1>=<v1>[&<p2>=<v2>]]
|------|------------|---|-----------|-----------|------|------|----------|-----------------------|
|driver|   protocol |   | username  | password  | host | port |  object  |  params               |

// URL example
tmq+ws://root:taosdata@localhost:6030/db1?timeout=never
```

The data within [] are optional parameters.

1. Different drivers (driver) have different parameters. The drivers include the following options:
   - taos: Use the query interface to retrieve data from TDengine.
   - tmq: Enable data subscription to retrieve data from TDengine.
   - local: Data backup or recovery.
   - pi: Enable pi-connector to retrieve data from the pi database.
   - opc: Enable opc-connector to retrieve data from the opc-server.
   - mqtt: Enable mqtt-connector to retrieve data from the mqtt-broker.
   - kafka: Enable Kafka connector to subscribe messages from Kafka Topics for writing.
   - influxdb: Enable influxdb connector to retrieve data from InfluxDB.
   - csv: Parse data from a CSV file.
2. +protocol includes the following options:
   - +ws: Used when the driver is either taos or tmq, indicating the use of REST to retrieve data. If +ws is not used, it indicates the use of a native connection, which requires taosx to be installed on the server.
   - +ua: Used when the driver is opc, indicating the collected data's OPC server is OPC-UA.
   - +da: Used when the driver is opc, indicating the collected data's OPC server is OPC-DA.
3. host:port indicates the address and port of the data source.
4. object represents the specific data source, which can be a TDengine database, supertable, table, the path of a local backup file, or the database on the corresponding data source server.
5. username and password represent the username and password for the data source.
6. params represent the parameters of the DSN.

### Other Parameters

1. --jobs `<number>` specifies the concurrency number of tasks, only supports tmq tasks.
2. -v is used to specify the log level of taosx, -v means enable info level logs, -vv corresponds to debug, -vvv corresponds to trace.

### Usage Examples

#### Import and Export User and Permission Information

Export usernames, passwords, permissions, and whitelist information from cluster A to cluster B:

```shell
taosx privileges -f "taos://root:taosdata@hostA:6030" \
  -t "taos+ws://root:password@hostB:6041"
```

Export usernames, passwords, permissions, and whitelist information from cluster A to a JSON file:

```shell
taosx privileges -f "taos+ws://root:taosdata@localhost:6041" \
  -o ./user-pass-privileges-backup.json
```

Restore from the exported JSON file to the local machine:

```shell
taosx privileges -i ./user-pass-privileges-backup.json -t "taos:///"
```

Available parameters:

| Parameter | Description                               |
| --------- | ----------------------------------------- |
| -u        | Includes user basic information (password, whether enabled, etc.) |
| -p        | Includes permission information           |
| -w        | Includes whitelist information             |

When the `-u`/`-p` parameter is applied, it will only include the specified information. When no parameters are included, it indicates all information (username, password, permission, and whitelist).

The `-w` parameter cannot be used alone; it is only valid when used with `-u` (using `-u` alone will not include the whitelist).

#### Migrating Data from Old Versions

1. Synchronize Historical Data

   Synchronize the entire database:

   ```shell
   taosx run -f 'taos://root:taosdata@localhost:6030/db1' -t 'taos:///db2' -v
   ```

   Synchronize a specified supertable:

   ```shell
   taosx run \
     -f 'taos://root:taosdata@localhost:6030/db1?stables=meters' \
     -t 'taos:///db2' -v
   ```

   Synchronize subtables or basic tables; support `{stable}.{table}` to specify a supertable's subtable or directly specify the table name `{table}`.

   ```shell
   taosx run \
     -f 'taos://root:taosdata@localhost:6030/db1?tables=meters.d0,d1,table1' \
     -t 'taos:///db2' -v
   ```

2. Synchronize Data within a Specified Time Range (using RFC3339 time format, note the timezone):

   ```shell
   taosx run -f 'taos:///db1?start=2022-10-10T00:00:00Z' -t 'taos:///db2' -v
   ```

3. Continuous Synchronization; `restro` specifies synchronizing the data from the last 5 minutes and continues to synchronize new data. In the example, it checks once every second, and `excursion` allows for a delay or out-of-order data of 500ms.

   ```shell
   taosx run \
     -f 'taos:///db1?mode=realtime&restro=5m&interval=1s&excursion=500ms' \
     -t 'taos:///db2' -v
   ```

4. Synchronize Historical Data + Real-Time Data:

   ```shell
   taosx run -f 'taos:///db1?mode=all' -t 'taos:///db2' -v
   ```

5. Configure Data Synchronization through `--transform` or `-T` (supports synchronization between versions 2.6 to 3.0 and 3.0); this process performs operations on table names and table fields. Currently, this cannot be set through Explorer. Configuration descriptions are as follows:

   1. AddTag, to add a TAG to the table. Set example: `-T add-tag:<tag1>=<value1>`.
   2. Rename Table:
       2.1 Rename Table Scope:
           2.1.1 RenameTable: Rename all matching tables.
           2.1.2 RenameChildTable: Rename all matching subtables.
           2.1.3 RenameSuperTable: Rename all matching supertables.
       2.2 Renaming Methods:
           2.2.1 Prefix: Add prefix.
           2.2.2 Suffix: Add suffix.
           2.2.3 Template: Template method.
           2.2.4 ReplaceWithRegex: Regular expression replacement. Added in taosx 1.1.0.

   Renaming configuration method: `<Table Scope>:<Renaming Method>:<Renaming Value>`

   Usage examples:

   1. Add prefix `<prefix>` to all tables: `--transform rename-table:prefix:<prefix>`
   2. Replace prefix1 with prefix2 for matching tables. In the following example, `<>` are no longer placeholders for regular expressions.

      ```text
      -T rename-child-table:replace_with_regex:^prefix1(?<old>)::prefix2_$old
      ```

   Example explanation: `^prefix1(?<old>)` is a regular expression that matches table names starting with prefix1 and records the suffix as old; prefix2$old will replace the old with prefix2. Note: The two parts are separated using the special character ::, so the regular expression must not include this character.

   For more complex replacement needs, please refer to: [https://docs.rs/regex/latest/regex/#example-replacement-with-named-capture-groups](https://docs.rs/regex/latest/regex/#example-replacement-with-named-capture-groups) or consult the taosx developers.

   3. Use a CSV mapping file for table renaming: The following example uses the map.csv file for table renaming.

   ```text
   `-T rename-child-table:map:@./map.csv`
   ```

   The CSV file `./map.csv` is formatted as follows:

   ```text
   name1,newname1
   name2,newname2
   ```

Please note that when migrating between incompatible versions on both ends and using a native connection, it is necessary to specify `libraryPath` in the DSN, for example: `taos:///db1?libraryPath=./libtaos.so`.

#### Import Data from CSV Files

The basic usage is as follows:

```shell
taosx run -f csv:./meters/meters.csv.gz \
  --parser '@./meters/meters.json' \
  -t taos:///csv1 -qq
```

For example, with the electric meter data, the CSV file is as follows:

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

The `--parser` option is used to set the database parameters. The example is as follows:

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

It will import data from `./meters/meters.csv.gz` (a gzip compressed CSV file) into the supertable `meters`, where each row is inserted into the specified table name - `${tbname}`, using the `tbname` column in the CSV content as the table name (i.e., in the JSON parser, it is defined in `.model.name`).

## Service Mode

This section describes how to deploy `taosX` in service mode. The functions of taosX running in service mode need to be accessed through the graphical interface on taosExplorer.

### Configuration

`taosX` can be configured through a configuration file. On Linux, the default configuration file path is `/etc/taos/taosx.toml`, and on Windows, the default configuration file path is `C:\\TDengine\\cfg\\taosx.toml`. It includes the following configuration items:

- `plugins_home`: The directory where external data source connectors are located.
- `data_dir`: The directory for storing data files.
- `instanceId`: The instance ID of the current taosX service. If multiple taosX instances are started on the same machine, ensure that the instance IDs are unique.
- `logs_home`: The directory where log files are stored. The log file prefix for `taosX` is `taosx.log`, and external data sources have their own log file prefix. This has been deprecated; please use `log.path` instead.
- `log_level`: The log level; optional levels include `error`, `warn`, `info`, `debug`, `trace`, with a default value of `info`. This has been deprecated; please use `log.level` instead.
- `log_keep_days`: The maximum storage days for logs; `taosX` logs will be divided into different files by day. This has been deprecated; please use `log.keepDays` instead.
- `jobs`: The maximum number of threads per runtime. In service mode, the total number of threads is `jobs*2`, with a default thread count of `current server cores*2`.
- `serve.listen`: The listening address for the `taosX` REST API, with a default value of `0.0.0.0:6050`.
- `serve.database_url`: The address of the `taosX` database, in the format `sqlite:<path>`.
- `serve.request_timeout`: The global API timeout duration.
- `monitor.fqdn`: The FQDN of the `taosKeeper` service, with no default value. If empty, monitoring functionality will be disabled.
- `monitor.port`: The port of the `taosKeeper` service, defaulting to `6043`.
- `monitor.interval`: The frequency of sending metrics to `taosKeeper`, defaulting to every 10 seconds, with only values from 1 to 10 being valid.
- `log.path`: The directory for storing log files.
- `log.level`: The log level, with optional values of "error", "warn", "info", "debug", "trace".
- `log.compress`: Whether to compress archived log files or not.
- `log.rotationCount`: The maximum number of files retained in the log directory; old files exceeding this number will be deleted.
- `log.rotationSize`: The file size (in bytes) that triggers log file rotation; when the log file exceeds this size, a new file will be generated, and new logs will be written to the new file.
- `log.reservedDiskSize`: The threshold for stopping log writes when the disk where the logs are located reaches this size (in bytes).
- `log.keepDays`: The number of days logs are retained; old log files exceeding this number will be deleted.
- `log.watching`: Whether to listen for changes in the `log.loggers` configuration in the log file and attempt to reload.
- `log.loggers`: Specifies the log output level of modules, in the format `"modname" = "level"`; it also adapts to the tracing library syntax and can be specified as `modname[span{field=value}]=level`, where `level` is the log level.

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

# database url
#database_url = "sqlite:taosx.db"

# default global request timeout which unit is second. This parameter takes effect for certain interfaces that require a timeout setting
#request_timeout = 30

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

On Linux, `taosX` can be started with the Systemd command:

```shell
systemctl start taosx
```

On Windows, find the `taosX` service in the system management tool "Services" and start it, or execute the following command in the command line tool (cmd.exe or PowerShell) to start it:

```shell
sc.exe start taosx
```

### Troubleshooting

1. Modify the log level of `taosX`
   The default log level for `taosX` is `info`. To specify a different level, modify the configuration file or use the following command-line parameters:
   - `error`: `taosx serve -qq`
   - `debug`: `taosx serve -q`
   - `info`: `taosx serve -v`
   - `debug`: `taosx serve -vv`
   - `trace`: `taosx serve -vvv`
   To specify command-line parameters while running `taosX` as a service, refer to the configuration.
2. View `taosX` logs
   You can view log files or use the `journalctl` command to check the logs of `taosX`.
   On Linux, the command to view logs with `journalctl` is as follows:

   ```bash
   journalctl -u taosx [-f]
   ```

## taosX Monitoring Metrics

taosX reports monitoring metrics to taosKeeper, which writes these metrics to the monitoring database, defaulting to the `log` database, which can be modified in the taosKeeper configuration file. The following is a detailed introduction to these monitoring metrics.

### taosX Service

| Field                      | Description                                                  |
| -------------------------- | ------------------------------------------------------------ |
| sys_cpu_cores              | Number of CPU cores in the system                            |
| sys_total_memory           | Total memory in the system, in bytes                         |
| sys_used_memory            | Used memory in the system, in bytes                          |
| sys_available_memory       | Available memory in the system, in bytes                     |
| process_uptime             | Running time of taosX, in seconds                            |
| process_id                 | Process ID of taosX                                          |
| running_tasks              | Number of tasks currently being executed by taosX            |
| completed_tasks            | Number of tasks completed by the taosX process within a monitoring cycle (e.g., 10s) |
| failed_tasks               | Number of tasks that failed within a monitoring cycle (e.g., 10s) |
| process_cpu_percent        | CPU percentage used by the taosX process, in %               |
| process_memory_percent     | Memory percentage used by the taosX process, in %            |
| process_disk_read_bytes    | Average number of bytes read from the disk by the taosX process within a monitoring cycle (e.g., 10s), in bytes/s |
| process_disk_written_bytes | Average number of bytes written to the disk by the taosX process within a monitoring cycle (e.g., 10s), in bytes/s |

### Agent

| Field                      | Description                                                  |
| -------------------------- | ------------------------------------------------------------ |
| sys_cpu_cores              | Number of CPU cores in the system                            |
| sys_total_memory           | Total memory in the system, in bytes                         |
| sys_used_memory            | Used memory in the system, in bytes                          |
| sys_available_memory       | Available memory in the system, in bytes                     |
| process_uptime             | Running time of the agent, in seconds                        |
| process_id                 | Process ID of the agent                                      |
| process_cpu_percent        | CPU percentage used by the agent process, in %               |
| process_memory_percent     | Memory percentage used by the agent process, in %            |
| process_disk_read_bytes    | Average number of bytes read from the disk by the agent process within a monitoring cycle (e.g., 10s), in bytes/s |
| process_disk_written_bytes | Average number of bytes written to the disk by the agent process within a monitoring cycle (e.g., 10s), in bytes/s |

### Connector

| Field                      | Description                                                  |
| -------------------------- | ------------------------------------------------------------ |
| process_id                 | Process ID of the connector                                  |
| process_uptime             | Start time of the process, in seconds                        |
| process_cpu_percent        | CPU percentage used by the process, in %                     |
| process_memory_percent     | Memory percentage used by the process, in %                  |
| process_disk_read_bytes    | Average number of bytes read from the disk by the connector process within a monitoring cycle (e.g., 10s), in bytes/s |
| process_disk_written_bytes | Average number of bytes written to the disk by the connector process within a monitoring cycle (e.g., 10s), in bytes/s |

### taosX General Data Source Task

| Field                | Description                                                  |
| -------------------- | ------------------------------------------------------------ |
| total_execute_time   | Cumulative running time of the task, in milliseconds         |
| total_written_rows   | Total number of rows successfully written to TDengine (including duplicate records) |
| total_written_points | Cumulative number of successfully written points (equal to the number of rows multiplied by the number of columns in the data block) |
| start_time           | Task start time (reset on each restart of the task)          |
| written_rows         | Total number of rows successfully written to TDengine in this task run (including duplicate records) |
| written_points       | Total number of successfully written points in this run (equal to the number of rows multiplied by the number of columns in the data block) |
| execute_time         | Running time of this task run, in seconds                    |

### taosX TDengine V2 Task

| Field                 | Description                                                  |
| --------------------- | ------------------------------------------------------------ |
| read_concurrency      | Number of workers reading data concurrently from the data source, equal to the number of workers writing to TDengine concurrently |
| total_stables         | Total number of supertables to be migrated                   |
| total_updated_tags    | Total number of tags updated                                 |
| total_created_tables  | Total number of subtables created                            |
| total_tables          | Total number of subtables to be migrated                     |
| total_finished_tables | Total number of subtables completed for data migration (may be greater than the actual value if the task is interrupted and restarted) |
| total_success_blocks  | Total number of data blocks successfully written             |
| finished_tables       | Total number of subtables migrated in this run               |
| success_blocks        | Total number of data blocks successfully written in this run |
| created_tables        | Total number of subtables created in this run                |
| updated_tags          | Total number of tags updated in this run                     |

### taosX TDengine V3 Task

| Field                  | Description                                                  |
| ---------------------- | ------------------------------------------------------------ |
| total_messages         | Total number of messages received through TMQ                |
| total_messages_of_meta | Total number of Meta-type messages received through TMQ      |
| total_messages_of_data | Total number of Data and MetaData-type messages received through TMQ |
| total_write_raw_fails  | Total number of failures in writing raw meta                 |
| total_success_blocks   | Total number of data blocks successfully written             |
| topics                 | Number of topics subscribed through TMQ                      |
| consumers              | Number of TMQ consumers                                      |
| messages               | Total number of messages received in this run through TMQ    |
| messages_of_meta       | Total number of Meta-type messages received in this run through TMQ |
| messages_of_data       | Total number of Data and MetaData-type messages received in this run through TMQ |
| write_raw_fails        | Total number of failures in writing raw meta in this run     |
| success_blocks         | Total number of data blocks successfully written in this run |

### taosX Other Data Source Tasks

These data sources include: InfluxDB, OpenTSDB, OPC UA, OPC DA, PI, CSV, MQTT, AVEVA Historian, and Kafka.

| Field                   | Description                                                  |
| ----------------------- | ------------------------------------------------------------ |
| total_received_batches  | Total batches of data received through IPC Stream            |
| total_processed_batches | Total number of batches processed                            |
| total_processed_rows    | Total number of rows processed (equal to the sum of data rows contained in each batch) |
| total_inserted_sqls     | Total number of INSERT SQL executed                          |
| total_failed_sqls       | Total number of failed INSERT SQL executions                 |
| total_created_stables   | Total number of supertables created (may be greater than the actual value) |
| total_created_tables    | Total number of attempts to create subtables (may be greater than the actual value) |
| total_failed_rows       | Total number of rows that failed to write                    |
| total_failed_points     | Total number of points that failed to write                  |
| total_written_blocks    | Total number of successfully written raw blocks              |
| total_failed_blocks     | Total number of failed raw blocks written                    |
| received_batches        | Total batches of data received in this task run              |
| processed_batches       | Total number of batches processed in this task run           |
| processed_rows          | Total number of rows processed in this task run (equal to the sum of data rows contained in each batch) |
| received_records        | Total number of records received in this task run            |
| inserted_sqls           | Total number of INSERT SQL executed in this task run         |
| failed_sqls             | Total number of failed INSERT SQL executions in this task run |
| created_stables         | Total number of attempts to create supertables in this task run |
| created_tables          | Total number of attempts to create subtables in this task run |
| failed_rows             | Total number of rows that failed to write in this task run   |
| failed_points           | Total number of points that failed to write in this task run |
| written_blocks          | Total number of successfully written raw blocks in this task run |
| failed_blocks           | Total number of failed raw blocks written in this task run   |

### Kafka Data Source Related Metrics

| Field                         | Description                                                  |
| ----------------------------- | ------------------------------------------------------------ |
| kafka_consumers               | Number of Kafka consumers in this task run                   |
| kafka_total_partitions        | Total number of partitions in the Kafka topic                |
| kafka_consuming_partitions    | Number of partitions currently being consumed in this task run |
| kafka_consumed_messages       | Number of messages already consumed in this task run         |
| total_kafka_consumed_messages | Total number of messages consumed so far                     |

## taosX Data Parsing Plugins

When connecting to Kafka/MQTT message middleware, you need to parse the raw data. If using JSON/regex or other pattern parsers does not meet the parsing requirements, and UDT (custom parsing scripts) also cannot meet performance requirements, you can create custom data parsing plugins.

### Plugin Overview

The taosX Parser plugin is a dynamic library developed in C/Rust language, requiring compatibility with the C ABI. This dynamic library must implement the specified API and be compiled into a dynamic library that can run correctly in the taosX operating environment. After that, it should be copied to a designated location for taosX to load it at runtime during data processing's parsing stage.

### Plugin Deployment

After completing plugin development, ensure that the compilation environment is compatible with the target runtime environment. Copy the compiled plugin dynamic library to the plugin directory. When taosX starts, it will initialize and load the plugin when the system first uses it. You can check whether it has loaded successfully in the Kafka or MQTT data access configuration page of Explorer. As shown in the image below, if it loads successfully, it will appear in the parser selection list.

![Plugin Example](../../assets/taosx-01.png)

The plugin directory reuses the plugins configuration in the `taosx.toml` configuration file, appending `/parsers` as the plugin installation path. The default value in a UNIX environment is `/usr/local/taos/plugins/parsers`, and in Windows, it is `C:\TDengine\plugins\parsers`.

### Plugin API Description

#### 1. Get Plugin Name

Retrieve the plugin name for frontend display.

**Function Signature**: `const char* parser_name()`

**Return Value**: String.

#### 2. Get Plugin Version

Retrieve the plugin version for troubleshooting purposes.

**Function Signature**: `const char* parser_version()`

**Return Value**: String.

#### 3. Configure the Parser

Parse a string parameter into a configuration object, for internal plugin use only.

**Function Signature**: `parser_resp_t parser_new(char* ctx, uint32_t len);`

`char* ctx`: User-defined configuration string.

`uint32_t len`: The binary length of the string (excluding `\0`).

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

Parse the input payload and return the result in JSON format [u8]. The returned JSON will be fully decoded (the root array and all objects will be expanded) using the default JSON parser.

```c
const char* parser_mutate(
  void* parser,
  const uint8_t* in_ptr, uint32_t in_len,
  const void* uint8_t* out_ptr, uint32_t* out_len
); 
```

`void* parser`: Pointer to the object generated by parser_new;

`const uint8_t* in_ptr`: Pointer to the input payload;

`uint32_t in_len`: Length of the input payload in bytes (excluding `\0`);

`const void* uint8_t* out_ptr`: Pointer to the output JSON string (excluding `\0`). If out_ptr points to NULL, it indicates that the output is empty.

`uint32_t * out_len`: Length of the output JSON string.

**Return Value**: When the call is successful, the return value is NULL.

#### 5. Free the Parser

Release the memory of the parser object.

**Function Signature**: `void parser_free(void* parser);`

`void* parser`: Pointer to the object generated by `parser_new`.
