---
toc_max_heading_level: 4
title: taosX Reference Guide
sidebar_label: taosX
---

taosX is zero-code platform for data ingestion, replication, and backup. This article describes the command-line parameters of taosX. This section will describe the two kinds of running models of taosX: Server mode and command line mode.

## Command Line Mode

**Note: Some parameters cannot be configured through taosExplorer.**

An example of taosX command-line parameters is shown as follows:

```shell
taosx -f <from-DSN> -t <to-DSN> <other-parameters>
```

Angled braces (\<\>) are used to denote content that you input based on your system configuration.

### Data Source Name (DSN)

taosX refers to data sources and destinations by their DSN. A standard DSN is shown as follows:

```bash
# url-like
<driver>[+<protocol>]://[[<username>:<password>@]<host>:<port>][/<object>][?<p1>=<v1>[&<p2>=<v2>]]
|------|------------|---|-----------|-----------|------|------|----------|-----------------------|
|driver|   protocol |   | username  | password  | host | port |  object  |  params               |

// URL example
tmq+ws://root:taosdata@localhost:6030/db1?timeout=never
```

Items within brackets (\[\]) are optional.

1. Each driver uses different parameters. taosX includes the following drivers:

- taos: queries data from TDengine
- tmq: subscribes to data in TDengine
- local: used to back up or restore data locally
- pi: obtains data from a PI System deployment
- opc: obtains data from an OPC server
- mqtt: obtains data from an MQTT broker
- kafka:  subscribes to data in Kafka topics
- influxdb:  obtains data from an InfluxDB deployment
- csv: parses data from a CSV file

2. taosX supports the following protocols:
- +ws: uses the REST API to connect with a TDengine server using the taos or tmq driver. If you do not specify the +ws protocol, the taos and tmq drivers use native connections to TDengine. Note that the TDengine Client must be installed on the same machine as taosX for native connections.
- +ua: uses OPC-UA to connect with an OPC server.
- +da: uses OPC-DA to connect with an OPC server.

3. host:port indicates the IP address and port of the data source.
4. object indicates the specific item to transfer. This can be a TDengine database, supertable, or table; a local backup file; or a database on a data source.
5. username and password indicate the credentials on the data source.
6. params indicate additional parameters for the data source.

### Other Parameters

1. jobs indicates the number of concurrent jobs that can be run. This option is used with the tmq driver only. This parameter cannot be configured in taosExplorer. You can specify the number of concurrent jobs with the `--jobs <number>` or `-j <number>` parameter.
2. The -v parameter specifies the log level of taosX. -v indicates info, -vv indicates debug, and -vvv indicates trace.

### Scenarios

#### Users and Privileges Import/Export

| ---- | ---- |
| -u | Includes user basic information (password, whether enabled, etc.) |
| -p | Includes permission information |
| -w | Includes whitelist information |

When the `-u`/`-p` parameters are applied, only the specified information will be included. Without any parameters, it means all information (username, password, permissions, and whitelist) will be included.

The `-w` parameter cannot be used alone. It is only effective when used together with `-u` (using `-u` alone will not include the whitelist).

#### Migrating Data from Older Versions

1. Synchronize historical data

Synchronize the entire database:

```shell
taosx run -f 'taos://root:taosdata@localhost:6030/db1' -t 'taos:///db2' -v
```

Synchronize a specified super table:

```shell
taosx run \
  -f 'taos://root:taosdata@localhost:6030/db1?stables=meters' \
  -t 'taos:///db2' -v
```

To synchronize sub-tables or regular tables, support specifying a sub-table of a super table with `{stable}.{table}`, or directly specify the table name `{table}`

```shell
taosx run \
  -f 'taos://root:taosdata@localhost:6030/db1?tables=meters.d0,d1,table1' \
  -t 'taos:///db2' -v
```

2. Synchronize data for a specific time range (using RFC3339 time format with time zone):

```shell
taosx run \
  -f 'taos:///db1?start=2022-10-10T00:00:00Z' \
  -t 'taos:///db2' -v
```

3. Continuous synchronization, restro specifies to synchronize data from the last 5 minutes and to synchronize new data. In the example, it checks every 1 second, excursion allows for 500ms of delay or out-of-order data.

```shell
taosx run \
  -f 'taos:///db1?mode=realtime&restro=5m&interval=1s&excursion=500ms' \
  -t 'taos:///db2' -v
```

4. Synchronize historical data + real-time data:

```shell
taosx run -f 'taos:///db1?mode=all' -t 'taos:///db2' -v
```

5. Configure data synchronization through --transform or -T (only supports synchronization between 2.6 to 3.0 and within 3.0) to perform operations on table names and table fields during the synchronization process. It cannot be set through Explorer yet. Configuration instructions are as follows:

  ```shell
  1. AddTag: adds a tag to a table. Example: `-T add-tag:<tag1>=<value1>`
  2. Table renaming:
      2.1 Conditions
          2.1.1 RenameTable: renames all tables that match the specified conditions
          2.1.2. RenameChildTable: renames all subtables that match the specified conditions
          2.1.3 RenameSuperTable: renames all supertables that match the specified conditions
      2.2 Options
          2.2.1 Prefix: adds a prefix
          2.2.2 Suffix: adds a suffix
          2.2.3 Template: template mode
          2.2.4 ReplaceWithRegex: replaces with a regular expression 
          2.2.5 Map: use `old,new` pairs in a csv file to rename tables
  Operations are performed as follows:
      <condition>:<option>:<value>
  Example:
      1. Add a prefix to all tables:
      `--transform rename-table:prefix:<prefix>`

      2. Change prefix1 to prefix2 for all tables:
      `-T rename-child-table:replace_with_regex:^prefix1(?<old>)::prefix2_$old`

      Note: ^prefix1(?<old>) is a regular expression that matches all tables whose name begins with prefix1 and adds the suffix old. prefix2$old replaces old with prefix2. Note: Because each part of the command is separated with a colon (:), your regular expression cannot contain colons.
      For more information about regular expressions, see <https://docs.rs/regex/latest/regex/#example-replacement-with-named-capture-groups>
      
      3. Rename tables with CSV file:
      `-T rename-child-table:map:@./map.csv`
      
      Example CSV file `./map.csv` contents:

      name1,newname1
      name2,newname2

  ```

It's important to note that: When migrating between versions that are not the same, and using a native connection, it's necessary to specify libraryPath in the DSN, for example: `taos:///db1?libraryPath=./libtaos.so`

#### Import Data from CSV File(s)

Usage:

```shell
taosx run -f csv:./meters/meters.csv.gz \
  --parser '@./meters/meters.json' \
  -t taos:///csv1 -qq
```

A demo csv content:

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

`--parser` should be in JSON format：

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

It will import data from `./meters/meters.csv.gz` (a gzip compressed CSV file) into STable `meters`, with each line inserted into specified table name - `${tbname}` use `tbname` column in CSV content as table name(i.e. `.model.name` in JSON parser).

## Server Mode

This section describes how to deploy `taosX`. Once the `taosX` installation package is installed, `taosX` is available in the system. For details, please refer to [Installation and Configuration](../install).

### Configuration

`taosX` supports configuration through a configuration file. On Linux, the default configuration file path is `/etc/taos/taosx.toml`, and on Windows, it is `C:\\TDengine\\cfg\\taosx.toml`. It includes the following configuration items:

- `plugins_home`: The directory for `taosX` external data source SDK.
- `data_dir`: The directory for `taosX` data file storage.
- `logs_home`: The directory for `taosX` log file storage. The `taosX` service log file has the prefix `taosx.log`, and external data sources have their own log file name prefixes.
- `log_level`: Log level string, with optional levels including: `error`, `warn`, `info`, `debug`, `trace`. The default is `info`.
- `log_keep_days`: The maximum storage days for logs. `taosX` logs will be split into different files by day.
- `jobs`: The maximum number of threads for each runtime. In service mode, the total number of threads is `jobs * 2`, and the default number of threads is `current server cores * 2`.
- `serve.listen`: The `taosX` REST API listening address. The default is `0.0.0.0:6050`.
- `serve.database_url`: The address of the `taosX` database, in the form of `sqlite:<path>`.
- `monitor.fqdn`: FQDN of taosKeeper service, no default value. If blank, disable the monitor function.
- `monitor.port`: Port of taosKeeper service, default 6043
- `monitor.interval`: How often to send metrics to taosKeeper, default every 10 seconds. Only value from 1 to 10 is valid.

As shown below:

```toml
# plugins home
#plugins_home = "/usr/local/taos/plugins" # on linux/macOS
#plugins_home = "C:\\TDengine\\plugins" # on windows

# data dir
#data_dir = "/var/lib/taos/taosx" # on linux/macOS
#data_dir = "C:\\TDengine\\data\\taosx" # on windows

# logs home
#logs_home = "/var/log/taos" # on linux/macOS
#logs_home = "C:\\TDengine\\log" # on windows

# log level: off/error/warn/info/debug/trace
#log_level = "info"

# log keep days
#log_keep_days = 30

# number of jobs, default to 0, will use `jobs` number of works for TMQ
#jobs = 0

[serve]
# listen to ip:port address
#listen = "0.0.0.0:6050"

# database url
#database_url = "sqlite:taosx.db"

[monitor]
# FQDN of taosKeeper service, no default value
#fqdn = "localhost"
# port of taosKeeper service, default 6043
#port = 6043
# how often to send metrics to taosKeeper, default every 10 seconds. Only value from 1 to 10 is valid.
#interval = 10
```

### Start taosX

On Linux, use `systemd` to start the `taosX` service:

```shell
systemctl start taosx
```

On Windows, open the **Services** app and start the **taosX** service. Alternatively, in the Windows command line (cmd.exe or PowerShell), run command below:

```shell
sc.exe start taosx
```

### Troubleshooting

1. Modifying the `taosX` log level

The default log level for `taosX` is `info`. To specify a different level, please modify the configuration file, or use the following command-line parameters:
- `error`: `taosx serve -qq`
- `debug`: `taosx serve -q`
- `info`: `taosx serve -v`
- `debug`: `taosx serve -vv`
- `trace`: `taosx serve -vvv`

To specify command-line parameters when `taosX` is run as a service, see Configuration.

2. Viewing `taosX` logs

You can view the log file or use the `journalctl` command to view `taosX` log files.

The command to view logs using `journalctl` on Linux is as follows:

```bash
journalctl -u taosx [-f]
```