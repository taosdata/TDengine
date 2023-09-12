---
toc_max_heading_level: 4
title: taosX
sidebar_label: taosX
---

## Introduction

taosXis a zero-code platform for data ingestion, replication, backup, and restore. This article describes the taosX command line.

## Command Line Parameters Description:

**Note: Some parameters cannot be set by explorer for the time being [see: Description of Other Parameters], and will be opened gradually afterwards) **.

The commands are as follows:

```shell
taosx -f <from-DSN> -t <to-DSN> <其他参数>
```

The format of `<content>` in the following parameter descriptions and examples is a placeholder unless otherwise specified, so you need to replace it with the actual parameter when using it.

## DSN (Data Source Name)

The taosX command line mode uses a DSN to represent a data source (source or destination source), a typical DSN is as follows:

```bash
# url-like
<driver>[+<protocol>]://[[<username>:<password>@]<host>:<port>][/<object>][?<p1>=<v1>[&<p2>=<v2>]]
|------|------------|---|-----------|-----------|------|------|----------|-----------------------|
|driver|   protocol |   | username  | password  | host | port |  object  |  params               |

// url example
tmq+ws://root:taosdata@localhost:6030/db1?timeout=never
```
[] are optional

1. Different drivers have different parameters. The driver contains the following options.

- taos: Getting data from TDengine using the query interface
- tmq: enable data subscription to get data from TDengine
- local: data backup or recovery
- pi: Enable pi-connector to fetch data from the pi database
- opc: enable opc-connector to get data from opc-server
- mqtt: Enable mqtt-connector to fetch data from mqtt-broker
- kafka: Enabling the Kafka Connector to Subscribe to Message Writes from Kafka Topics
- influxdb:  Enabling the influxdb connector to fetch data from InfluxDB
- csv: parsing data from CSV files

2. +protocol contains the following options:
- +ws: Used when driver is taos or tmq, to indicate that rest is used to fetch data. Not using +ws means that a native connection is used to get the data, which requires taosc to be installed on the server where taosx is hosted.
- +ua: Used when the driver value is opc, indicating that the opc-server of the collected data is opc-ua.
- +da: Used when the driver value is opc, indicating that the opc-server of the collected data is opc-da.

3. host:port Indicates the address and port of the data source.
4. object Indicates the specific data source, which can be the database, super table, table of TDengine, or the path of the local backup file, or the database in the corresponding data source server.
5. username and password indicate the username and password for this data source.
6. params represents the parameters of dsn.

## Other notes

1. parser is set by --parser or -p, set the parser of transform to take effect. This can be set up through Explorer's task configuration in data sources such as CSV, MQTT, and KAFKA.

  For example:

  ```shell
  --parser "{\"parse\":{\"ts\":{\"as\":\"timestamp(ms)\"},\"topic\":{\"as\":\"varchar\",\"alias\":\"t\"},\"partition\":{\"as\":\"int\",\"alias\":\"p\"},\"offset\":{\"as\":\"bigint\",\"alias\":\"o\"},\"key\":{\"as\":\"binary\",\"alias\":\"k\"},\"value\":{\"as\":\"binary\",\"alias\":\"v\"}},\"model\":[{\"name\":\"t_{t}\",\"using\":\"kafka_data\",\"tags\":[\"t\",\"p\"],\"columns\":[\"ts\",\"o\",\"k\",\"v\"]}]}"

  ```

2. transform Configures some operations on table names and fields during data synchronization (only supported from 2.6 to 3.0 and between 3.0) with the --transform or -T setting. This setting cannot be made with Explorer at this time. The data structure is described as follows:
   
  ```shell
  1. AddTag, add TAG for the table. Example of setting: -T add-tag:<tag1>=<value1>.
  2. Table renaming:
      2.1 Renaming table qualifications
          2.1.1 RenameTable: Renames all eligible tables.
          2.1.2 RenameChildTable: Renames all eligible child tables.
          2.1.3 RenameSuperTable: Renames all eligible super tables.
      2.2 Renaming methods
          2.2.1 Prefix: Add a prefix.
          2.2.2 Suffix: add a suffix.
          2.2.3 Template: the template approach.
          2.2.4 ReplaceWithRegex: regular replacement. taosx 1.1.0 Added.
  Rename the configuration method:
      <table-qualified>:<rename-mode>:<rename-value>
  Usage examples：
      1. Add prefix <prefix> to all tables
      --transform rename-table:prefix:<prefix>
      2. Replace prefixes for eligible tables: prefix1 is replaced with prefix2, and <> in the following example is no longer a placeholder for a regular expression.
      -T rename-child-table:replace_with_regex:^prefix1(?<old>)::prefix2_$old

      Example description: ^prefix1(? <old>) is a regular expression that matches table names that start with prefix1 and records the suffix as old. prefix2$old replaces old with prefix2. Note: The two parts are separated by the key character ::, so you need to make sure that the regular expression cannot contain that character.
      For more complex replacement needs please refer to: https://docs.rs/regex/latest/regex/#example-replacement-with-named-capture-groups or consult the taosx developers.
  ```

3. jobs Specifies the number of concurrent jobs. only tmq jobs are supported. This setting cannot be made with Explorer at this time. Set via --jobs `<number>` or -j `<number>`.
4. -v is used to specify the taosx logging level, -v means enable info level logging, -vv corresponds to debug, -vvv corresponds to trace.

