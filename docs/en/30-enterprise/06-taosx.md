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
taosx -f <from-DSN> -t <to-DSN> <Other Parameters>
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
  1. AddTag, add TAG for the table. For example: `-T add-tag:<tag1>=<value1>`
  2. Rename tables：
      2.1 Renaming scope
          2.1.1 RenameTable: rename all table matching the criterias
          2.1.2 RenameChildTable: rename all child tables matching the criterias
          2.1.3 RenameSuperTable: rename all supertables matching the criterias
      2.2 Renaming methods
          2.2.1 Prefix: rename by adding prefix
          2.2.2 Suffix: rename by adding suffix
          2.2.3 Template: rename by using template
          2.2.4 ReplaceWithRegex: rename by regular replacing
  Configuration forrenaming：
      <Renaming scope>:<renaming method>:<renaming value>

  Examples:
      1. Add prefix for all tables
      --transform rename-table:prefix:<prefix>

      2.Replace `prefix1` with `prefix2` for all tables matching the criterias, in the example below `<>` is used for regular expression instead of place holder
      -T rename-child-table:replace_with_regex:^prefix1(?<old>)::prefix2_$old

      More explanation: ^prefix1(?<old>) is regular exppression, it will match the table name with `prefix1` as prefix and the remaining part as `old`, then replace `prefix1` with `prefix2`, the final table name is `prefix2_old`

      For more details about regular replacement please refer to https://docs.rs/regex/latest/regex/#example-replacement-with-named-capture-groups 
  ```

3. jobs specify the number of parallel tasks, it is only valid for taks of tmq type. It can be specified using --jobs `<number>` or -j `<number>` .
4. -v specifies the log level of taosx, -v means info level log, -vv means debug level log, -vvv means trace level log.


