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
  1. AddTag, add TAG for the table. 设置示例：-T add-tag:<tag1>=<value1>。
  2.表重命名：
      2.1 重命名表限定
          2.1.1 RenameTable：对所有符合条件的表进行重命名。
          2.1.2 RenameChildTable：对所有符合条件的子表进行重命名。
          2.1.3 RenameSuperTable：对所有符合条件的超级表进行重命名。
      2.2 重命名方式
          2.2.1 Prefix：添加前缀。
          2.2.2 Suffix：添加后缀。
          2.2.3 Template：模板方式。
          2.2.4 ReplaceWithRegex：正则替换。taosx 1.1.0 新增。
  重命名配置方式：
      <表限定>:<重命名方式>:<重命名值>
  使用示例：
      1.为所有表添加前缀 <prefix>
      --transform rename-table:prefix:<prefix>
      2.为符合条件的表替换前缀：prefix1 替换为 prefix2，以下示例中的 <> 为正则表达式的不再是占位符。
      -T rename-child-table:replace_with_regex:^prefix1(?<old>)::prefix2_$old

      示例说明：^prefix1(?<old>) 为正则表达式，该表达式会匹配表名中包含以 prefix1 开始的表名并将后缀部分记录为 old，prefix2$old 则会使用 prefix2 与 old 进行替换。注意：两部分使用关键字符 :: 进行分隔，所以需要保证正则表达式中不能包含该字符。
      若有更复杂的替换需求请参考：https://docs.rs/regex/latest/regex/#example-replacement-with-named-capture-groups 或咨询 taosx 开发人员。
  ```

3. jobs 指定任务并发数，仅支持 tmq 任务。 This setting cannot be made with Explorer at this time. 通过 --jobs `<number>` 或 -j `<number>` 进行设置。
4. -v 用于指定 taosx 的日志级别，-v 表示启用 info 级别日志，-vv 对应 debug，-vvv 对应 trace。

