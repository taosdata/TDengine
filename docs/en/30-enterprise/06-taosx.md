---
toc_max_heading_level: 4
title: taosX
sidebar_label: taosX
---

## Introduction

taosX is zero-code platform for data ingestion, replication, and backup. This article describes the command-line parameters of taosX.

## Description

**Note: Some parameters cannot be configured through taosExplorer.**

An example of taosX command-line parameters is shown as follows:

```shell
taosx -f <from-DSN> -t <to-DSN> <other-parameters>
```

Angled braces (\<\>) are used to denote content that you input based on your system configuration.

## Data Source Name (DSN)

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

## Other Parameters

1. Use the -p or --parser parameter to configure the transform parser. This item can be configured in taosExplorer.

  Example:

  ```shell
  --parser "{\"parse\":{\"ts\":{\"as\":\"timestamp(ms)\"},\"topic\":{\"as\":\"varchar\",\"alias\":\"t\"},\"partition\":{\"as\":\"int\",\"alias\":\"p\"},\"offset\":{\"as\":\"bigint\",\"alias\":\"o\"},\"key\":{\"as\":\"binary\",\"alias\":\"k\"},\"value\":{\"as\":\"binary\",\"alias\":\"v\"}},\"model\":[{\"name\":\"t_{t}\",\"using\":\"kafka_data\",\"tags\":[\"t\",\"p\"],\"columns\":[\"ts\",\"o\",\"k\",\"v\"]}]}"

  ```

2. Use the -T or --transform parameter to perform operations on database names or fields when migrating from TDengine 2.6 or 3.0 to 3.0. This parameter cannot be configured in taosExplorer. The usage of this parameter is described as follows:
   
  ```shell
  1. AddTag: adds a tag to a table. Example: -T add-tag:<tag1\>=\<value1>
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
  Operations are performed as follows:
      <condition>:<option>:<value>
  Example:
      1. Add a prefix to all tables:
      --transform rename-table:prefix:<prefix>
      2. Change prefix1 to prefix2 for all tables:
      -T rename-child-table:replace_with_regex:^prefix1(?<old>)::prefix2_$old

      Note: ^prefix1(?<old>) is a regular expression that matches all tables whose name begins with prefix1 and adds the suffix old. prefix2$old replaces old with prefix2. Note: Because each part of the command is separated with a colon (:), your regular expression cannot contain colons.
      For more information about regular expressions, see <https://docs.rs/regex/latest/regex/#example-replacement-with-named-capture-groups>
  ```

3. jobs indicates the number of concurrent jobs that can be run. This option is used with the tmq driver only. This parameter cannot be configured in taosExplorer. You can specify the number of concurrent jobs with the --jobs <number> or -j <number> parameter.
4. The -v parameter specifies the log level of taosX. -v indicates info, -vv indicates debug, and -vvv indicates trace.

