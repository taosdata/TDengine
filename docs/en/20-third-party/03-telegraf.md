---
title: Telegraf writing
sidebar_label: Telegraf
description: This document describes how to integrate TDengine with Telegraf.
---

import Telegraf from "../14-reference/_telegraf.mdx"

Telegraf is a viral, open-source, metrics collection software. Telegraf can collect the operation information of various components without having to write any scripts to collect regularly, reducing the difficulty of data acquisition.

Telegraf's data can be written to TDengine by simply adding the output configuration of Telegraf to the URL corresponding to taosAdapter and modifying several configuration items. The presence of Telegraf data in TDengine can take advantage of TDengine's efficient storage query performance and clustering capabilities for time-series data.

## Prerequisites

To write Telegraf data to TDengine requires the following preparations.
- The TDengine cluster is deployed and functioning properly
- taosAdapter is installed and running properly. Please refer to the [taosAdapter manual](../../reference/taosadapter) for details.
- Telegraf has been installed. Please refer to the [official documentation](https://docs.influxdata.com/telegraf/v1.22/install/) for Telegraf installation.
- Telegraf collects the running status measurements of current system. You can enable [input plugins](https://docs.influxdata.com/telegraf/v1.22/plugins/) to insert [other formats](https://docs.influxdata.com/telegraf/v1.24/data_formats/input/) data to Telegraf then forward to TDengine.

## Configuration steps
<Telegraf />

## Verification method

Restart Telegraf service:

```
sudo systemctl restart telegraf
```

Use TDengine CLI to verify Telegraf correctly writing data to TDengine and read out:

```
taos> show databases;
              name              |
=================================
 information_schema             |
 performance_schema             |
 telegraf                       |
Query OK, 3 rows in database (0.010568s)

taos> use telegraf;
Database changed.

taos> show stables;
              name              |
=================================
 swap                           |
 cpu                            |
 system                         |
 diskio                         |
 kernel                         |
 mem                            |
 processes                      |
 disk                           |
Query OK, 8 row(s) in set (0.000521s)

taos> select * from telegraf.system limit 10;
              ts               |           load1           |           load5           |          load15           |        n_cpus         |        n_users        |        uptime         | uptime_format |              host
|
=============================================================================================================================================================================================================================================
 2022-04-20 08:47:50.000000000 |               0.000000000 |               0.050000000 |               0.070000000 |                     4 |                     1 |                  5533 |  1:32         | shuduo-1804
|
 2022-04-20 08:48:00.000000000 |               0.000000000 |               0.050000000 |               0.070000000 |                     4 |                     1 |                  5543 |  1:32         | shuduo-1804
|
 2022-04-20 08:48:10.000000000 |               0.000000000 |               0.040000000 |               0.070000000 |                     4 |                     1 |                  5553 |  1:32         | shuduo-1804
|
Query OK, 3 row(s) in set (0.013269s)
```

:::note

- TDengine take influxdb format data and create unique ID for table names by the rule.
The user can configure `smlChildTableName` parameter to generate specified table names if he/she needs. And he/she also need to insert data with specified data format.
For example, Add `smlChildTableName=tname` in the taos.cfg file. Insert data `st,tname=cpu1,t1=4 c1=3 1626006833639000000` then the table name will be cpu1. If there are multiple lines has same tname but different tag_set, the first line's tag_set will be used to automatically creating table and ignore other lines. Please refer to [TDengine Schemaless](../../reference/schemaless/#Schemaless-Line-Protocol)
:::

