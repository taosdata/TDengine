---
sidebar_label: Telegraf
title: Telegraf writing
---

import Telegraf from "../14-reference/_telegraf.mdx"

Telegraf is a viral metrics collection open-source software. Telegraf can collect the operation information of various components without writing any scripts to collect regularly, reducing the difficulty of data acquisition.

Telegraf's data can be written to TDengine by simply adding the output configuration of Telegraf to the URL corresponding to taosAdapter and modifying several configuration items. The presence of Telegraf data in TDengine can take advantage of TDengine's efficient storage query performance and clustering capabilities for time-series data.

## Prerequisites

To write Telegraf data to TDengine requires the following preparations.
- The TDengine cluster is deployed and functioning properly
- taosAdapter is installed and running properly. Please refer to the [taosAdapter manual](/reference/taosadapter) for details.
- Telegraf has been installed. Please refer to the [official documentation](https://docs.influxdata.com/telegraf/v1.22/install/) for Telegraf installation.

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
              name              |      created_time       |   ntables   |   vgroups   | replica | quorum |  days  |           keep           |  cache(MB)  |   blocks    |   minrows   |   maxrows   | wallevel |    fsync    | comp | cachelast | precision | update |   status   |
====================================================================================================================================================================================================================================================================================
 telegraf                       | 2022-04-20 08:47:53.488 |          22 |           1 |       1 |      1 |     10 | 3650                     |          16 |           6 |         100 |        4096 |        1 |        3000 |    2 |         0 | ns        |      2 | ready      |
 log                            | 2022-04-20 07:19:50.260 |           9 |           1 |       1 |      1 |     10 | 3650                     |          16 |           6 |         100 |        4096 |        1 |        3000 |    2 |         0 | ms        |      0 | ready      |
Query OK, 2 row(s) in set (0.002401s)

taos> use telegraf;
Database changed.

taos> show stables;
              name              |      created_time       | columns |  tags  |   tables    |
============================================================================================
 swap                           | 2022-04-20 08:47:53.532 |       7 |      1 |           1 |
 cpu                            | 2022-04-20 08:48:03.488 |      11 |      2 |           5 |
 system                         | 2022-04-20 08:47:53.512 |       8 |      1 |           1 |
 diskio                         | 2022-04-20 08:47:53.550 |      12 |      2 |          15 |
 kernel                         | 2022-04-20 08:47:53.503 |       6 |      1 |           1 |
 mem                            | 2022-04-20 08:47:53.521 |      35 |      1 |           1 |
 processes                      | 2022-04-20 08:47:53.555 |      12 |      1 |           1 |
 disk                           | 2022-04-20 08:47:53.541 |       8 |      5 |           2 |
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
