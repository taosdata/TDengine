---
sidebar_label: StatsD
title: StatsD writing
---

import StatsD from "../14-reference/_statsd.mdx"

StatsD is a simple daemon for aggregating application metrics, which has evolved rapidly in recent years into a unified protocol for collecting application performance metrics.

You can write StatsD data to TDengine by simply modifying in the configuration file of StatsD with the domain name (or IP address) of the server running taosAdapter and the corresponding port. It can take full advantage of TDengine's efficient storage query performance and clustering capabilities for time-series data.

## Prerequisites

To write StatsD data to TDengine requires the following preparations.
- The TDengine cluster has been deployed and is working properly
- taosAdapter is installed and running properly. Please refer to the [taosAdapter manual](/reference/taosadapter) for details.
- StatsD has been installed. To install StatsD, please refer to [official documentation](https://github.com/statsd/statsd)

## Configuration steps
<StatsD />

## Verification method

Start StatsD:

```
$ node stats.js config.js &
[1] 8546
$ 20 Apr 09:54:41 - [8546] reading config file: exampleConfig.js
20 Apr 09:54:41 - server is up INFO
```

Using the utility software `nc` to write data for test:

```
$ echo "foo:1|c" | nc -u -w0 127.0.0.1 8125
```

Use the TDengine CLI to verify that StatsD data is written to TDengine and can read out correctly.

```
Welcome to the TDengine shell from Linux, Client Version:2.4.0.0
Copyright (c) 2020 by TAOS Data, Inc. All rights reserved.

taos> show databases;
              name              |      created_time       |   ntables   |   vgroups   | replica | quorum |  days  |           keep           |  cache(MB)  |   blocks    |   minrows   |   maxrows   | wallevel |    fsync    | comp | cachelast | precision | update |   status   |
====================================================================================================================================================================================================================================================================================
 log                            | 2022-04-20 07:19:50.260 |          11 |           1 |       1 |      1 |     10 | 3650                     |          16 |           6 |         100 |        4096 |        1 |        3000 |    2 |         0 | ms        |      0 | ready      |
 statsd                         | 2022-04-20 09:54:51.220 |           1 |           1 |       1 |      1 |     10 | 3650                     |          16 |           6 |         100 |        4096 |        1 |        3000 |    2 |         0 | ns        |      2 | ready      |
Query OK, 2 row(s) in set (0.003142s)

taos> use statsd;
Database changed.

taos> show stables;
              name              |      created_time       | columns |  tags  |   tables    |
============================================================================================
 foo                            | 2022-04-20 09:54:51.234 |       2 |      1 |           1 |
Query OK, 1 row(s) in set (0.002161s)

taos> select * from foo;
              ts               |         value         |         metric_type          |
=======================================================================================
 2022-04-20 09:54:51.219614235 |                     1 | counter                      |
Query OK, 1 row(s) in set (0.004179s)

taos>
```
