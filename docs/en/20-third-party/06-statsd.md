---
title: StatsD Writing
sidebar_label: StatsD
description: This document describes how to integrate TDengine with StatsD.
---

import StatsD from "../14-reference/_statsd.mdx"

StatsD is a simple daemon for aggregating application metrics, which has evolved rapidly in recent years into a unified protocol for collecting application performance metrics.

You can write StatsD data to TDengine by simply modifying the configuration file of StatsD with the domain name (or IP address) of the server running taosAdapter and the corresponding port. It can take full advantage of TDengine's efficient storage query performance and clustering capabilities for time-series data.

## Prerequisites

To write StatsD data to TDengine requires the following preparations.
1. The TDengine cluster is deployed and functioning properly
2. taosAdapter is installed and running properly. Please refer to the taosAdapter manual for details.
- StatsD has been installed. To install StatsD, please refer to [official documentation](https://github.com/statsd/statsd)

## Configuration steps
<StatsD />

## Verification method

Start StatsD:

```
$ node stats.js config.js &
[1] 8546
$ 20 Apr 09:54:41 - [8546] reading config file: config.js
20 Apr 09:54:41 - server is up INFO
```

Using the utility software `nc` to write data for test:

```
$ echo "foo:1|c" | nc -u -w0 127.0.0.1 8125
```

Use the TDengine CLI to verify that StatsD data is written to TDengine and can read out correctly.

```
taos> show databases;
              name              |
=================================
 information_schema             |
 performance_schema             |
 statsd                         |
Query OK, 3 row(s) in set (0.003142s)

taos> use statsd;
Database changed.

taos> show stables;
              name              |
=================================
 foo                            |
Query OK, 1 row(s) in set (0.002161s)

taos> select * from foo;
              ts               |         value         |         metric_type          |
=======================================================================================
 2022-04-20 09:54:51.219614235 |                     1 | counter                      |
Query OK, 1 row(s) in set (0.004179s)

taos>
```

:::note

- TDengine will automatically create unique IDs for sub-table names by the rule.
:::
