---
title: StatsD
description: Writing to TDengine Using StatsD
slug: /third-party-tools/data-collection/statsd
---

import StatsD from "./_statsd.mdx"

StatsD is a simple daemon for aggregating and summarizing application metrics. In recent years, it has rapidly evolved into a unified protocol for collecting application performance metrics.

By simply filling in the domain name (or IP address) of the server running taosAdapter and the corresponding port in the StatsD configuration file, the data from StatsD can be written into TDengine, fully utilizing TDengine's efficient storage and query performance for time-series data as well as its cluster processing capabilities.

## Prerequisites

To write StatsD data into TDengine, several preparations are needed:

- The TDengine cluster has been deployed and is running normally.
- The taosAdapter has been installed and is running normally. For details, please refer to the [taosAdapter User Manual](../../../tdengine-reference/components/taosadapter/).
- StatsD has been installed. For installation instructions, please refer to the [official documentation](https://github.com/statsd/statsd).

## Configuration Steps

<StatsD />

## Verification Method

Run StatsD:

```shell
$ node stats.js config.js &
[1] 8546
$ 20 Apr 09:54:41 - [8546] reading config file: config.js
20 Apr 09:54:41 - server is up INFO
```

Use `nc` to write test data:

```shell
echo "foo:1|c" | nc -u -w0 127.0.0.1 8125
```

Use TDengine CLI to verify that data is written from StatsD to TDengine and can be read correctly:

```text
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

TDengine will automatically create unique IDs for subtable names by the rule.

:::
