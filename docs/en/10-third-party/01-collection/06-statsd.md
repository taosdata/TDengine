---
title: StatsD
slug: /third-party-tools/data-collection/statsd
---

import StatsD from "../../assets/resources/_statsd.mdx"

StatsD is a simple daemon for aggregating and summarizing application metrics that has rapidly evolved in recent years into a unified protocol for collecting application performance metrics.

Simply fill in the domain name (or IP address) and corresponding port of the server running taosAdapter in the StatsD configuration file to write StatsD data into TDengine, fully leveraging TDengine's efficient storage, query performance, and cluster processing capabilities for time-series data.

## Prerequisites

The following preparations are needed to write StatsD data into TDengine:

- TDengine cluster is deployed and running normally
- taosAdapter is installed and running normally. For details, please refer to the [taosAdapter user manual](../../../tdengine-reference/components/taosadapter)
- StatsD is installed. For installation of StatsD, please refer to the [official documentation](https://github.com/statsd/statsd)

## Configuration Steps

<StatsD />

## Verification Method

Run StatsD:

```text
$ node stats.js config.js &
[1] 8546
$ 20 Apr 09:54:41 - [8546] reading config file: config.js
20 Apr 09:54:41 - server is up INFO
```

Use nc to write test data:

```shell
echo "foo:1|c" | nc -u -w0 127.0.0.1 8125
```

Use TDengine CLI to verify that data is written from StatsD to TDengine and can be correctly read:

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

- TDengine will automatically create unique IDs for subtable names by the rule.

:::
