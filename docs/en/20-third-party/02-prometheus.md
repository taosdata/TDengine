---
title: Prometheus writing and reading
sidebar_label: Prometheus
description: This document describes how to integrate TDengine with Prometheus.
---

import Prometheus from "../14-reference/_prometheus.mdx"

Prometheus is a widespread open-source monitoring and alerting system. Prometheus joined the Cloud Native Computing Foundation (CNCF) in 2016 as the second incubated project after Kubernetes, which has a very active developer and user community.

Prometheus provides `remote_write` and `remote_read` interfaces to leverage other database products as its storage engine. To enable users of the Prometheus ecosystem to take advantage of TDengine's efficient writing and querying, TDengine also provides support for these two interfaces.

Prometheus data can be stored in TDengine via the `remote_write` interface with proper configuration. Data stored in TDengine can be queried via the `remote_read` interface, taking full advantage of TDengine's efficient storage query performance and clustering capabilities for time-series data.

## Prerequisites

To write Prometheus data to TDengine requires the following preparations.
- The TDengine cluster is deployed and functioning properly
- taosAdapter is installed and running properly. Please refer to the [taosAdapter manual](../../reference/taosadapter) for details.
- Prometheus has been installed. Please refer to the [official documentation](https://prometheus.io/docs/prometheus/latest/installation/) for installing Prometheus

## Configuration steps

<Prometheus />

## Verification method

After restarting Prometheus, you can refer to the following example to verify that data is written from Prometheus to TDengine and can read out correctly. 

### Query and write data using TDengine CLI

```
taos> show databases;
              name              |
=================================
 information_schema             |
 performance_schema             |
 prometheus_data                |
Query OK, 3 row(s) in set (0.000585s)

taos> use prometheus_data;
Database changed.

taos> show stables;
              name              |
=================================
 metrics                        |
Query OK, 1 row(s) in set (0.000487s)

taos> select * from metrics limit 10;
              ts               |           value           |             labels             |
=============================================================================================
 2022-04-20 07:21:09.193000000 |               0.000024996 | {"__name__":"go_gc_duration... |
 2022-04-20 07:21:14.193000000 |               0.000024996 | {"__name__":"go_gc_duration... |
 2022-04-20 07:21:19.193000000 |               0.000024996 | {"__name__":"go_gc_duration... |
 2022-04-20 07:21:24.193000000 |               0.000024996 | {"__name__":"go_gc_duration... |
 2022-04-20 07:21:29.193000000 |               0.000024996 | {"__name__":"go_gc_duration... |
 2022-04-20 07:21:09.193000000 |               0.000054249 | {"__name__":"go_gc_duration... |
 2022-04-20 07:21:14.193000000 |               0.000054249 | {"__name__":"go_gc_duration... |
 2022-04-20 07:21:19.193000000 |               0.000054249 | {"__name__":"go_gc_duration... |
 2022-04-20 07:21:24.193000000 |               0.000054249 | {"__name__":"go_gc_duration... |
 2022-04-20 07:21:29.193000000 |               0.000054249 | {"__name__":"go_gc_duration... |
Query OK, 10 row(s) in set (0.011146s)
```

### Use promql-cli to read data from TDengine via remote_read

Install promql-cli

```
 go install github.com/nalbury/promql-cli@latest
```

Query Prometheus data in the running state of TDengine and taosAdapter services

```
ubuntu@shuduo-1804 ~ $ promql-cli --host "http://127.0.0.1:9090" "sum(up) by (job)"
JOB           VALUE    TIMESTAMP
prometheus    1        2022-04-20T08:05:26Z
node          1        2022-04-20T08:05:26Z
```

Stop taosAdapter service and query Prometheus data to verify

```
ubuntu@shuduo-1804 ~ $ sudo systemctl stop taosadapter.service
ubuntu@shuduo-1804 ~ $ promql-cli --host "http://127.0.0.1:9090" "sum(up) by (job)"
VALUE    TIMESTAMP

```

:::note

- TDengine will automatically create unique IDs for sub-table names by the rule.
:::
