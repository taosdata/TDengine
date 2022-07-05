---
sidebar_label: Prometheus
title: Prometheus for TDengine Cloud
---
Prometheus is a widespread open-source monitoring and alerting system. Prometheus joined the Cloud Native Computing Foundation (CNCF) in 2016 as the second incubated project after Kubernetes, which has a very active developer and user community.

Prometheus provides `remote_write` and `remote_read` interfaces to leverage other database products as its storage engine. To enable users of the Prometheus ecosystem to take advantage of TDengine's efficient writing and querying, TDengine also provides support for these two interfaces.

Prometheus data can be stored in TDengine via the `remote_write` interface with proper configuration. Data stored in TDengine can be queried via the `remote_read` interface, taking full advantage of TDengine's efficient storage query performance and clustering capabilities for time-series data.

## Install Prometheus

Please refer to the [official documentation](https://prometheus.io/docs/prometheus/latest/installation/) for installing Prometheus

## Configuration steps

Configuring Prometheus is done by editing the Prometheus configuration file prometheus.yml (default location `/etc/prometheus/prometheus.yml`).

```yaml
remote_write:
  - url: "http://localhost:6041/prometheus/v1/remote_write/prometheus_data?token=<token>"

remote_read:
  - url: "http://localhost:6041/prometheus/v1/remote_read/prometheus_data?token=<token>"
    remote_timeout: 10s
    read_recent: true
```

## Verification plugin

After restarting Prometheus, you can refer to the following example to verify that data is written from Prometheus to TDengine and can read out correctly.

```
taos> show databases;
              name              |      created_time       |   ntables   |   vgroups   | replica | quorum |  days  |           keep           |  cache(MB)  |   blocks    |   minrows   |   maxrows   | wallevel |    fsync    | comp | cachelast | precision | update |   status   |
====================================================================================================================================================================================================================================================================================
 test                           | 2022-04-12 08:07:58.756 |           1 |           1 |       1 |      1 |     10 | 3650                     |          16 |           6 |         100 |        4096 |        1 |        3000 |    2 |         0 | ms        |      0 | ready      |
 log                            | 2022-04-20 07:19:50.260 |           2 |           1 |       1 |      1 |     10 | 3650                     |          16 |           6 |         100 |        4096 |        1 |        3000 |    2 |         0 | ms        |      0 | ready      |
 prometheus_data                | 2022-04-20 07:21:09.202 |         158 |           1 |       1 |      1 |     10 | 3650                     |          16 |           6 |         100 |        4096 |        1 |        3000 |    2 |         0 | ns        |      2 | ready      |
 db                             | 2022-04-15 06:37:08.512 |           1 |           1 |       1 |      1 |     10 | 3650                     |          16 |           6 |         100 |        4096 |        1 |        3000 |    2 |         0 | ms        |      0 | ready      |
Query OK, 4 row(s) in set (0.000585s)

taos> use prometheus_data;
Database changed.

taos> show stables;
              name              |      created_time       | columns |  tags  |   tables    |
============================================================================================
 metrics                        | 2022-04-20 07:21:09.209 |       2 |      1 |        1389 |
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

## Use promql-cli

Install promql-cli

```bash
 go install github.com/nalbury/promql-cli@latest
```

Query Prometheus data in the running state of TDengine and taosAdapter services

```bash
promql-cli --host "<url>" "sum(up) by (job)"
```

