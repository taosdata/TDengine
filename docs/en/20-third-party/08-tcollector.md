---
sidebar_label: TCollector
title: TCollector writing
---

import TCollector from "../14-reference/_tcollector.mdx"

TCollector is part of openTSDB and collects client computer's logs to send to the database.

You can write the data collected by TCollector to TDengine by simply changing the configuration of TCollector to point to the domain name (or IP address) and corresponding port of the server running taosAdapter. It can take full advantage of TDengine's efficient storage query performance and clustering capability for time-series data.

## Prerequisites

To write data to the TDengine via TCollector requires the following preparations.
- The TDengine cluster has been deployed and is working properly
- taosAdapter is installed and running properly. Please refer to the [taosAdapter manual](/reference/taosadapter) for details.
- TCollector has been installed. Please refer to [official documentation](http://opentsdb.net/docs/build/html/user_guide/utilities/tcollector.html#installation-of-tcollector) for TCollector installation

## Configuration steps
<TCollector />

## Verification method

Restart taosAdapter:

```
sudo systemctl restart taosadapter
```

Run `sudo ./tcollector.py`:

Wait for a few seconds and then use the TDengine CLI to query whether the corresponding database has been created and data are written in TDengine.

```
taos> show databases;
              name              |      created_time       |   ntables   |   vgroups   | replica | quorum |  days  |           keep           |  cache(MB)  |   blocks    |   minrows   |   maxrows   | wallevel |    fsync    | comp | cachelast | precision | update |   status   |
====================================================================================================================================================================================================================================================================================
 tcollector                     | 2022-04-20 12:44:49.604 |          88 |           1 |       1 |      1 |     10 | 3650                     |          16 |           6 |         100 |        4096 |        1 |        3000 |    2 |         0 | ns        |      2 | ready      |
 log                            | 2022-04-20 07:19:50.260 |          11 |           1 |       1 |      1 |     10 | 3650                     |          16 |           6 |         100 |        4096 |        1 |        3000 |    2 |         0 | ms        |      0 | ready      |
Query OK, 2 row(s) in set (0.002679s)

taos> use tcollector;
Database changed.

taos> show stables;
              name              |      created_time       | columns |  tags  |   tables    |
============================================================================================
 proc.meminfo.hugepages_rsvd    | 2022-04-20 12:44:53.945 |       2 |      1 |           1 |
 proc.meminfo.directmap1g       | 2022-04-20 12:44:54.110 |       2 |      1 |           1 |
 proc.meminfo.vmallocchunk      | 2022-04-20 12:44:53.724 |       2 |      1 |           1 |
 proc.meminfo.hugepagesize      | 2022-04-20 12:44:54.004 |       2 |      1 |           1 |
 tcollector.reader.lines_dro... | 2022-04-20 12:44:49.675 |       2 |      1 |           1 |
 proc.meminfo.sunreclaim        | 2022-04-20 12:44:53.437 |       2 |      1 |           1 |
 proc.stat.ctxt                 | 2022-04-20 12:44:55.363 |       2 |      1 |           1 |
 proc.meminfo.swaptotal         | 2022-04-20 12:44:53.158 |       2 |      1 |           1 |
 proc.uptime.total              | 2022-04-20 12:44:52.813 |       2 |      1 |           1 |
 tcollector.collector.lines_... | 2022-04-20 12:44:49.895 |       2 |      2 |          51 |
 proc.meminfo.vmallocused       | 2022-04-20 12:44:53.704 |       2 |      1 |           1 |
 proc.meminfo.memavailable      | 2022-04-20 12:44:52.939 |       2 |      1 |           1 |
 sys.numa.foreign_allocs        | 2022-04-20 12:44:57.929 |       2 |      2 |           1 |
 proc.meminfo.committed_as      | 2022-04-20 12:44:53.639 |       2 |      1 |           1 |
 proc.vmstat.pswpin             | 2022-04-20 12:44:54.177 |       2 |      1 |           1 |
 proc.meminfo.cmafree           | 2022-04-20 12:44:53.865 |       2 |      1 |           1 |
 proc.meminfo.mapped            | 2022-04-20 12:44:53.349 |       2 |      1 |           1 |
 proc.vmstat.pgmajfault         | 2022-04-20 12:44:54.251 |       2 |      1 |           1 |
...
```
