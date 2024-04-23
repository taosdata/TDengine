---
title: TCollector writing
sidebar_label: TCollector
description: This document describes how to integrate TDengine with TCollector.
---

import TCollector from "../14-reference/_tcollector.mdx"

TCollector is part of openTSDB and collects client computer's logs to send to the database.

You can write the data collected by TCollector to TDengine by simply changing the configuration of TCollector to point to the domain name (or IP address) and corresponding port of the server running taosAdapter. It can take full advantage of TDengine's efficient storage query performance and clustering capability for time-series data.

## Prerequisites

To write data to the TDengine via TCollector requires the following preparations.
- The TDengine cluster has been deployed and is working properly
- taosAdapter is installed and running properly. Please refer to the [taosAdapter manual](../../reference/taosadapter) for details.
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
              name              |
=================================
 information_schema             |
 performance_schema             |
 tcollector                     |
Query OK, 3 rows in database (0.001647s)

taos> use tcollector;
Database changed.

taos> show stables;
              name              |
=================================
 proc.meminfo.hugepages_rsvd    |
 proc.meminfo.directmap1g       |
 proc.meminfo.vmallocchunk      |
 proc.meminfo.hugepagesize      |
 tcollector.reader.lines_dro... |
 proc.meminfo.sunreclaim        |
 proc.stat.ctxt                 |
 proc.meminfo.swaptotal         |
 proc.uptime.total              |
 tcollector.collector.lines_... |
 proc.meminfo.vmallocused       |
 proc.meminfo.memavailable      |
 sys.numa.foreign_allocs        |
 proc.meminfo.committed_as      |
 proc.vmstat.pswpin             |
 proc.meminfo.cmafree           |
 proc.meminfo.mapped            |
 proc.vmstat.pgmajfault         |
...
```

:::note

- TDengine will automatically create unique IDs for sub-table names by the rule.
:::
