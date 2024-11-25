---
title: TCollector
description: Writing to TDengine Using TCollector
slug: /third-party-tools/data-collection/tcollector
---

import TCollector from "./_tcollector.mdx"

TCollector is a part of openTSDB that collects client logs and sends them to the database.

By simply modifying the TCollector configuration to point to the server domain name (or IP address) running taosAdapter and the corresponding port, the data collected by TCollector can be stored in TDengine, fully utilizing TDengine's efficient storage and query performance for time-series data as well as its cluster processing capabilities.

## Prerequisites

To write TCollector data into TDengine, several preparations are needed:

- The TDengine cluster has been deployed and is running normally.
- The taosAdapter has been installed and is running normally. For details, please refer to the [taosAdapter User Manual](../../../tdengine-reference/components/taosadapter/).
- TCollector has been installed. For installation instructions, please refer to the [official documentation](http://opentsdb.net/docs/build/html/user_guide/utilities/tcollector.html#installation-of-tcollector).

## Configuration Steps

<TCollector />

## Verification Method

Restart the taosAdapter:

```shell
sudo systemctl restart taosadapter
```

Manually execute `sudo ./tcollector.py`.

After waiting a few seconds, use the TDengine CLI to query TDengine to verify if the corresponding database has been created and if data has been written.

```text
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

TDengine will automatically create unique IDs for subtable names by the rule.

:::
