---
title: TCollector
slug: /third-party-tools/data-collection/tcollector
---

import TCollector from "../../assets/resources/_tcollector.mdx"

TCollector is part of openTSDB, used for collecting client logs and sending them to the database.

Simply modify the TCollector configuration to point to the server domain name (or IP address) and corresponding port where taosAdapter is running to store the data collected by TCollector into TDengine, fully utilizing TDengine's efficient storage and query performance and cluster processing capabilities for time-series data.

## Prerequisites

To write TCollector data into TDengine, the following preparations are needed:

- TDengine cluster is deployed and running normally
- taosAdapter is installed and running normally. For details, please refer to [taosAdapter user manual](../../../tdengine-reference/components/taosadapter)
- TCollector is installed. For TCollector installation, please refer to the [official documentation](http://opentsdb.net/docs/build/html/user_guide/utilities/tcollector.html#installation-of-tcollector)

## Configuration Steps

<TCollector />

## Verification Method

Restart taosAdapter:

```shell
sudo systemctl restart taosadapter
```

Manually execute `sudo ./tcollector.py`

After waiting a few seconds, use the TDengine CLI to query whether TDengine has created the corresponding database and written data.

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

- TDengine by default generates subtable names based on a rule-generated unique ID value.

:::
