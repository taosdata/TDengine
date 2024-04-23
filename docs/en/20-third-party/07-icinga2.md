---
title: icinga2 writing
sidebar_label: icinga2
description: This document describes how to integrate TDengine with icinga2.
---

import Icinga2 from "../14-reference/_icinga2.mdx"

icinga2 is an open-source, host and network monitoring software initially developed from the Nagios network monitoring application. Currently, icinga2 is distributed under the GNU GPL v2 license.

You can write the data collected by icinga2 to TDengine by simply modifying the icinga2 configuration to point to the taosAdapter server and the corresponding port, taking advantage of TDengine's efficient storage and query performance and clustering capabilities for time-series data.

## Prerequisites

To write icinga2 data to TDengine requires the following preparations.
- The TDengine cluster is deployed and working properly
- taosAdapter is installed and running properly. Please refer to the [taosAdapter manual](../../reference/taosadapter) for details.
- icinga2 has been installed. Please refer to the [official documentation](https://icinga.com/docs/icinga-2/latest/doc/02-installation/) for icinga2 installation

## Configuration steps
<Icinga2 />

## Verification method

Restart taosAdapter:
```
sudo systemctl restart taosadapter
```

Restart icinga2:

```
sudo systemctl restart icinga2
```

After waiting about 10 seconds, use the TDengine CLI to query TDengine to verify that the appropriate database has been created and data are written.

```
taos> show databases;
              name              |
=================================
 information_schema             |
 performance_schema             |
 icinga2                        |
Query OK, 3 row(s) in set (0.001867s)

taos> use icinga2;
Database changed.

taos> show stables;
              name              |
=================================
 icinga.service.users.state_... |
 icinga.service.users.acknow... |
 icinga.service.procs.downti... |
 icinga.service.users.users     |
 icinga.service.procs.procs_min |
 icinga.service.users.users_min |
 icinga.check.max_check_atte... |
 icinga.service.procs.state_... |
 icinga.service.procs.procs_... |
 icinga.service.users.users_... |
 icinga.check.latency           |
 icinga.service.procs.procs_... |
 icinga.service.users.downti... |
 icinga.service.users.users_... |
 icinga.service.users.reachable |
 icinga.service.procs.procs     |
 icinga.service.procs.acknow... |
 icinga.service.procs.state     |
 icinga.service.procs.reachable |
 icinga.check.current_attempt   |
 icinga.check.execution_time    |
 icinga.service.users.state     |
Query OK, 22 row(s) in set (0.002317s)
```

:::note

- TDengine will automatically create unique IDs for sub-table names by the rule.
:::
