---
title: Icinga2
description: Writing to TDengine Using Icinga2
slug: /third-party-tools/data-collection/icinga2
---

import Icinga2 from "./_icinga2.mdx"

Icinga2 is an open-source host and network monitoring software that evolved from the Nagios network monitoring application. Currently, Icinga2 is released under the GNU GPL v2 license.

By simply modifying the Icinga2 configuration to point to the corresponding server and port of the taosAdapter, the data collected by Icinga2 can be stored in TDengine, fully utilizing TDengine's efficient storage and query performance for time-series data as well as its cluster processing capabilities.

## Prerequisites

To write Icinga2 data into TDengine, several preparations are needed:

- The TDengine cluster has been deployed and is running normally.
- The taosAdapter has been installed and is running normally. For details, please refer to the [taosAdapter User Manual](../../../tdengine-reference/components/taosadapter/).
- Icinga2 has been installed. For installation instructions, please refer to the [official documentation](https://icinga.com/docs/icinga-2/latest/doc/02-installation/).

## Configuration Steps

<Icinga2 />

## Verification Method

Restart the taosAdapter:

```shell
sudo systemctl restart taosadapter
```

Restart Icinga2:

```shell
sudo systemctl restart icinga2
```

After waiting for about 10 seconds, use the TDengine CLI to query TDengine to verify if the corresponding database has been created and if data has been written:

```text
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

TDengine will automatically create unique IDs for subtable names by the rule.

:::
