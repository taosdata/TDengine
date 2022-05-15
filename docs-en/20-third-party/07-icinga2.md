---
sidebar_label: icinga2
title: icinga2 writing
---

import Icinga2 from "../14-reference/_icinga2.mdx"

icinga2 is an open-source software monitoring host and network initially developed from the Nagios network monitoring application. Currently, icinga2 is distributed under the GNU GPL v2 license.

You can write the data collected by icinga2 to TDengine by simply modifying the icinga2 configuration to point to the taosAdapter server and the corresponding port, taking advantage of TDengine's efficient storage and query performance and clustering capabilities for time-series data.

## Prerequisites

To write icinga2 data to TDengine requires the following preparations.
- The TDengine cluster is deployed and working properly
- taosAdapter is installed and running properly. Please refer to the [taosAdapter manual](/reference/taosadapter) for details.
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
              name              |      created_time       |   ntables   |   vgroups   | replica | quorum |  days  |           keep           |  cache(MB)  |   blocks    |   minrows   |   maxrows   | wallevel |    fsync    | comp | cachelast | precision | update |   status   |
====================================================================================================================================================================================================================================================================================
 log                            | 2022-04-20 07:19:50.260 |          11 |           1 |       1 |      1 |     10 | 3650                     |          16 |           6 |         100 |        4096 |        1 |        3000 |    2 |         0 | ms        |      0 | ready      |
 icinga2                        | 2022-04-20 12:11:39.697 |          13 |           1 |       1 |      1 |     10 | 3650                     |          16 |           6 |         100 |        4096 |        1 |        3000 |    2 |         0 | ns        |      2 | ready      |
Query OK, 2 row(s) in set (0.001867s)

taos> use icinga2;
Database changed.

taos> show stables;
              name              |      created_time       | columns |  tags  |   tables    |
============================================================================================
 icinga.service.users.state_... | 2022-04-20 12:11:39.726 |       2 |      1 |           1 |
 icinga.service.users.acknow... | 2022-04-20 12:11:39.756 |       2 |      1 |           1 |
 icinga.service.procs.downti... | 2022-04-20 12:11:44.541 |       2 |      1 |           1 |
 icinga.service.users.users     | 2022-04-20 12:11:39.770 |       2 |      1 |           1 |
 icinga.service.procs.procs_min | 2022-04-20 12:11:44.599 |       2 |      1 |           1 |
 icinga.service.users.users_min | 2022-04-20 12:11:39.809 |       2 |      1 |           1 |
 icinga.check.max_check_atte... | 2022-04-20 12:11:39.847 |       2 |      3 |           2 |
 icinga.service.procs.state_... | 2022-04-20 12:11:44.522 |       2 |      1 |           1 |
 icinga.service.procs.procs_... | 2022-04-20 12:11:44.576 |       2 |      1 |           1 |
 icinga.service.users.users_... | 2022-04-20 12:11:39.796 |       2 |      1 |           1 |
 icinga.check.latency           | 2022-04-20 12:11:39.869 |       2 |      3 |           2 |
 icinga.service.procs.procs_... | 2022-04-20 12:11:44.588 |       2 |      1 |           1 |
 icinga.service.users.downti... | 2022-04-20 12:11:39.746 |       2 |      1 |           1 |
 icinga.service.users.users_... | 2022-04-20 12:11:39.783 |       2 |      1 |           1 |
 icinga.service.users.reachable | 2022-04-20 12:11:39.736 |       2 |      1 |           1 |
 icinga.service.procs.procs     | 2022-04-20 12:11:44.565 |       2 |      1 |           1 |
 icinga.service.procs.acknow... | 2022-04-20 12:11:44.554 |       2 |      1 |           1 |
 icinga.service.procs.state     | 2022-04-20 12:11:44.509 |       2 |      1 |           1 |
 icinga.service.procs.reachable | 2022-04-20 12:11:44.532 |       2 |      1 |           1 |
 icinga.check.current_attempt   | 2022-04-20 12:11:39.825 |       2 |      3 |           2 |
 icinga.check.execution_time    | 2022-04-20 12:11:39.898 |       2 |      3 |           2 |
 icinga.service.users.state     | 2022-04-20 12:11:39.704 |       2 |      1 |           1 |
Query OK, 22 row(s) in set (0.002317s)
```
