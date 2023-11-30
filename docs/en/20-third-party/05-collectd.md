---
title: collectd writing
sidebar_label: collectd
description: This document describes how to integrate TDengine with collectd.
---

import CollectD from "../14-reference/_collectd.mdx"


collectd is a daemon used to collect system performance metric data. collectd provides various storage mechanisms to store different values. It periodically counts system performance statistics while the system is running and storing information. You can use this information to help identify current system performance bottlenecks and predict future system load.

You can write the data collected by collectd to TDengine by simply modifying the configuration of collectd to the domain name (or IP address) and corresponding port of the server running taosAdapter. It can take full advantage of TDengine's efficient storage query performance and clustering capability for time-series data.

## Prerequisites

Writing collectd data to the TDengine requires several preparations.
- The TDengine cluster is deployed and running properly
- taosAdapter is installed and running, please refer to [taosAdapter's manual](../../reference/taosadapter) for details
- collectd has been installed. Please refer to the [official documentation](https://collectd.org/download.shtml) to install collectd

## Configuration steps
<CollectD />

## Verification method

Restart collectd 

```
sudo systemctl restart collectd
```

Use the TDengine CLI to verify that collectd's data is written to TDengine and can read out correctly.

```
taos> show databases;
              name              |
=================================
 information_schema             |
 performance_schema             |
 collectd                       |
Query OK, 3 row(s) in set (0.003266s)

taos> use collectd;
Database changed.

taos> show stables;
              name              |
=================================
 load_1                         |
 memory_value                   |
 df_value                       |
 load_2                         |
 load_0                         |
 interface_1                    |
 irq_value                      |
 interface_0                    |
 entropy_value                  |
 swap_value                     |
Query OK, 10 row(s) in set (0.002236s)

taos> select * from collectd.memory_value limit 10;
              ts               |           value           |              host              |         type_instance          |           type           |
=========================================================================================================================================================
 2022-04-20 09:27:45.459653462 |        54689792.000000000 | shuduo-1804                    | buffered                       | memory                   |
 2022-04-20 09:27:55.453168283 |        57212928.000000000 | shuduo-1804                    | buffered                       | memory                   |
 2022-04-20 09:28:05.453004291 |        57942016.000000000 | shuduo-1804                    | buffered                       | memory                   |
 2022-04-20 09:27:45.459653462 |      6381330432.000000000 | shuduo-1804                    | free                           | memory                   |
 2022-04-20 09:27:55.453168283 |      6357643264.000000000 | shuduo-1804                    | free                           | memory                   |
 2022-04-20 09:28:05.453004291 |      6349987840.000000000 | shuduo-1804                    | free                           | memory                   |
 2022-04-20 09:27:45.459653462 |       107040768.000000000 | shuduo-1804                    | slab_recl                      | memory                   |
 2022-04-20 09:27:55.453168283 |       107536384.000000000 | shuduo-1804                    | slab_recl                      | memory                   |
 2022-04-20 09:28:05.453004291 |       107634688.000000000 | shuduo-1804                    | slab_recl                      | memory                   |
 2022-04-20 09:27:45.459653462 |       309137408.000000000 | shuduo-1804                    | used                           | memory                   |
Query OK, 10 row(s) in set (0.010348s)
```

:::note

- TDengine will automatically create unique IDs for sub-table names by the rule.
:::
