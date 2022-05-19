---
sidebar_label: Cache
title: Cache
description: "The latest row of each table is kept in cache to provide high performance query of latest state."
---

The cache management policy in TDengine is First-In-First-Out (FIFO), which is also known as insert driven cache management policy and different from read driven cache management, i.e. Least-Recent-Used (LRU). It simply stores the latest data in cache and flushes the oldest data in cache to disk when the cache usage reaches a threshold. In IoT use cases, the most cared about data is the latest data, i.e. current state. The cache policy in TDengine is based the nature of IoT data.

Caching the latest data provides the capability of retrieving data in milliseconds. With this capability, TDengine can be configured properly to be used as caching system without deploying another separate caching system to simplify the system architecture and minimize the operation cost. The cache will be emptied after TDengine is restarted, TDengine doesn't reload data from disk into cache like a real key-value caching system.

The memory space used by TDengine cache is fixed in size, according to the configuration based on application requirement and system resources. Independent memory pool is allocated for and managed by each vnode (virtual node) in TDengine, there is no sharing of memory pools between vnodes. All the tables belonging to a vnode share all the cache memory of the vnode.

Memory pool is divided into blocks and data is stored in row format in memory and each block follows FIFO policy. The size of each block is determined by configuration parameter `cache`, the number of blocks for each vnode is determined by `blocks`. For each vnode, the total cache size is `cache * blocks`. It's better to set the size of each block to hold at least tends of rows.

`last_row` function can be used to retrieve the last row of a table or a STable to quickly show the current state of devices on monitoring screen. For example below SQL statement retrieves the latest voltage of all meters in Chaoyang district of Beijing.

```sql
select last_row(voltage) from meters where location='Beijing.Chaoyang';
```
