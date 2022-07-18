---
sidebar_label: Cache
title: Cache
description: "The latest row of each table is kept in cache to provide high performance query of latest state."
---

The cache management policy in TDengine is First-In-First-Out (FIFO). FIFO is also known as insert driven cache management policy and it is different from read driven cache management, which is more commonly known as Least-Recently-Used (LRU). FIFO simply stores the latest data in cache and flushes the oldest data in cache to disk, when the cache usage reaches a threshold. In IoT use cases, it is the current state i.e. the latest or most recent data that is important. The cache policy in TDengine, like much of the design and architecture of TDengine, is based on the nature of IoT data.

Caching the latest data provides the capability of retrieving data in milliseconds. With this capability, TDengine can be configured properly to be used as a caching system without deploying another separate caching system. This simplifies the system architecture and minimizes operational costs. The cache is emptied after TDengine is restarted. TDengine does not reload data from disk into cache, like a key-value caching system.

The memory space used by the TDengine cache is fixed in size and configurable. It should be allocated based on application requirements and system resources. An independent memory pool is allocated for and managed by each vnode (virtual node) in TDengine. There is no sharing of memory pools between vnodes. All the tables belonging to a vnode share all the cache memory of the vnode.

The memory pool is divided into blocks and data is stored in row format in memory and each block follows FIFO policy. The size of each block is determined by configuration parameter `cache` and the number of blocks for each vnode is determined by the parameter `blocks`. For each vnode, the total cache size is `cache * blocks`.  A cache block needs to ensure that each table can store at least dozens of records, to be efficient.

`last_row` function can be used to retrieve the last row of a table or a STable to quickly show the current state of devices on monitoring screen. For example the below SQL statement retrieves the latest voltage of all meters in San Francisco, California.

```sql
select last_row(voltage) from meters where location='California.SanFrancisco';
```
