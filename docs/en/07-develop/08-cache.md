---
sidebar_label: Cache
title: Cache
description: "Caching System inside TDengine"
---

To achieve the purpose of high performance data writing and querying, TDengine employs a lot of caching technologies in both server side and client side. 

## Write Cache

The cache management policy in TDengine is First-In-First-Out (FIFO). FIFO is also known as insert driven cache management policy and it is different from read driven cache management, which is more commonly known as Least-Recently-Used (LRU). FIFO simply stores the latest data in cache and flushes the oldest data in cache to disk, when the cache usage reaches a threshold. In IoT use cases, it is the current state i.e. the latest or most recent data that is important. The cache policy in TDengine, like much of the design and architecture of TDengine, is based on the nature of IoT data.

The memory space used by each vnode as write cache is determined when creating a database. Parameter `vgroups` and `buffer` can be used to specify the number of vnode and the size of write cache for each vnode when creating the database. Then, the total size of write cache for this database is `vgroups * buffer`.

```sql
create database db0 vgroups 100 buffer 16MB
```

The above statement creates a database of 100 vnodes while each vnode has a write cache of 16MB.

Even though in theory it's always better to have a larger cache, the extra effect would be very minor once the size of cache grows beyond a threshold. So normally it's enough to use the default value of `buffer` parameter.

## Read Cache

When creating a database, it's also possible to specify whether to cache the latest data of each sub table, using parameter `cachelast`. There are 3 cases:
- 0: No cache for latest data
- 1: The last row of each table is cached, `last_row` function can benefit significantly from it
- 2: The latest non-NULL value of each column for each table is cached, `last` function can benefit very much when there is no `where`, `group by`, `order by` or `interval` clause
- 3: Bot hthe last row and the latest non-NULL value of each column for each table are cached, identical to the behavior of both 1 and 2 are set together


## Meta Cache

To process data writing and querying efficiently, each vnode caches the metadata that's already retrieved. Parameters `pages` and `pagesize` are used to specify the size of metadata cache for each vnode.

```sql
create database db0 pages 128 pagesize 16kb
```

The above statement will create a database db0 each of whose vnode is allocated a meta cache of `128 * 16 KB = 2 MB` .

## File System Cache

TDengine utilizes WAL to provide basic reliability. The essential of WAL is to append data in a disk file, so the file system cache also plays an important role in the writing performance. Parameter `wal` can be used to specify the policy of writing WAL, there are 2 cases:
- 1: Write data to WAL without calling fsync, the data is actually written to the file system cache without flushing immediately, in this way you can get better write performance
- 2: Write data to WAL and invoke fsync, the data is immediately flushed to disk, in this way you can get higher reliability

## Client Cache

To improve the overall efficiency of processing data, besides the above caches, the core library `libtaos.so` (also referred to as `taosc`) which all client programs depend on also has its own cache. `taosc` caches the metadata of the databases, super tables, tables that the invoking client has accessed, plus other critical metadata such as the cluster topology. 

When multiple client programs are accessing a TDengine cluster, if one of the clients modifies some metadata, the cache may become invalid in other clients. If this case happens, the client programs need to "reset query cache" to invalidate the whole cache so that `taosc` is enforced to repull the metadata it needs to rebuild the cache.
