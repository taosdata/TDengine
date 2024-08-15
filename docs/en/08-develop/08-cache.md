---
title: Caching
sidebar_label: Caching
description: This document describes the caching component of TDengine.
---

TDengine uses various kinds of caching techniques to efficiently write and query data. This document describes the caching component of TDengine.

## Write Cache

TDengine uses an insert-driven cache management policy, known as first in, first out (FIFO). This policy differs from read-driven "least recently used (LRU)" cache management. A FIFO policy stores the latest data in cache and flushes the oldest data from cache to disk when the cache usage reaches a threshold. In IoT use cases, the most recent data or the current state is most important. The cache policy in TDengine, like much of the design and architecture of TDengine, is based on the nature of IoT data.

When you create a database, you can configure the size of the write cache on each vnode. The **vgroups** parameter determines the number of vgroups that process data in the database, and the **buffer** parameter determines the size of the write cache for each vnode. The unit of buffer is MB.

```sql
create database db0 vgroups 100 buffer 16
```

In theory, larger cache sizes are always better. However, at a certain point, it becomes impossible to improve performance by increasing cache size. In most scenarios, you can retain the default cache settings.

## Read Cache

When you create a database, you can configure whether the latest data from every subtable is cached. To do so, set the *cachemodel* parameter as follows:
- none: Caching is disabled.
- last_row: The latest row of data in each subtable is cached. This option significantly improves the performance of the `LAST_ROW` function
- last_value: The latest non-null value in each column of each subtable is cached. This option significantly improves the performance of the `LAST` function in normal situations, such as WHERE, ORDER BY, GROUP BY, and INTERVAL statements.
- both: Rows and columns are both cached. This option is equivalent to simultaneously enabling option last_row and last_value.

## Metadata Cache

To improve query and write performance, each vnode caches the metadata that it receives. When you create a database, you can configure the size of the metadata cache through the *pages* and *pagesize* parameters. The unit of pagesize is kb.

```sql
create database db0 pages 128 pagesize 16
```

The preceding SQL statement creates 128 pages on each vnode in the `db0` database. Each page has a 16 KB metadata cache.

## File System Cache

TDengine implements data reliability by means of a write-ahead log (WAL). Writing data to the WAL is essentially writing data to the disk in an ordered, append-only manner. For this reason, the file system cache plays an important role in write performance. When you create a database, you can use the *wal* parameter to choose higher performance or higher reliability.
- 1: This option writes to the WAL but does not enable fsync. New data written to the WAL is stored in the file system cache but not written to disk. This provides better performance.
- 2: This option writes to the WAL and enables fsync. New data written to the WAL is immediately written to disk. This provides better data reliability.

## Client Cache

In addition to the server-side caching discussed previously, the core client library `libtaos.so` also makes use of caching. TDengine Client caches the metadata of all databases, supertables, and subtables that it has accessed, as well as the cluster topology.

If a client modifies certain metadata while multiple clients are simultaneously accessing a TDengine cluster, the metadata caches on each client may fail or become out of sync. If this occurs, run the `reset query cache` command on the affected clientsto force them to obtain fresh metadata and reset their caches.
