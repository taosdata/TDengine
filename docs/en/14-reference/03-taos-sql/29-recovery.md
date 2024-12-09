---
title: Troubleshooting
description: How to terminate problematic connections, queries, and transactions to restore the system to normal
slug: /tdengine-reference/sql-manual/troubleshooting
---

In a complex application scenario, connections and query tasks may enter an erroneous state or take too long to finish, necessitating methods to terminate these connections or tasks.

## Terminate Connection

```sql
KILL CONNECTION conn_id;
```

conn_id can be obtained through `SHOW CONNECTIONS`.

## Terminate Query

```sql
KILL QUERY 'kill_id';
```

kill_id can be obtained through `SHOW QUERIES`.

## Terminate Transaction

```sql
KILL TRANSACTION trans_id
```

trans_id can be obtained through `SHOW TRANSACTIONS`.

## Reset Client Cache

```sql
RESET QUERY CACHE;
```

If metadata synchronization issues occur in a multi-client environment, this command can be used to forcibly clear the client cache, after which the client will pull the latest metadata from the server.
