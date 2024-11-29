---
title: Troubleshooting
description: How to terminate problematic connections, queries, and transactions to restore the system to normal
slug: /tdengine-reference/sql-manual/troubleshooting
---

In a complex application scenario, connections and query tasks may enter an erroneous state or take an excessively long time to complete. In such cases, it is necessary to have methods to terminate these connections or tasks.

## Terminating Connections

```sql
KILL CONNECTION conn_id;
```

The `conn_id` can be obtained by using `SHOW CONNECTIONS`.

## Terminating Queries

```sql
KILL QUERY 'kill_id';
```

The `kill_id` can be obtained by using `SHOW QUERIES`.

## Terminating Transactions

```sql
KILL TRANSACTION trans_id;
```

The `trans_id` can be obtained by using `SHOW TRANSACTIONS`.

## Resetting Client Cache

```sql
RESET QUERY CACHE;
```

If there is a metadata desynchronization issue in a multi-client environment, this command can be used to forcefully clear the client cache, after which the client will pull the latest metadata from the server.
