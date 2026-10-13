---
sidebar_label: Troubleshooting
title: Troubleshooting
description: How to terminate problematic connections, queries, and transactions to restore the system to normal
---

In a complex application scenario, connections and query tasks may enter an erroneous state or take too long to finish, necessitating methods to terminate these connections or tasks.

## Terminate Connection

```sql
KILL CONNECTION conn_id;
```

`conn_id` can be obtained through [`SHOW CONNECTIONS`](../09-system-info/03-show.md#show-connections).

## Terminate Query

```sql
KILL QUERY 'kill_id';
```

`kill_id` can be obtained through [`SHOW QUERIES`](../09-system-info/03-show.md#show-queries).

## Terminate Transaction

```sql
KILL TRANSACTION trans_id;
```

`trans_id` can be obtained through [`SHOW TRANSACTIONS`](../09-system-info/03-show.md#show-transactions).

## Reset Client Cache

```sql
RESET QUERY CACHE;
```

If metadata synchronization issues occur in a multi-client environment, this command can be used to forcibly clear the client cache, after which the client will pull the latest metadata from the server.

## Shared Storage Migration

```sql
SSMIGRATE DATABASE db_name;
```

Migrates the data of the specified database to shared storage. All of the following conditions must be met to execute this statement:

- The [`ssEnabled`](../../12-operations-and-tooling/03-components/01-taosd.md#storage-related) configuration is enabled (off by default);
- The deployment holds the shared-storage enterprise license;
- The executing user holds the [`SSMIGRATE`](../07-user-and-privilege/02-grant.md#object-type-and-permission-type-mapping) privilege on the database (only `SYSDBA` has it by default).

Migration task progress can be viewed through [`SHOW SSMIGRATES`](../09-system-info/03-show.md#show-ssmigrates) and tasks can be terminated through `KILL SSMIGRATE`.
