---
sidebar_label: 异常恢复
title: 异常恢复
description: 终止异常连接、查询与事务，以及重置客户端缓存
---

在复杂应用场景中，连接或查询任务可能进入错误状态，或耗时过长无法结束。此时可用下列语句终止对应连接或任务，使系统恢复正常。

## 终止连接

```sql
KILL CONNECTION conn_id;
```

`conn_id` 可通过 [`SHOW CONNECTIONS`](../09-system-info/03-show.md#show-connections) 获取。

## 终止查询

```sql
KILL QUERY 'kill_id';
```

`kill_id` 可通过 [`SHOW QUERIES`](../09-system-info/03-show.md#show-queries) 获取。

## 终止事务

```sql
KILL TRANSACTION trans_id;
```

`trans_id` 可通过 [`SHOW TRANSACTIONS`](../09-system-info/03-show.md#show-transactions) 获取。

## 重置客户端缓存

```sql
RESET QUERY CACHE;
```

多客户端环境下若出现元数据不同步，可用本命令强制清空客户端缓存；之后客户端会从服务端拉取最新元数据。
