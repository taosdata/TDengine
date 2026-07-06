---
sidebar_label: 异常恢复
title: 异常恢复
description: 如何终止出现问题的连接、查询和事务以使系统恢复正常
---

在一个复杂的应用场景中，连接和查询任务等有可能进入一种错误状态或者耗时过长迟迟无法结束，此时需要有能够终止这些连接或任务的方法。

## 终止连接

```sql
KILL CONNECTION conn_id;
```

conn_id 可以通过 `SHOW CONNECTIONS` 获取。

## 终止查询

```sql
KILL QUERY 'kill_id';
```

kill_id 可以通过 `SHOW QUERIES` 获取。

## 终止事务

```sql
KILL TRANSACTION trans_id
```

trans_id 可以通过 `SHOW TRANSACTIONS` 获取。

## 重置客户端缓存

```sql
RESET QUERY CACHE;
```

如果在多客户端情况下出现元数据不同步的情况，可以用这条命令强制清空客户端缓存，随后客户端会从服务端拉取最新的元数据。
