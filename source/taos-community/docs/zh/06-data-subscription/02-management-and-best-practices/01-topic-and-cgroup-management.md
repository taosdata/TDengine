---
sidebar_label: 主题与消费组管理
title: 主题与消费组管理
toc_max_heading_level: 4
---

## 查看主题与消费组状态

可使用以下 SQL 查看主题、消费者和订阅状态：

```sql
SHOW TOPICS;
SHOW CONSUMERS;
SHOW SUBSCRIPTIONS;
```

在排查消费延迟、分区分配不均、消费停滞时，建议先从以上三个视图入手确认状态。

## 重新加载主题（RELOAD）

当查询主题涉及的列、标签长度或 `SELECT *` 展开结构发生变化时，可使用 RELOAD 使主题重新生效：

```sql
RELOAD TOPIC IF EXISTS topic_name AS subquery;
```

建议顺序：先停止消费 → 调整表结构 → `RELOAD TOPIC` → 恢复消费。

## 删除主题与消费组（DROP）

删除主题：

```sql
DROP TOPIC [IF EXISTS] [FORCE] topic_name;
```

删除消费组：

```sql
DROP CONSUMER GROUP [IF EXISTS] [FORCE] cgroup_name ON topic_name;
```

`FORCE` 适用于仍有消费者在线的场景，但强制删除后在线消费者会报错，生产环境中请先完成下线与切流。
