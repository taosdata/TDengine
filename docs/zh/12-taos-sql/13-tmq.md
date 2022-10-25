---
sidebar_label: 数据订阅
title: 数据订阅
description: TDengine 消息队列提供的数据订阅功能
---

TDengine 3.0.0.0 开始对消息队列做了大幅的优化和增强以简化用户的解决方案。

## 创建订阅主题

```sql
CREATE TOPIC [IF NOT EXISTS] topic_name AS subquery;
```


TOPIC 支持过滤和标量函数和 UDF 标量函数，不支持 JOIN、GROUP BY、窗口切分子句、聚合函数和 UDF 聚合函数。列订阅规则如下：

1. TOPIC 一旦创建则返回结果的字段确定
2. 被订阅或用于计算的列不可被删除、修改
3. 列可以新增，但新增的列不出现在订阅结果字段中
4. 对于 select \*，则订阅展开为创建时所有的列（子表、普通表为数据列，超级表为数据列加标签列）


## 删除订阅主题

```sql
DROP TOPIC [IF EXISTS] topic_name;
```

此时如果该订阅主题上存在 consumer，则此 consumer 会收到一个错误。

## 查看订阅主题

## SHOW TOPICS

```sql
SHOW TOPICS;
```

显示当前数据库下的所有主题的信息。

## 创建消费组

消费组的创建只能通过 TDengine 客户端驱动或者连接器所提供的 API 创建。

## 删除消费组

```sql
DROP CONSUMER GROUP [IF EXISTS] cgroup_name ON topic_name;
```

删除主题 topic_name 上的消费组 cgroup_name。

## 查看消费组

```sql
SHOW CONSUMERS;
```

显示当前数据库下所有活跃的消费者的信息。
