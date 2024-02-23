---
sidebar_label: 数据订阅
title: 数据订阅
description: TDengine 消息队列提供的数据订阅功能
---

TDengine 3.0.0.0 开始对消息队列做了大幅的优化和增强以简化用户的解决方案。

## 创建 topic

TDengine 创建 topic 的个数上限通过参数 tmqMaxTopicNum 控制，默认 20 个。

TDengine 使用 SQL 创建一个 topic，共有三种类型的 topic：

### 查询 topic

语法：

```sql
CREATE TOPIC [IF NOT EXISTS] topic_name as subquery
```

通过 `SELECT` 语句订阅（包括 `SELECT *`，或 `SELECT ts, c1` 等指定查询订阅，可以带条件过滤、标量函数计算，但不支持聚合函数、不支持时间窗口聚合）。需要注意的是：

- 该类型 TOPIC 一旦创建则订阅数据的结构确定。
- 被订阅或用于计算的列或标签不可被删除（`ALTER table DROP`）、修改（`ALTER table MODIFY`）。
- 若发生表结构变更，新增的列不出现在结果中。
- 对于 select \*，则订阅展开为创建时所有的列（子表、普通表为数据列，超级表为数据列加标签列）
### 超级表 topic

语法：

```sql
CREATE TOPIC [IF NOT EXISTS] topic_name [with meta] AS STABLE stb_name [where_condition]
```

与 `SELECT * from stbName` 订阅的区别是：

- 不会限制用户的表结构变更。
- 返回的是非结构化的数据：返回数据的结构会随之超级表的表结构变化而变化。
- with meta 参数可选，选择时将返回创建超级表，子表等语句，主要用于taosx做超级表迁移
- where_condition 参数可选，选择时将用来过滤符合条件的子表，订阅这些子表。where 条件里不能有普通列，只能是tag或tbname，where条件里可以用函数，用来过滤tag，但是不能是聚合函数，因为子表tag值无法做聚合。也可以是常量表达式，比如 2 > 1（订阅全部子表），或者 false（订阅0个子表）
- 返回数据不包含标签。

### 数据库 topic

语法：

```sql
CREATE TOPIC [IF NOT EXISTS] topic_name [with meta] AS DATABASE db_name;
```

通过该语句可创建一个包含数据库所有表数据的订阅

- with meta 参数可选，选择时将返回创建数据库里所有超级表，子表的语句，主要用于taosx做数据库迁移

说明： 超级表订阅和库订阅属于高级订阅模式，容易出错，如确实要使用，请咨询专业人员。

## 删除 topic

如果不再需要订阅数据，可以删除 topic，需要注意：只有当前未在订阅中的 TOPIC 才能被删除。

```sql
/* 删除 topic */
DROP TOPIC [IF EXISTS] topic_name;
```

此时如果该订阅主题上存在 consumer，则此 consumer 会收到一个错误。

## 查看 topic

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

## 查看订阅信息

```sql
SHOW SUBSCRIPTIONS;
```

显示 consumer 与 vgroup 之间的分配关系和消费信息