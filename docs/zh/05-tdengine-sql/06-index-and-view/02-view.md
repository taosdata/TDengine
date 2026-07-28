---
title: 视图
sidebar_label: 视图
description: 视图的创建、查询、权限与使用限制
toc_max_heading_level: 4
---

自 `v3.2.1.0` 起，TDengine 企业版提供视图功能，便于简化操作并在用户之间共享查询定义。

视图（View）本质上是存储在数据库中的一条查询语句。非物化视图本身不包含数据，只有在读取视图时才动态执行其定义的查询。创建视图时为其指定名称后，可像普通表一样进行查询等操作。使用规则如下：

- 视图可以嵌套定义和使用；视图与创建时指定的数据库（或当前数据库）绑定。
- 同一数据库内，视图名称不允许重名；视图名与表名也建议不要重名（不强制）。若视图与表同名，写入、查询、授权、回收权限等操作优先使用同名表。

## 语法

### 创建（更新）视图

```sql
CREATE [ OR REPLACE ] VIEW [db_name.]view_name AS query
```

**说明**

- 创建时可指定视图所属数据库 `db_name`；未指定时默认为当前连接所绑定的数据库。
- 查询语句（`query`）中建议显式指定数据库名，以支持跨库视图；未指定时默认使用与视图绑定的数据库（可能不是当前连接的数据库）。

### 查看视图

1. 查看某个数据库下的所有视图：

```sql
SHOW [db_name.]VIEWS;
```

2. 查看视图的创建语句：

```sql
SHOW CREATE VIEW [db_name.]view_name;
```

3. 查看视图列信息：

```sql
DESCRIBE [db_name.]view_name;
```

4. 查看所有视图信息：

```sql
SELECT ... FROM information_schema.ins_views;
```

### 删除视图

```sql
DROP VIEW [IF EXISTS] [db_name.]view_name;
```

## 权限

### 说明

视图权限分为 `READ`、`WRITE`、`ALTER` 三种：查询需要 `READ`，写入需要 `WRITE`，对视图本身的删除与修改需要 `ALTER`。

### 规则

- 视图的创建者和 `root` 用户默认具备全部权限。
- 对其它用户的授权与回收通过 `GRANT` / `REVOKE` 完成，且只能由 `root` 执行。
- 视图权限需单独授权与回收；通过 `db.*` 进行的授权与回收不包含视图权限。
- 视图可嵌套定义与使用，权限校验也会递归进行。
- 为便于分享与使用，引入视图有效用户（即视图创建用户）概念：被授权用户可以使用该有效用户对库、表及嵌套视图的读写权限。注意：视图被 `REPLACE` 后，有效用户也会更新。

具体权限控制如下：

| 序号 | 操作 | 权限要求 |
| -- | --- | --- |
| 1  | `CREATE VIEW`（创建新视图） | 用户对视图所属数据库有 `WRITE` 权限，且对视图引用的库、表、视图有查询权限；若引用对象仍是视图，还需满足下表第 8 条 |
| 2  | `CREATE OR REPLACE VIEW`（覆盖旧视图） | 用户对视图所属数据库有 `WRITE` 权限，且对旧视图有 `ALTER` 权限；并对视图引用的库、表、视图有查询权限；若引用对象仍是视图，还需满足第 8 条 |
| 3  | `DROP VIEW` | 用户对视图有 `ALTER` 权限 |
| 4  | `SHOW VIEWS` | 无 |
| 5  | `SHOW CREATE VIEW` | 无 |
| 6  | `DESCRIBE VIEW` | 无 |
| 7  | 系统表查询 | 无 |
| 8  | `SELECT FROM VIEW` | 操作用户对视图有 `READ` 权限，且操作用户或视图有效用户对视图引用的库、表、视图有 `READ` 权限 |
| 9  | `INSERT INTO VIEW` | 操作用户对视图有 `WRITE` 权限，且操作用户或视图有效用户对视图引用的库、表、视图有 `WRITE` 权限 |
| 10 | `GRANT` / `REVOKE` | 仅 `root` 有权限 |

### 语法

#### 授权

```sql
GRANT privileges ON [db_name.]view_name TO user_name

privileges: {
    ALL
  | priv_type [, priv_type] ...
}

priv_type: {
    READ
  | WRITE
  | ALTER
}
```

#### 回收权限

```sql
REVOKE privileges ON [db_name.]view_name FROM user_name

privileges: {
    ALL
  | priv_type [, priv_type] ...
}

priv_type: {
    READ
  | WRITE
  | ALTER
}
```

## 使用场景

| SQL 查询 | SQL 写入 | STMT 查询 | STMT 写入 | 订阅 | 流式计算 |
| --- | --- | --- | --- | --- | --- |
| 支持 | 暂不支持 | 暂不支持 | 暂不支持 | 支持 | 暂不支持 |

## 示例

创建视图：

```sql
CREATE VIEW view1 AS SELECT _wstart, COUNT(*) FROM table1 INTERVAL(1d);
CREATE VIEW view2 AS SELECT ts, col2 FROM table1;
CREATE VIEW view3 AS SELECT * FROM view1;
```

查询数据：

```sql
SELECT * FROM view1;
```

删除视图：

```sql
DROP VIEW view1;
```
