---
toc_max_heading_level: 4
title: "视图"
sidebar_label: "视图"
---

从 TDengine 3.2.1.0 开始，TDengine 企业版提供视图功能，便于用户简化操作，提升用户间的分享能力。 

视图（View）本质上是一个存储在数据库中的查询语句。视图（非物化视图）本身不包含数据，只有在从视图读取数据时才动态执行视图所指定的查询语句。我们在创建视图时指定一个名称，然后可以像使用普通表一样对其进行查询等操作。视图的使用需遵循以下规则：
- 视图可以嵌套定义和使用，视图与创建时指定的或当前数据库绑定使用。
- 在同一个数据库内，视图名称不允许重名，视图名跟表名也推荐不重名（不强制）。当出现视图与表名重名时，写入、查询、授权、回收权限等操作优先使用同名表。



## 语法

### 创建（更新）视图

```sql
CREATE [ OR REPLACE ] VIEW [db_name.]view_name AS query
```

说明：
- 创建视图时可以指定视图绑定的数据库名（db_name），未明确指定时默认为当前连接绑定的数据库；
- 查询语句（query）中推荐指定数据库名，支持跨库视图，未指定时默认为与视图绑定的数据库(有可能非当前连接指定的数据库)；

### 查看视图
1. 查看某个数据库下的所有视图
  ```sql
  SHOW [db_name.]VIEWS;
  ```

2. 查看视图的创建语句
  ```sql
  SHOW CREATE VIEW [db_name.]view_name;
  ```

3. 查看视图列信息
  ```sql
  DESCRIBE [db_name.]view_name;
  ```

4. 查看所有视图信息
  ```sql
  SELECT ... FROM information_schema.ins_views;
  ```

### 删除视图
```sql
DROP VIEW [IF EXISTS] [db_name.]view_name;
```

## 权限

### 说明
视图的权限分为 READ、WRITE、ALTER 三种，查询操作需要具备 READ 权限，写入操作需要具备 WRITE 权限，对视图本身的删改操作需要具备 ALTER 权限。

### 规则
- 视图的创建者和 root 用户默认具备所有权限。
- 对其他用户进行授权与回收权限可以通过 GRANT 和 REVOKE 语句进行，该操作只能由 root 用户进行。
- 视图权限需单独授权与回收，通过db.*进行的授权与回收不含视图权限。
- 视图可以嵌套定义与使用，同理对视图权限的校验也是递归进行的。
- 为了方便视图的分享与使用，引入视图有效用户（即视图的创建用户）的概念，被授权用户可以使用视图有效用户的库、表及嵌套视图的读写权限。注：视图被 REPLACE 后有效用户也会被更新。

具体相关权限控制细则如下表所示：

| 序号 | 操作                                    | 权限要求                                                                                                                                                    |
| ---- | --------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------- |
| 1    | CREATE VIEW <br/>(创建新用户)            | 用户对视图所属数据库有 WRITE 权限<br/>且<br/> 用户对视图的目标库、表、视图有查询权限，若查询中的对象是视图需满足当前表中第8条规则                             |
| 2    | CREATE OR REPLACE VIEW <br/>(覆盖旧视图) | 用户对视图所属数据库有 WRITE 权限 且 对旧有视图有 ALTER 权限 <br/>且<br/> 用户对视图的目标库、表、视图有查询权限，若查询中的对象是视图需满足当前表中第8条规则 |
| 3    | DROP VIEW                               | 用户对视图有 ALTER 权限                                                                                                                                     |
| 4    | SHOW VIEWS                              | 无                                                                                                                                                          |
| 5    | SHOW CREATE VIEW                        | 无                                                                                                                                                          |
| 6    | DESCRIBE VIEW                           | 无                                                                                                                                                          |
| 7    | 系统表查询                              | 无                                                                                                                                                          |
| 8    | SELECT FROM VIEW                        | 操作用户对视图有 READ 权限  且 操作用户或视图有效用户对视图的目标库、表、视图有 READ 权限                                                                   |
| 9    | INSERT INTO VIEW                        | 操作用户对视图有 WRITE 权限  且 操作用户或视图有效用户对视图的目标库、表、视图有 WRITE 权限                                                                 |
| 10   | GRANT/REVOKE                            | 只有 root 用户有权限                                                                                                                                        |

### 语法

#### 授权

```sql
GRANT privileges ON [db_name.]view_name TO user_name
privileges: {
    ALL,
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
    ALL,
  | priv_type [, priv_type] ...
}
priv_type: {
    READ
  | WRITE
  | ALTER
}
```

## 使用场景

| SQL 查询 | SQL 写入 | STMT 查询 | STMT 写入 | 订阅 | 流计算   |
| -------- | -------- | --------- | --------- | ---- | -------- |
| 支持     | 暂不支持 | 暂不支持  | 暂不支持  | 支持 | 暂不支持 |


## 举例

- 创建视图
  
  ```sql
  CREATE VIEW view1 AS SELECT _wstart, count(*) FROM table1 INTERVAL(1d);
  CREATE VIEW view2 AS SELECT ts, col2 FROM table1;
  CREATE VIEW view3 AS SELECT * from view1;
  ```
- 查询数据
  
  ```sql
  SELECT * from view1;
  ```
- 删除视图
  
  ```sql
  DROP VIEW view1;
  ```
