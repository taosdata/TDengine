---
sidebar_label: 认证与授权
title: 认证与授权
description: TDengine 用户认证与权限控制概述；完整语法见 SQL 用户与权限手册
toc_max_heading_level: 4
---

TDengine 默认仅配置一个 `root` 用户，该用户拥有最高权限。TDengine 支持对系统资源、库、表、视图和主题的访问权限控制。本节介绍用户与权限管理的基本用法与版本差异；完整语法、参数默认值与权限矩阵以 SQL 手册为准：[用户管理](../05-tdengine-sql/07-user-and-privilege/01-user.md)、[权限管理](../05-tdengine-sql/07-user-and-privilege/02-grant.md)。

:::info
细粒度用户与权限管理为企业版能力。社区版在 `3.3.x.y` 及更早版本中可解析部分授权语法但不生效；自 `v3.4.0.0` 起，社区版执行授权语法会报错。
:::

自 `v3.4.0.0` 起，TDengine 企业版通过基于角色的访问控制（RBAC）实现了三权分立，权限体系改动较大。`v3.4.0.0` 至 `v3.4.0.10` 与 `3.3.x.y` 的部分语法不兼容；自 `v3.4.0.11` 起逐步兼容 `3.3.x.y` 语法。为进行更精细的权限管理，建议使用 `v3.4.0.0` 及之后的新语法。

## 版本对比

| 特性 | `3.3.x.y` 及更早 | `v3.4.0.0` 及之后 |
| --- | --- | --- |
| 基础用户管理 | ✓ | ✓ |
| RBAC 角色管理 | ✗ | ✓ |
| 三权分立（SYSDBA / SYSSEC / SYSAUDIT） | ✗ | ✓ |
| 细粒度权限 | ✗ | ✓ |
| 审计库权限 | ✗ | ✓ |
| 表权限 | ✓ | ✓ |
| 行权限 | ✗ | ✓ |
| 列权限 | ✗ | ✓ |

## 用户管理

除本节所列常用操作外，强密码策略、会话限制、IP 黑白名单、登录时间窗、TOTP 与 Token（`CREATE TOKEN` / `SHOW TOKENS`）等，详见 [用户管理](../05-tdengine-sql/07-user-and-privilege/01-user.md)。IP 白名单运维说明另见 [数据安全](./03-data-security.md#ip-白名单)。

### 创建用户

创建用户的语法如下：

```sql
CREATE USER user_name PASS 'password' [SYSINFO {1|0}] [CREATEDB {1|0}];
```

相关参数说明如下：

- `user_name`：用户名最长不超过 23 个字节。
- `password`：密码长度必须为 8 到 255 个字节。密码至少包含大写字母、小写字母、数字、特殊字符中的三类。特殊字符包括 `! @ # $ % ^ & * ( ) - _ + = [ ] { } : ; > < ? | ~ , .`（自 `v3.3.5.0` 起）。可通过在 `taos.cfg` 中添加参数 `enableStrongPassword 0` 关闭此强制要求，或通过如下 SQL 关闭（自 `v3.3.6.0` 起）：

```sql
ALTER ALL DNODES 'EnableStrongPassword' '0';
```

- `sysinfo`：用户是否可以查看系统信息。`1` 表示可以查看，`0` 表示不可以查看。系统信息包括服务端配置信息、各类节点信息（如 dnode、查询节点 qnode 等），以及与存储相关的信息等。默认为可以查看系统信息。
- `createdb`：用户是否可以创建数据库。`1` 表示可以创建，`0` 表示不可以创建。缺省值为 `0`。自企业版 `v3.3.2.0` 起支持。

如下 SQL 可创建密码为 `abc123!@#`、且可以查看系统信息的用户 `test`：

```sql
CREATE USER test PASS 'abc123!@#' SYSINFO 1;
```

### 查看用户

查看系统中的用户信息可使用如下 SQL：

```sql
SHOW USERS;
```

也可以通过查询系统表 `information_schema.ins_users` 获取用户信息，示例如下：

```sql
SELECT * FROM information_schema.ins_users;
```

### 修改用户信息

修改用户信息的 SQL 如下：

```sql
ALTER USER user_name alter_user_clause;
alter_user_clause: {
  PASS 'literal'
  | ENABLE value
  | SYSINFO value
  | CREATEDB value
}
```

相关参数说明如下：

- `pass`：修改用户密码。密码变更后，服务端会在心跳中检测并踢除使用旧密码建立的连接；被踢除的连接上后续请求将返回认证失败（`0x80000357`）。执行密码变更操作的连接本身不受影响。Token 连接不受此机制影响。
- `enable`：是否启用用户。`1` 表示启用，`0` 表示禁用。
- `sysinfo`：用户是否可查看系统信息。`1` 表示可以查看，`0` 表示不可以查看。
- `createdb`：用户是否可创建数据库。`1` 表示可以创建，`0` 表示不可以创建。自企业版 `v3.3.2.0` 起支持。

如下 SQL 禁用 `test` 用户：

```sql
ALTER USER test ENABLE 0;
```

### 删除用户

删除用户的 SQL 如下：

```sql
DROP USER user_name;
```

## 权限管理 - `3.3.x.y` 及之前版本

仅 `root` 用户可以管理用户、节点、vnode、qnode、snode 等系统信息，包括查询、新增、删除和修改。

更完整的权限对象、角色与新版语法见 [权限管理](../05-tdengine-sql/07-user-and-privilege/02-grant.md)。

### 库和表的授权

在 TDengine 中，库和表的权限分为 `read` 和 `write` 两种。这些权限可以单独授予，也可以同时授予用户。

- `read` 权限：拥有 `read` 权限的用户仅能查询库或表中的数据，而无法对数据进行修改或删除。适用于需要访问数据但不需要写入的场景，如数据分析、报表生成等。
- `write` 权限：拥有 `write` 权限的用户可以向库或表中写入数据。适用于数据采集、数据处理等写入场景。如果只拥有 `write` 权限而没有 `read` 权限，则只能写入数据但不能查询数据。

对某个用户进行库和表访问授权的语法如下：

```sql
GRANT privileges ON resources [WITH tag_filter] TO user_name;
privileges: {
  ALL
  | priv_type [, priv_type] ...
}
priv_type: {
  READ
  | WRITE
}
resources: {
  dbname.tbname
  | dbname.*
  | *.*
}
```

相关参数说明如下：

- `resources`：可以访问的库或表。`.` 之前为数据库名称，`.` 之后为表名称。`dbname.tbname` 表示名为 `dbname` 的数据库中的 `tbname` 表，且必须为普通表或超级表。`dbname.*` 表示该数据库中的所有表。`*.*` 表示所有数据库中的所有表。
- `tag_filter`：超级表的过滤条件。

上述 SQL 既可以授权一个库或所有库，也可以授权一个库下的普通表或超级表；还可以通过 `dbname.tbname` 与 `WITH` 子句的组合，授权符合过滤条件的一张超级表下的所有子表。

如下 SQL 将数据库 `power` 的 `read` 权限授权给用户 `test`：

```sql
GRANT READ ON power TO test;
```

如下 SQL 将数据库 `power` 下超级表 `meters` 的全部权限授权给用户 `test`：

```sql
GRANT ALL ON power.meters TO test;
```

如下 SQL 将超级表 `meters` 中标签值 `groupId` 等于 `1` 的子表的写权限相关授权授予用户 `test`：

```sql
GRANT ALL ON power.meters WITH groupId=1 TO test;
```

如果用户被授予了数据库的写权限，那么用户对该数据库下的所有表都有读和写权限。但如果一个数据库只有读权限，甚至没有读权限，表级授权仍可使该用户读取或写入部分表。详细授权组合见 [权限管理](../05-tdengine-sql/07-user-and-privilege/02-grant.md)。

### 视图授权

在 TDengine 中，视图（view）的权限分为 `read`、`write` 和 `alter` 三种，分别决定用户对视图的访问与操作权限。使用规则如下：

- 视图的创建者和 `root` 用户默认具备所有权限，可以查询、写入和修改视图。
- 对其他用户进行授权和回收可通过 `GRANT` 和 `REVOKE` 语句进行，这些操作只能由 `root` 用户执行。
- 视图权限需要单独授权和回收；通过 `db.*` 进行的授权和回收不包含视图权限。
- 视图可以嵌套定义和使用，对视图权限的校验也是递归进行的。

为便于视图的共享和使用，TDengine 引入了视图有效用户（即视图的创建用户）的概念。被授权用户可以使用视图有效用户的库、表及嵌套视图的读写权限。当视图被 `REPLACE` 后，有效用户也会被更新。

视图操作和权限要求的详细对应关系见 [权限管理](../05-tdengine-sql/07-user-and-privilege/02-grant.md)。

视图授权语法如下：

```sql
GRANT privileges ON [db_name.]view_name TO user_name;
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

在数据库 `power` 下将视图 `view_name` 的读权限授权给用户 `test`：

```sql
GRANT READ ON power.view_name TO test;
```

在数据库 `power` 下将视图 `view_name` 的全部权限授权给用户 `test`：

```sql
GRANT ALL ON power.view_name TO test;
```

### 消息订阅授权

为保障订阅信息的安全性，TDengine 可针对消息订阅进行授权。使用前须了解如下规则：

- 任意用户在拥有读权限的数据库上都可以创建主题；`root` 用户可在任意数据库上创建主题。
- 每个主题的订阅权限可以独立授权给任何用户，无论其是否具备该数据库的访问权限。
- 删除主题的操作只有 `root` 用户或该主题的创建者可以执行。
- 只有超级用户、主题的创建者或被显式授权订阅权限的用户才能订阅主题。

消息订阅授权的 SQL 语法如下：

```sql
GRANT privileges ON priv_level TO user_name;
privileges: {
  ALL
  | priv_type [, priv_type] ...
}
priv_type: {
  SUBSCRIBE
}
priv_level: {
  topic_name
}
```

将名为 `topic_name` 的主题授权给用户 `test`：

```sql
GRANT SUBSCRIBE ON topic_name TO test;
```

### 查看授权

当存在多个数据库用户时，可使用如下命令查询某一用户所拥有的全部授权：

```sql
SHOW USER PRIVILEGES;
```

### 撤销授权

由于数据库访问、数据订阅和视图的特性不同，撤销授权的语法也略有差异。

撤销数据库访问授权的 SQL 如下：

```sql
REVOKE privileges ON priv_level [WITH tag_condition] FROM user_name;
privileges: {
  ALL
  | priv_type [, priv_type] ...
}
priv_type: {
  READ
  | WRITE
}
priv_level: {
  dbname.tbname
  | dbname.*
  | *.*
}
```

撤销视图授权的 SQL 如下：

```sql
REVOKE privileges ON [db_name.]view_name FROM user_name;
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

撤销数据订阅授权的 SQL 如下：

```sql
REVOKE privileges ON priv_level FROM user_name;
privileges: {
  ALL
  | priv_type [, priv_type] ...
}
priv_type: {
  SUBSCRIBE
}
priv_level: {
  topic_name
}
```

撤销用户 `test` 对于数据库 `power` 的所有授权：

```sql
REVOKE ALL ON power FROM test;
```

撤销用户 `test` 对于数据库 `power` 中视图 `view_name` 的读授权：

```sql
REVOKE READ ON power.view_name FROM test;
```

撤销用户 `test` 对于消息订阅 `topic_name` 的 `subscribe` 授权：

```sql
REVOKE SUBSCRIBE ON topic_name FROM test;
```

## 权限管理 - `v3.4.0.0` 及之后版本

自 `v3.4.0.0` 起，TDengine 企业版通过基于角色的访问控制（RBAC）实现三权分立，将 `root` 用户的管理权限拆分为 SYSDBA、SYSSEC 和 SYSAUDIT 三种系统管理权限，从而实现权限隔离与制衡。

详细内容请参阅 [权限管理](../05-tdengine-sql/07-user-and-privilege/02-grant.md)。

## 实践建议

1. **最小权限**：按应用拆分账号或 Token，只授予所需库、表与主题权限；避免共享 `root` 凭据。
2. **职责分离**：在 `v3.4.0.0` 及之后的企业版中启用三权分立，避免单一超级账号同时负责建库、授权与审计。
3. **凭据生命周期**：定期轮换密码与 Token；密码变更后，使用旧密码的连接会被踢除（Token 连接除外）。
4. **审计联动**：关键授权与用户变更应纳入审计，详见 [审计与合规](./05-audit-and-compliance.md)。
