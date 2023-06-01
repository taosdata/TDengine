---
sidebar_label: TDengine
title: TDengine 企业版
description: "本文档介绍只在 TDengine 企业版中才具备的功能，以及它们的详细使用手册。"
---

## 简介

本文档介绍只在 TDengine 企业版中才具备的功能，以及它们的详细使用手册。TDengine 企业版除了支持社区版的所有功能之外，为企业用户的数据安全提供了丰富的权限管理功能，为了提升存储效率和查询性能提供了数据重整功能，为接入各种数据源提供了数据接入功能，为了方便企业用户对数据进行备份和管理提供了数据备份和恢复功能，提供了将数据导出成各种业界标准存储格式的数据导出功能，提供了从 2.x 版本将数据迁移到 3.0 版本的数据迁移功能。为了企业用户的易于使用，还提供了丰富的可视化操作、管理和运维工具。

## 联系我们

咨询或购买企业版请联系

电子邮箱: business@taosdata.com

电话： 13520639865/13260223108

## 授权策略

企业版授权由涛思交付团队根据销售合同约定条款，对客户购买的TDengine实例进行授权。授权限制项主要有：时间线(测点)数量、授权截止日期、授权节点数等。

超过合同约定以外的授权，请与涛思商务团队联系。

## 企业版特性

### 权限控制

TDengine 中的权限管理分为用户管理、数据库授权管理以及消息订阅授权管理。

当 TDengine 安装并部署成功后，系统中内置有 "root" 用户。持有默认 "root" 用户密码的系统管理员应该第一时间修改 root 用户的密码，并根据业务需要创建普通用户并为这些用户授予适当的权限。在未授权的情况下，普通用户可以创建DATABASE，并拥有自己创建的 DATABASE 的所有权限，包括删除数据库、修改数据库、查询时序数据和写入时序数据。超级用户可以给普通用户授予其他（即非该用户所创建的） DATABASE 的读写权限，使其可以在这些 DATABASE 上读写数据，但不能对其进行删除和修改数据库的操作。超级用户或者 topic 的创建者也可以给其它用户授予对某个 topic 的订阅权限。

#### 用户管理

用户管理涉及用户的整个生命周期，从创建用户、对用户进行授权、撤销对用户的授权、查看用户信息、直到删除用户。

1.  创建用户

创建用户的操作只能由 root 用户进行，语法如下

```sql
CREATE USER use_name PASS 'password' [SYSINFO {1\|0}]; 
```

说明：

-   use_name 最长为 23 字节。
-   password 最长为 128 字节，合法字符包括"a-zA-Z0-9!?\$%\^&\*()_–+={[}]:;@\~\#\|\<,\>.?/"，不可以出现单双引号、撇号、反斜杠和空格，且不可以为空。
-   SYSINFO 表示用户是否可以查看系统信息。1 表示可以查看，0 表示不可以查看。系统信息包括服务端配置信息、服务端各种节点信息（如 DNODE、QNODE等）、存储相关的信息等。默认为可以查看系统信息。

示例：创建密码为123456且可以查看系统信息的用户 test

```
SQL taos\> create user test pass '123456' sysinfo 1; Query OK, 0 of 0 rows affected (0.001254s)
```

2.  查看用户

查看系统中的用户信息请使用 show users 命令，示例如下

```sql
show users;
```

也可以通过查询系统表 `INFORMATION_SCHEMA.INS_USERS` 获取系统中的用户信息，示例如下

```sql
select * from information_schema.ins_users;  
```

3.  删除用户

删除用户请使用

```sql
DROP USER user_name; 
```

4.  修改用户信息

修改用户信息的命令如下

```sql
ALTER USER user_name alter_user_clause   alter_user_clause: {  PASS 'literal'  \| ENABLE value  \| SYSINFO value } 
```

说明：

-   PASS：修改用户密码。
-   ENABLE：修改用户是否启用。1 表示启用此用户，0 表示禁用此用户。
-   SYSINFO：修改用户是否可查看系统信息。1 表示可以查看系统信息，0 表示不可以查看系统信息。

示例：禁用 test 用户

```sql
alter user test enable 0; Query OK, 0 of 0 rows affected (0.001160s) 
```

#### 数据库访问授权

系统管理员可以根据业务需要对系统中的每个用户针对每个数据库进行特定的授权，以防止业务数据被不恰当的用户读取或修改。对某个用户进行数据库访问授权的语法如下：

```sql
GRANT privileges ON priv_level TO user_name   privileges : {  ALL  \| priv_type [, priv_type] ... }   priv_type : {  READ  \| WRITE }   priv_level : {  dbname.\*  \| \*.\* } 
```

对数据库的访问权限包含读和写两种权限，它们可以被分别授予，也可以被同时授予。

说明

-   priv_level 格式中 "." 之前为数据库名称， "." 之后为表名称，但目前不支持表级别的授权控制，所以 "." 之后必须写为 "\*" ，意为 "." 前所指定的数据库中的所有表
-   "dbname.\*" 意思是名为 "dbname" 的数据库中的所有表
-   "\*.\*" 意思是所有数据库名中的所有表

#### 数据库权限说明

对 root 用户和普通用户的权限的说明如下表

| 用户     | 描述                               | 权限说明                                                                                                                                                                                                                                                                                                                                                                                                                                                                              |
|----------|------------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| 超级用户 | 只有 root 是超级用户               |  DB 外部 所有操作权限，例如user、dnode、udf、qnode等的CRUD DB 权限，包括 创建 删除 更新，例如修改 Option，移动 Vgruop等 读 写 Enable/Disable 用户                                                                                                                                                                                                                                                                                                                                     |
| 普通用户 | 除 root 以外的其它用户均为普通用户 | 在可读的 DB 中，普通用户可以进行读操作 select describe show subscribe 在可写 DB 的内部，用户可以进行写操作： 创建、删除、修改 超级表 创建、删除、修改 子表 创建、删除、修改 topic 写入数据 被限制系统信息时，不可进行如下操作 show dnode、mnode、vgroups、qnode、snode 修改用户包括自身密码 show db时只能看到自己的db，并且不能看到vgroups、副本、cache等信息 无论是否被限制系统信息，都可以 管理 udf 可以创建 DB 自己创建的 DB 具备所有权限 非自己创建的 DB ，参照读、写列表中的权限 |

#### 消息订阅授权

任意用户都可以在自己拥有读权限的数据库上创建 topic。超级用户 root 可以在任意数据库上创建 topic。每个 topic 的订阅权限都可以被独立授权给任何用户，不管该用户是否拥有该数据库的访问权限。删除 topic 只能由 root 用户或者该 topic 的创建者进行。topic 只能由超级用户、topic的创建者或者被显式授予 subscribe 权限的用户订阅。

授予订阅权限的语法如下：

```sql
GRANT privileges ON priv_level TO user_name  privileges : {  ALL  | priv_type [, priv_type] ... }   priv_type : {  SUBSCRIBE }   priv_level : {  topic_name } 
```

#### 查看用户授权

使用下面的命令可以显示一个用户所拥有的授权：

```sql
show user privileges 
```

### 撤销授权

1.  撤销数据库访问的授权

```sql
REVOKE privileges ON priv_level FROM user_name   privileges : {  ALL  \| priv_type [, priv_type] ... }   priv_type : {  READ  \| WRITE }   priv_level : {  dbname.\*  \| \*.\* }  
```

2.  撤销数据订阅的授权

```sql
REVOKE privileges ON priv_level FROM user_name   privileges : {  ALL  \| priv_type [, priv_type] ... }   priv_type : {  SUBSCRIBE }   priv_level : {  topi_name } 
```

### 数据重整

TDengine 面向多种写入场景，而很多写入场景下，TDengine 的存储会导致数据存储的放大或数据文件的空洞等。这一方面影响数据的存储效率，另一方面也会影响查询效率。为了解决上述问题，TDengine 企业版提供了对数据的重整功能，即 DATA COMPACT 功能，将存储的数据文件重新整理，删除文件空洞和无效数据，提高数据的组织度，从而提高存储和查询的效率。

**语法**

```sql
COMPACT DATABASE db_name [start with 'XXXX'] [end with 'YYYY']； 
```

**效果**

-   扫描并压缩指定的 DB 中所有 VGROUP 中 VNODE 的所有数据文件
-   COMPCAT 会删除被删除数据以及被删除的表的数据
-   COMPACT 会合并多个 STT 文件
-   可通过 start with 关键字指定 COMPACT 数据的起始时间
-   可通过 end with 关键字指定 COMPACT 数据的终止时间

**补充说明**

-   COMPACT 为异步，执行 COMPACT 命令后不会等 COMPACT 结束就会返回。如果上一个 COMPACT 没有完成则再发起一个 COMPACT 任务，则会等上一个任务完成后再返回。
-   COMPACT 可能阻塞写入，但不阻塞查询
-   COMPACT 的进度不可观测

### 集群负载再平衡

当多副本集群中的一个或多个节点因为升级或其它原因而重启后，有可能出现集群中各个 dnode 负载不均衡的现象，极端情况下会出现所有 vgroup 的 leader 都位于同一个 dnode 的情况。为了解决这个问题，可以使用下面的命令

```sql
balance vgroup leader;
```

**功能**

让所有的 vgroup 的 leade r在各自的replica节点上均匀分布。这个命令会让 vgroup 强制重新选举，通过重新选举，在选举的过程中，变换 vgroup 的leader，通过这个方式，最终让leader均匀分布。

**注意**

Raft选举本身带有随机性，所以通过选举的重新分布产生的均匀分布也是带有一定的概率，不会完全的均匀。**该命令的副作用是影响查询和写入**，在vgroup重新选举时，从开始选举到选举出新的 leader 这段时间，这 个vgroup 无法写入和查询。选举过程一般在秒级完成。所有的vgroup会依次逐个重新选举。

### 恢复数据节点

当集群中的某个数据节点（dnode）的数据全部丢失或被破坏，比如磁盘损坏或者目录被误删除，可以通过 `restore dnode` 命令来恢复该数据节点上的部分或全部逻辑节点，该功能依赖多副本中的其它副本进行数据复制，所以只在集群中 dnode 数量大于等于 3 且副本数为 3 的情况下能够工作。


```sql
restore dnode <dnode_id>；# 恢复dnode上的mnode，所有vnode和qnode
restore mnode on dnode <dnode_id>；# 恢复dnode上的mnode
restore vnode on dnode <dnode_id> ；# 恢复dnode上的所有vnode
restore qnode on dnode <dnode_id>；# 恢复dnode上的qnode
```

**限制**
- 该功能是基于已有的复制功能的恢复，不是灾难恢复或者备份恢复，所以对于要恢复的 mnode 和 vnode来说，使用该命令的前提是还存在该 mnode 或 vnode 的其它两个副本仍然能够正常工作。
- 该命令不能修复数据目录中的个别文件的损坏或者丢失。例如，如果某个 mnode 或者 vnode 中的个别文件或数据损坏，无法单独恢复损坏的某个文件或者某块数据。此时，可以选择将该  mnode/vnode 的数据全部清空再进行恢复。

### 通过 SQL 命令管理 license

可以通过 `alter dnode` 命令来设置指定数据节点或全部数据节点的授权码; 设置完成后，可以通过 `show dnodes` 命令查看数据节点的授权码; 执行 `drop dnode` 后，需要重新设置。


```sql
alter {dnode <dnode_id>|all dnodes} {'activeCode'|'cActiveCode'} ['value']；# 设置指定数据节点或全部数据节点的授权码。
```
**注意**
- activeCode 为 TDengine cluster 的授权码，其 value 的有效长度为：0 或 108；cActiveCode 为 TDengine connectors 的授权码，其 value 的有效长度为： 0 或 [108,254]。
- 集群的授权信息是所有数据节点授权信息的并集：如果任意一项指标变大，则授权信息在 1 分钟内生效；如果授权指标减少，则在 1 小时内生效。集群授权信息通过 `show grants` 命令查看。