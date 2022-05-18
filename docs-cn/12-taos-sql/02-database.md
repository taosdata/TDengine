---
sidebar_label: 数据库管理
title: 数据库管理
description: "创建、删除数据库，查看、修改数据库参数"
---

## 创建数据库

```
CREATE DATABASE [IF NOT EXISTS] db_name [KEEP keep] [DAYS days] [UPDATE 1];
```

:::info
1. KEEP 是该数据库的数据保留多长天数，缺省是 3650 天(10 年)，数据库会自动删除超过时限的数据；<!-- REPLACE_OPEN_TO_ENTERPRISE__KEEP_PARAM_DESCRIPTION -->
2. UPDATE 标志数据库支持更新相同时间戳数据；（从 2.1.7.0 版本开始此参数支持设为 2，表示允许部分列更新，也即更新数据行时未被设置的列会保留原值。）（从 2.0.8.0 版本开始支持此参数。注意此参数不能通过 `ALTER DATABASE` 指令进行修改。）
   1. UPDATE 设为 0 时，表示不允许更新数据，后发送的相同时间戳的数据会被直接丢弃；
   2. UPDATE 设为 1 时，表示更新全部列数据，即如果更新一个数据行，其中某些列没有提供取值，那么这些列会被设为 NULL；
   3. UPDATE 设为 2 时，表示支持更新部分列数据，即如果更新一个数据行，其中某些列没有提供取值，那么这些列会保持原有数据行中的对应值；
   4. 更多关于 UPDATE 参数的用法，请参考[FAQ](/train-faq/faq)。
3. 数据库名最大长度为 33；
4. 一条 SQL 语句的最大长度为 65480 个字符；
5. 创建数据库时可用的参数有：
   - cache: [Description](/reference/config/#cache)
   - blocks: [Description](/reference/config/#blocks)
   - days: [Description](/reference/config/#days)
   - keep: [Description](/reference/config/#keep)
   - minRows: [Description](/reference/config/#minrows)
   - maxRows: [Description](/reference/config/#maxrows)
   - wal: [Description](/reference/config/#wallevel)
   - fsync: [Description](/reference/config/#fsync)
   - update: [Description](/reference/config/#update)
   - cacheLast: [Description](/reference/config/#cachelast)
   - replica: [Description](/reference/config/#replica)
   - quorum: [Description](/reference/config/#quorum)
   - maxVgroupsPerDb: [Description](/reference/config/#maxvgroupsperdb)
   - comp: [Description](/reference/config/#comp)
   - precision: [Description](reference/config/#precision)
6. 请注意上面列出的所有参数都可以配置在配置文件 `taosd.cfg` 中作为创建数据库时使用的默认配置， `create database` 的参数中明确指定的会覆盖配置文件中的设置。

:::

## 显示系统当前参数

```
SHOW VARIABLES;
```

## 使用数据库

```
USE db_name;
```

使用/切换数据库（在 REST 连接方式下无效）。

## 删除数据库

```
DROP DATABASE [IF EXISTS] db_name;
```

删除数据库。指定 Database 所包含的全部数据表将被删除，谨慎使用！

## 修改数据库参数

```
ALTER DATABASE db_name COMP 2;
```

COMP 参数是指修改数据库文件压缩标志位，缺省值为 2，取值范围为 [0, 2]。0 表示不压缩，1 表示一阶段压缩，2 表示两阶段压缩。

```
ALTER DATABASE db_name REPLICA 2;
```

REPLICA 参数是指修改数据库副本数，取值范围 [1, 3]。在集群中使用，副本数必须小于或等于 DNODE 的数目。

```
ALTER DATABASE db_name KEEP 365;
```

KEEP 参数是指修改数据文件保存的天数，缺省值为 3650，取值范围 [days, 365000]，必须大于或等于 days 参数值。

```
ALTER DATABASE db_name QUORUM 2;
```

QUORUM 参数是指数据写入成功所需要的确认数，取值范围 [1, 2]。对于异步复制，quorum 设为 1，具有 master 角色的虚拟节点自己确认即可。对于同步复制，quorum 设为 2。原则上，Quorum >= 1 并且 Quorum <= replica(副本数)，这个参数在启动一个同步模块实例时需要提供。

```
ALTER DATABASE db_name BLOCKS 100;
```

BLOCKS 参数是每个 VNODE (TSDB) 中有多少 cache 大小的内存块，因此一个 VNODE 的用的内存大小粗略为（cache \* blocks）。取值范围 [3, 1000]。

```
ALTER DATABASE db_name CACHELAST 0;
```

CACHELAST 参数控制是否在内存中缓存子表的最近数据。缺省值为 0，取值范围 [0, 1, 2, 3]。其中 0 表示不缓存，1 表示缓存子表最近一行数据，2 表示缓存子表每一列的最近的非 NULL 值，3 表示同时打开缓存最近行和列功能。（从 2.0.11.0 版本开始支持参数值 [0, 1]，从 2.1.2.0 版本开始支持参数值 [0, 1, 2, 3]。）  
说明：缓存最近行，将显著改善 LAST_ROW 函数的性能表现；缓存每列的最近非 NULL 值，将显著改善无特殊影响（WHERE、ORDER BY、GROUP BY、INTERVAL）下的 LAST 函数的性能表现。

:::tip
以上所有参数修改后都可以用 show databases 来确认是否修改成功。另外，从 2.1.3.0 版本开始，修改这些参数后无需重启服务器即可生效。
:::tip

## 显示系统所有数据库

```
SHOW DATABASES;
```

## 显示一个数据库的创建语句

```
SHOW CREATE DATABASE db_name;
```

常用于数据库迁移。对一个已经存在的数据库，返回其创建语句；在另一个集群中执行该语句，就能得到一个设置完全相同的 Database。

