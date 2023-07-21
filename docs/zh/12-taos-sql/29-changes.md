---
sidebar_label: 语法变更
title: 语法变更
description: "TDengine 3.0 版本的语法变更说明"
---

## SQL 基本元素变更

| # | **元素**  | **<div style={{width: 60}}>差异性</div>** | **说明** |
| - | :------- | :-------- | :------- |
| 1 | VARCHAR | 新增 | BINARY类型的别名。
| 2 | TIMESTAMP字面量 | 新增 | 新增支持 TIMESTAMP 'timestamp format' 语法。
| 3 | _ROWTS伪列 | 新增 | 表示时间戳主键。是_C0伪列的别名。
| 4 | _IROWTS伪列 | 新增 | 用于返回 interp 函数插值结果对应的时间戳列。
| 5 | INFORMATION_SCHEMA | 新增 |	包含各种SCHEMA定义的系统数据库。
| 6 | PERFORMANCE_SCHEMA | 新增 | 包含运行信息的系统数据库。
| 7 | 连续查询 | 废除 | 不再支持连续查询。相关的各种语法和接口废除。
| 8 | 混合运算 | 增强 | 查询中的混合运算（标量运算和矢量运算混合）全面增强，SELECT的各个子句均全面支持符合语法语义的混合运算。
| 9 | 标签运算 | 新增 |在查询中，标签列可以像普通列一样参与各种运算，用于各种子句。
| 10 | 时间线子句和时间函数用于超级表查询 | 增强 |没有PARTITION BY时，超级表的数据会被合并成一条时间线。
| 11 | GEOMETRY | 新增 | 几何类型。

## SQL 语句变更

在 TDengine 中，普通表的数据模型中可使用以下数据类型。

| # | **语句**  | **<div style={{width: 60}}>差异性</div>** | **说明** |
| - | :------- | :-------- | :------- |
| 1 | ALTER ACCOUNT | 废除 | 2.x中为企业版功能，3.0不再支持。语法暂时保留了，执行报“This statement is no longer supported”错误。
| 2 | ALTER ALL DNODES | 新增 | 修改所有DNODE的参数。
| 3 | ALTER DATABASE | 调整	| <p>废除</p><ul><li>QUORUM：写入需要的副本确认数。3.0 版本默认行为是强一致性，且不支持修改为弱一致性。</li><li>BLOCKS：VNODE使用的内存块数。3.0版本使用BUFFER来表示VNODE写入内存池的大小。</li><li>UPDATE：更新操作的支持模式。3.0版本所有数据库都支持部分列更新。</li><li>CACHELAST：缓存最新一行数据的模式。3.0版本用CACHEMODEL代替。</li><li>COMP：3.0版本暂不支持修改。</li></ul><p>新增</p><ul><li>CACHEMODEL：表示是否在内存中缓存子表的最近数据。</li><li>CACHESIZE：表示缓存子表最近数据的内存大小。</li><li>WAL_FSYNC_PERIOD：代替原FSYNC参数。</li><li>WAL_LEVEL：代替原WAL参数。</li><li>WAL_RETENTION_PERIOD：3.0.4.0版本新增，wal文件的额外保留策略，用于数据订阅。</li><li>WAL_RETENTION_SIZE：3.0.4.0版本新增，wal文件的额外保留策略，用于数据订阅。</li></ul><p>调整</p><ul><li>KEEP：3.0版本新增支持带单位的设置方式。</li></ul>
| 4 | ALTER STABLE | 调整 | 废除<ul><li>CHANGE TAG：修改标签列的名称。3.0版本使用RENAME TAG代替。<br/>新增</li><li>RENAME TAG：代替原CHANGE TAG子句。</li><li>COMMENT：修改超级表的注释。</li></ul>
| 5 | ALTER TABLE | 调整 | 废除<ul><li>CHANGE TAG：修改标签列的名称。3.0版本使用RENAME TAG代替。<br/>新增</li><li>RENAME TAG：代替原CHANGE TAG子句。</li><li>COMMENT：修改表的注释。</li><li>TTL：修改表的生命周期。</li></ul>
| 6 | ALTER USER | 调整 | 废除<ul><li>PRIVILEGE：修改用户权限。3.0版本使用GRANT和REVOKE来授予和回收权限。<br/>新增</li><li>ENABLE：启用或停用此用户。</li><li>SYSINFO：修改用户是否可查看系统信息。</li></ul>
| 7 | COMPACT VNODES | 暂不支持 | 整理指定VNODE的数据。3.0.0版本暂不支持。
| 8 | CREATE ACCOUNT | 废除 | 2.x中为企业版功能，3.0不再支持。语法暂时保留了，执行报“This statement is no longer supported”错误。
| 9 | CREATE DATABASE | 调整 | <p>废除</p><ul><li>BLOCKS：VNODE使用的内存块数。3.0版本使用BUFFER来表示VNODE写入内存池的大小。</li><li>CACHE：VNODE使用的内存块的大小。3.0版本使用BUFFER来表示VNODE写入内存池的大小。</li><li>CACHELAST：缓存最新一行数据的模式。3.0版本用CACHEMODEL代替。</li><li>DAYS：数据文件存储数据的时间跨度。3.0版本使用DURATION代替。</li><li>FSYNC：当 WAL 设置为 2 时，执行 fsync 的周期。3.0版本使用WAL_FSYNC_PERIOD代替。</li><li>QUORUM：写入需要的副本确认数。3.0版本使用STRICT来指定强一致还是弱一致。</li><li>UPDATE：更新操作的支持模式。3.0版本所有数据库都支持部分列更新。</li><li>WAL：WAL 级别。3.0版本使用WAL_LEVEL代替。</li></ul><p>新增</p><ul><li>BUFFER：一个 VNODE 写入内存池大小。</li><li>CACHEMODEL：表示是否在内存中缓存子表的最近数据。</li><li>CACHESIZE：表示缓存子表最近数据的内存大小。</li><li>DURATION：代替原DAYS参数。新增支持带单位的设置方式。</li><li>PAGES：一个 VNODE 中元数据存储引擎的缓存页个数。</li><li>PAGESIZE：一个 VNODE 中元数据存储引擎的页大小。</li><li>RETENTIONS：表示数据的聚合周期和保存时长。</li><li>STRICT：表示数据同步的一致性要求。</li><li>SINGLE_STABLE：表示此数据库中是否只可以创建一个超级表。</li><li>VGROUPS：数据库中初始VGROUP的数目。</li><li>WAL_FSYNC_PERIOD：代替原FSYNC参数。</li><li>WAL_LEVEL：代替原WAL参数。</li><li>WAL_RETENTION_PERIOD：wal文件的额外保留策略，用于数据订阅。</li><li>WAL_RETENTION_SIZE：wal文件的额外保留策略，用于数据订阅。</li></ul><p>调整</p><ul><li>KEEP：3.0版本新增支持带单位的设置方式。</li></ul>
| 10 | CREATE DNODE | 调整 | 新增主机名和端口号分开指定语法<ul><li>CREATE DNODE dnode_host_name PORT port_val</li></ul>
| 11 | CREATE INDEX	| 新增 | 创建SMA索引。
| 12 | CREATE MNODE	| 新增 | 创建管理节点。
| 13 | CREATE QNODE	| 新增 | 创建查询节点。
| 14 | CREATE STABLE | 调整	| 新增表参数语法<li>COMMENT：表注释。</li>
| 15 | CREATE STREAM | 新增 | 创建流。
| 16 | CREATE TABLE | 调整 | 新增表参数语法<ul><li>COMMENT：表注释。</li><li>WATERMARK：指定窗口的关闭时间。</li><li>MAX_DELAY：用于控制推送计算结果的最大延迟。</li><li>ROLLUP：指定的聚合函数，提供基于多层级的降采样聚合结果。</li><li>SMA：提供基于数据块的自定义预计算功能。</li><li>TTL：用来指定表的生命周期的参数。</li></ul>
| 17 | CREATE TOPIC | 新增 | 创建订阅主题。
| 18 | DROP ACCOUNT | 废除 | 2.x中为企业版功能，3.0不再支持。语法暂时保留了，执行报“This statement is no longer supported”错误。
| 19 | DROP CONSUMER GROUP | 新增 | 删除消费组。
| 20 | DROP INDEX	| 新增 | 删除索引。
| 21 | DROP MNODE	| 新增 | 创建管理节点。
| 22 | DROP QNODE	| 新增 | 创建查询节点。
| 23 | DROP STREAM | 新增 | 删除流。
| 24 | DROP TABLE | 调整 | 新增批量删除语法
| 25 | DROP TOPIC | 新增 | 删除订阅主题。
| 26 | EXPLAIN | 新增 | 查看查询语句的执行计划。
| 27 | GRANT | 新增 | 授予用户权限。
| 28 | KILL TRANSACTION | 新增 | 终止管理节点的事务。
| 29 | KILL STREAM | 废除 | 终止连续查询。3.0版本不再支持连续查询，而是用更通用的流计算来代替。
| 31 | REVOKE | 新增 | 回收用户权限。
| 32 | SELECT	| 调整 | <ul><li>SELECT关闭隐式结果列，输出列均需要由SELECT子句来指定。</li><li>DISTINCT功能全面支持。2.x版本只支持对标签列去重，并且不可以和JOIN、GROUP BY等子句混用。</li><li>JOIN功能增强。增加支持：JOIN后WHERE条件中有OR条件；JOIN后的多表运算；JOIN后的多表GROUP BY。</li><li>FROM后子查询功能大幅增强。不限制子查询嵌套层数；支持子查询和UNION ALL混合使用；移除其他一些之前版本的语法限制。</li><li>WHERE后可以使用任意的标量表达式。</li><li>GROUP BY功能增强。支持任意标量表达式及其组合的分组。</li><li>SESSION可以用于超级表了。没有PARTITION BY时，超级表的数据会被合并成一条时间线。</li><li>STATE_WINDOW可以用于超级表了。没有PARTITION BY时，超级表的数据会被合并成一条时间线。</li><li>ORDER BY功能大幅增强。不再必须和GROUP BY子句一起使用；不再有排序表达式个数的限制；增加支持NULLS FIRST/LAST语法功能；支持符合语法语义的任意表达式。</li><li>新增PARTITION BY语法。替代原来的GROUP BY tags。</li></ul>
| 33 | SHOW ACCOUNTS | 废除 | 2.x中为企业版功能，3.0不再支持。语法暂时保留了，执行报“This statement is no longer supported”错误。
| 34 | SHOW APPS |新增 | 显示接入集群的应用（客户端）信息。
| 35 | SHOW CONSUMERS	| 新增 | 显示当前数据库下所有活跃的消费者的信息。
| 36 | SHOW DATABASES	| 调整 | 3.0版本只显示数据库名。
| 37 | SHOW FUNCTIONS	| 调整 | 3.0版本只显示自定义函数名。
| 38 | SHOW LICENCE | 新增 | 和SHOW GRANTS 命令等效。
| 39 | SHOW INDEXES | 新增 | 显示已创建的索引。
| 40 | SHOW LOCAL VARIABLES | 新增 | 显示当前客户端配置参数的运行值。
| 41 | SHOW MODULES	| 废除 | 显示当前系统中所安装的组件的信息。
| 42 | SHOW QNODES	| 新增 | 显示当前系统中QNODE的信息。
| 43 | SHOW STABLES	| 调整 | 3.0版本只显示超级表名。
| 44 | SHOW STREAMS	| 调整 | 2.x版本此命令显示系统中已创建的连续查询的信息。3.0版本废除了连续查询，用流代替。此命令显示已创建的流。
| 45 | SHOW SUBSCRIPTIONS | 新增 | 显示当前数据库下的所有的订阅关系
| 46 | SHOW TABLES | 调整 | 3.0版本只显示表名。
| 47 | SHOW TABLE DISTRIBUTED | 新增 | 显示表的数据分布信息。代替2.x版本中的SELECT _block_dist() FROM { tb_name | stb_name }方式。
| 48 | SHOW TOPICS | 新增 | 显示当前数据库下的所有订阅主题。
| 49 | SHOW TRANSACTIONS | 新增 | 显示当前系统中正在执行的事务的信息。
| 50 | SHOW DNODE VARIABLES | 新增 |显示指定DNODE的配置参数。
| 51 | SHOW VNODES | 暂不支持 | 显示当前系统中VNODE的信息。3.0.0版本暂不支持。
| 52 | TRIM DATABASE | 新增 | 删除过期数据，并根据多级存储的配置归整数据。
| 53 | REDISTRIBUTE VGROUP | 新增 | 调整VGROUP中VNODE的分布。
| 54 | BALANCE VGROUP | 新增 | 自动调整VGROUP中VNODE的分布。

## SQL 函数变更

| # | **函数**  | ** <div style={{width: 60}}>差异性</div> ** | **说明** |
| - | :------- | :-------- | :------- |
| 1 | TWA	| 增强 | 可以直接用于超级表了。没有PARTITION BY时，超级表的数据会被合并成一条时间线。
| 2 | IRATE | 增强 | 可以直接用于超级表了。没有PARTITION BY时，超级表的数据会被合并成一条时间线。
| 3 | LEASTSQUARES | 增强 | 可以用于超级表了。
| 4 | ELAPSED | 增强 | 可以直接用于超级表了。没有PARTITION BY时，超级表的数据会被合并成一条时间线。
| 5 | DIFF | 增强 | 可以直接用于超级表了。没有PARTITION BY时，超级表的数据会被合并成一条时间线。
| 6 | DERIVATIVE | 增强 | 可以直接用于超级表了。没有PARTITION BY时，超级表的数据会被合并成一条时间线。
| 7 | CSUM | 增强 | 可以直接用于超级表了。没有PARTITION BY时，超级表的数据会被合并成一条时间线。
| 8 | MAVG | 增强 | 可以直接用于超级表了。没有PARTITION BY时，超级表的数据会被合并成一条时间线。
| 9 | SAMPLE | 增强 | 可以直接用于超级表了。没有PARTITION BY时，超级表的数据会被合并成一条时间线。
| 10 | STATECOUNT | 增强 | 可以直接用于超级表了。没有PARTITION BY时，超级表的数据会被合并成一条时间线。
| 11 | STATEDURATION | 增强 | 可以直接用于超级表了。没有PARTITION BY时，超级表的数据会被合并成一条时间线。
| 12 | TIMETRUNCATE | 增强 | 增加ignore_timezone参数，可选是否使用，默认值为1.


## SCHEMALESS 变更

| # | **元素**  | **<div style={{width: 60}}>差异性</div>** | **说明** |
| - | :------- | :-------- | :------- |
| 1 | 主键ts 变更为 _ts | 变更 | schemaless自动建的列名用 _ 开头，不同于2.x。
