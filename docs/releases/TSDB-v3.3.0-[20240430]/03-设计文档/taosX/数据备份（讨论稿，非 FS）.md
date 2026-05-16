# 数据备份（讨论稿，非 FS）

## 1. 背景

### 1.1 目前的方案

目前方案是一个演示版本，还没有在生产环境实际应用。

#### 1.1.1 实现方式：

基于数据库订阅，会将创建的订阅topic 相关信息记录在备份目录下。 
```toml
created_at = "2024-03-13T08:00:00.388967451+08:00"
last_modified = "2024-03-15T08:00:00.278026229+08:00"
group_id = "x4bff8c42889"
client_id = "taosx"

[[topics]]
name = "zachary"
database = "zachary"
vgroups = 2
database_sql = "CREATE DATABASE `zachary` BUFFER 32 CACHESIZE 1 CACHEMODEL 'none' COMP 2 DURATION 72000m WAL_FSYNC_PERIOD 3000 MAXROWS 4096 MINROW    S 100 STT_TRIGGER 2 KEEP 5256000m,5256000m,5256000m PAGES 256 PAGESIZE 4 PRECISION 'ms' REPLICA 1 WAL_LEVEL 1 VGROUPS 2 SINGLE_STABLE 0 TABLE_PREF    IX 0 TABLE_SUFFIX 0 TSDB_PAGESIZE 4 WAL_RETENTION_PERIOD 3600 WAL_RETENTION_SIZE 0 KEEP_TIME_OFFSET 0"
topic_type = "database-with-meta"
```

按配置的周期，定时执行备份程序，订阅数据，将订阅到的数据写入备份文件， 从上次 offset 增量备份数据到最新的 offset。

数据恢复时，按照备份时间顺序依次恢复备份文件到 taosX 所连接的 TDengine 实例。

#### 1.1.2 风险点

1. 首次备份，如果数据量大(T级别)，备份时间长，对 taosX 稳定性要求很高。备份过程无法追踪，出现问题难以定位。
2. 对磁盘空间要求很大，有没有磁盘写满保护机制？
3. 恢复只能恢复到当前实例，而且只能是全量恢复，不可用。
4. 目前最小备份周期是一天

### 1.2 现有备份方案

#### 1.2.1 企业版备份方案

方案：
1. 利用 TDengine replica 能力：可以做到同机房备份能力
2. 利用 taosX 数据同步，在两个数据库实例间做数据同步，可以做到异地备份
缺点：
如果数据有误删，并且已经同步到目标实例，则无法恢复。

#### 1.2.2 云服务备份方案

方案：
1. 利用云服务的磁盘镜像功能，直接将磁盘做 snapshot，每天打一个 snapshot，做到按天级别的

缺点：
备份周期粒度太粗，一旦磁盘损坏，用户会丢失当天数据。

### 1.3 讨论点

1. 确定产品目标（下面的哪一个？）
   - 只是作为我们产品完善度的一个体现，可演示即可
   - 做成 运维的一个技术解决方案
2. 产品需求
  1. 
1. 具体解决方案选型
   - 磁盘快照(全量）+ 增量备份


## 2. 使用场景

### 2.1 本地数据备份

要求：安全性，低成本。
具体场景：数据磁盘损坏。
单机环境考虑数据的安全性，最小成本应对磁盘损坏的风险，复制一份数据到另一台备份服务器上。
备份服务器仅需要安装 taosX + explorer 即可。
具体恢复步骤：
1. 重建 TDengine 实例，endpoint 需要和损坏的实例保持一致；
2. 登录备份服务器上的 taos-explorer，选择对应的备份集，选择全量恢复。

### 2.2 异地灾备

要求：安全性，数据最小。
具体场景：火灾等不可抗力的物理因素，造成机房服务器物理设备团灭。
需要异地机房部署 taosX + explorer。

### 2.3 误操作

#### 2.3.1 误删子表数据

恢复步骤：
1. 新建数据库实例 B
2. 确定误删的时间范围
3. 在实例 B 上尝试恢复时间范围的数据
4. 使用 taosX，建立数据迁移 data in, 将实例 B 上的数据 对应的子表数据 精准恢复到目标库上。

#### 2.3.2 误删子表

恢复步骤：
1. 新建数据库实例 B
2. 在实例 B 上恢复子表所在数据库的数据
3. 使用 taosX，建立数据迁移 data in, 将实例 B 上的数据 对应的子表数据 精准恢复到目标库上。

#### 2.3.3 误删超级表

恢复步骤：
1. 新建数据库实例 B
2. 在实例 B 上恢复超级表所在数据库的数据
3. 使用 taosX，建立数据迁移 data in, 将实例 B 上的数据 对应的子表数据 精准恢复到目标库上。


#### 2.3.4 误删数据库


## 3. 竞品比对

| 对比项 | TDengine | influxDB | timescaleDB |
| --- | --- | --- | --- |
| 方案 | 基于 taosX | 命令行工具 pg_dump pg_restore | 命令行工具 influxd backup <path> Influxd restore |
| 备份 meta 数据 |  | 支持 |  |
| 备份 DB 数据 |  | 不支持 |  |
|  |  |  |  |




两种数据可以备份，一种是元数据meta，一种是db数据。

### 3.1 备份元数据

元数据包含系统状态的内部信息，包括用户信息、数据库/分片元数据、CQs、RPs和订阅等。
```bash
influxd backup <path-to-backup>
influxd restore -metadir <path-to-meta-or-data-directory> <path-to-backup>
```

### 3.2 备份 DB 数据
