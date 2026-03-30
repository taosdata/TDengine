# keepTimeOffset

## 1. 背景

keepTimeOffset 参数在 2.6 版本生效，未进入 3.0 版本
- jira：
  TS-3239

- 2.6 版本                                                                                                                                           参数说明：

  | 名称 | 内部配置 | 适用范围 | 含义 | 单位 | 缺省值 | 设计取值范围 | 代码中取值范围 |
| --- | --- | --- | --- | --- | --- | --- | --- |
| keepTimeOffset | No | 仅服务端适用 | 多级存储中，将数据迁移的时间点延迟 | 小时 | 0 | [0, 23] | [-23, 23] |

  为 Dnode 级别参数
  可以在配置文件中设置
  可通过 Alter 操作重新设置，动态修改后，重启 taosd keepTimeOffset 值会变为 配置文件设置值。
  ```sql
  alter dnode 1 keepTimeOffset 12;
  ```

## 2. 疑问点

### 2.1 为什么需要该参数？

- 能否定时触发迁移？
- 能否触发迁移并指定迁移动作时间？
A：当前伴随 **写入操作 自动 **触发迁移

### 2.2 为何参数范围为[-23, 23]？

自身逻辑上 限定在触发一天之内执行迁移
A：应调整为 [0, 23]，并无特殊要求

### 2.3 为什么是 Dnode 级别参数

A：未将其保存至 Database 元数据中，仅做了快速实现

### 2.4 alter 设置后重启失效是否符合预期？

A：设计如此。alter 操作并不修改 cfg

## 3.x 版本实现

目前保留 2.6 版本行为，平行移植至 3.x 版本

| 名称 | 内部配置 | 适用范围 | 含义 | 单位 | 缺省值 | 取值范围 |
| --- | --- | --- | --- | --- | --- | --- |
| keepTimeOffset | No | 仅服务端适用 | 多级存储中，延迟数据迁移的时间点 | 小时 | 0 | [0, 23] |

为 Dnode 级别参数
可以在配置文件中设置
可通过 alter 操作重新设置，动态修改后，重启 taosd `keepTimeOffset` 值会变为 配置文件设置值。设置操作不区分大小写
```sql
alter dnode 1 'keepTimeOffset' '12';
```

## 3. 迁移功能简述

### 3.1 迁移判定

我们以 `1970-01-01 0:00:00 UTC+0` 为起始点，以建库参数 `duration` 为单位，切分数据文件。
每个数据文件中保存 `(timepoint, timepoint+duration)` 之间的数据。
若某文件的 `timepoint` 满足，`now > timepoint + keep[i] + keeptimeoffset + duration`，则该文件可以被迁移。
<callout emoji="bulb" background-color="light-orange" border-color="light-orange">
**FAQ: 为什么已经满足了迁移判断但并没执行迁移操作？**
迁移执行需被触发，在 worst case 中需额外等待 `trimVDbIntervalSec`。或可通过 SQL 命令， 手动立即触发。
</callout>

### 3.2 迁移执行

1. 迁移操作由 Mnode 定期触发，通过 `trimVDbIntervalSec` (def: 1h) 参数控制。
2. 或由用户触发
```cpp {wrap}
trim database db;
```

## 4. 实例

### 4.1 例1

```sql {wrap}
taos> show create database db\G;
*************************** 1.row ***************************
       Database: db
Create Database: CREATE DATABASE `db` BUFFER 256 CACHESIZE 1 CACHEMODEL 'none' COMP 2 DURATION 60m WAL_FSYNC_PERIOD 3000 MAXROWS 4096 MINROWS 100 STT_TRIGGER 1 KEEP 1440m,1560m,2880m PAGES 256 PAGESIZE 4 PRECISION 'ms' REPLICA 1 WAL_LEVEL 1 VGROUPS 2 SINGLE_STABLE 0 TABLE_PREFIX 0 TABLE_SUFFIX 0 TSDB_PAGESIZE 4 WAL_RETENTION_PERIOD 0 WAL_RETENTION_SIZE 0


root@u1-51 ~ $ grep -rn keepTimeOffset /root/TDinternal/sim/dnode2/cfg/taos.cfg
31:keepTimeOffset 10
```

```sql
FileTime := 2023-07-15 16:00:00 UTC+8
Keep0 := 1440m
Keep1 := 1560m
Keep2 := 2880m
Duration := 60m
KeepTimeOffset := 10h

TimeZeroUTC0 := 1970-01-01 0:00:00 UTC+0
FileTimeUTC0 := 2023-07-15 8:00:00 UTC+0
FileTimeUTC0ModDurationVal := (FileTimeUTC0 - TimeZeroUTC0) / Duration
FileTimeUTC0ModDuration := 2023-07-15 8:00:00 UTC+0

ToLevel1Time := FileTimeUTC0ModDuration + Keep0 + KeepTimeOffset + Duration
ToLevel2Time := FileTimeUTC0ModDuration + Keep1 + KeepTimeOffset + Duration
ToDropTime   := FileTimeUTC0ModDuration + Keep2 + KeepTimeOffset + Duration

ToLevel1TimeValue := 2023-07-17 03:00:00 UTC+8
ToLevel2TimeValue := 2023-07-17 05:00:00 UTC+8
ToDropTimeValue   := 2023-07-18 03:00:00 UTC+8
```

以上设置的 db 中的 `2023-07-15 16:00:00 UTC+8` 数据所在的文件。
最早将在 `2023-07-17 03:00:00 UTC+8` 从第零层迁移至第一层
最早将在 `2023-07-17 05:00:00 UTC+8` 从第一层迁移至第二层
最早将在 `2023-07-18 03:00:00 UTC+8` 从第二层中删除

### 4.2 例2

```cpp {wrap}
taos> show create database db\G;
*************************** 1.row ***************************
       Database: db
Create Database: CREATE DATABASE `db` BUFFER 256 CACHESIZE 1 CACHEMODEL 'none' COMP 2 DURATION 1440m WAL_FSYNC_PERIOD 3000 MAXROWS 4096 MINROWS 100 STT_TRIGGER 1 KEEP 1440m,1560m,2880m PAGES 256 PAGESIZE 4 PRECISION 'ms' REPLICA 1 WAL_LEVEL 1 VGROUPS 2 SINGLE_STABLE 0 TABLE_PREFIX 0 TABLE_SUFFIX 0 TSDB_PAGESIZE 4 WAL_RETENTION_PERIOD 0 WAL_RETENTION_SIZE 0


root@u1-51 ~ $ grep -rn keepTimeOffset /root/TDinternal/sim/dnode2/cfg/taos.cfg
31:keepTimeOffset 10
```

```sql
FileTime := 2023-07-15 16:00:00 UTC+8
Keep0 := 1440m
Keep1 := 1560m
Keep2 := 2880m
Duration := 1440m
KeepTimeOffset := 10h

TimeZeroUTC0 := 1970-01-01 0:00:00 UTC+0
FileTimeUTC0 := 2023-07-15 8:00:00 UTC+0
FileTimeUTC0ModDurationVal := (FileTimeUTC0 - TimeZeroUTC0) / Duration
FileTimeUTC0ModDuration := 2023-07-15 0:00:00 UTC+0

ToLevel1Time := FileTimeUTC0ModDuration + Keep0 + KeepTimeOffset + Duration
ToLevel2Time := FileTimeUTC0ModDuration + Keep1 + KeepTimeOffset + Duration
ToDropTime   := FileTimeUTC0ModDuration + Keep2 + KeepTimeOffset + Duration

ToLevel1TimeValue := 2023-07-17 18:00:00 UTC+8
ToLevel2TimeValue := 2023-07-17 20:00:00 UTC+8
ToDropTimeValue   := 2023-07-18 18:00:00 UTC+8
```

以上设置的 db 中的 `2023-07-15 16:00:00 UTC+8` 数据所在的文件。
最早将在 `2023-07-17 18:00:00 UTC+8` 从第零层迁移至第一层
最早将在 `2023-07-17 20:00:00 UTC+8` 从第一层迁移至第二层
最早将在 `2023-07-18 18:00:00 UTC+8` 从第二层中删除

## 5. 未来可能的修改

- [ ] 参数调整至更为直观的“迁移时间”
- [x] 考虑修改为 Database 级别参数。将参数保存至 Database 元数据中，移出 Dnode 配置文件
- [ ] 异步定时触发，与写入流程脱钩
