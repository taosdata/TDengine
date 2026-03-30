# Rollup SMA 功能 - FS

## 1. 背景

1. [原始需求调研 ](https://jira.taosdata.com:18090/pages/viewpage.action?pageId=135108852)
2. [TS-6113](https://jira.taosdata.com:18080/browse/TS-6113) 中能建广西/拾贝云
```plaintext {wrap}
需求描述
    数据库超过指定时间的数据，进行降采样后持久化，超过设定期限的原始数据丢弃
    允许设定多组，例如超过1年1分钟降采样，超过3年5分钟降采样...
    经与 Kaili Xu 确认，目前的设计支持两级降采样持久化，但是：
      - 该特性一年多没有动过了，当时存在bug，是否能发布还未知
      - 该特性是基于流计算实现的，资源消耗大
      - 如果改为 force_window_close 应该能大幅提升性能和降低 CPU 消耗，但问题是：
       不支持逾期重算
       不支持对历史数据重算
期望
    完善该特性，达成可交付状态
    重点关注并解决：
    资源消耗大的问题
    支持历史数据重算
    支持逾期重算
```

## 2. 变更历史

| 日期 | 版本 | 负责人 | 主要修改内容 |
| --- | --- | --- | --- |
| 2025/08/18 | 0.2 | 徐开礼 | 基于线下会议 Simon Guan 的思路修改 |
|  |  |  |  |

## 3. 定义

- Rollup SMA(Small Materialized Aggregration) 简称 RSMA，是一种按照时间范围对用户提供的原始数据自动进行降采样（downsampling）存储的 SMA。适用于原始数据保存时长较短，降采样数据保存时长较长的场景，主要目标是减少总体磁盘空间占用，同时在降采样查询时也能提高查询性能。

## 4. 行为说明

### 4.1 基本逻辑

1. RSMA 降采样计算支持自动触发。通过 DB keep (keep0/keep1/keep2) 参数控制，由`低层级`向`高层级`迁移时自动完成降采样存储。
2. RSMA 降采样计算支持手动触发。适用于`降采样存储的数据更新或删除后需要重算` 以及 `已经迁移至目标存储层级但未进行降采样存储`的场景。
```sql {wrap}
1/2/3 存储层级的数据均支持更新和删除，但是更新和删除不会触发重算。因此，level 2/3 层级存在乱序数据写入时，原有聚合过的窗口内可能存在多条数据；此时进行手动重算会基于聚合数据和乱序数据，对于 avg/first/last 等函数再次聚合的结果可能不准确，对于 min/max/sum 等函数无影响。
```

1. 存在 RSMA 时，可以进行表结构修改、表的创建删除，这些操作在计算和重算时延迟生效。
2. RSMA 计算完成后删除原有数据文件，降采样数据与原始数据时间范围不存在重叠。
3. RSMA 和 S3 迁移之间存在依赖关系，在 S3 首次迁移时，需要进行 RSMA 的计算。
4. RSMA 不影响任何查询行为。因为只是更新了数据文件。如果查询范围跨越了存储层级，有可能同时返回原始数据和降采样数据。

### 4.2 SQL 语法

```sql {wrap}
-- RSMA 表各个存储层级的保存时长，通过 DB KEEP 参数指定，分别为 keep 0,keep 1,keep 2。
CREATE DATABASE [IF NOT EXISTS] db_name KEEP keep0[,keep1[,keep2]]；

-- 创建基于超级表的 RSMA(子表/普通表等其他类型的表不支持)。
CREATE RSMA [IF NOT EXISTS] rsma_name ON [dbname.]table_name FUNCTION([func_name(col_name)[,...]]) INTERVAL(interval1[,interval2]; 

-- 删除 RSMA 
DROP RSMA [IF EXISTS] [db_name.]rsma_name;

-- 修改列的聚合函数(适用于新增列的场景，只允许修改未指定 function 的列。注意：因为未指定聚合函数时，默认聚合函数为 last，在修改聚合函数后，会存在聚合结果的类型不一致。)
ALTER RSMA [IF EXISTS] [db_name.]rsma_name FUNCTION ([func_name(col_name)[,...]])

-- 显示创建语句
SHOW CREATE RSMA [db_name.]rsma_name;

-- 显示所有 RSMA
SHOW [db_name.]RSMAS;
SELECT * FROM information_schema.ins_rsmas;

-- 手动重算 RSMA 在某一时间范围的数据，可指定 vgroups。1）未指定时间范围时，计算 keep 在 2/3 级的所有文件组；2）指定时间范围时，则计算时间范围内包含的整个文件组的。3）如果 rollup 后，未写入新的数据，则不会重复 rollup。4）如果 rollup 时间范围的文件组在 keep 1，则不进行计算，即 rollup 只对 keep 2/3 的文件组生效。
-- 手动重算适用于对不满足多级存储迁移条件的 2/3 级文件组进行计算。需要注意的是：如果需要重算的文件组已经在 s3 上，则重算生成的文件组会重新保存到本地，参照 7.1 节的说明。
ROLLUP DATABASE db_name [start_opt end_opt]
ROLLUP [db_name] VGROUPS IN (vgroup_ids) [start_opt end_opt]

start_opt(A) ::= .   
start_opt(A) ::= START WITH NK_INTEGER(B).
start_opt(A) ::= START WITH NK_STRING(B).
start_opt(A) ::= START WITH TIMESTAMP NK_STRING(B).
end_opt(A) ::= .
end_opt(A) ::= END WITH NK_INTEGER(B).
end_opt(A) ::= END WITH NK_STRING(B).
end_opt(A) ::= END WITH TIMESTAMP NK_STRING(B).


-- 显示所有的 retention 任务。RSMA 计算任务，均通过 retention 线程完成，在 retention 任务中，增加是否为 rsma task 的标识，可区分自动触发和手动触发。
SHOW [db_name.]RETENTIONS; 
SELECT * FROM information_schema.ins_retentions; 
SHOW RETENTION retention_task_id; -- 显示详细信息，包含进度。
KILL RETENTION retention_task_id; -- 终止指定 id 的 RSMA 计算任务。
```

#### 4.2.1 约束规则

1. 要实现 RSMA 迁移时自动降采样计算，DB 的 [KEEP](https://docs.taosdata.com/reference/taos-sql/database/#keep) 参数需满足 keep0 < keep1 < keep 2。如果未配置多级存储，则不会迁移数据，但仍然会在原存储层级进行降采样存储。
2. RSMA 名称 rsma_name 在集群内唯一。
3. RSMA func_name 取值范围为 min, max, sum, avg, first, last(~~ count, spread, cols, interp 不支持)~~。
4. RSMA 每一个非主键列均可指定一个 func_name，但是只能指定一个，非数值类型的列不能指定 sum/avg 等数值计数函数。FUNCTION 参数可为空。未显式指定 func_name  的列默认函数为 last。
5. 非 Primary Key 复合主键列，func_name 仅可指定 first/last，不指定默认函数为 last。
6. RSMA interval 取值范围为 [0, duration] 之间整数，至少一个 非 0。interval 1 取 0，表示多级存储 level 2 存储层级的数据不进行降采样存储；interval 2  取 0，表示多级存储 level 3 存储层级的数据不进行降采样存储。
7. RSMA 表的 interval 都大于 0 时，需要满足 interval1 < interval2 <= duration， 且必须指定单位，单位格式与查询 [interval ](https://docs.taosdata.com/reference/taos-sql/distinguished/#%E7%AA%97%E5%8F%A3%E5%88%87%E5%88%86%E6%9F%A5%E8%AF%A2)相同。
8. 跨文件边界的 interval 计算，即使不考虑 s3，也会增加系统的复杂性、资源消耗和文件碎片化，降低查询读取效率，并且带来的收益较低。因此，约定 DB duration 参数必须能够被 RSMA 表的 interval 整除。
```sql {wrap}
假设 duration 为 1 天（86400s），interval 为 1 至 3600 之间的数字，则可以做为 interval 的有 82 个（1, 2, 3, 4, 5, 6, 8, 9, 10, 12, 15, 16, 18, 20, 24, 25, 27, 30, 32, 36, 40, 45, 48, 50, 54, 60, 64, 72, 75, 80, 90, 96, 100, 108, 120, 128, 135, 144, 150, 160, 180, 192, 200, 216, 225, 240, 270, 288, 300, 320, 360, 384, 400, 432, 450, 480, 540, 576, 600, 640, 675, 720, 800, 864, 900, 960, 1080, 1152, 1200, 1350, 1440, 1600, 1728, 1800, 1920, 2160, 2400, 2700, 2880, 3200, 3456, 3600），基本上不会对用户的需求造成影响。
```

1. 为保证 interval 1/2 窗口聚合结果相对于原始数据的正确性，约定 interval 2 必须为 interval 1 的整数倍(interval 1/2 均为非 0 正整数时)。这样，可以保证 min/max/sum/first/last 结果的正确性；但是，avg 结果正确性仍然无法保证，还可能有误差。

## 5. 性能

- 数据迁移过程，会先进行降采样计算，因此，耗时会比原来时间长。

## 6. 兼容性

无

## 7. 运维

### 7.1 涉及 s3 的 rollup 操作说明

- Rollup SMA 可以认为是一种特殊的 compact 操作。因此，对某一个文件组进行 rollup 之后，会记录 last rollup 时间，同时也会记录 last compact 时间，这两个时间是相同的。与 compact 操作类似，只有 last commit > last rollup 时，才会执行 rollup 操作，否则，会跳过。
- 如果 rollup 时间范围包含的文件组，已经在 s3 上且未执行过 rollup 操作，此时，last rollup 默认值为 0，满足 last rollup < last commit，因此，会执行 rollup 操作。在执行完 rollup 后，新生成的文件组会保存在本地，s3 远端的文件组不再生效。后续再触发 s3 上传时会报错，需要手工删除远端的文件组。该逻辑与 compact 操作是相同的。

## 8. 使用场景

- 原始数据保存时长较短，降采样数据保留时长较长，可大幅减少存储成本。

## 9. 约束和限制

- 仅企业版支持。

## 10. 常见错误和排查

用户操作失败，错误码对照表

| Error code | description | note |
| --- | --- | --- |
|  |  |  |
|  |  |  |

## 11. 可观测性

- RSMA 降采样计算通过 vnode 的 retention 线程完成。计算进度可通过 show retentions 和 show retention {retention_id} 进行查看。

## 12. 安装和卸载

无特殊要求

## 13. 文档

需要修改官网文档

## 14. 参考

- [时序数据库产品 Rollup 功能调研](https://jira.taosdata.com:18090/pages/viewpage.action?pageId=135108852)
- [RSMA 功能](https://taosdata.feishu.cn/wiki/SVf3wv8VriIxRUkKPUzcLGmWnig)
- [TSMA 功能](https://taosdata.feishu.cn/wiki/WpVfwsKjeilOtckp3U2cIaz0nef)
- [流计算新需求与重构 FS](https://taosdata.feishu.cn/wiki/HlKTwwnA2iaF3IkFfY5ctzqknne)
- [Rollup SMA 功能 - FS(V1)](https://taosdata.feishu.cn/wiki/QkFbwBxGXijUATkLCcRcsYhSnoh)

## 15. 附录

无
