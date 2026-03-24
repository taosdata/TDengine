# Rollup SMA 功能 - FS(V1)

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
| 2025/08/15 | 0.1 | 徐开礼 | 初稿 |
|  |  |  |  |

## 3. 定义

- Rollup SMA(Small Materialized Aggregration) 简称 RSMA，是一种按照时间范围对用户提供的原始数据自动进行降采样（downsampling）存储的 SMA。适用于原始数据保存时长较短，降采样数据保存时长较长的场景，主要目标是减少总体磁盘空间占用，同时在降采样查询时也能提高查询性能。

## 4. 行为说明

### 4.1 RSMA 存储

#### 4.1.1 基本逻辑

1. RSMA 最多支持 3 个存储层级（retention level）， level 1 存储原始数据，level 2/3 存储降采样数据；每一层级都可以设置保存时长（keep），level 2/3 可以设置降采样窗口大小（interval）。
2. RSMA 以子表为单位生成和保存 SMA，结果存储在与原始数据表相同 vnode 中，并共用原始数据表的 schema。

#### 4.1.2 建库语法

##### 4.1.2.1 创建 RSMA 数据库

```sql {wrap}
CREATE DATABASE [IF NOT EXISTS] db_name RETENTIONS -:keep1[,interval2:keep2[,interval3:keep3]];
```

##### 4.1.2.2 参数说明

| **参数名称** | **说明** |
| --- | --- |
| RETENTIONS | `RETENTIONS -:keep1[,interval2:keep2[,interval3:keep3]]` - 在 create database 时，通过 retentions 参数，指定数据的降采样周期 ( interval ) 和保存时长 ( keep )。其中 level 1 为原始数据, 其 interval 为占位符 "-"；level 2/3 为聚合数据。 - 如果用户写入数据时间戳小于(当前时间 - level1 的 keep 值)，则写入报错。 - 时间单位不支持 "月" 和 "年"; interval 小于对应的 keep; 高层级的 interval `大于`低层级的 interval，高层级的 keep `大于等于` 低层级的 keep; - Interval 支持的单位包括：s, m, h, d, w，取值范围：[0, 对应层级的 keep 值) - Keep 支持的单位包括：m, h, d, 取值范围：非纳秒库 [1d, 1000*365d] ，纳秒库 [1d, 292*365d] - 每个存储层级的 interval，是基于 GMT 时间 1970 年 01 月 01 日 00 时 00 分 00 秒开始切分的时间窗口。 - 最多支持 3 个 存储层级。 - RETENTIONS 参数的存储时长（keep 1/2/3）与 DB 的 KEEP 参数（keep_0/1/2） 共同作用生效。按照多级存储的 keep 值定义，每一个存储层级 L 的实际 keep 值为：min(keep{L},keep_0),min(keep{L},keep_1),min(keep{L},keep_2)，并且满足 keep{L} <= keep_2，否则建库时报错。 - 对于指定了 retentions 的数据库，可以创建包含或不包含 ROLLUP 参数的超级表。 - 该参数不支持 alter。 |

##### 4.1.2.3 示例

```sql {wrap}
create database if not exists db0 retentions -:7d,1m:30d,15m:365d; -- db0 为 RSMA 数据库 ，一共有 3 个存储层级。原始数据 keep 为 7 天；level 2 数据 interval 为 1 分钟，keep 为 30 天；level 3 数据 interval 为 15 分钟，keep 为 365 天。
```

#### 4.1.3 建表语法

##### 4.1.3.1 创建 RSMA 超级表

- 参考：https://docs.taosdata.com/reference/taos-sql/stable
```sql {wrap}
CREATE TABLE|STABLE [IF NOT EXISTS] [db_name].tb_name (create_definition [, create_definition] ...) TAGS (create_definition [, create_definition] ...) ROLLUP(func_name(col_name)[,func_name(col_name)]) [table_options]

create_definition:
    col_name column_definition
 
column_definition:
    type_name [COMPOSITE KEY] [ENCODE 'encode_type'] [COMPRESS 'compress_type'] [LEVEL 'level_type']

table_options:
    table_option ...

table_option: {
   INTERVAL(interval2[,interval3])
  | WATERMARK watermark2[,watermark3]
  | MAX_DELAY delay2[,delay3]
  | EXPIRED_TIME(exp_time2[,exp_time3])
  | IGNORE_DISORDER
  | DELETE_RECALC
  | FORCE_OUTPUT
  | IGNORE_NODATA_TRIGGER
}
```

##### 4.1.3.2 参数说明

- RSMA 聚合数据的生成是通过 `流计算` 完成的，因此，部分[参数](https://taosdata.feishu.cn/wiki/HlKTwwnA2iaF3IkFfY5ctzqknne#share-VA66dQRjPocNyoxE0lLczQSBnJe)和[术语](https://taosdata.feishu.cn/wiki/HlKTwwnA2iaF3IkFfY5ctzqknne#share-IwgZdFn4IoKgQ6xSaDsctPSTngc)定义均源自 [流计算 FS](https://taosdata.feishu.cn/wiki/HlKTwwnA2iaF3IkFfY5ctzqknne) 中的定义。

| **参数名称** | **说明** |
| --- | --- |
| ROLLUP | `ROLLUP(func_name(col_name)[,func_name(col_name)])` - 创建 RSMA 超级表的必选参数，用于指定列的计算函数 func_name。 - 每个列只能指定一个计算函数，不同列可以指定不同的计算函数。func_name 的取值范围为 avg, sum, min, max, last, first，非数值类型的列只能指定 last, first。主键列不可以指定计算函数。因为聚合数据与原始数据共用 schema，所以，聚合数据的列数与原始数据相同。对于在 ROLLUP 中未指定 func_name 的非主键列，数值类型默认取 avg，非数值类型默认取 first。 - level 1 的原始数据和 level 2/3 的降采样数据采用相同的表 schema。 - 只有指定了 RETENTIONS 参数的数据库才允许使用 ROLLUP 参数。 - 只支持在超级表上使用 ROLLUP 参数， 子表/普通表/虚拟表/视图暂不支持。 - 带有 ROLLUP 参数的超级表，是否支持删除列？是否支持增加列？ - 该参数不支持 alter。 **备注：** - level 2/3 的聚合数据在 `存储/查询` 时，与 level 1 共用表的 schema。因此，针对 sum 类型的计算，在存储时可能溢出，导致结果不正确。在用户层设计表的 schema 时，将该问题考虑进去。 |
| INTERVAL | `INTERVAL(interval2[,interval3])` - 可选参数，表示超级表 retention level 2/3 的窗口大小。如果不指定，默认使用 DB RETENTIONS 参数中的 interval；如果指定了，则完全覆盖 DB RETENTIONS 参数中的 interval，不与 RETENTIONS 中的 interval 取并集。 - Interval 参数值的个数，必须小于等于 DB RETENTIONS 参数值的个数。 - 只有包含 ROLLUP 参数的超级表，才可以指定 INTERVAL。 |
| [WATERMARK](https://taosdata.feishu.cn/wiki/HlKTwwnA2iaF3IkFfY5ctzqknne#share-HNjmd05VLoLO48xsi7VclsTwnRg) | `WATERMARK watermark2[,watermark3]` - 可选参数，源自流计算，表示 retention level 2/3 的数据乱序的容忍时长，超过该时长的数据会被当做乱序数据。默认单位毫秒，取值范围 [0, 900000]，未指定时默认值为 0。 - 只有指定了 ROLLUP 参数的表才允许使用该参数。 |
| [MAX_DELAY](https://taosdata.feishu.cn/wiki/HlKTwwnA2iaF3IkFfY5ctzqknne#share-WOnndUOERohlCUxWA65cisqvnrd) | `MAX_DELAY delay2[,delay3]` - 可选参数，源自流计算，指定窗口未关闭时的最长触发等待时长（处理时间），从窗口开启时每经过该时间段且窗口仍未关闭时产生触发。`delay`为等待时长，支持的时间单位包括：秒(s)、分(m)、小时(h)、天(d)，最小允许的值为 3 秒，误差范围在 1 秒以内，特别的是当计算时长超过`delay_time`时忽略期间的`MAX_DELAY`触发。 - 取值范围 [3, INT32_MAX]，默认单位是秒，默认值为 interval 的值。 - 只有指定了 ROLLUP 参数的表才允许使用该参数。 |
| [EXPIRE_TIME](https://taosdata.feishu.cn/wiki/HlKTwwnA2iaF3IkFfY5ctzqknne#share-AX8XdCcDNoCNibxYXtfcTBhBnib) | `EXPIRED_TIME(exp_time2[,exp_time3])` - 可选参数，源自流计算，指定过期数据间隔并忽略过期数据，未指定时无过期数据，如果业务不需要感知超过一定时间范围的数据写入或更新时可以指定。`exp_time` 为过期时间间隔，支持的时间单位包括：毫秒(a)、秒(s)、分(m)、小时(h)、天(d)。 |
| [IGNORE_DISORDER](https://taosdata.feishu.cn/wiki/HlKTwwnA2iaF3IkFfY5ctzqknne#share-S0tWdIPIcozkF9xZTIScSJJInNb) | `IGNORE_DISORDER` - 可选参数，源自流计算，指定忽略触发表的乱序数据，未指定时不忽略乱序数据，对于业务非常注重计算或通知的时效性、触发表乱序数据不影响计算结果等场景可以指定。乱序数据既包括新的乱序数据的写入，也包括对已写入数据的更新操作。 |
| [DELETE_RECALC](https://taosdata.feishu.cn/wiki/HlKTwwnA2iaF3IkFfY5ctzqknne#share-TkBWd8YEnoF16Xx5aldcaJoznFd) | `DELETE_RECALC` - 可选参数，源自流计算，指定触发表的数据删除（包含触发子表被删除场景）需要自动重新计算。未指定时忽略数据删除，只有触发表数据删除会影响计算结果的场景才需要指定。 |
| [FORCE_OUTPUT](https://taosdata.feishu.cn/wiki/HlKTwwnA2iaF3IkFfY5ctzqknne#share-Ez6VdIEU5oX7bRxqW6AchSaUnsd) | `FORCE_OUTPUT` - 可选参数，源自流计算，指定计算结果强制输出选项，当某次触发没有计算结果时将强制输出一行数据，除常量外（含常量对待列）其他列的值都为 NULL。 |
| [IGNORE_NODATA_TRIGGER](https://taosdata.feishu.cn/wiki/HlKTwwnA2iaF3IkFfY5ctzqknne#share-ARN2dLXl1o4QWvxn4FmcqpDXnMf) | `IGNORE_NODATA_TRIGGER` - 可选参数，源自流计算，指定忽略触发表无输入数据时的触发。如果窗口内触发表没有数据则忽略该次触发。未指定时不忽略无输入数据时的触发。 |

##### 4.1.3.3 示例

```sql {wrap}
1）create stable if not exists db0.stb0 (ts timestamp, c0 int, c1 varchar(10), c2 bool, c3 float) tags(t0 int, t1 varchar(10) rollup(avg(c0),last(c1),sum(c3)); -- stb0 为 RSMA 超级表；c0 列的聚合函数为 avg，c1 列的聚合函数为 last，c2 列的聚合函数为 first，c3 列的聚合函数为 sum；聚合数据的 interval 和 keep 复用 db0 retentions 参数； watermark 取默认值 0，表示窗口关闭后的数据均为乱序；max_delay 取默认值 0，表示窗口到期未关闭时也不会触发计算；expire_time 取默认值 0，表示数据均不会过期(小于 -keep1 的数据除外)；ignore_disorder 未指定，表示乱序数据会触发重新计算；delete_recalc 未指定，表示数据删除不会触发重新计算; force_output 未指定，表示无计算结果则对应的窗口无数据；ignore_nodata_trigger 未指定，表示窗口内无数据也会触发计算。

2）create stable if not exists db0.stb1 (ts timestamp, c0 int, c1 varchar(10), c2 bool, c3 float) tags(t0 int, t1 varchar(10) rollup(avg(c0),avg(c3)) interval 30m:3650d watermark 60000 max_delay 35m expire_time 1h ignore_disorder force_output; -- stb1 为 RSMA 超级表；c0 列的聚合函数为 avg，c1/c2 列的聚合函数为 first，c3 列的聚合函数为 sum；只有一个层级的聚合数据 level 2，interval 为 30m，keep 为 3650d； watermark 取值 1 分钟，表示 _twend 之后 1 分钟内的事件时间的数据不会视为乱序；max_delay 取值 35 分钟 ，表示窗口 _twstart 到 _twend 仍未关闭时，会在 "_twstart + 35m" 时触发计算并关闭；expire_time 取值 1 小时，表示小于 "当前时间 - expire_time" 的数据会被丢弃；ignore_disorder 表示乱序数据不会触发重新计算；delete_recalc 未指定，表示数据删除不会触发重新计算; force_output 显式指定，表示无计算结果也会输出 NULL；ignore_nodata_trigger 取默认值，表示窗口内无数据也会触发计算。

3）create stable if not exists db0.stb2 (ts timestamp, c0 int, c1 varchar(10), c2 bool, c3 float) tags(t0 int, t1 varchar(10) rollup(avg(c0),max(c3),last(c2)) interval 5m:30d,15m:180d watermark 30000,30000 max_delay 6m,16m expire_time 1h,1h ignore_disorder delete_recalc force_out ignore_nodata_trigger; -- stb2 为 RSMA 超级表；c0 列的聚合函数为 avg，c1 列的聚合函数为 first，c2 列的聚合函数为 last，c2 列的聚合函数为 max；有 2个层级的聚合数据, level 2 的 interval 为 5m，keep 为 30d，level 3 的 interval 为 15m，keep 为 180d，； level 2/3 的 watermark 均取值 30 秒，表示 _twend 之后 30 秒内的事件时间的数据会被正常计算，不会视为乱序；max_delay 分别取值 6m 和 16m，表示窗口 _twstart 到 _twend 仍未关闭时，level 2/3 会在分别在 _twstart + 6m 和 _twstart + 16m 时触发计算并关闭；expire_time 均取值 1 小时，表示小于 "当前时间 - expire_time" 的数据会被丢弃；ignore_disorder 表示乱序数据不会触发重新计算；delete_recalc 表示数据删除会触发重新计算; force_output 显式指定，表示无计算结果也会输出 NULL；ignore_nodata_trigger 显式指定，表示窗口内无数据不会触发计算。
```

### 4.2 RSMA 计算与输出

#### 4.2.1 RSMA 聚合数据计算

1. RSMA level 2/3 存储的数据，本质上是来自 interval + partition by tbname 的流计算结果。
2. 以 `create stable if not exists db0.stb0 (ts timestamp, c0 int, c1 varchar(10), c2 bool, c3 float) tags(t0 int, t1 varchar(10) rollup(avg(c0),last(c1),sum(c3)); ` 为例，level 2/3 底层对应的建流语句分别为：
```sql {wrap}
level 2: create stream `rsma_l2_db0_stb0` INTERVAL(1m) SLIDING(1m) from db0.stb0 PARTITION BY tbname into db0.stb0 as select _twstart, avg(c0),last(c1),first(c2),sum(c3) from %%tbname where _c0 >= _twstart and _c0 <= _twend;

level 3: create stream `rsma_l3_db0_stb0` INTERVAL(15m) SLIDING(15m) from db0.stb0 PARTITION BY tbname into db0.stb0 as select _twstart, avg(c0),last(c1),first(c2),sum(c3) from %%tbname where _c0 >= _twstart and _c0 <= _twend;
```

#### 4.2.2 RSMA 聚合结果输出

1. 由于 RSMA level 2/3 与 level 1 的原始数据共用表 schema，因此，流计算的结果要 sink 到原始表所在的 vnode，并且能够区分出是 RSMA 的计算结果以及属于哪一存储层级（retention level）。

### 4.3 RSMA 查询

#### 4.3.1 查询规则

1. 查询时，根据查询的时间范围，自动查询对应区间的数据。
2. 如果某次查询的时间范围覆盖了多个 retention level，则只查询并返回最高 retention level 的数据。
3. 如果未指定查询时间，则默认时间范围为 [INT64_MIN, INT64_MAX]，只查询最高 retention level 的数据。

#### 4.3.2 示例

```sql {wrap}
drop database if exists d0;
create database d0 retentions -:7d,1m:21d,15m:365d;
use d0;
create table if not exists stb0 (ts timestamp, c1 int) tags (city binary(20),district binary(20)) rollup(min(c1));
create table ct1 using stb tags("BeiJing", "ChaoYang");
insert into ct1 values(now+0s, 10);
insert into ct1 values(now+0s, 1);
insert into ct1 values(now+0s, 100);
select * from ct1 where ts > now-3d;　// 返回 level 1 的数据
select * from ct1 where ts > now-8d;　// 返回 level 2 的数据
select * from ct1 where ts > now-30d; // 返回 level 3 的数据
select * from ct1; 　　　　　　　　　　　// 返回 level 3 的数据
```

## 5. 性能

- 因为 RSMA level 2/3 数据的聚合在底层是使用流计算实现的，因此，对资源的消耗依赖流计算。新版流计算提供了丰富的参数来控制乱序/删除/过期等行为，根据实际的需求，选择合适的参数以减少更新/删除触发重算对计算资源带来的消耗。

## 6. 兼容性

无

## 7. 运维

无

## 8. 使用场景

- 原始数据保存时长较短，降采样数据保留时长较长，可大幅减少存储成本。

## 9. 约束和限制

无

## 10. 常见错误和排查

用户操作失败，错误码对照表

| Error code | description | note |
| --- | --- | --- |
|  |  |  |
|  |  |  |

## 11. 可观测性

无

## 12. 安装和卸载

无特殊要求

## 13. 文档

需要修改官网文档

## 14. 参考

- [时序数据库产品 Rollup 功能调研](https://jira.taosdata.com:18090/pages/viewpage.action?pageId=135108852)
- [RSMA 功能](https://taosdata.feishu.cn/wiki/SVf3wv8VriIxRUkKPUzcLGmWnig)
- [TSMA 功能](https://taosdata.feishu.cn/wiki/WpVfwsKjeilOtckp3U2cIaz0nef)
- [流计算新需求与重构 FS](https://taosdata.feishu.cn/wiki/HlKTwwnA2iaF3IkFfY5ctzqknne)

## 15. 附录

无
