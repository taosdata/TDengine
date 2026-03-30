# RSMA 功能

请@徐开礼检查现有文档缺失，提供完整的user 检查现有文档缺失，提供完整的user manual。

## 1. 功能描述

- Rollup SMA(rsma) 是一种按照时间范围对用户提供的原始数据自动进行降采样存储的 SMA (Small Materialized Aggregration)，适用于不保留原始数据而仅仅保留 SMA 数据的场景，可以大大减少磁盘的空间占用。在查询时，可以根据查询的时间范围，自动查询对应区间的数据。
- Rollup 数据采集/聚合时，最多支持 3 个层级。Level 1 为原始数据, level 2/3 为降采样数据，如果窗口内没有数据则对应窗口无数据。
- 若level 1 的数据存在用户更新或删除 ，level 2/3 的计算结果也会自动更新。
- rsma 以子表为单位生成和保留结果 SMA 信息，结果存在与原始数据表相同vnode中；

## 2. 功能限制

- 带有"retentions"参数的数据库中超级表必须带有"rollup"参数，普通表没有限制（不支持ROLLUP SMA）；
- 单个超级表只能含有一个rollup函数；
- 写入的历史数据主键时戳范围必须在Level1的keep参数范围内；
- 对于数值计算类型的rollup函数只支持double/float列类型，其他函数无限制；
- 带 rollup 的超级表，不支持修改超级表的 schema。

## 3. 语法描述

- 创建 rsma database
```cpp {wrap}
create database d0 retentions -:7d,1m:21d,15m:365d; // -:keep1,interval2:keep2,interval3:keep3
```

参数说明

| **参数名称** | **说明** |
| --- | --- |
| retentions | - 在 create database 时，通过 retentions 参数，指定数据的采样或聚合周期 ( interval ) 和保存时长 ( keep )。例如，retentions -:7d,1m:21d,15m:365d，表示一共有 3 个 数据保存层级( retention level )。其中 level 1 为原始数据, interval 位置为占位符 "-"，keep 为 7 天；level 2 为聚合数据，interval 为 1 分钟，keep 为 21 天；level 3 为聚合数据，interval 为 15 分钟，keep 为 365 天。 - 如果用户写入数据时间戳小于(当前时间-level1的keep值)，则写入报错； - 该参数不支持 alter; - 时间单位不支持 "月" 和 "年"; interval 小于对应的 keep; 高层级的 interval 大于低层级的 interval，高层级的 keep 大于 低层级的 keep; - Interval 支持的单位包括：s, m, h, d, w，取值范围：[0, 对应层级的 keep 值) - Keep 支持的单位包括：m, h, d, 取值范围：非纳秒库 [1d, 1000*365d] ，纳秒库 [1 d, 292*365d] - 每个存储层级的 interval，均是基于 GMT 时间 1970 年 01 月 01 日 00 时 00 分 00 秒开始切分的时间窗口; - 最多支持 3 个 retention level。 - 针对 rsma database，因为 retentions 参数自带存储时长, 所以rsma的结果存储时长与创建数据库时指定的 keep 参数无关。 - 对于指定了retentions的数据库，创建的所有超级表都必须生成RSMA信息； |

- 创建stable（使用rsma）
```cpp {wrap}
create table if not exists stb (ts timestamp, c1 int) tags (city binary(20),district binary(20)) rollup(min) watermark 0s,1s max_delay 1m,180s;
```

   参数说明

| **参数名称** | **说明** |
| --- | --- |
| ROLLUP | - 必选参数。 - 指定的聚合函数（只能一个函数）。取值范围： avg, sum, min, max, last, first。 - 只有指定了 retentions 参数数据库才允许使用该参数。 - 只支持在超级表上创建 rollup SMA，并在其对应的子表进行降采样存储及查询 ( 不支持普通表 ); - 该参数不支持 alter。 - 聚合函数应用于除主键时间戳列外的所有数据列（不含tag列），若存在聚合函数不支持的数据类型的列则建表失败。 - level 1 的原始数据和 level 2/3 的聚合数据采用相同的表schema。 - 聚合类的数值类型计算函数（ avg, sum ）只支持double/float类型，其余的创建会报错； |
| WATERMARK | - 可选参数。 - 指定窗口的关闭时间。用于控制允许的最大延迟，窗口关闭时会向 level 2/3 推送聚合结果。默认单位毫秒，取值范围 [0, 900000]，默认值为 5 秒。 - 只有指定了 retentions 参数数据库才允许使用该参数。 |
| MAX_DELAY | - 可选参数。 - 用于控制推送计算结果的最大延迟，定时器每隔 MAX_DELAY 会检测是否有结果需要推送。默认单位毫秒，取值范围 [1, 900000]，默认值为 interval 的值，若 interval 值大于最大值则使用最大值。 - 不建议 MAX_DELAY 设置太小，否则会过于频繁的推送结果，影响存储和查询性能，如无特殊需求，取默认值即可。 - 只有指定了 retentions 参数数据库才允许使用该参数。 |

- 查询 rsma 数据
根据查询的时间范围，自动查询对应区间的数据。规则如下：
- 根据查询条件的主键时间范围起始值确定从哪个层级进行数据查询，按照从leve1到level3依次匹配来决定，若存在于某个level的keep值范围内则从当前level进行查询；
- 若查询条件中无主键时间范围，则从level3进行查询；
- 投影查询的结果根据level的不同而不同，level1返回的是原始数据，level2/level3返回的是rsma的聚合数据；
- 其他查询的结果也根据level的不同而不同，输入对象可能是原始数据或rsma的聚合数据；比如sum(*)查询如果根据查询范围输入数据是level3，则sum(*）返回的是level3系列输入数据的sum值；


例如，retentions 取值为 15s:7d,1m:21d,15m:365d。
```cpp
select * from ct1 where ts > now-7d;　  // 返回 level 1 的数据
select * from ct1 where ts > now-21d;   // 返回 level 2 的数据
select * from ct1 where ts > now-365d;  // 返回 level 3 的数据
select * from ct1; 　　　　　　　　　　　   // 不指定 ts 返回 level 3 的数据
```

## 4. 用法示例

```cpp {wrap}
drop database if exists d0;
create database d0 retentions -:7d,1m:21d,15m:365d; // 不支持月和年
#create database d0 retentions 15s:7d,1m:21d;
#create database d0 retentions 15s:7d;
use d0;
create table if not exists stb (ts timestamp, c1 int) tags (city binary(20),district binary(20)) rollup(min) watermark 0s,1s max_delay 1m,180s;
#create table if not exists stb (ts timestamp, c1 int) tags (city binary(20),district binary(20)) rollup(min) max_delay 300s;
#create table if not exists stb (ts timestamp, c1 int, c2 int) tags (city binary(20),district binary(20)) rollup(min) watermark 10m max_delay 45s,2m;
create table ct1 using stb tags("BeiJing", "ChaoYang");
insert into ct1 values(now+0s, 10);
insert into ct1 values(now+0s, 1);
insert into ct1 values(now+0s, 100);
select * from ct1 where ts > now-3d;　// 返回 level 1 的数据
select * from ct1 where ts > now-8d;　// 返回 level 2 的数据
select * from ct1; 　　　　　　　　　　　// 返回 level 3 的数据
```
