# tsma功能(废弃)

请@徐开礼检查现有文档缺失，提供完整的user 检查现有文档缺失，提供完整的user manual。

## 1. 需求背景

- Time Range SMA(tsma)  是一种按时间范围预聚合的 sma (Small Materialized Aggregation)，可以根据用户指定的 interval 窗口和函数来进行预聚合计算，同时结果保存在临时表中，可以大大提升窗口查询的速度，适用于高频使用 interval 查询或interval查询性能无法满足的场景。

## 2. 功能描述

- 每个 tsma 的预聚合结果存储在一个单独的单副本 vnode中，可以通过查看 show vgroups 命令返回结果中 tsma 标识是否为 1 区分。 
- interval 预聚合结果的生成会一定的延迟，具体延迟依赖参数(watermark/max_delay)配置及资源负载情况。
- interval 查询时只有同时满足以下所有条件才会自动使用tsma预聚合结果: 1) 有查询语句中的 interval/offset/sliding 等参数与 create tsma 的参数完全一致，2) 客户端启动了 querySmaOptimize配置项。不满足上述条件，自动查询原始数据。
- 时序数据更新或删除，对应窗口的聚合结果也会被更新或删除。

## 3. **使用限制**

- 仅允许在超级表上创建，每个超级表可以建多个tsma， 但是对于已经指定了retentions 参数的数据库, 不允许创建tsma 。
- tsma 预聚合结果的计算过程消耗资源较多，非必要不要创建太多 TSMA。
- 如果某个超级表上某一列被 tsma 使用了, 则该列不允许被删除( 必须先 drop tsma, 再删除列); 添加列不受影响。

## 4. 语法描述

- 创建 SMA 
```cpp {wrap}
CREATE SMA [db_name.]sma_name ON stb_name sma_option
 
sma_option:
    FUNCTION(functions) INTERVAL(interval_val [, interval_offset]) [SLIDING(sliding_val)] [WATERMARK(watermark_val)] [MAX_DELAY(max_delay_val)]
 
functions:
    function [, function] ...
```

   参数说明

| **参数名称** | **说明** | **取值** |
| --- | --- | --- |
| FUNCTION | 指定的函数，范围同流计算支持的函数。 | min/max/sum/first/last/apercentile/avg/count/spread/stddev/hyperloglog 等。 |
| WATERMARK | 指定窗口的关闭时间。用于控制允许的最大延迟，窗口关闭时会向应用方推送聚合结果。 | [0, 900000]，最小单位毫秒，默认值为 5 秒。 |
| MAX_DELAY | 用于控制推送计算结果的最大延迟，定时器每隔 MAX_DELAY 会检测是否有结果需要推送。 不建议 MAX_DELAY 设置太小，否则会过于频繁的推送结果，影响存储和查询性能，如无特殊需求，取默认值即可。 | [1, 900000]，最小单位毫秒，默认值为 interval 的值(但不能超过最大值)。 |

- 删除 SMA 
```cpp
DROP SMA [db_name.]sma_name
```

- 查看 SMA 索引
```cpp
SHOW SMA FROM table_name;
SHOW SMA;
SHOW CREATE SMA;
```

- 查询语句
```cpp {wrap}
SELECT max(c2),min(c1) from Stable_Name interval(6m,10s) sliding(6m);
 
## 5. 支持包含 _wstart/_wend/_wduration 等伪列的查询

SELECT _wstart, _wend, _wduration, max(c2),min(c1) from Stable_Name interval(6m,10s) sliding(6m);
```

- 查询 tsma 开关： querySmaOptimize
```cpp {wrap}
功能描述：是否利用 tsma 预计算结果进行查询加速。
1）取值范围：0/1，0 查询原始数据(默认值 0)，1 查询 tsma 结果     
2）配置方式：可以通过在 taos.cfg 中配置（e.g. querySmaOptimize 1），也支持通过 alter 命令动态更新：alter local 'querySmaOptimize' '0'/'1';
```

## 6. 用法示例

```cpp {wrap}
drop database if exists d0;
create database d0;
use d0;
create table if not exists stb (ts timestamp, c1 int, c2 float, c3 double) tags (t1 int unsigned);
show stables;
create table ct1 using stb tags(1000);
create table ct2 using stb tags(2000);
insert into ct1 values(now+0s, 10, 2.0, 3.0);
insert into ct1 values(now+1s, 11, 2.1, 3.1)(now+2s, 12, 2.2, 3.2)(now+3s, 13, 2.3, 3.3);
select * from ct1;

## 7. create sma index

create sma index sma_index_name1 on stb function(max(c1),max(c2),min(c1)) interval(6m,10s) sliding(6m);

## 8. create sma index sma_index_name1 on stb function(max(c1),max(c2),min(c1)) interval(6m,10s) sliding(6m) watermark 5s max_delay 1m;

## 9. query from sma index

alter local 'querySmaOptimize' '1';
select max(c2),min(c1) from stb interval(6m,10s) sliding(6m);
select _wstart,_wend,_wduration,max(c2),min(c1) from stb interval(6m,10s) sliding(6m);

## 10. show index

show indexes from stb;

## 11. drop index

drop index sma_index_name1;
```
