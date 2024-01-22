---
sidebar_label: 流式计算
title: 流式计算
description: 流式计算的相关 SQL 的详细语法
---


## 创建流式计算

```sql
CREATE STREAM [IF NOT EXISTS] stream_name [stream_options] INTO stb_name[(field1_name, ...)] [TAGS (create_definition [, create_definition] ...)] SUBTABLE(expression) AS subquery
stream_options: {
 TRIGGER        [AT_ONCE | WINDOW_CLOSE | MAX_DELAY time]
 WATERMARK      time
 IGNORE EXPIRED [0|1]
 DELETE_MARK    time
 FILL_HISTORY   [0|1]
 IGNORE UPDATE  [0|1]
}

```

其中 subquery 是 select 普通查询语法的子集:

```sql
subquery: SELECT select_list
    from_clause
    [WHERE condition]
    [PARTITION BY tag_list]
    [window_clause]
```

支持会话窗口、状态窗口与滑动窗口，其中，会话窗口与状态窗口搭配超级表时必须与partition by tbname一起使用

stb_name 是保存计算结果的超级表的表名，如果该超级表不存在，会自动创建；如果已存在，则检查列的schema信息。详见 写入已存在的超级表

TAGS 子句定义了流计算中创建TAG的规则，可以为每个partition对应的子表生成自定义的TAG值，详见 自定义TAG
```sql
create_definition:
    col_name column_definition
column_definition:
    type_name [COMMENT 'string_value']
```

subtable 子句定义了流式计算中创建的子表的命名规则，详见 流式计算的 partition 部分。

```sql
window_clause: {
    SESSION(ts_col, tol_val)
  | STATE_WINDOW(col)
  | INTERVAL(interval_val [, interval_offset]) [SLIDING (sliding_val)]
}
```

其中，SESSION 是会话窗口，tol_val 是时间间隔的最大范围。在 tol_val 时间间隔范围内的数据都属于同一个窗口，如果连续的两条数据的时间超过 tol_val，则自动开启下一个窗口。

窗口的定义与时序数据特色查询中的定义完全相同，详见 [TDengine 特色查询](../distinguished)

例如，如下语句创建流式计算，同时自动创建名为 avg_vol 的超级表，此流计算以一分钟为时间窗口、30 秒为前向增量统计这些电表的平均电压，并将来自 meters 表的数据的计算结果写入 avg_vol 表，不同 partition 的数据会分别创建子表并写入不同子表。

```sql
CREATE STREAM avg_vol_s INTO avg_vol AS
SELECT _wstart, count(*), avg(voltage) FROM meters PARTITION BY tbname INTERVAL(1m) SLIDING(30s);
```

## 流式计算的 partition

可以使用 PARTITION BY TBNAME，tag，普通列或者表达式，对一个流进行多分区的计算，每个分区的时间线与时间窗口是独立的，会各自聚合，并写入到目的表中的不同子表。

不带 PARTITION BY 子句时，所有的数据将写入到一张子表。

在创建流时不使用 SUBTABLE 子句时，流式计算创建的超级表有唯一的 tag 列 groupId，每个 partition 会被分配唯一 groupId。与 schemaless 写入一致，我们通过 MD5 计算子表名，并自动创建它。

若创建流的语句中包含 SUBTABLE 子句，用户可以为每个 partition 对应的子表生成自定义的表名，例如：

```sql
CREATE STREAM avg_vol_s INTO avg_vol SUBTABLE(CONCAT('new-', tname)) AS SELECT _wstart, count(*), avg(voltage) FROM meters PARTITION BY tbname tname INTERVAL(1m);
```

PARTITION 子句中，为 tbname 定义了一个别名 tname, 在PARTITION 子句中的别名可以用于 SUBTABLE 子句中的表达式计算，在上述示例中，流新创建的子表将以前缀 'new-' 连接原表名作为表名(从3.2.3.0开始，为了避免 sutable 中的表达式无法区分各个子表，即误将多个相同时间线写入一个子表，在指定的子表名后面加上 _groupId)。

注意，子表名的长度若超过 TDengine 的限制，将被截断。若要生成的子表名已经存在于另一超级表，由于 TDengine 的子表名是唯一的，因此对应新子表的创建以及数据的写入将会失败。

## 流式计算读取历史数据

正常情况下，流式计算不会处理创建前已经写入源表中的数据，若要处理已经写入的数据，可以在创建流时设置 fill_history 1 选项，这样创建的流式计算会自动处理创建前、创建中、创建后写入的数据。例如：

```sql
create stream if not exists s1 fill_history 1 into st1  as select count(*) from t1 interval(10s)
```

结合 fill_history 1 选项，可以实现只处理特定历史时间范围的数据，例如：只处理某历史时刻（2020年1月30日）之后的数据

```sql
create stream if not exists s1 fill_history 1 into st1  as select count(*) from t1 where ts > '2020-01-30' interval(10s)
```

再如，仅处理某时间段内的数据，结束时间可以是未来时间

```sql
create stream if not exists s1 fill_history 1 into st1  as select count(*) from t1 where ts > '2020-01-30' and ts < '2023-01-01' interval(10s)
```

如果该流任务已经彻底过期，并且您不再想让它检测或处理数据，您可以手动删除它，被计算出的数据仍会被保留。

## 删除流式计算

```sql
DROP STREAM [IF EXISTS] stream_name;
```

仅删除流式计算任务，由流式计算写入的数据不会被删除。

## 展示流式计算

```sql
SHOW STREAMS;
```

若要展示更详细的信息，可以使用：

```sql
SELECT * from information_schema.`ins_streams`;
```

## 流式计算的触发模式

在创建流时，可以通过 TRIGGER 指令指定流式计算的触发模式。

对于非窗口计算，流式计算的触发是实时的；对于窗口计算，目前提供 3 种触发模式，默认为 WINDOW_CLOSE：

1. AT_ONCE：写入立即触发

2. WINDOW_CLOSE：窗口关闭时触发（窗口关闭由事件时间决定，可配合 watermark 使用）

3. MAX_DELAY time：若窗口关闭，则触发计算。若窗口未关闭，且未关闭时长超过 max delay 指定的时间，则触发计算。

由于窗口关闭是由事件时间决定的，如事件流中断、或持续延迟，则事件时间无法更新，可能导致无法得到最新的计算结果。

因此，流式计算提供了以事件时间结合处理时间计算的 MAX_DELAY 触发模式。

MAX_DELAY 模式在窗口关闭时会立即触发计算。此外，当数据写入后，计算触发的时间超过 max delay 指定的时间，则立即触发计算

## 流式计算的窗口关闭

流式计算以事件时间（插入记录中的时间戳主键）为基准计算窗口关闭，而非以 TDengine 服务器的时间，以事件时间为基准，可以避免客户端与服务器时间不一致带来的问题，能够解决乱序数据写入等等问题。流式计算还提供了 watermark 来定义容忍的乱序程度。

在创建流时，可以在 stream_option 中指定 watermark，它定义了数据乱序的容忍上界。

流式计算通过 watermark 来度量对乱序数据的容忍程度，watermark 默认为 0。

T = 最新事件时间 - watermark

每次写入的数据都会以上述公式更新窗口关闭时间，并将窗口结束时间 < T 的所有打开的窗口关闭，若触发模式为 WINDOW_CLOSE 或 MAX_DELAY，则推送窗口聚合结果。


![TDengine 流式计算窗口关闭示意图](./watermark.webp)


图中，纵轴表示不同时刻，对于不同时刻，我们画出其对应的 TDengine 收到的数据，即为横轴。

横轴上的数据点表示已经收到的数据，其中蓝色的点表示事件时间(即数据中的时间戳主键)最后的数据，该数据点减去定义的 watermark 时间，得到乱序容忍的上界 T。

所有结束时间小于 T 的窗口都将被关闭（图中以灰色方框标记）。

T2 时刻，乱序数据（黄色的点）到达 TDengine，由于有 watermark 的存在，这些数据进入的窗口并未被关闭，因此可以被正确处理。

T3 时刻，最新事件到达，T 向后推移超过了第二个窗口关闭的时间，该窗口被关闭，乱序数据被正确处理。

在 window_close 或 max_delay 模式下，窗口关闭直接影响推送结果。在 at_once 模式下，窗口关闭只与内存占用有关。


## 流式计算对于过期数据的处理策略

对于已关闭的窗口，再次落入该窗口中的数据被标记为过期数据.

TDengine 对于过期数据提供两种处理方式，由 IGNORE EXPIRED 选项指定：

1. 重新计算，即 IGNORE EXPIRED 0：从 TSDB 中重新查找对应窗口的所有数据并重新计算得到最新结果

2. 直接丢弃，即 IGNORE EXPIRED 1：默认配置，忽略过期数据


无论在哪种模式下，watermark 都应该被妥善设置，来得到正确结果（直接丢弃模式）或避免频繁触发重算带来的性能开销（重新计算模式）。

## 流式计算对于修改数据的处理策略

TDengine 对于修改数据提供两种处理方式，由 IGNORE UPDATE 选项指定：

1. 检查数据是否被修改，即 IGNORE UPDATE 0：默认配置，如果被修改，则重新计算对应窗口。

2. 不检查数据是否被修改，全部按增量数据计算，即 IGNORE UPDATE 1。


## 写入已存在的超级表
```sql
[field1_name,...]
```
用来指定stb_name的列与subquery输出结果的对应关系。如果stb_name的列与subquery输出结果的位置、数量全部匹配，则不需要显示指定对应关系。如果stb_name的列与subquery输出结果的数据类型不匹配，会把subquery输出结果的类型转换成对应的stb_name的列的类型。

对于已经存在的超级表，检查列的schema信息
1. 检查列的schema信息是否匹配，对于不匹配的，则自动进行类型转换，当前只有数据长度大于4096byte时才报错，其余场景都能进行类型转换。
2. 检查列的个数是否相同，如果不同，需要显示的指定超级表与subquery的列的对应关系，否则报错；如果相同，可以指定对应关系，也可以不指定，不指定则按位置顺序对应。

## 自定义TAG

用户可以为每个 partition 对应的子表生成自定义的TAG值。
```sql
CREATE STREAM streams2 trigger at_once INTO st1 TAGS(cc varchar(100)) as select  _wstart, count(*) c1 from st partition by concat("tag-", tbname) as cc interval(10s));
```

PARTITION 子句中，为 concat("tag-", tbname)定义了一个别名cc, 对应超级表st1的自定义TAG的名字。在上述示例中，流新创建的子表的TAG将以前缀 'new-' 连接原表名作为TAG的值。

会对TAG信息进行如下检查
1. 检查tag的schema信息是否匹配，对于不匹配的，则自动进行数据类型转换，当前只有数据长度大于4096byte时才报错，其余场景都能进行类型转换。
2. 检查tag的个数是否相同，如果不同，需要显示的指定超级表与subquery的tag的对应关系，否则报错；如果相同，可以指定对应关系，也可以不指定，不指定则按位置顺序对应。

## 清理中间状态

```
DELETE_MARK    time
```
DELETE_MARK用于删除缓存的窗口状态，也就是删除流计算的中间结果。如果不设置，默认值是10年
T = 最新事件时间 - DELETE_MARK

## 流式计算支持的函数

1. 所有的 [单行函数](../function/#单行函数) 均可用于流计算。
2. 以下 19 个聚合/选择函数 <b>不能</b> 应用在创建流计算的 SQL 语句。此外的其他类型的函数均可用于流计算。

- [leastsquares](../function/#leastsquares)
- [percentile](../function/#percentile)
- [top](../function/#top)
- [bottom](../function/#bottom)
- [elapsed](../function/#elapsed)
- [interp](../function/#interp)
- [derivative](../function/#derivative)
- [irate](../function/#irate)
- [twa](../function/#twa)
- [histogram](../function/#histogram)
- [diff](../function/#diff)
- [statecount](../function/#statecount)
- [stateduration](../function/#stateduration)
- [csum](../function/#csum)
- [mavg](../function/#mavg)
- [sample](../function/#sample)
- [tail](../function/#tail)
- [unique](../function/#unique)
- [mode](../function/#mode)

## 暂停、恢复流计算
1.流计算暂停计算任务
PAUSE STREAM [IF EXISTS] stream_name;
没有指定IF EXISTS，如果该stream不存在，则报错；如果存在，则暂停流计算。指定了IF EXISTS，如果该stream不存在，则返回成功；如果存在，则暂停流计算

2.流计算恢复计算任务
RESUME STREAM [IF EXISTS] [IGNORE UNTREATED] stream_name;
没有指定IF EXISTS，如果该stream不存在，则报错，如果存在，则恢复流计算；指定了IF EXISTS，如果stream不存在，则返回成功；如果存在，则恢复流计算。如果指定IGNORE UNTREATED，则恢复流计算时，忽略流计算暂停期间写入的数据。

