---
sidebar_label: 流计算
title: 流计算
toc_max_heading_level: 4
---

在时序数据的处理中，经常要对原始数据进行清洗、预处理，再使用时序数据库进行长久的储存，而且经常还需要使用原始的时序数据通过计算生成新的时序数据。在传统的时序数据解决方案中，常常需要部署 Kafka、Flink 等流处理系统，而流处理系统的复杂性，带来了高昂的开发与运维成本。

TDengine 的流计算引擎提供了实时处理写入的数据流的能力，使用 SQL 定义实时流变换，当数据被写入流的源表后，数据会被以定义的方式自动处理，并根据定义的触发模式向目的表推送结果。它提供了替代复杂流处理系统的轻量级解决方案，并能够在高吞吐的数据写入的情况下，提供毫秒级的计算结果延迟。

流计算可以包含数据过滤，标量函数计算（含 UDF），以及窗口聚合（支持滑动窗口、会话窗口与状态窗口），能够以超级表、子表、普通表为源表，写入到目的超级表。在创建流时，目的超级表将被自动创建，随后新插入的数据会被流定义的方式处理并写入其中，通过 partition by 子句，可以以表名或标签划分 partition，不同的 partition 将写入到目的超级表的不同子表。

TDengine 的流计算能够支持分布在多个节点中的超级表聚合，能够处理乱序数据的写入。它提供 watermark 机制以度量容忍数据乱序的程度，并提供了 ignore expired 配置项以决定乱序数据的处理策略 —— 丢弃或者重新计算。

下面详细介绍流计算使用的具体方法。

## 创建流计算

语法如下：
```sql
CREATE STREAM [IF NOT EXISTS] stream_name [stream_options] INTO stb_name
[(field1_name, ...)] [TAGS (column_definition [, column_definition] ...)] 
SUBTABLE(expression) AS subquery

stream_options: {
 TRIGGER        [AT_ONCE | WINDOW_CLOSE | MAX_DELAY time]
 WATERMARK      time
 IGNORE EXPIRED [0|1]
 DELETE_MARK    time
 FILL_HISTORY   [0|1]
 IGNORE UPDATE  [0|1]
}

column_definition:
    col_name col_type [COMMENT 'string_value']
```

其中 subquery 是 select 普通查询语法的子集。
```sql
subquery: SELECT select_list
    from_clause
    [WHERE condition]
    [PARTITION BY tag_list]
    [window_clause]
    
window_cluse: {
    SESSION(ts_col, tol_val)
  | STATE_WINDOW(col)
  | INTERVAL(interval_val [, interval_offset]) [SLIDING (sliding_val)]
  | EVENT_WINDOW START WITH start_trigger_condition END WITH end_trigger_condition
  | COUNT_WINDOW(count_val[, sliding_val])
}
```

subquery 支持会话窗口、状态窗口与滑动窗口。其中，会话窗口与状态窗口搭配超级表时必须与 partition by tbname 一起使用。

1. 其中，SESSION 是会话窗口，tol_val 是时间间隔的最大范围。在 tol_val 时间间隔范围内的数据都属于同一个窗口，如果连续的两条数据的时间间隔超过 tol_val，则自动开启下一个窗口。

2. EVENT_WINDOW 是事件窗口，根据开始条件和结束条件来划定窗口。当 start_trigger_condition 满足时则窗口开始，直到 end_trigger_condition 满足时窗口关闭。 start_trigger_condition 和 end_trigger_condition 可以是任意 TDengine 支持的条件表达式，且可以包含不同的列。

3. COUNT_WINDOW 是计数窗口，按固定的数据行数来划分窗口。 count_val 是常量，是正整数，必须大于等于 2，小于 2147483648。 count_val 表示每个 COUNT_WINDOW 包含的最大数据行数，总数据行数不能整除 count_val 时，最后一个窗口的行数会小于 count_val 。 sliding_val 是常量，表示窗口滑动的数量，类似于 INTERVAL 的 SLIDING 。

窗口的定义与时序数据窗口查询中的定义完全相同，具体可参考 TDengine 窗口函数部分。

如下 SQL 将创建一个流计算，执行后 TDengine 会自动创建名为avg_vol 的超级表，此流计算以 1min 为时间窗口、30s 为前向增量统计这些智能电表的平均电压，并将来自 meters 的数据的计算结果写入 avg_vol，不同分区的数据会分别创建子表并写入不同子表。
```sql
CREATE STREAM avg_vol_s INTO avg_vol AS
SELECT _wstart, count(*), avg(voltage) FROM power.meters PARTITION BY tbname INTERVAL(1m) SLIDING(30s);
```

本节涉及的相关参数的说明如下。
- stb_name 是保存计算结果的超级表的表名，如果该超级表不存在，则会自动创建；如果已存在，则检查列的 schema 信息。详见 6.3.8 节。
- tags 子句定义了流计算中创建标签的规则。通过 tags 字段可以为每个分区对应的子表生成自定义的标签值。

## 流式计算的规则和策略

### 流计算的分区

在 TDengine 中，我们可以利用 partition by 子句结合 tbname、标签列、普通列或表达式，对一个流进行多分区的计算。每个分区都拥有独立的时间线和时间窗口，它们会分别进行数据聚合，并将结果写入目的表的不同子表中。如果不使用 partition by 子句，所有数据将默认写入同一张子表中。

特别地，partition by + tbname 是一种非常实用的操作，它表示对每张子表进行流计算。这样做的好处是可以针对每张子表的特点进行定制化处理，以提高计算效率。

在创建流时，如果不使用 substable 子句，流计算所创建的超级表将包含一个唯一的标签列 groupId。每个分区将被分配一个唯一的 groupId，并通过 MD5 算法计算相应的子表名称。TDengine 将自动创建这些子表，以便存储各个分区的计算结果。这种机制使得数据管理更加灵活和高效，同时也方便后续的数据查询和分析。

若创建流的语句中包含 substable 子句，用户可以为每个分区对应的子表生成自定义的表名。示例如下。
```sql
CREATE STREAM avg_vol_s INTO avg_vol SUBTABLE(CONCAT('new-', tname)) AS SELECT _wstart, count(*), avg(voltage) FROM meters PARTITION BY tbname tname INTERVAL(1m);
```

PARTITION 子句中，为 tbname 定义了一个别名 tname， 在 PARTITION 子句中的别名可以用于 SUBTABLE 子句中的表达式计算，在上述示例中，流新创建的子表规则为 new- + 子表名 + _超级表名 + _groupId。

**注意**：子表名的长度若超过 TDengine 的限制，将被截断。若要生成的子表名已经存在于另一超级表，由于 TDengine 的子表名是唯一的，因此对应新子表的创建以及数据的写入将会失败。

### 流计算处理历史数据

在正常情况下，流计算任务不会处理那些在流创建之前已经写入源表的数据。这是因为流计算的触发是基于新写入的数据，而非已有数据。然而，如果我们需要处理这些已有的历史数据，可以在创建流时设置 fill_history 选项为 1。

通过启用 fill_history 选项，创建的流计算任务将具备处理创建前、创建过程中以及创建后写入的数据的能力。这意味着，无论数据是在流创建之前还是之后写入的，都将纳入流计算的范围，从而确保数据的完整性和一致性。这一设置为用户提供了更大的灵活性，使其能够根据实际需求灵活处理历史数据和新数据。

比如，创建一个流，统计所有智能电表每 10s 产生的数据条数，并且计算历史数据。SQL 如下：
```sql
create stream if not exists count_history_s fill_history 1 into count_history as select count(*) from power.meters interval(10s)
```

结合 fill_history 1 选项，可以实现只处理特定历史时间范围的数据，例如只处理某历史时刻（2020 年 1 月 30 日）之后的数据。
```sql
create stream if not exists count_history_s fill_history 1 into count_history  as select count(*) from power.meters where ts > '2020-01-30' interval(10s)
```

再如，仅处理某时间段内的数据，结束时间可以是未来时间。
```sql
create stream if not exists count_history_s fill_history 1 into count_history as select count(*) from power.meters where ts > '2020-01-30' and ts < '2023-01-01' interval(10s)
```

如果该流任务已经彻底过期，并且不再想让它检测或处理数据，您可以手动删除它，被计算出的数据仍会被保留。

### 流计算的触发模式

在创建流时，可以通过 TRIGGER 指令指定流计算的触发模式。对于非窗口计算，流计算的触发是实时的，对于窗口计算，目前提供 4 种触发模式，默认为 WINDOW_CLOSE。
1. AT_ONCE：写入立即触发。
2. WINDOW_CLOSE：窗口关闭时触发（窗口关闭由事件时间决定，可配合 watermark 使用）。
3. MAX_DELAY time：若窗口关闭，则触发计算。若窗口未关闭，且未关闭时长超过 max delay 指定的时间，则触发计算。
4. FORCE_WINDOW_CLOSE：以操作系统当前时间为准，只计算当前关闭窗口的结果，并推送出去。窗口只会在被关闭的时刻计算一次，后续不会再重复计算。该模式当前只支持 INTERVAL 窗口（不支持滑动）；FILL_HISTORY必须为 0，IGNORE EXPIRED 必须为 1，IGNORE UPDATE 必须为 1；FILL 只支持 PREV 、NULL、 NONE、VALUE。

窗口关闭是由事件时间决定的，如事件流中断、或持续延迟，此时事件时间无法更新，可能导致无法得到最新的计算结果。

因此，流计算提供了以事件时间结合处理时间计算的 MAX_DELAY 触发模式。MAX_DELAY 模式在窗口关闭时会立即触发计算。此外，当数据写入后，计算触发的时间超过 max delay 指定的时间，则立即触发计算。

### 流计算的窗口关闭

流计算的核心在于以事件时间（即写入记录中的时间戳主键）为基准来计算窗口的关闭时间，而不是依赖于 TDengine 服务器的时间。采用事件时间作为基准可以有效地规避客户端与服务器时间不一致所带来的问题，并且能够妥善解决数据乱序写入等挑战。

为了进一步控制数据乱序的容忍程度，流计算引入了 watermark 机制。在创建流时，用户可以通过 stream_option 参数指定 watermark 的值，该值定义了数据乱序的容忍上界，默认情况下为 0。

假设 T= 最新事件时间－ watermark，那么每次写入新数据时，系统都会根据这个公式更新窗口的关闭时间。具体而言，系统会将窗口结束时间小于 T 的所有打开的窗口关闭。如果触发模式设置为 window_close 或 max_delay，则会推送窗口聚合的结果。下图展示了流计算的窗口关闭流程。

![流计算窗口关闭图解](./stream-window-close.png)

在上图中，纵轴表示时刻，横轴上的圆点表示已经收到的数据。相关流程说明如下。

1. T1 时刻，第 7 个数据点到达，根据 T =  Latest event - watermark，算出的时间在第二个窗口内，所以第二个窗口没有关闭。
2. T2 时刻，第 6 和第 8 个数据点延迟到达 TDengine，由于此时的 Latest event 没变，T 也没变，乱序数据进入的第二个窗口还未被关闭，因此可以被正确处理。
3. T3 时刻，第 10 个数据点到达，T 向后推移超过了第二个窗口关闭的时间，该窗口被关闭，乱序数据被正确处理。

在 window_close 或 max_delay 模式下，窗口关闭直接影响推送结果。在 at_once 模式下，窗口关闭只与内存占用有关。

### 过期数据处理策略

对于已关闭的窗口，再次落入该窗口中的数据被标记为过期数据。TDengine 对于过期数据提供两种处理方式，由 IGNORE EXPIRED 选项指定。

1. 重新计算，即 IGNORE EXPIRED 0：从 TSDB 中重新查找对应窗口的所有数据并重新计算得到最新结果
2. 直接丢弃，即 IGNORE EXPIRED 1：默认配置，忽略过期数据

无论在哪种模式下，watermark 都应该被妥善设置，来得到正确结果（直接丢弃模式）或避免频繁触发重算带来的性能开销（重新计算模式）

### 数据更新的处理策略

TDengine 对于修改数据提供两种处理方式，由 IGNORE UPDATE 选项指定。
1. 检查数据是否被修改，即 IGNORE UPDATE 0：默认配置，如果被修改，则重新计算对应窗口。
2. 不检查数据是否被修改，全部按增量数据计算，即 IGNORE UPDATE 1。

## 流计算的其它策略

### 写入已存在的超级表

当流计算结果需要写入已存在的超级表时，应确保 stb_name 列与 subquery 输出结果之间的对应关系正确。如果 stb_name 列与 subquery 输出结果的位置、数量完全匹配，那么不需要显式指定对应关系；如果数据类型不匹配，系统会自动将 subquery 输出结果的类型转换为对应的 stb_name 列的类型。

对于已经存在的超级表，系统会检查列的 schema 信息，确保它们与 subquery 输出结果相匹配。以下是一些关键点。
1. 检查列的 schema 信息是否匹配，对于不匹配的，则自动进行类型转换，当前只有数据长度大于 4096 bytes 时才报错，其余场景都能进行类型转换。
2. 检查列的个数是否相同，如果不同，需要显示的指定超级表与 subquery 的列的对应关系，否则报错。如果相同，可以指定对应关系，也可以不指定，不指定则按位置顺序对应。

**注意** 虽然流计算可以将结果写入已经存在的超级表，但不能让两个已经存在的流计算向同一张（超级）表中写入结果数据。这是为了避免数据冲突和不一致，确保数据的完整性和准确性。在实际应用中，应根据实际需求和数据结构合理设置列的对应关系，以实现高效、准确的数据处理。

### 自定义目标表的标签

用户可以为每个 partition 对应的子表生成自定义的 TAG 值，如下创建流的语句，
```sql
CREATE STREAM output_tag trigger at_once INTO output_tag_s TAGS(alias_tag varchar(100)) as select _wstart, count(*) from power.meters partition by concat("tag-", tbname) as alias_tag interval(10s));
```

在 PARTITION 子句中，为 concat（"tag-"， tbname）定义了一个别名 alias_tag， 对应超级表 output_tag_s 的自定义 TAG 的名字。在上述示例中，流新创建的子表的 TAG 将以前缀 'tag-' 连接原表名作为 TAG 的值。会对 TAG 信息进行如下检查。

1. 检查 tag 的 schema 信息是否匹配，对于不匹配的，则自动进行数据类型转换，当前只有数据长度大于 4096 bytes 时才报错，其余场景都能进行类型转换。
2. 检查 tag 的 个数是否相同，如果不同，需要显示的指定超级表与 subquery 的 tag 的对应关系，否则报错。如果相同，可以指定对应关系，也可以不指定，不指定则按位置顺序对应。

### 清理流计算的中间状态

```sql
DELETE_MARK time
```
DELETE_MARK 用于删除缓存的窗口状态，也就是删除流计算的中间结果。缓存的窗口状态主要用于过期数据导致的窗口结果更新操作。如果不设置，默认值是 10 年。

## 流计算的具体操作

### 删除流计算

仅删除流计算任务，由流计算写入的数据不会被删除，SQL 如下：
```sql
DROP STREAM [IF EXISTS] stream_name;
```


### 展示流计算

查看流计算任务的 SQL 如下：
```sql
SHOW STREAMS;
```

若要展示更详细的信息，可以使用
```sql
SELECT * from information_schema.`ins_streams`;
```

### 暂停流计算任务

暂停流计算任务的 SQL 如下：
```sql
PAUSE STREAM [IF EXISTS] stream_name; 
```
没有指定 IF EXISTS，如果该 stream 不存在，则报错。如果存在，则暂停流计算。指定了 IF EXISTS，如果该 stream 不存在，则返回成功。如果存在，则暂停流计算。

### 恢复流计算任务

恢复流计算任务的 SQL 如下。如果指定了 ignore expired，则恢复流计算任务时，忽略流计算任务暂停期间写入的数据。

```sql
RESUME STREAM [IF EXISTS] [IGNORE UNTREATED] stream_name; 
```

没有指定 IF EXISTS，如果该 stream 不存在，则报错。如果存在，则恢复流计算。指定了 IF EXISTS，如果 stream 不存在，则返回成功。如果存在，则恢复流计算。如果指定 IGNORE UNTREATED，则恢复流计算时，忽略流计算暂停期间写入的数据。