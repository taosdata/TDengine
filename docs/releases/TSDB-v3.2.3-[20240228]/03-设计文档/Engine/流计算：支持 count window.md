# 流计算：支持 count window

## 1. 背景

流计算目前支持按时间、状态、事件来划分窗口，需要支持按照固定的数据行数来划分窗口。

TD-22023

## 2. 定义

语法：
```sql
window_clause: {
    SESSION(ts_col, tol_val)
  | STATE_WINDOW(col)
  | INTERVAL(interval_val [, interval_offset]) [SLIDING (sliding_val)] [FILL(fill_mod_and_val)]
  | EVENT_WINDOW START WITH start_trigger_condition END WITH end_trigger_condition
  | COUNT_WINDOW(count_val[, sliding_val])
}
```

COUNT_WINDOW：指定窗口类型为count window，按固定的数据行数来划分窗口。
count_val：常量，是正整数，必须大于等于2，最大INT32_MAX。count_val表示每个count window包含的最大数据行数，总数据行数不能整除count_val时，最后一个窗口的行数会小于count_val。
sliding_val：是常量，表示窗口滑动的数量，类似于 interval的SLIDING 。

## 3. 变更历史

| 日期 | 版本 | 负责人 | 修改内容 |
| --- | --- | --- | --- |
| 2024/01/02 | 0.1 | 刘垚 | 基础版本 |
| 2024/01/24 | 0.2 | 刘垚 | 支持sliding_val |

## 4. 行为说明

- 将数据按时间戳排序，再按照count_val的值，将数据划分为多个窗口，然后做聚合计算。流计算不支持时间戳重复的场景，即不支持多条时间线合成一条时间线的场景，详细见后面“**约束和限制**”。
- 语义上，会把历史数据和实时数据统一处理，当历史数据最后一个窗口不足count_val，会和实时数据合并。所以只有实时数据的最后一个窗口，可能行数会少于count_val。
- 支持Partition by、自定义表名、自定义TAG、过滤等。
- 对于聚集函数、伪列，没有额外限制，与其他流计算窗口相同。
- 建流的配置选项中，ignore expired必须为1，否则建流失败并报错。原因：避免因为一个乱序写入，导致后续所有的窗口重算，这个需要重新读取大量数据，需要重算大量窗口，成本极高。
- 建流的配置选项中，watermark不能是0，否则建流失败并报错。这个会影响过期数据的判断，用户必须显示的指定，必须是大于0的值。

## 5. 性能因素

- count_val不建议太小，如果等于1，那么每个窗口只包含一条数据，写入N条数据，就会生成N个窗口，会导致IO资源浪费。
- 数据乱序写入，会导致重算多个窗口：乱序数据所属窗口以及后续的窗口。
- 删除数据，会导致重算多个窗口：删除数据所属窗口以及后续的窗口。

## 6. 兼容性

无

## 7. 运维

无

## 8. 使用场景

以数据量为维度，对数据进行聚合分析，下图以count_val是3为例：

| 窗口 |
| --- |
| 2023-12-29 17:49:10.000 | 11 | 3.1 |
| 2023-12-29 17:49:11.000 | 2 | 2.6 |
| 2023-12-29 17:49:12.000 | 32 | 4.8 |
| 2023-12-29 17:49:19.000 | 48 | 9.1 |
| 2023-12-29 17:49:20.000 | 51 | 7.6 |
| 2023-12-29 17:49:26.000 | 60 | 8.2 |
| 2023-12-29 17:49:35.000 | 71 | 1.2 |
| 2023-12-29 17:49:36.000 | 8 | 3.6 |
| 2023-12-29 17:49:48.000 | 93 | 7.9 |
| 2023-12-29 17:49:58.000 | 101 | 7.6 |
| 2023-12-29 17:49:59.000 | 112 | 3.1 |

## 9. 约束和限制

### 9.1 流计算结果被覆盖

流计算的结果会保存到表中，所以时间戳不能重复，否则会发生覆盖。如下场景会发生结果覆盖（也就是结果丢失），count_val是3，数据源是超级表且没有partition by tbname：

| 子表名 | 窗口 |
| --- | --- |
| t1 | 2023-12-29 17:49:10.000 | 7 | 8.1 |
| t2 | 2023-12-29 17:49:10.000 | 3 | 2.6 |
| t3 | 2023-12-29 17:49:10.000 | 9 | 4.8 |
| t4 | 2023-12-29 17:49:10.000 | 4 | 1.1 |
| t5 | 2023-12-29 17:49:10.000 | 4 | 7.6 |
| t6 | 2023-12-29 17:49.10.000 | 6 | 8.2 |

如上述表格，窗口1和窗口2的起始时间戳相同，那么在同一个子表中，会认为这是一次update 操作，会用窗口2的结果覆盖掉窗口1的结果，加入partition by tbname，上述问题就不存在了。

### 9.2 流计算的源：

1. 子表、普通表。
2. 如果要应用在超级表上，需要搭配 `partition by tbname, [tag | column]`，tbname是必须有的，tag和column是可选的。强制流计算引擎在每个时间线独立处理，避免时间线合并，从而避免产生时间戳相同的结果。见示例：
```sql {wrap}
taos> CREATE STREAM streams3 
                    TRIGGER at_once IGNORE EXPIRED 0 IGNORE UPDATE 0 
                    INTO streamt3                              
                    AS 
                       SELECT _wstart AS s, count(*) c1, sum(b), max(c) 
                       FROM   st 
                       COUNT_WINWOW(9);

DB error: Count window for stream on super table must patitioned by table name (0.004874s)

taos> CREATE STREAM streams3 
                    TRIGGER at_once IGNORE EXPIRED 1 IGNORE UPDATE 0 WATERMARK 100s 
                    INTO streamt3 
                    AS
                       SELECT _wstart AS s, count(*) c1, sum(b), max(c) 
                       FROM st 
                       PARTITION BY tbname, ta, a 
                       COUNT_WINWOW(9);
Create OK, 0 row(s) affected (0.174491s)

```

## 10. 常见错误和排查

在建流的时候，如果不符合上述限制，会报错，按错误修改语句即可。
1. 对于超级表，没有带partition by tbname，建流会报错，并提示需包含partition by tbname
2. 对于ignore_expired = 0 或 watermark=0，建流会报错，并提示ignore_expired必须为1， watermark必须大于0
3. count_val小于等于1时，建流会报错，并提示count_val必须大于等于2，不能大于INT32_MAX。
