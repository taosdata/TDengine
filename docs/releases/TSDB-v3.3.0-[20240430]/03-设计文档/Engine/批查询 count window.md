# 批查询 count window

## 1. 背景

目前批查询支持按时间、状态、事件来划分窗口，需要支持按照固定的数据行数来划分窗口

TD-28665

## 2. 定义

无

## 3. 变更历史

| 日期 | 版本 | 负责人 |
| --- | --- | --- |
| 2024/01/19 | 0.1 | 刘垚 |

## 4. 行为说明

### 4.1 语法：

```sql
window_clause: {
    SESSION(ts_col, tol_val)
  | STATE_WINDOW(col)
  | INTERVAL(interval_val [, interval_offset]) [SLIDING (sliding_val)] [FILL(fill_mod_and_val)]
  | EVENT_WINDOW START WITH start_trigger_condition END WITH end_trigger_condition
  | COUNT_WINDOW(count_val[, sliding_val])
}
```

COUNT_WINDOW：指定窗口类型为计数窗口，按固定的数据行数来划分窗口。
count_val：常量，是正整数，必须大于等于2，最大INT32_MAX。count_val表示每个count window包含的最大数据行数，总数据行数不能整除count_val时，最后一个窗口的行数会小于count_val。
sliding_val：是常量，表示窗口滑动的数量，类似于 interval的SLIDING 。必须大于等于1，小于等于count_val。

### 4.2 计数窗口语义：

- 默认将数据按时间戳排序，再按照count_val的值，将数据划分为多个窗口，然后做聚合计算。只有最后一个窗口，可能行数会少于 count_val。
- 支持Partition by、过滤等。
- 对于聚集函数、伪列，没有额外限制，与其他窗口相同。对于现有的伪列，支持_WSTART/_WEND/_WDURATION/_QSTART/_QEND。
- 对于包含子查询的场景，要求子查询必须输出主键的时间戳列或者是类似_wstart等输出时间戳的伪列，并且按该时间戳列有序。

### 4.3 测试场景

```sql {wrap}
select _wstart,count(*) from (select d,a from t1 order by d) interval(4s); --报错
DB error: Window query not supported, since the result of subquery not include valid timestamp column (0.001688s)

select _wstart,count(*) from (select ts from t1 group by ts) interval(4s); --报错
DB error: Window query not supported, since the result of subquery not include valid timestamp column (0.001327s)
select _wstart,count(*) from (select ts from t1 group by ts order by ts) count_window(4); --支持

select _wstart, _wend, sum(c1), max(c1) from (select * from t1 partition by tbname) count_window(4);--报错
DB error: COUNT_WINDOW requires valid time series input (0.001476s)
select _wstart, _wend, sum(c1), max(c1),count(*) from (select * from t1 partition by tbname order by ts) count_window(4);--支持

select _wstart,count(*) from (select _wstart,count(*) from t1 interval(2s)) count_window(4); --支持
```

## 5. 性能因素

- count_val 不建议太小，如果等于1，那么每个窗口只包含一条数据，写入N条数据，就会生成N个窗口，浪费计算资源。

## 6. 兼容性

无

## 7. 运维

无

## 8. 使用场景

### 8.1 普通场景

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

### 8.2 包含重复时间戳的场景

对于包含重复时间戳的场景，需要用户在子查询里使用order by，对数据排好序，以保证count window的结果的唯一性，即每次查询结果都一样不发生变化。select count(*) from (select * from st order by ts, tbname) count_window(3);

| t1 | 2023-12-29 17:49:10.000 | 7 | 8.1 |
| --- | --- | --- | --- |
| t2 | 2023-12-29 17:49:10.000 | 3 | 2.6 |
| t3 | 2023-12-29 17:49:10.000 | 9 | 4.8 |
| t4 | 2023-12-29 17:49:10.000 | 4 | 1.1 |
| t5 | 2023-12-29 17:49:10.000 | 4 | 7.6 |
| t6 | 2023-12-29 17:49.10.000 | 6 | 8.2 |

## 

## 9. 约束和限制

子查询至少要按照时间戳排序，可以额外增加其他列，这个限制于其他窗口相同。如果排序的列，有相同的值，count window划分可能是不稳定的。

## 10. 常见错误和排查

如果不符合上述限制，会报错，按错误修改语句即可。
1. count_val小于等于1时，查询时会报错，并提示count_val必须大于等于2，不能大于INT32_MAX。
2. 子查询不包含时间戳列时，会报错。
3. sliding_val小于1或者大于count_val时，会报错
