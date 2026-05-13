# Event Window

### 1. 背景

TDengine 已经支持按时间间隔划分窗口（INTERVAL）、按状态量变化划分窗口（STATE_WINDOW）和按时间戳连续性划分窗口（SESSION），但是还不能支持按用户指定的条件来决定窗口的边界。所以我们需要一种新的窗口类型，这种窗口可以按照用户指定的条件来决定开启和结束的边界，我们称之为事件窗口。

### 2. 语法

```sql
window_clause: {
    SESSION(ts_col, tol_val)
  | STATE_WINDOW(col)
  | INTERVAL(interval_val [, interval_offset]) [SLIDING (sliding_val)] [FILL(fill_mod_and_val)]
  | EVENT_WINDOW START WITH start_trigger_condition END WITH end_trigger_condition
}
```

在现有的window_clause基础上增加EVENT_WINDOW子句，用来使用事件窗口查询。

### 3. 语义

#### 3.1 窗口子句公共语义

EVENT_WINDOW子句是窗口子句的一部分，所以在语义上继承窗口子句现有的位置和约束。
TDengine按如下方式处理窗口子句：
- 窗口子句位于标签切分子句之后，GROUP BY子句之前，且不可以和GROUP BY子句一起使用。
- 窗口子句将数据按窗口进行切分，对每个窗口进行SELECT列表中的表达式的计算，SELECT列表中的表达式只能包含：
  - 常量。
  - 聚集函数。
  - 包含上面表达式的表达式。

#### 3.2 事件窗口语义

事件窗口根据开始条件和结束条件来划定窗口，当start_trigger_condition满足时则窗口开始，直到end_trigger_condition满足时窗口关闭。start_trigger_condition和end_trigger_condition可以是任意 TDengine 支持的条件表达式，且可以包含不同的列。
事件窗口可以仅包含一条数据。即当一条数据同时满足start_trigger_condition和end_trigger_condition，且当前不在一个窗口内时，这条数据自己构成了一个窗口。
事件窗口无法关闭时，不构成一个窗口，不会被输出。即有数据满足start_trigger_condition，此时窗口打开，但后续数据都不能满足end_trigger_condition，这个窗口无法被关闭，这部分数据不够成一个窗口，不会被输出。
如果直接在超级表上进行事件窗口查询，TDengine 会将超级表的数据汇总成一条时间线，然后进行事件窗口的计算。
如果需要对子查询的结果集进行事件窗口查询，那么子查询的结果集需要满足按时间线输出的要求，且可以输出有效的时间戳列。

### 4. 例子

对子表计算事件窗口，窗口以 f1 >= 0 开始，以 f3 = true 结束。
```sql
taos> select count(*) from tba1 event_window start with f1 >= 0 end with f3 = true;
       count(*)        |
========================
                     2 |
                     4 |
Query OK, 2 row(s) in set (0.004298s)
```

对超级表的每个子表计算事件窗口，窗口以 f1 >= 0 开始，以 f3 = true 结束。
```sql
taos> select tbname, count(*) from sta partition by tbname event_window start with f1 >= 0 end with f3 = true;
                                                tbname                                                |       count(*)        |
===============================================================================================================================
 tba1                                                                                                 |                     2 |
 tba1                                                                                                 |                     4 |
 tba2                                                                                                 |                     2 |
 tba2                                                                                                 |                     4 |
Query OK, 4 row(s) in set (0.009039s)
```
