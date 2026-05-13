# Event Window Test Report

### 1. 概述

此测试报告为研发自测报告，报告中涉及到的测试用例已经加入CI之中
测试脚本路径TDengine/tests/script/tsim/query/event.sim。

### 2. 基本功能测试

测试事件窗口的基本功能，主要考察窗口边界切分是否正确，应用在不同表上时的行为是否符合预期。
单表语句形如
```sql
select func_list from child_table event_window start with start_cond end with end_cond;
```

超级表汇总语句形如
```sql
select func_list from super_table event_window start with start_cond end with end_cond;
```

超级表按子表划分语句形如
```sql
select tbname, func_list from super_table partition by tbname event_window start with start_cond end with end_cond;
```

测试情况

|  | 单表 | 超级表汇总 | 超级表按子表划分 |
| --- | --- | --- | --- |
| 无窗口 | 通过 | 通过 | 通过 |
| 单行窗口 | 通过 | 通过 | 通过 |
| 多行窗口 | 通过 | 通过 | 通过 |
| 多窗口 | 通过 | 通过 | 通过 |

### 3. 组合子句测试

测试事件窗口子句和其他子句组合下的场景，包括where子句、partition by子句、order by子句、limit子句和嵌套查询。主要考察和其他子句组合时，作用范围和执行顺序是否符合预期。
组合语句形如
```sql
select func_list from table 
where where_cond 
partition by part_by_list 
event_window start with start_cond end with end_cond
order by order_by_list
limit limit_value;
```

测试情况

|  | where子句 | partition by子句 | order by子句 | limit子句 | 在内层查询 | 在外层查询 |
| --- | --- | --- | --- | --- | --- | --- |
| where子句 | -- | 通过 | 通过 | 通过 | 通过 | 通过 |
| partition by子句 | -- | -- | 通过 | 通过 | 通过 | 通过 |
| order by子句 | -- | -- | -- | 通过 | 通过 | 通过 |
| limit子句 | -- | -- | -- | -- | 通过 | 通过 |
| 在内层查询 | -- | -- | -- | -- | -- | 通过 |
| 在外层查询 | -- | -- | -- | -- | -- | -- |

### 4. 异常测试

测试事件窗口对语法、语义有误的语句的容错性，应该正常返回相应的错误。

|  | 结果 |
| --- | --- |
| 没有有效函数 | 通过 |
| 子句位置有误 | 通过 |
| 不可以和group by组合使用 | 通过 |
| 不可以和fill组合使用 | 通过 |
