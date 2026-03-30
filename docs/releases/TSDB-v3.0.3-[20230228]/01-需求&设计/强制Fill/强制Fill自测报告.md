# 强制Fill自测报告

### 1. 概述

此测试报告为研发自测报告，报告中涉及到的测试用例已经加入CI之中
测试脚本路径TDengine/tests/script/tsim/query/forceFill.sim 和 tsim/stream/fillIntervalValue.sim。

### 2. INTERVAL 语句测试

- 测试查询范围内有数据场景下，强制FILL与不强制FILL子句作用相同（填充）。
语句形如
```sql {wrap}
select avg(f1) from tba1 where ts >= '2022-04-26 15:15:00' and ts <= '2022-04-26 15:15:06' interval(1s) fill(value_f, 8.8);
select avg(f1) from tba1 where ts >= '2022-04-26 15:15:00' and ts <= '2022-04-26 15:15:06' interval(1s) fill(value, 8.8);
select avg(f1) from tba1 where ts >= '2022-04-26 15:15:00' and ts <= '2022-04-26 15:15:06' interval(1s) fill(null);
select avg(f1) from tba1 where ts >= '2022-04-26 15:15:00' and ts <= '2022-04-26 15:15:06' interval(1s) fill(null_f);
```

- 测试查询范围内无数据场景下，强制FILL会返回FILL结果，不强制FILL结果为空。
```sql {wrap}
select avg(f1) from tba1 where ts >= '2022-04-26 15:15:06' and ts <= '2022-04-26 15:15:10' interval(1s) fill(value, 8.8);
select avg(f1) from tba1 where ts >= '2022-04-26 15:15:06' and ts <= '2022-04-26 15:15:10' interval(1s) fill(value_f, 8.8);
select avg(f1) from tba1 where ts >= '2022-04-26 15:15:06' and ts <= '2022-04-26 15:15:10' interval(1s) fill(null);
select avg(f1) from tba1 where ts >= '2022-04-26 15:15:06' and ts <= '2022-04-26 15:15:10' interval(1s) fill(null_f);
```

- 测试查询范围较大，强制FILL结果多次返回场景。
```sql {wrap}
select avg(f1) from tba1 where ts >= '2022-04-26 15:16:00' and ts <= '2022-04-26 19:15:59' interval(1s) fill(value_f, 8.8);
select avg(f1) from tba1 where ts >= '2022-04-26 15:16:00' and ts <= '2022-04-26 19:15:59' interval(1s) fill(null_f);
```

测试情况
所有测试用例都通过。

### 3. INTERP 语句测试

- 测试查询范围内有数据场景下，强制FILL与不强制FILL子句作用相同（填充）。
语句形如
```sql {wrap}
select interp(f1) from tba1 range('2022-04-26 15:15:00','2022-04-26 15:15:06') every(1s) fill(value_f, 8.8);
select interp(f1) from tba1 range('2022-04-26 15:15:00','2022-04-26 15:15:06') every(1s) fill(value, 8.8);
select interp(f1) from tba1 range('2022-04-26 15:15:00','2022-04-26 15:15:06') every(1s) fill(null);
select interp(f1) from tba1 range('2022-04-26 15:15:00','2022-04-26 15:15:06') every(1s) fill(null_f);
```

- 测试查询范围内无数据场景下，强制FILL与不强制FILL子句作用相同（填充）。
```sql {wrap}
select interp(f1) from tba1 range('2022-04-26 15:15:06','2022-04-26 15:15:10') every(1s) fill(value, 8.8);
select interp(f1) from tba1 range('2022-04-26 15:15:06','2022-04-26 15:15:10') every(1s) fill(value_f, 8.8);
select interp(f1) from tba1 range('2022-04-26 15:15:06','2022-04-26 15:15:10') every(1s) fill(null);
select interp(f1) from tba1 range('2022-04-26 15:15:06','2022-04-26 15:15:10') every(1s) fill(null_f);
```

- 测试查询范围较大，强制FILL结果多次返回场景。
```sql {wrap}
select interp(f1) from tba1 range('2022-04-26 15:16:00','2022-04-26 19:15:59') every(1s) fill(value_f, 8.8);
select interp(f1) from tba1 range('2022-04-26 15:16:00','2022-04-26 19:15:59') every(1s) fill(null_f);
```

测试情况
所有测试用例都通过。

### 4. 流计算测试

- 测试强制FILL与不强制FILL子句作用相同。
语句形如
```sql {wrap}
create stream streams1a trigger at_once  into streamta as select  _wstart ts, count(*) c1 from t1 where ts > 1648791210000 and ts < 1648791413000 interval(10s) fillvalue_f, 100);
create stream streams4a trigger at_once  into streamt4a as select  _wstart ts, count(*) c1, concat(tbname, 'aaa') as pname, timezone()  from st where ts > 1648791000000 and ts < 1648793000000 partition by tbname interval(10s) fill(NULL_F);
```

测试情况
所有测试用例都通过。
