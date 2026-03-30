# 获取单行选择函数所在行的其他列值 RS

## 一、需求来源

### 1. 报表

在制作报表时，按照时间窗口取得平均值、最大值、最小值、最大值的时间、最小值的时间。
```sql
-- 支持
select ts, f, max(v) from tb;

-- 不支持，ts 对应多个值
select ts, f, max(v), min(v) from tb;
select ts, f, max(v), avg(v) from tb;

-- 支持
select ts, f, max(v) from tb interval(5m);

-- 不支持，ts 对应多个值
select ts, f, max(v), min(v) from tb interval(5m);
select ts, f, max(v), avg(v) from tb interval(5m);
```

### 2. 最近非空值

```sql
-- 数据模型
taos> select * from db.t1;
           ts            |     v1      |     v2      |
======================================================
 2024-08-02 13:43:12.849 | NULL        |          10 |
 2024-08-02 13:43:18.892 |          20 | NULL        |
Query OK, 2 row(s) in set (0.003572s)

-- 没办法得到 v1 和 v2 最近非空值所对应的 ts
taos> select last(ts, v1), last(ts, v2) from t1;
        last(ts)         |  last(v1)   |        last(ts)         |  last(v2)   |
================================================================================
 2024-08-02 13:43:18.892 |          20 | 2024-08-02 13:43:18.892 |          10 |
Query OK, 1 row(s) in set (0.004947s)
```

### 3. 加速查询

南京信普科技提到，创建 TSMA 以加速查询。但 TSMA 虽支持 max、min 函数，却没办法获得最大值、最小值所在行的时间戳和其他列。

## 二、需求目标

### 4. 需求描述

1. 增加函数或者表达式，获取单行选择函数所在行的其他列值，现有的单行选择函数如下
   - MAX
   - MIN
   - LAST
   - LAST_ROW
   - FIRST
   - MODE
2. 支持与平均值、Percentile 等聚合函数混合使用
3. 支持在 TSMA 中使用
4. 支持在流计算中使用
5. 支持在表和超级表中使用（Partiton By）

### 5. 建议语法

#### 5.1 举例一

推荐采用此语法，组合运算、别名设置都非常方便。
```sql
COL(func_name(expr1), expr2)
```

**返回数据类型**：expr2 的数据类型
**应用举例**
```sql
select max(v), col(max(v), ts), min(v), col(min(v), ts), avg(v) from tb;
select max(v), col(max(v), log(10, f)), min(v), avg(v) from tb;
select max(v) max_v, col(max(v), ts) as max_ts, avg(v) avg_v from tb interval(1m);
```

#### 5.2 举例二

```sql
GET_MAX_COL(expr1, expr2)
GET_MIN_COL(expr1, expr2)
GET_LAST_COL(expr1, expr2)
GET_LAST_ROW_COL(expr1, expr2)
GET_FIRST_COL(expr1, expr2)
……
```

**返回数据类型**：expr2 的数据类型
**应用举例**
```sql
select max(v), get_max_col(v, ts), min(v), get_min_col(v, ts), avg(v) from tb;
select max(v), get_max_col(v, log(10, f)), min(v), avg(v) from tb;
select max(v) max_v, get_max_col(v, ts) as max_ts, avg(v) avg_v from tb interval(1m);
```

#### 5.3 举例三

和 LAST_ROW 类似，可以输入多个参数
```sql
GET_MAX_COL(expr1, expr2, ……)
GET_MIN_COL(expr1, expr2, ……)
……
```

**应用举例**
```sql
select get_max_col(v, ts), get_min_col(v, ts), avg(v) from tb;
select get_max_col(v, log(10, f)), min(v), avg(v) from tb;
select get_max_col(v, ts) <can not set alias>, avg(v) avg_v from tb interval(1m);
```
