# 需求说明：count window

## 1. 引言

### 1.1 术语与缩写名词

| 名词 | 描述 |
| --- | --- |
| count window | 称为计数窗口或数量窗口 当累计的记录数量达到预定义数量时，触发窗口计算逻辑 |

### 1.2 相关文档资料

| 文档 | 连接 |
| --- | --- |
| [流计算：支持 count window](https://taosdata.feishu.cn/wiki/EmmUwNAk5iCzEhkXrP8cL9wPnIc) |
| [批查询 count window](https://taosdata.feishu.cn/wiki/T6mLwjOJBiHFKIk86EOck833nSg) |

### 1.3 优先级要求

预期在二月底的 3.2.3.0 版本正式发布。

### 1.4 版本要求

在社区版支持。

## 2. 需求目标

计数窗口功能，流计算编码基本完成，查询编码刚刚开始，Function Spec 编写完成正在评审。本文再做一次分析，提出几个扩展需求。

| 需求目标 | 重要程度 | 需求描述 |
| --- | --- | --- |
| 支持数据排序 | 中等 | 窗口边界存在多条时间戳相同的记录时，支持对其进行排序，使结果没有随机性 仅查询支持 |
| 支持滑动窗口 | 中等 | 查询、流计算都支持 |

## 3. 功能需求

### 3.1 数据排序

在计数窗口的窗口边界，指 _wstart、_wend 时刻，可能存在多条时间戳相同的记录，如果不能指定这些记录的排序依据，那么分窗结果是不确定的，查询结果也是不确定的。以如下场景为例。
1. 超级表的不同子表：表 t1 和 t2 存在两条时间戳相同的记录
```sql
create table stb (ts timestamp, col1 int, col2 int) tags (tag1 int);
create table t1 using stb tags (1);
create table t2 using stb tags (2);
insert into t1 values(162999999999999, 1);
insert into t2 values(162999999999999, 2);
```

1. 子表存在复合主键：表 t1 和 t2 存在四条时间戳相同的记录
```sql
create table stb (ts timestamp, col1 int primary, col2 int) tags (tag1 int);
create table t1 using stb tags (1);
create table t2 using stb tags (2);
insert into t1 values(162999999999999, 1, 1);
insert into t1 values(162999999999999, 2, 1);
insert into t2 values(162999999999999, 1, 2);
insert into t2 values(162999999999999, 2, 2);
```


| 需求编号 | **需求描述** | 研发确认 |
| --- | --- | --- |
| R101 | 窗口边界存在多条时间戳相同的记录时，可以依据指定列进行排序，然后再按记录数量切分计数窗口，排序列支持标签列、普通列、tbname，需要扩展 SQL 语法 - 例如 `count_window(count_size) sort by tbname, col1` - 排序语法不同于 partition by - 当不输入“排序列”或者“排序列 + 时间戳列”不能唯一确定一条记录时，查询结果随机 - 仅查询支持，流计算不需支持 | 接受（实现方法可能不同） |

### 3.2 滑动窗口

FLINK 支持计数窗口，且有滑动窗口的概念，参考如下语法。
```java
DataStream<Tuple2<String, Integer>> sumed = windowCount.keyBy(0)
                .countWindow(10, 2)
                .sum(1);
```


| 需求编号 | **需求描述** | 研发确认 |
| --- | --- | --- |
| R201 | 在计数窗口中支持 sliding 语法 - 例如 `count_window(count_size, sliding_size)` - 查询、流计算都支持 | 接受 |

### 3.3 其他需求

| 需求编号 | **需求描述** | 研发确认 |
| --- | --- | --- |
| R301 | 在计数之前，支持对普通列进行筛选，被筛选出的记录不会被分窗函数累计 - 应已隐含实现，但明确提出要求，例如时间窗口已支持 `select _wstart, max(col1) from table where col1 <= 2 interval(1s)` - 查询、流计算都支持 | 接受 |
| R302 | partition by tbname - 查询不必须与 partition by tbname 一起使用 - 流计算必须与 partition by tbname 一起使用 | 接受 |
