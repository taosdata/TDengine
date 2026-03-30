# 流计算计算资源优化 FS

## 1. 背景

JIRA：[TS-5468](https://jira.taosdata.com:18080/browse/TS-5468)
原始需求：为流计算增加特殊选项，窗口关闭时才做计算，而不是来新的数据就计算，节省计算资源
不需要缓存数据，当窗口结束时从 TSDB 读取数据进行计算。

## 2. 变更历史

| 日期 | 版本 | 负责人 | 主要修改内容 |
| --- | --- | --- | --- |
| 2024/11/08 | 0.1 | 刘垚 |  |
| 2024/11/22 | 0.2 | 刘垚 | 增加新模式 CONTINUOUS_WINDOW_CLOSE，不修改原有模式。本次先做包含 partition by tbname 的 interval |

## 3. 定义

1. 实时数据：数据时间戳大于创建流计算时刻的数据。
2. 历史数据：数据时间戳小于等于创建流计算时刻的数据。
3. 重算数据：乱序、修改、删除、延迟（依赖方案）的数据，是需要重算的数据。
4. 流计算结果正确性：流查询结果与SQL查询结果保持一致。当有乱序写入、修改、删除数据时，流计算结果与SQL查询不同，重算后会保持一致。

## 4. 行为说明

### 4.1 语法

#### 4.1.1 触发模式 CONTINUOUS_WINDOW_CLOSE 

1. 新创建的流，没有指定触发模式，那么 continuous_window_close 是默认模式
2. 支持历史数据的计算，即 fill history 1
3. 不能设置 Ignore update、ignore expried，如果设置报错

#### 4.1.2 在流计算状态的系统表中增加重算区间信息

对于每个流，包括正在重算的重算时间区间列表信息，以及正在重算的时间区间信息

#### 4.1.3 自动批量重算

```sql
stream_options: {
 TRIGGER        [AT_ONCE | WINDOW_CLOSE | MAX_DELAY time | FORCE_WINDOW_CLOSE | CONTINUOUS_WINDOW_CLOSE [recalculate rec_time_val] ]
 WATERMARK      time
 IGNORE EXPIRED [0|1]
 DELETE_MARK    time
 FILL_HISTORY   [0|1]
 IGNORE UPDATE  [0|1]
}
```

1. 如果指定了recalculate rec_time_val，在创建流计算成功后，经过 rec_time_val 长时间，检查自动重算区间，如果存在需要重算区间时自动进行重算，重算结束后等待 rec_time_val 长时间，开启下一次重算。如果重算的时间长度超过 rec_time_val，则在本次重算后，自动开启下一次重算。
2. 流计算引擎根据写入的乱序、更新、删除、延迟等情况自动计算出需要重算的时间区间，然后重新扫描数据，并重新计算结果。
3. rec_time_val必须要大于等于 10 分钟，否则报错，避免频繁的重算。
4. 如果不指定 recalculate rec_time_val，会按默认值60分钟开启重算。

#### 4.1.4 手动批量重算

```sql
recalculate stream stream_name range(start_time, end_time);
```

流计算引擎会自动根据窗口类型，以及 start_time、end_time 计算出需要重算的时间区间，然后重新扫描数据，并重新计算结果。手动重算为强制重算，整个指定区间内的数据都会被重新计算。用户可以查询流的系统表获取需要重算的区间信息，根据需要手动重算。这个是异步操作。不能同时发起多个重算，重复发起会返回失败。

#### 4.1.5 停止重算任务

```sql {wrap}
stop stream recalculation stream_name;
```

终止正在进行的重算任务，并且停止定时重算。

#### 4.1.6 暂停重算任务

```sql {wrap}
pause stream recalculation stream_name;
```

暂停正在进行的重算任务，并且停止定时重算。

#### 4.1.7 启动重算任务

```sql {wrap}
start stream recalculation stream_name；
```

如果有暂停的重算任务，则继续重算；如果没有，则进行下一步操作，恢复定时重算，当达到定时重算的时间间隔，会继续开启自动重算。

### 4.2 实时数据处理

所有的窗口（interval、session、state 等），每间隔固定的时间，扫描一次数据，做一次计算。
1. 对于 interval 窗口，以 interval 值为时间间隔
2. 其他类型窗口，需要增加语法，由用户指定时间间隔。
例如：时间间隔是 1 分钟，那么每 1 分钟计算一次，即用户每间隔 1 分钟，会看到新的、已关闭窗口的结果；对于未关闭的窗口，用户看不到结果。当数据时间与系统时间差别大时，会导致计算结果错误，需要用户设置WATERMARK，延迟窗口的计算时间，能缓解这个问题，但是不能解决。

### 4.3 历史数据数据

用户在建流后，流计算会实时增量输出历史数据计算结果。在计算历史数据计算过程中，用户写入、修改、删除历史数据，也会触发自动重算。

## 5. 性能

无

## 6. 兼容性

无

## 7. 运维

无

## 8. 使用场景

用户需要评估自己对计算结果准确性的接受程度，如果用户能接受近似结果，用户不关心数据乱序、修改、删除导致的结果变化，那么建议不开启自动重算；如果用户对结果的准确性要求很高，建议用户开启自动重算。
1. 不自动重算：如果用户只要求近似结果，并不要求精确的结果，可以不指定自动重算。后续，用户可以参考系统表的提示的重算区间信息，进行手动重算。下面是手动重算的建流示例：
```sql {wrap}
create database test vgroups 1;
use test;

create stable st(ts timestamp, a int, b int, c int) tags(ta int, tb int, tc int);
create table t1 using st tags(1, 1, 1);
create table t2 using st tags(2, 2, 2);

create stream streams1 trigger continuous_window_close into streamt as select _wstart, sum(a), count(b), now from st partition by tbname interval(5s) fill(prev);
```

1. 自动重算：当有修改、删除、乱序写入数据时，并且用户并不要求立即重算时，可以指定自动重算，并指定重算时间间隔，时间间隔长度，取决于用户对准确结果的容忍程度。在上述场景，用户想立即重算，应该使用at_once、window_close 模式。下面是自动重算的建流示例：
```sql {wrap}
create database test vgroups 1;
use test;

create stable st(ts timestamp, a int, b int , c int) tags(ta int, tb int, tc int);
create table t1 using st tags(1 ,1 ,1);
create table t2 using st tags(2, 2, 2);

create stream streams1 trigger continuous_window_close recalculate 10s into streamt as select _wstart, sum(a), count(b), now from st partition by tbname interval(5s) fill(prev);
```

1. 手动重算。当有修改、删除、乱序写入数据时，用户想重算，并且不想等系统的自动重算，可以手动下命令。下面是手动重算示例：
```sql {wrap}
create database test vgroups 1;
use test;

create stable st(ts timestamp, a int, b int , c int) tags(ta int, tb int, tc int);
create table t1 using st tags(1, 1, 1);
create table t2 using st tags(2, 2, 2);

create stream streams1 trigger continue_window_close recalculate 10s IGNORE EXPIRED 1 IGNORE UPDATE 1 into  streamt as select _wstart, sum(a), count(b), now from st partition by tbname interval(10s) fill(prev);

recalculate stream_name range("2024-11-08 15:05:06.799","2024-11-08 15:08:56.799");
```

## 9. 约束和限制

当前由于时间紧张，主要发布interval窗口，带fill功能。主要测试interval窗口。state 、session window后续版本再支持。当前只支持自动重算，手动重算（包括手动批量重算、停止、暂停、启动）后续版本再支持

## 10. 常见错误和排查

1. 如果发现 continuous_window_close 模式的流的结果不对，可以手动下 sql 重算结果错误的窗口数据，或者等触发自动重算后，再看结果，重算区间信息，可以在流的系统表中查询。
2. 如果发现流的 cpu、io 较高，可能是因为存在乱序，自动重算的间隔设置较短导致的

## 11. 可观测性

无

## 12. 安装和卸载

无

## 13. 文档

需要修改官网文档

## 14. 参考文档

无

## 15. 附录

无

## 16. 结论

11 月 22 日，开会讨论后，决定先做包含 partition by tbname 的 interval，原因
1. sma 的流一定包含 partition by tbname
2. nevados 的 10 个流中有 9 个包含 partition by tbname
做完之后要观察效果。
