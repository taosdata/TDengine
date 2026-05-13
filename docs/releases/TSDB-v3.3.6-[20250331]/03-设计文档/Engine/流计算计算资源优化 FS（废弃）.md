# 流计算计算资源优化 FS（废弃）

## 1. 正式 FS 链接：

[TS-5468 [产品] 流计算计算资源优化 FS](https://taosdata.feishu.cn/wiki/OY6KwIiFhi37HqkH70RcnC12ngg)

## 2. 背景

JIRA：[TS-5468](https://jira.taosdata.com:18080/browse/TS-5468)
原始需求：为流计算增加特殊选项，窗口关闭时才做计算，而不是来新的数据就计算，节省计算资源
不需要缓存数据，当窗口结束时从 TSDB 读取数据进行计算

## 3. 变更历史

| 日期 | 版本 | 负责人 | 主要修改内容 |
| --- | --- | --- | --- |
| 2024/11/08 | 0.1 | 刘垚 |  |

## 4. 定义

实时数据：时间戳大于创建流计算时刻的数据。
历史数据：时间戳小于等于创建流计算时刻的数据。

## 5. 行为说明

### 5.1 语法

#### 5.1.1 自动批量重算

```sql
stream_options: {
 TRIGGER        [AT_ONCE | WINDOW_CLOSE | MAX_DELAY time | FORCE_WINDOW_CLOSE [recalculate time_val]]
 WATERMARK      time
 IGNORE EXPIRED [0|1]
 DELETE_MARK    time
 FILL_HISTORY   [0|1]
 IGNORE UPDATE  [0|1]
}
```

1. 如果指定了recalculate time_val，则经过 time_val 长时间，自动重算。重算所有已经算过的，乱序插入、修改、删除导致需要重算的窗口；会重新扫描重算窗口所在时间范围内的数据，并重新计算。time_val 必须要大于等于10s，否则报错，避免频繁的重算。如果不指定 recalculate time_val，不会自动重算。
2. 对于需要汇总的流计算（不包含 Partition by tbname），需要各个 vnode 上传重算区间信息，由汇总节点计算出总的重算区间，广播给所有的 vnode 重新计算。

#### 5.1.2 手动批量重算

```sql
recalculate stream_name range(start_time, end_time)
```

1. 会自动将 start_time 移动到该时间所在窗口的起始边界值，即 _wstart，自动将 end_time 移动到该时间所在窗口的结束边界值，即 _wend。流计算的算子从流计算结果表中获取窗口边界信息。
2. 时间区间必须是已经算过的窗口区间，超出的部分，不会计算。即 end_time 会取 end_time 所在窗口 _wend与计算过窗口 _wend 的最小值。
3. 对于需要汇总的流计算（不包含 Partition by tbname），需要各个 vnode 上传重算区间信息，由汇总节点计算出总的重算区间，广播给所有的 vnode 重新计算。

#### 5.1.3 窗口重算删除结果

在窗口重算前，流计算的算子从流计算结果表获取结果信息，然后从 tsdb 读取重算区间的数据，计算结果，发送结果；对于窗口起始边界发生变化的窗口，或者是没有重新生成的窗口，发送删除请求。

#### 5.1.4 默认触发模式

新创建的流，没有指定触发模式，那么 force_window_close 是默认模式；

### 5.2 实时数据计算

1. 确定扫描区间。
   - interval 通过系统时间来确定扫描区间；
   - 非 interval 窗口，通过扫描 wal，交给上层算子处理，发现有窗口关闭后，用关闭窗口的时间区间作为扫描区间。缺点是会多一次 IO，即多扫描一次数据。
2. 通过窗口起止时间，来确定扫描区间。通过扫描区间，读取 TSDB 数据。
3. 输出计算结果。对于未关闭的窗口，保留窗口状态的规则：
   - 非 interval 窗口，每个 Partition 最多保留 1 个
   - interval 不带 sliding 的窗口，不需要保留
   - interval 带 sliding的窗口，保留(interval/sliding - 1)个窗口状态。

### 5.3 历史数据计算

1. interval 窗口，历史数据与实时数据并行计算，互不影响。
2. 非 interval 窗口，检查每个 Partition 的最后一个窗口，以及实时数据的第一个窗口，判断两者是否是同一个窗口，如果是，则做 combine 操作。并给流计算结果表发送删除请求；如果不是，则忽略。

## 6. 性能

1. 因为在窗口关闭时计算，所以 cpu、io 资源使用量会成脉冲状
2. 与其他模式相比，会减少内存、IO
3. recalculate 设置的合理，与其他触发模式相比，会减少窗口重算的次数

## 7. 兼容性

无

## 8. 运维

无

## 9. 使用场景

1. 建流，不自动重算
```sql {wrap}
create database test vgroups 1;
use test;

create stable st(ts timestamp, a int, b int , c int) tags(ta int, tb int, tc int);
create table t1 using st tags(1, 1, 1);
create table t2 using st tags(2, 2, 2);

create stream streams1 trigger force_window_close IGNORE EXPIRED 1 IGNORE UPDATE 1 into streamt as select _wstart, sum(a), count(b), now from st partition by tbname interval(5s) fill(prev);
```

1. 建流，自动重算。每过 10 秒，重算所有包含修改、删除、乱序数据的计算过的窗口
```sql {wrap}
create database test vgroups 1;
use test;

create stable st(ts timestamp, a int, b int, c int) tags(ta int, tb int, tc int);
create table t1 using st tags(1, 1, 1);
create table t2 using st tags(2, 2, 2);

create stream streams1 trigger force_window_close recalculate 10s IGNORE EXPIRED 1 IGNORE UPDATE 1 into streamt as select _wstart, sum(a), count(b), now from st partition by tbname interval(5s) fill(prev);
```

1. 手动重算。
```sql {wrap}
create database test vgroups 1;
use test;

create stable st(ts timestamp, a int, b int, c int)tags(ta int, tb int, tc int);
create table t1 using st tags(1,1,1);
create table t2 using st tags(2,2,2);

create stream streams1 trigger force_window_close recalculate 10s IGNORE EXPIRED 1 IGNORE UPDATE 1 into streamt as select _wstart, sum(a), count(b), now from st partition by tbname interval(10s) fill(prev);

recalculate stream_name range("2024-11-08 15:05:06.799","2024-11-08 15:08:56.799")
```

1. 如果当前计算过的最大窗口是("2024-11-08 15:06:00.000","2024-11-08 15:06:09.999")，则重算的区间是 ("2024-11-08 15:05:00.000","2024-11-08 15:06:09.999")
2. 如果当前计算过的最大窗口是("2024-11-08 16:06:00.000","2024-11-08 16:06:09.999")，则重算的区间是("2024-11-08 15:05:00.000","2024-11-08 15:08:59.999")

## 10. 约束和限制

无

## 11. 常见错误和排查

1. 如果发现 force_window_close 模式的流的结果不对，可以手动下 sql 重算结果错误的窗口数据，或者等触发自动重算后，再看结果。
2. 如果发现流的 cpu、io 较高，可能是因为存在乱序，自动重算的间隔设置较短导致的

## 12. 可观测性

无

## 13. 安装和卸载

无

## 14. 文档

需要修改官网文档

## 15. 参考文档

无

## 16. 附录

无
