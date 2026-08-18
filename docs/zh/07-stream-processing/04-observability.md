---
sidebar_label: 可观测性与故障诊断
title: 可观测性与故障诊断
description: 使用系统视图查看流式计算的延迟、吞吐、历史计算和重算进度
---

TDengine 通过 `information_schema.ins_streams`、`information_schema.ins_stream_tasks` 和 `information_schema.ins_stream_recalculates` 提供流式计算运行状态。建议先在流级视图中发现异常，再进入任务级视图定位节点或任务，最后按需查看重算作业。

## 查看流的总体状态

下面的查询同时显示流状态、错误信息和主要运行指标：

```sql
SELECT stream_name,
       status,
       message,
       realtime_lag_ms,
       input_rows_per_sec_1m,
       output_rows_per_sec_1m,
       runner_result_latency_avg_1m_ms,
       history_progress_pct
FROM information_schema.ins_streams
ORDER BY stream_name;
```

| 指标 | 单位 | 含义 |
| --- | --- | --- |
| `realtime_lag_ms` | 毫秒 | 最慢有效入口 Reader 的实时处理延迟 |
| `input_rows_per_sec_1m` | 行/秒 | 最近一个完整 60 秒窗口内，Trigger 接纳的逻辑输入速率 |
| `output_rows_per_sec_1m` | 行/秒 | 最近一个完整 60 秒窗口内，所有最终结果 Runner 成功交付的结果速率 |
| `runner_result_latency_avg_1m_ms` | 毫秒 | 最近一个完整 60 秒窗口内，从 Runner 开始处理计算请求到形成逻辑结果的加权平均时间 |
| `history_progress_pct` | 百分比 | 建流时历史数据计算的完成进度，取值为 0 到 100 |

`realtime_lag_ms` 选择所有有效入口 Reader 中最慢的进度。已经追平但暂时没有新数据的 Reader 不会使该值持续增长。引用外部数据源的流没有 WAL 实时进度，该字段为 `NULL`。

输入速率统计过滤和路由后由流实际接纳的逻辑行，输出速率只统计成功交付的最终结果行。因此输入与输出不是同一层数据，不能用二者直接推导丢失率。窗口聚合、过滤以及一个输入产生多个结果等情况都可能使两者不同。

结果延迟不包含请求进入 Runner 前的排队和网络时间，也不包含结果形成后的写入或通知时间。

## 下钻到任务

当流状态异常、流级指标为 `NULL` 或需要定位具体节点时，查询任务视图：

```sql
SELECT stream_name,
       task_id,
       type,
       deploy_id,
       node_type,
       node_id,
       status,
       last_update,
       message,
       input_rows_per_sec_1m,
       output_rows_per_sec_1m,
       runner_result_latency_avg_1m_ms
FROM information_schema.ins_stream_tasks
WHERE stream_name = 'your_stream_name'
ORDER BY type, deploy_id, task_id;
```

任务级指标按职责提供：

- 入口 `Reader` 提供物理输入速率。它表示 Reader 实际读取和处理的输入行数。
- 负责最终结果交付的 `Runner` 提供输出速率和结果形成延迟。
- `Trigger`、计算数据 Reader 和非最终结果 Runner 的上述列为 `NULL`。

使用 `status` 查看任务是否正常，并使用 `last_update` 判断状态和指标是否仍然新鲜。流级视图没有 `last_update` 列。

## 查看历史计算进度

使用 `STREAM_OPTIONS(FILL_HISTORY)` 或 `STREAM_OPTIONS(FILL_HISTORY_FIRST)` 创建流后，`ins_streams.history_progress_pct` 显示初始历史范围的计算进度：

- `0` 到 `99`：历史计算尚未完成。
- `100`：历史计算已经完成。
- `NULL`：未启用历史计算，或当前没有有效进度信息。

该百分比表示已完成的原始历史时间范围覆盖率，不表示已经输出的结果行比例。

## 查看手动重算

下面的查询显示每个手动重算作业的范围、进度和状态：

```sql
SELECT stream_name,
       recalc_id,
       start,
       end,
       progress,
       status
FROM information_schema.ins_stream_recalculates
WHERE stream_name = 'your_stream_name'
ORDER BY start, recalc_id;
```

| 状态 | 含义 |
| --- | --- |
| `Pending` | 请求已接受，重算尚未开始 |
| `Running` | 重算已经开始，但尚未完成 |
| `Finished` | 重算已经完成，进度为 `100%` |
| `Failed` | 发生不可恢复错误，重算无法完成 |

滚动升级期间，如果旧版本任务只能提供重算进度，`status` 可能为 `NULL`，但 `progress` 仍可用。

已结束的重算记录从 mnode 首次观察到终态开始保留 1 小时，每个流最多保留 100 条。`Pending` 和 `Running` 记录不受该数量上限影响。记录只保存在内存中，进程重启后可能消失。

## 理解 `NULL` 和零值

一分钟指标统计最近 60 个已经结束的完整秒，不包含当前秒。任务启动、重启或重新部署后，需要形成首个完整窗口；在此之前相应指标为 `NULL`。

- 完整窗口内没有输入或输出时，对应速率为 `0`。
- Runner 在完整窗口内没有形成结果样本时，结果延迟为 `NULL`。
- 流级输出速率要求所有最终结果 Runner 都具有有效的完整窗口；任一相关 Runner 尚未就绪时，该字段为 `NULL`。
- 流级结果延迟还要求至少有一个结果样本，否则为 `NULL`。
- 指标不适用于当前任务类型时为 `NULL`。
- 某个字段为 `NULL` 不会使其他无关字段失效。例如，没有结果延迟样本时，输入速率仍可正常显示。
- 心跳短暂中断时，管理节点可能保留最后一次成功的指标快照。应结合任务的 `status` 和 `last_update` 判断其是否新鲜。
- 滚动升级期间，尚未升级的任务无法提供新增指标，对应列可能暂时为 `NULL`。

## 常见诊断顺序

1. 查询 `ins_streams` 的 `status` 和 `message`，先确认流是否处于正常状态。
2. 查看 `realtime_lag_ms` 是否持续增长。如果流使用外部数据源，该字段为 `NULL` 是正常行为。
3. 查看输入、输出和结果延迟。输入不为零但输出为零不一定是故障，窗口尚未关闭或过滤后没有结果时也会出现这种情况。
4. 查询 `ins_stream_tasks`，结合 `status`、`last_update`、`node_id` 和任务级指标定位异常任务。
5. 对历史计算或手动重算问题，分别查看 `history_progress_pct` 和 `ins_stream_recalculates`。

三个系统视图的完整字段定义见[系统信息](../05-tdengine-sql/09-system-info/01-meta.md#ins_streams)。
