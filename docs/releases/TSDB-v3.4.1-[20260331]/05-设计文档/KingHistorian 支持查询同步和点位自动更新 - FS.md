# KingHistorian 支持查询同步和点位自动更新 - FS

## 1. 背景

taosX 的 KingHistorian 数据源当前支持两种采集模式：历史数据迁移（History）和实时数据订阅（RealTime）。在实际使用中存在以下需求：
1. **查询同步模式**：部分场景需要先补齐历史数据，再持续跟踪最新数据，但数据源不支持实时订阅推送。需要一种结合历史迁移与周期性增量查询的采集模式。
2. **实时点位自动更新**：在实时订阅模式下，KingHistorian Server 中可能动态新增点位。当前需要手动重启任务才能订阅新点位，需要支持自动发现并追加订阅新增点位。
本文档描述以上两个功能的行为规格。
任务：


## 2. 变更历史

| **日期** | **版本** | **负责人** | **主要修改内容** |
| --- | --- | --- | --- |
| 2026/1/4 | 0.1 | @杨志宇 | 初稿 |
| 2026/3/17 | 1.0 | @杨志宇 | 定稿 |

## 3. 定义

1. 点位（Tag/Variable）：KingHistorian 中的变量，代表一个数据采集点。
2. 变量组（VarGroup）：KingHistorian 中的变量组，树状结构，用于组织点位。

## 4. 行为说明

### 4.1 **查询同步模式（Query Sync）**

#### 4.1.1 **概述**

查询同步模式（`mode=query_sync` 或 `mode=sync`）结合历史数据迁移和周期性增量同步。任务启动后分为两个阶段执行：
- **阶段一（初始历史追赶）**：从用户指定的 `start` 时间到任务启动时的当前时间，完成历史数据迁移。
- **阶段二（周期性增量同步）**：每隔 `sync_interval` 查询一次增量数据并写入 TDengine，持续运行直到任务被取消。

#### 4.1.2 **DSN 参数**

| 参数 | 类型 | 必填 | 默认值 | 说明 |
| --- | --- | --- | --- | --- |
| `kinghist_task_mode` | string | 是 | - | 设为 `sync` 或 `query_sync` |
| `start` | datetime | 是 | - | 初始迁移的起始时间，如 `2023-10-01T00:00:00Z` |
| `time_range` | duration | 否 | `1d`( 24h) | 每次查询的时间窗口步长 |
| `restro` | duration | 否 | `0s` | 乱序回溯时长，仅在阶段二生效 |
| `interval` | number (ms) | 否 | `1000` | 两次查询之间的等待时长（毫秒），用于控制查询频率 |
| `sync_interval` | duration | 否 | `10s` | 周期性同步的间隔时间 |

DSN 示例：
```plaintext
kinghist://sa:sa@127.0.0.1:5678?mode=sync&start=2023-10-01T00:00:00Z&time_range=1h&restro=10m&interval=500&sync_interval=5m
```

#### 4.1.3 **执行流程**

```plaintext
任务启动
  │
  ├─ 1. 解析 DSN → KingHistConfig (mode=QuerySync)
  ├─ 2. 记录 current_ts = now()
  ├─ 3. 按数据类型分组，建立 IPC 连接
  │
  ▼
阶段一：初始历史追赶
  │
  ├─ 构造 HistQueryCriteria:
  │     start = config.start
  │     end   = current_ts
  │     restro = 0（阶段一不使用乱序回溯）
  │     time_range / interval 取自配置
  │
  ├─ 调用 collect_history() 完成 [start, current_ts) 的数据迁移
  ├─ 设置 last_sync_ts = current_ts
  │
  ▼
阶段二：周期性增量同步
  │
  └─ loop {
         - 等待 sync_interval（期间检查 cancel 信号）
         - 记录 current_ts = now()
         - 构造 HistQueryCriteria:
            start = last_sync_ts - restro
            end   = current_ts
         - 调用 collect_history() 完成增量数据迁移
         - 设置 last_sync_ts = current_ts
     }
```

#### 4.1.4 **阶段一与阶段二的差异**

| 特性 | 阶段一（初始追赶） | 阶段二（周期性同步） |
| --- | --- | --- |
| 时间范围 | `[config.start, current_ts)` | `[last_sync_ts - restro, current_ts)` |
| 乱序回溯 | 不使用（restro=0） | 使用配置的 restro 值 |
| 执行次数 | 一次 | 循环执行直到取消 |
| 触发方式 | 任务启动时立即执行 | 每隔 sync_interval 触发 |

#### 4.1.5 **前端参数映射**

前端通过 `collect_options.kinghist_task_mode` 下拉选择 `query_sync` 时，显示以下字段：

| 前端字段 | DSN 参数 | 说明 |
| --- | --- | --- |
| `collect_options.start` | `start` | 初始迁移起始时间 |
| `collect_options.step` | `time_range` | 查询窗口步长 |
| `collect_options.excursion` | `restro` | 乱序回溯时长（仅阶段二生效） |
| `collect_options.sync_interval` | `sync_interval` | 同步间隔，默认 10s |
| `collect_options.interval` | `interval` | 查询间隔（毫秒） |

注意：`end` 字段不在查询同步模式下显示，系统自动使用当前时间作为结束时间。

### 4.2 **实时模式点位自动更新**

#### 4.2.1 **概述**

在实时数据同步模式（`mode=realtime`）下，支持自动发现 KingHistorian Server 中新增的点位，并追加到当前订阅中，无需重启任务。该功能通过 `KingHistPointUpdater` 组件实现，采用"只追加不删除"（Append）策略。

#### 4.2.2 **DSN 参数**

| 参数 | 类型 | 必填 | 默认值 | 说明 |
| --- | --- | --- | --- | --- |
| `mode` | string | 是 | - | 设为 `realtime` |
| `min_elapsed` | number (ms) | 否 | `1000` | 订阅最小间隔时间（毫秒） |
| `update_mode` | string | 否 | `none` | 点位更新模式：`none`（不更新）或 `append`（只追加） |
| `update_interval` | number (s) | 否 | `600` | 点位更新轮询间隔（秒），`update_mode=append` 时生效 |

DSN 示例：
```plaintext
kinghist://sa:sa@127.0.0.1:5678?mode=realtime&min_elapsed=1000&update_mode=append&update_interval=10
```

#### 4.2.3 **启用条件**

点位自动更新仅当以下三个条件同时满足时启用：

| 条件 | 参数 | 值 |
| --- | --- | --- |
| 点位配置模式 | `dataset_mode` | `Query`（通过表达式查询点位） |
| 采集模式 | `collect_mode` | `RealTime` |
| 更新模式 | `update_mode` | `Append` |

当 `dataset_mode` 为 `CSV`（上传 CSV 文件）或 `Groups`（选择变量组）时，点位列表是静态的，不支持自动更新。

#### 4.2.4 **执行流程**

```plaintext
kinghist_to_taos() 入口
  │
  ├─ 解析 DSN → KingHistConfig
  ├─ 判断 enable_point_update（三个条件同时满足）
  ├─ 按 IpcDataType 分组，建立 IPC 连接
  │
  ▼
spawn_collectors()
  │
  ├─ 为每种 IpcDataType 启动一个 collector 线程（spawn_blocking）
  │   └─ 如果 enable_point_update=true，为每个 collector 创建 flume 通道 (update_tx, update_rx)
  │
  ├─ 启动 collect_realtime()（每种数据类型一个）
  │   ├─ 建立 KingHistorian 连接
  │   ├─ 调用 data_subscribe() 订阅初始点位
  │   ├─ 进入接收循环：
  │   │   ├─ 接收 SDK 推送数据 → 转换为 RecordBatch → 发送到 IPC Sink
  │   │   └─ 检查 update_rx 通道是否有新增点位
  │   │       └─ 如有，调用 data_subscribe() 追加订阅
  │   └─ 取消信号到达时退出
  │
  └─ 启动 KingHistPointUpdater::run()（仅 enable_point_update=true 时）
      └─ loop {
               - 等待 update_interval
               - 重新查询 KingHistorian Server 获取当前点位列表
               - 与已知点位集合对比，找出新增点位
               - 按 IpcDataType 分组
               - 通过 update_tx 通道发送给对应的 collector
               - 更新已知点位集合
         }
```

#### 4.2.5 **前端参数映射**

前端通过 `collect_options.kinghist_task_mode` 选择 `realtime` 时，显示以下字段：

| 前端字段 | DSN 参数 | 说明 |
| --- | --- | --- |
| `collect_options.min_elapsed` | `min_elapsed` | 订阅最小间隔（毫秒），默认 1000 |
| `collect_options.update_mode` | `update_mode` | 点位更新模式：`none` / `append` |
| `collect_options.update_interval` | `update_interval` | 点位更新间隔（秒），默认 10 |

选择 `update_mode=append` 后，`update_interval` 字段可见。

## 5. 性能

### 5.1 **查询同步模式**

- 阶段一复用 `collect_history()` 逻辑，性能与历史迁移模式一致。
- 阶段二每轮查询的数据量取决于 `sync_interval` 和数据产生速率，通常远小于阶段一。
- `interval` 参数控制查询频率，避免过快查询导致 KingHistorian Server 负载过高。

### 5.2 **实时点位自动更新**

- `KingHistPointUpdater` 的轮询操作为轻量级元数据查询，不涉及历史数据读取。
- 新增点位的 `data_subscribe()` 调用为增量操作，不影响已有订阅的性能。
- `update_interval` 默认 600 秒（DSN），前端默认 10 秒，可根据实际场景调整以平衡实时性与服务器负载。

## 6. 兼容性

- **查询同步模式**：新增模式，不影响现有 `history` 和 `realtime` 模式的行为。DSN 参数 `mode=sync` 和 `mode=query_sync` 均可识别。
- **实时点位自动更新**：`update_mode` 默认为 `none`，不启用自动更新，与现有行为完全一致。仅当用户显式设置 `update_mode=append` 时才启用。
- 前端配置界面新增相应字段，不影响已有任务的配置。

## 7. 运维

无

## 8. 使用场景

### 8.1 **查询同步模式**

| 场景 | 说明 |
| --- | --- |
| 历史补齐 + 持续跟踪 | 用户需要先将历史数据迁移到 TDengine，然后持续同步最新数据。适用于 KingHistorian 不支持实时订阅推送的场景。 |
| 定时增量同步 | 用户只需要定期（如每 5 分钟）将 KingHistorian 中的增量数据同步到 TDengine，不要求实时性。 |
| 乱序数据场景 | 数据源存在乱序到达的情况，通过 `restro` 参数在阶段二回溯一段时间，确保不遗漏乱序数据。 |

### 8.2 **实时点位自动更新**

| 场景 | 说明 |
| --- | --- |
| 动态扩展点位 | 生产环境中持续新增传感器/变量，需要自动纳入实时订阅，无需人工干预。 |
| 表达式匹配新点位 | 使用 `tag_name_mask` 表达式（如 `*Temperature*`）配置点位，新增的匹配点位自动被发现并订阅。 |

## 9. 约束和限制

### 9.1 约束

- 查询同步模式要求 `start` 参数必填，用于确定初始历史追赶的起始时间。
- 点位自动更新仅在 `dataset_mode=Query` 时可用。使用 CSV 文件或变量组选择方式配置点位时，不支持自动更新。
- 点位自动更新仅在实时模式（`mode=realtime`）下可用，查询同步模式不支持动态点位。

### 9.2 **限制**

- 查询同步模式的数据实时性受 `sync_interval` 限制，最小延迟为一个同步周期。
- 点位自动更新采用"只追加不删除"策略，KingHistorian Server 中删除的点位不会自动取消订阅。
- 点位自动更新的发现延迟取决于 `update_interval`，新增点位最迟在一个轮询周期后被发现。
- KingHistorian SDK 仅支持 Windows 平台，相关功能在非 Windows 平台上不可用。

## 10. 常见错误和排查

| 错误现象 | 可能原因 | 排查方法 |
| --- | --- | --- |
| 查询同步阶段一长时间无进展 | `start` 时间过早，历史数据量大 | 检查日志中的窗口进度，适当增大 `time_range` |
| 阶段二数据有遗漏 | 数据乱序到达，`restro` 设置过小 | 增大 `restro` 参数 |
| 点位自动更新未生效 | 三个启用条件未同时满足 | 确认 `dataset_mode=Query`、`mode=realtime`、`update_mode=append` |
| 新增点位未被发现 | `update_interval` 过大 | 减小 `update_interval` 值 |
| KingHistorian Server 负载过高 | `interval` 或 `update_interval` 过小 | 适当增大查询间隔或轮询间隔 |

## 11. 可观测性

- taos Explorer 的任务管理界面可查看查询同步任务的运行状态。
- 前端配置界面新增查询同步模式的选项和参数字段。
- 实时模式下选择 `update_mode=append` 后，`update_interval` 字段可见。

## 12. 安装和卸载

无

## 13. 文档

需要修改企业版文档

## 14. 参考文档

无

## 15. 附录

无
