# tmq-to-local 备份点生成机制

## 概念说明

| 术语     | 说明                                                                                               |
| -------- | -------------------------------------------------------------------------------------------------- |
| 增量备份 | 通过 TDengine 数据订阅（TMQ），将数据库自上次备份以来的所有变更备份为本地文件                      |
| 初始备份 | 增量备份中的第一次备份，此时 topic 尚不存在，需要创建 topic 并从最早的 offset 开始消费全部历史数据 |
| 全量备份 | 一种结果而非行为——将初始备份与后续所有增量备份的文件按顺序恢复，即可还原出源数据库的全部数据       |
| 备份计划 | 按时间计划和指定的备份配置，周期性地执行增量备份任务                                               |
| 备份点   | 备份计划每启动一次增量备份，视为一个备份点，使用时间戳进行标记                                     |

## 备份点的两种生成模式

备份点的生成方式由 `BackupPointGenMode` 枚举定义（`conf.rs:280-286`），根据 DSN 中是否包含 `upcoming` 参数自动判断：

```rust
pub enum BackupPointGenMode {
    /// 备份计划模式：消费到 latest offset 后停止，备份点时间戳 = 任务开始时间
    ByOffset,
    /// 手动备份模式：超时无数据后停止，备份点时间戳 = 文件关闭时间
    ByTimeout,
}
```

- DSN 中包含 `upcoming` 参数 → `ByOffset`（计划备份）
- DSN 中不包含 `upcoming` 参数 → `ByTimeout`（手动备份）

### ByOffset 模式（计划备份）

适用于备份计划场景，由调度系统周期性触发。

**备份点时间戳**：任务开始执行时的 `upcoming` 时间。

**完成条件**（满足任一即可）：

1. **Offset 精确完成**：所有 vgroup 的 `current` offset 追上各自的 `end_offset`
2. **时间兜底完成**：当前任务的运行时间达到一个备份周期（`interval`）

引入时间兜底的原因：`position()` 方法通过查询 `information_schema.ins_subscriptions` 获取 offset，依赖与 TDengine 的网络通信。在网络抖动或服务端响应慢的场景下，`position()` 可能因超时（`deadline has elapsed`）持续失败，导致无法通过 offset 判断完成状态。时间兜底确保即使 `position()` 始终失败，备份点仍能按周期正常生成。

**流程**：

1. 调度系统设置 `upcoming` 时间，任务等待至该时刻开始执行（`lib.rs:68-74`）
2. `upcoming` 被更新为实际开始时间 `Utc::now()`（`lib.rs:72-74`）
3. 创建 `ZFileMan` 时，将 `upcoming` 作为文件时间戳（`lib.rs:96`）
4. 生成的备份文件名中直接包含该时间戳：`{topic}-{upcoming_millis}-{vgroup_id}-{index}.z`
5. BackupWorker 在启动时记录任务开始时间 `task_start`
6. 每个 BackupWorker 在首次收到消息时，尝试通过 `position()` 获取各 vgroup 的 `latest` offset 作为终止点（`lib.rs:267-278`）
7. 在每次收到消息后，检查以下两个退出条件：
   - **Offset 完成**：`position()` 成功时，若某个 vgroup 的 `current == end_offset`，标记该 vgroup 完成；所有 vgroup 完成后退出
   - **时间兜底**：若 `task_start.elapsed() >= interval`，直接退出
8. 若 `position()` 调用失败，记录警告日志并跳过本轮 offset 检查，继续正常消费和写入数据，等待下一条消息时重试或等待时间兜底触发退出

**`position()` 失败时的容错处理**：

`position()` 失败不再通过 `?` 向上传播错误，而是：

- 记录 `warn` 级别日志，包含失败的 vgroup_id 和错误信息
- 跳过本轮 offset 完成检查，消息的接收和写入不受影响
- 在后续消息到达时自动重试 `position()`
- 若持续失败，由时间兜底条件保证任务正常退出

**周期调度**：当 `self_repeat = true` 时，任务在循环中反复执行，每次完成后计算下次 upcoming：`next_upcoming = current_upcoming + interval`（`lib.rs:54-61`）。默认间隔为 10 分钟。

**数据边界说明**：当通过时间兜底退出时，当前备份点的数据边界不再是精确的 WAL offset，而是截至该时刻已消费到的数据。这不会导致数据丢失或重复，因为下一个备份周期会从 TMQ commit 的位置继续消费。

### ByTimeout 模式（手动备份）

适用于用户手动触发的一次性备份。

**备份点时间戳**：备份文件关闭时的当前时间。

**流程**：

1. 创建 `ZFileMan` 时，`ts` 为 `None`（因为 DSN 中没有 `upcoming`）
2. 文件创建时使用 `Utc::now()` 作为临时文件名中的时间戳（`taoz/mod.rs:178`）
3. BackupWorker 以 500ms 间隔轮询 TMQ 消息（`lib.rs:223`）
4. 当超过 `consumer.default_timeout()` 未收到新消息时，认为数据已消费完毕，任务退出（`lib.rs:228-234`）
5. 文件关闭（轮转或 shutdown）时，由于 `name.1` 为 `None`，使用 `Utc::now()` 生成最终时间戳并重命名文件（`taoz/mod.rs:293-317`）

## 备份文件的生命周期

### 文件命名

格式：`{topic}-{timestamp_millis}-{vgroup_id}-{index}.z`

- `topic`：备份使用的 TMQ topic 名称
- `timestamp_millis`：备份点时间戳（毫秒）
- `vgroup_id`：TDengine vgroup ID，每个 vgroup 独立写入各自的文件
- `index`：文件序号，从 1 开始递增（单个 vgroup 在一次备份中可能产生多个文件）

### 文件轮转

当满足以下任一条件时，当前文件关闭并创建新文件（`taoz/mod.rs:273-339`）：

- 文件大小 ≥ `max_file_size`（默认 1 GB）
- 距上次写入超过 `timeout` 且文件非空

轮转时：

1. flush 并关闭当前 writer
2. 若为 ByTimeout 模式（`name.1` 为 `None`），用当前时间重命名文件
3. 若配置了 `move_to`，将文件移动到目标目录
4. 创建新文件，`index` 递增

### 文件管理（ZFileMan）

`ZFileMan`（`lib.rs:388-508`）为每个 vgroup 维护独立的 `ZFile` writer（通过 `DashMap<i32, Mutex<ZFile>>`），确保不同 vgroup 的数据写入互不干扰。

任务结束时调用 `ZFileMan::shutdown()`（`lib.rs:167`），依次 flush 并关闭所有 vgroup 的文件。

## 增量备份的实现基础

增量备份依赖 TDengine TMQ 的 offset 管理机制：

1. **group.id 持久化**：每个备份计划使用固定的 `group.id`（由 task_id、from DSN、to DSN 哈希生成），TDengine 服务端记录该 group 的消费进度（`conf.rs:63-73`）
2. **offset commit**：每处理完一条消息后立即 commit offset（`lib.rs:342`），确保进度持久化
3. **auto.offset.reset = earliest**：首次订阅时从最早的数据开始消费（`conf.rs:146-147`）
4. **experimental.snapshot.enable = true**：启用快照消费，支持获取历史存量数据（`conf.rs:150-151`）

因此：

- 初始备份：topic 不存在 → 创建 topic → 从 earliest 消费全部数据
- 增量备份：topic 已存在 → 从上次 commit 的 offset 继续消费新增数据

## 整体流程图

```
备份计划触发 / 手动触发
        │
        ▼
  等待 upcoming 时间到达（仅 ByOffset 模式）
        │
        ▼
  判断是否初始备份（topic 是否存在）
        │
   ┌────┴────┐
   │ 是      │ 否
   ▼         ▼
 创建 topic  跳过
 创建目录
   │         │
   └────┬────┘
        │
        ▼
  创建 TMQ consumers（按 vgroup 数量）
        │
        ▼
  创建 ZFileMan（管理各 vgroup 的备份文件）
        │
        ▼
  启动 BackupWorker（每个 consumer 一个），记录 task_start
        │
        ▼
  ┌──────────────────────────────────────────────────┐
  │  轮询 TMQ 消息                                     │
  │  ├─ 收到消息 → 写入 ZFile → commit                  │
  │  │   ├─ ByOffset:                                  │
  │  │   │   ├─ position() 成功 → 检查 offset 完成      │
  │  │   │   ├─ position() 失败 → warn 日志，跳过检查    │
  │  │   │   └─ 检查时间兜底: elapsed >= interval → 退出 │
  │  │   └─ ByTimeout: 超时无数据 → 退出                 │
  │  └─ 无消息:                                         │
  │      ├─ ByOffset: 检查时间兜底 → 退出                │
  │      └─ ByTimeout: 超时 → 退出                      │
  └──────────────────────────────────────────────────┘
        │
        ▼
  关闭所有 ZFile（flush + shutdown + 重命名）
        │
        ▼
  备份点生成完毕
        │
        ▼
  （若为周期计划）计算 next_upcoming，进入下一轮
```

## 关键源码位置

| 文件                              | 内容                                           |
| --------------------------------- | ---------------------------------------------- |
| `crates/tmq-to-local/src/lib.rs`  | 任务入口、BackupWorker、ZFileMan               |
| `crates/tmq-to-local/src/conf.rs` | BackupConfig、BackupPointGenMode、TMQ DSN 构建 |
| `taosx-core/src/taoz/mod.rs`      | ZFile 文件读写、轮转、重命名逻辑               |
