# AVEVA Historian 断链重连方案

## 1. 问题分析

### 1.1 用户场景

用户配置了 `synchronize` 模式 + `Runtime.dbo.Live` 表的 DataIn 任务。任务运行后某个时间点后停止写入新数据，任务状态显示 Idle。

### 1.2 根因定位

阅读 `source-historian` 全部代码后确认：**当前代码完全没有断链重连机制**。连接断开后查询立刻报错，错误通过 `?` 向上传播，导致整个任务函数退出。

以下三个 worker 函数均存在此问题：

#### `sync_live`（worker/mod.rs:208-291）— 用户命中的场景

```rust
// 在函数开始时建立唯一一次连接
let mut client = HistorianQuery::try_connect(task_config.clone().connect).await?;

// 主循环中，任何查询错误都会导致函数直接退出
loop {
    let stream = client.select_from_live(task_config.tags.clone()).await?;  // ← 断链时这里报错，函数退出
    let batch = to_record_batch(stream).await?;
    tx.send_async(batch).await?;
    tokio::time::sleep(task_config.retrieve_interval.to_std().unwrap()).await;
}
```

#### `sync_history`（worker/mod.rs:78-206）

同样在循环中使用 `client.select_from_history(...)?.await?`，断链后直接退出。

#### `migrate_history` 的 Consumer（worker/mod.rs:560-709）

Consumer 在循环中使用 `client.select_from_history(...)?.await?`，断链后整个 consumer 退出。不过 migrate 是一次性任务且有 breakpoint 支持，优先级较低。

### 1.3 配置缺失

前端已配置了 `connection_timeout`、`reconnect_times`、`reconnect_interval` 三个参数（见 `12-avevaHistorian.ts`），`docs/dev/design/config.md` 中也有文档描述，但后端 `ConnectConfig` 完全没有解析和使用这些参数。

| 参数 | 前端字段 | 后端 ConnectConfig | 状态 |
|------|----------|-------------------|------|
| 连接超时 | `connection_timeout` | ❌ 未实现 | 需新增 |
| 重连尝试次数 | `reconnect_times` | ❌ 未实现 | 需新增 |
| 重连间隔 | `reconnect_interval` | ❌ 未实现 | 需新增 |

---

## 2. 修改方案

### 2.1 总体思路

1. **`ConnectConfig` 新增三个重连参数**，从 DSN 解析
2. **`HistorianQuery` 新增 `reconnect` 方法**，封装带重试的重连逻辑
3. **`sync_live` / `sync_history` 的查询循环中加入错误捕获与重连**
4. **`migrate_history` 的 Consumer 也加入重连**（优先级较低，可同批或后续迭代）

### 2.2 ConnectConfig 改造

**文件**：`src/config/mod.rs`

在 `ConnectConfig` 结构体中新增字段：

```rust
#[derive(Debug, Clone)]
pub struct ConnectConfig {
    pub(crate) host: String,
    pub(crate) port: u16,
    pub(crate) username: String,
    pub(crate) password: String,
    pub encryption: Option<tiberius::EncryptionLevel>,
    // --- 新增 ---
    pub connection_timeout: u64,    // 秒，默认 120
    pub reconnect_times: usize,     // 最大重试次数，默认 10
    pub reconnect_interval: u64,    // 重试间隔秒数，默认 5
}
```

在 `ConnectConfig::from_dsn` 中解析：

```rust
let connection_timeout = utils::parse_key_in_dsn::<u64>(dsn, "connection_timeout")?
    .unwrap_or(120)
    .max(1);

let reconnect_times = utils::parse_key_in_dsn::<usize>(dsn, "reconnect_times")?
    .unwrap_or(10)
    .max(1);

let reconnect_interval = utils::parse_key_in_dsn::<u64>(dsn, "reconnect_interval")?
    .unwrap_or(5)
    .max(1);
```

在 `ConnectConfig::connect` 方法中使用 `connection_timeout`：

```rust
pub async fn connect(&self) -> anyhow::Result<Client<Compat<TcpStream>>> {
    let mut config = tiberius::Config::new();
    // ... 现有配置 ...

    let tcp = tokio::time::timeout(
        Duration::from_secs(self.connection_timeout),
        TcpStream::connect(config.get_addr()),
    )
    .await
    .map_err(|_| anyhow::anyhow!(
        "connection to {}:{} timed out after {}s",
        self.host, self.port, self.connection_timeout
    ))??;

    tcp.set_nodelay(true)?;
    let client = Client::connect(config, tcp.compat_write()).await?;
    Ok(client)
}
```

### 2.3 HistorianQuery 新增 reconnect 方法

**文件**：`src/query/mod.rs`

```rust
impl HistorianQuery {
    // 现有方法不变...

    /// 尝试重新连接，使用 ConnectConfig 中的重连参数
    /// 成功时替换内部 client，失败时返回最后一次错误
    pub async fn reconnect(&mut self, config: &ConnectConfig) -> anyhow::Result<()> {
        let max_retries = config.reconnect_times;
        let interval = Duration::from_secs(config.reconnect_interval);

        for attempt in 1..=max_retries {
            tracing::warn!(
                attempt,
                max_retries,
                "attempting to reconnect to AVEVA Historian ({}:{})",
                config.host,
                config.port
            );

            match config.connect().await {
                Ok(new_client) => {
                    self.client = new_client;
                    tracing::info!(
                        attempt,
                        "successfully reconnected to AVEVA Historian"
                    );
                    return Ok(());
                }
                Err(err) => {
                    tracing::error!(
                        attempt,
                        max_retries,
                        "reconnect attempt failed: {:#}",
                        err
                    );
                    if attempt < max_retries {
                        tokio::time::sleep(interval).await;
                    }
                }
            }
        }

        anyhow::bail!(
            "failed to reconnect to AVEVA Historian after {} attempts",
            max_retries
        )
    }
}
```

### 2.4 sync_live 加入重连逻辑

**文件**：`src/worker/mod.rs`，`sync_live` 函数

改造主循环，将查询失败时的行为从"直接退出"变为"尝试重连后继续"：

```rust
pub async fn sync_live(task_config: TaskConfig, logger: Sender<String>) -> anyhow::Result<()> {
    // ... 前半部分保持不变（IPC stream、schema 获取等）...

    let mut client = HistorianQuery::try_connect(task_config.clone().connect).await?;

    // ... describe_table, schema 构建等保持不变 ...
    // ... IPC writer / ACK reader 启动保持不变 ...

    let mut count: u64 = 1;
    loop {
        let query_result = client.select_from_live(task_config.tags.clone()).await;

        match query_result {
            Ok(stream) => {
                let batch = to_record_batch(stream).await?;
                logger.send_async(to_csv_string(&batch)?).await?;
                tx.send_async(batch).await?;
                count += 1;
            }
            Err(err) => {
                tracing::error!("sync live query failed at round {count}: {err:#}");
                // 尝试重连
                client.reconnect(&task_config.connect).await?;
                tracing::info!("sync live reconnected, resuming from round {count}");
                // 重连成功后 continue，进入下一次循环自动重新查询
                continue;
            }
        }

        tokio::time::sleep(task_config.retrieve_interval.to_std().unwrap()).await;
    }
}
```

**关键点**：
- `select_from_live` 失败 → 不再用 `?` 直接退出，改为 `match` 处理
- 重连成功 → `continue` 跳过 sleep，立即重新查询
- 重连失败（超过 `reconnect_times` 次）→ `reconnect` 返回 Err，由 `?` 向上传播，任务报错退出

### 2.5 sync_history 加入重连逻辑

**文件**：`src/worker/mod.rs`，`sync_history` 函数

同样改造内层查询循环（约第 183-201 行）：

```rust
// 在主循环内部
for tags in &tags_group {
    let query_result = client
        .select_from_history(tags.clone(), window_start, window_end)
        .await;

    match query_result {
        Ok(stream) => {
            let batch = to_record_batch(stream).await?;
            // ... logger + tx.send_async 保持不变 ...
            count += 1;
        }
        Err(err) => {
            tracing::error!("sync history query failed at round {count}: {err:#}");
            client.reconnect(&task_config.connect).await?;
            tracing::info!("sync history reconnected, retrying current query");
            // 重连后重新执行当前 tags 的查询
            let stream = client
                .select_from_history(tags.clone(), window_start, window_end)
                .await?;
            let batch = to_record_batch(stream).await?;
            // ... logger + tx.send_async ...
            count += 1;
        }
    }
}
```

### 2.6 migrate_history Consumer 加入重连逻辑（可选，优先级较低）

**文件**：`src/worker/mod.rs`，`Consumer::consume` 方法

在 `while let Ok(mut task) = receiver.recv_async().await` 循环内，对 `select_from_history` 同样做 match + reconnect 处理。

migrate 任务已有 breakpoint 机制，用户可手动重启恢复，因此此项优先级低于 sync_live 和 sync_history。

---

## 3. `to_record_batch` 的错误处理补充

`to_record_batch(stream).await?` 在流式读取过程中也可能遇到连接中断。建议：
- 如果 `to_record_batch` 返回错误，也走重连 → 重新查询的流程
- 将 query + to_record_batch 合并在同一个 match/retry 块中

改进后的 `sync_live` 伪代码：

```rust
loop {
    match query_and_convert(&mut client, &task_config).await {
        Ok(batch) => {
            logger.send_async(to_csv_string(&batch)?).await?;
            tx.send_async(batch).await?;
            count += 1;
        }
        Err(err) => {
            tracing::error!("sync live failed at round {count}: {err:#}");
            client.reconnect(&task_config.connect).await?;
            continue;
        }
    }
    tokio::time::sleep(task_config.retrieve_interval.to_std().unwrap()).await;
}
```

其中 `query_and_convert` 封装了 `select_from_live` + `to_record_batch`：

```rust
async fn query_and_convert_live(
    client: &mut HistorianQuery,
    tags: Vec<String>,
) -> anyhow::Result<RecordBatch> {
    let stream = client.select_from_live(tags).await?;
    to_record_batch(stream).await
}
```

---

## 4. 涉及文件清单

| 文件 | 改动类型 | 说明 |
|------|----------|------|
| `src/config/mod.rs` | 修改 | `ConnectConfig` 新增 3 个字段 + DSN 解析 + connect 超时 |
| `src/query/mod.rs` | 修改 | `HistorianQuery` 新增 `reconnect` 方法 |
| `src/worker/mod.rs` | 修改 | `sync_live`、`sync_history`、`Consumer::consume` 加入重连逻辑 |
| `src/lib.rs` | 无需修改 | `exec_task` 中的初始连接和 tag 查询是一次性的，失败时任务直接报错是合理的 |

---

## 5. 测试方案

### 5.1 单元测试

- `ConnectConfig::from_dsn` 测试：验证 `connection_timeout`、`reconnect_times`、`reconnect_interval` 的解析（含默认值、最小值边界）
- `ConnectConfig::connect` 测试：验证连接超时生效

### 5.2 集成测试（需要实际 AVEVA Historian 环境）

- 启动 `sync_live` 任务 → 中断网络/重启 SQL Server → 验证任务自动重连并继续同步
- 启动 `sync_history` 任务 → 中断网络 → 验证重连后继续同步
- 设置 `reconnect_times=1` → 中断网络且不恢复 → 验证任务在 1 次重试后报错退出

### 5.3 日志验证

重连过程应输出以下日志：
- `WARN` 级：`attempting to reconnect to AVEVA Historian`（每次重试）
- `ERROR` 级：`reconnect attempt failed`（重试失败时）
- `INFO` 级：`successfully reconnected to AVEVA Historian`（重连成功时）
- `ERROR` 级：`failed to reconnect after N attempts`（全部重试耗尽时）

---

## 6. 风险与注意事项

1. **IPC 连接不受影响**：断链重连仅针对 Historian SQL Server 连接。IPC stream（写入 TDengine）是独立的本地 TCP 连接，不需要重连。
2. **数据不丢失**：`sync_live` 每次查询的是 Live 表的全量快照（非增量），重连后重新查即可。`sync_history` 基于时间窗口，重连后从当前 `window_start` 重新查询。
3. **重连期间任务状态**：重连等待期间任务仍处于运行状态（循环未退出），不会变为 Idle。这是期望行为。
4. **`describe_table` 在循环外**：`sync_live` 和 `sync_history` 的 `describe_table`（获取 schema）仅在初始化时执行一次，不在循环内。如果初始连接就失败，任务直接报错是合理的，无需重连。
