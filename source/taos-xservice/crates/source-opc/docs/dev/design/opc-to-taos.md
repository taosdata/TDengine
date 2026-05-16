# `opc_to_taos` 设计文档

> 源码位置：`crates/source-opc/src/lib.rs`

## 概述

`opc_to_taos` 是 OPC 类数据源（OPC UA / OPC DA）采集任务的**顶层入口函数**。它负责整个任务的生命周期管理：从解析 DSN 配置、启动 `taosx-opc` 子进程、建立 IPC 数据通道，到监控子进程运行状态并处理退出。

## 函数签名

```rust
pub async fn opc_to_taos(
    from: Dsn,
    to: Dsn,
    port_pool: &PortPool,
    cancel: CancellationToken,
    with_agent: Option<Via>,
    task_job_id: Option<(i64, i64)>,
    notify: TaskNotifySender,
) -> anyhow::Result<()>
```

### 参数

| 参数 | 类型 | 说明 |
|------|------|------|
| `from` | `Dsn` | 源端 DSN，包含 OPC 服务器地址、认证信息、点位配置、采集参数等 |
| `to` | `Dsn` | 目标端 DSN，指向 TDengine 数据库（必须包含 database 名称） |
| `port_pool` | `&PortPool` | 端口池，为 IPC 通信分配可用端口 |
| `cancel` | `CancellationToken` | 取消令牌，外部通过此令牌停止任务 |
| `with_agent` | `Option<Via>` | Agent 信息。`Some` 表示通过 Agent 执行（via 模式），`None` 表示本地执行 |
| `task_job_id` | `Option<(i64, i64)>` | `(task_id, job_id)`，用于指标收集、日志文件命名、配置文件路径等 |
| `notify` | `TaskNotifySender` | 任务通知发送器，用于向 Explorer 发送任务活动信息 |

## 调用时机

### 任务路由

`opc_to_taos` 由任务路由层（`crates/task/src/lib.rs`）根据 DSN 的 driver 类型分发调用：

```rust
("opc" | "opcda" | "opcua", "taos") => {
    opc_to_taos(from, to, port_pool, cancel, with_agent, task_job_id, notify).await
}
```

当用户在 Explorer 创建 OPC UA / OPC DA 类型的 DataIn 任务并启动时，控制流经过任务调度器最终到达此函数。

### 在整体架构中的位置

```
用户在 Explorer 创建 OPC 任务
       │
       ▼
任务调度器 (scheduler)
       │
       ▼
任务路由 (crates/task/src/lib.rs)
  └─ driver="opc|opcua|opcda", sink="taos"
       │
       ▼
  opc_to_taos()  ← ★ 本函数
       │
       ├── 解析配置 & 生成 PointModelConfig
       ├── 建立 IPC 通道
       ├── 启动 taosx-opc 子进程
       ├── 启动点位更新器
       └── 事件循环（等待退出/错误/取消）
```

## 功能详解

### 整体执行流程

```
opc_to_taos()
  │
  ├── 1. 前置校验 & 指标初始化
  │
  ├── 2. 解析 OPC 配置 (OPCConfig::from_dsn_collect_mode)
  │       ├── PointsMode::ByCsv  → 解析用户上传的 CSV 文件
  │       └── PointsMode::ByCommand → 执行 taosx-opc points 获取点位
  │
  ├── 3. 处理 TLS 证书临时文件
  │
  ├── 4. 构建 IPC 通道 (build_ipc)
  │       ├── with_agent 模式 → listen_tcp_socket_with_agent
  │       └── 本地模式 → listen_tcp_socket
  │
  ├── 5. 生成 collect.toml 配置文件
  │
  ├── 6. 启动 taosx-opc collect 子进程
  │
  ├── 7. 启动 PointsUpdater 后台任务（动态点位更新）
  │
  ├── 8. 启动 stderr 日志采集任务
  │
  └── 9. 事件循环 (tokio::select!)
          ├── 子进程退出 → 清理并报错
          ├── IPC 写入错误 → 终止子进程并报错
          └── cancel 取消 → 优雅退出
```

### 步骤一：前置校验 & 指标初始化

- 校验目标 DSN 必须包含数据库名称（`to.subject`）
- 若运行在 Agent 模式（`with_agent` 为 `Some`），初始化任务指标（`init_task_metrics`）
- 从端口池中获取一个可用端口用于 IPC 通信

### 步骤二：解析 OPC 配置

调用 `OPCConfig::from_dsn_collect_mode` 解析 DSN，根据点位模式生成配置：

#### PointsMode::ByCommand（选择数据点位模式）

1. 执行 `taosx-opc points` 命令查询 OPC 服务器上的点位列表
2. 从 DSN 解析点位映射规则 `PointMappingRule`（超级表模板、子表命名规则、值列名等）
3. 调用 `rule.generate(points)` 生成 `point_config_map` 和 `table_config_map`
4. 调用 `rule.generate_node_config_map(points)` 生成 Object Node 配置
5. 组装 `PointModelConfig`

#### PointsMode::ByCsv（上传 CSV 配置文件模式）

1. 从 DSN 中获取 CSV 内容（内联或文件路径）
2. 调用 `CsvParser::from_dsn` 解析 CSV 文件
3. 生成 `PointModelConfig`

最终 `OPCConfig` 包含完整的连接配置、上报配置、采集配置和点位模型配置。

### 步骤三：TLS 证书处理

对于 OPC UA 的安全连接，从 DSN 中提取 TLS 相关参数并写入临时文件：

| DSN 参数 | 说明 |
|---------|------|
| `certificate` | 客户端证书 |
| `private_key` | 客户端私钥 |
| `auth_certificate` | 认证证书 |
| `auth_private_key` | 认证私钥 |

如果参数值以 `@` 开头，视为文件路径（不创建临时文件）；否则视为文件内容，写入 `NamedTempFile` 后传递路径给子进程。

### 步骤四：构建 IPC 通道

调用 `build_ipc` 创建 IPC 数据通道，根据运行模式选择不同路径：

| 模式 | 调用函数 | 说明 |
|------|---------|------|
| Agent 模式 | `listen_tcp_socket_with_agent` | 通过 Agent 的 Arrow Flight RPC 转发数据 |
| 本地模式 | `listen_tcp_socket` | 直接在本地建立 TCP Socket，taosx-opc 直连 |

`build_ipc` 接收 `PointModelConfig`，将其注入 IPC Schema 的 metadata 中。后续 IPC 写入端（`IpcStreamWorker`）在初始化时会从 metadata 反序列化出 `PointModelConfig`，用于 `handle_point_message_init` 和 `consume_point_record`。

`connector` 参数根据 OPC 类型设置：
- OPC UA → `"opc_ua"`
- OPC DA → `"opc_da"`
- FAKE → `None`（测试用）

### 步骤五：生成 collect.toml

将 `OPCConfig` 序列化为 TOML 格式，写入任务专属目录：

```
{DATA_DIR}/tasks/{task_id}/{job_id}/collect.toml
```

此配置文件包含 taosx-opc 子进程所需的全部采集参数。

### 步骤六：启动 taosx-opc 子进程

通过 `tokio::process::Command` 启动外部可执行文件：

```bash
taosx-opc collect --conf {DATA_DIR}/tasks/{task_id}/{job_id}/collect.toml
```

- `stdout` 继承父进程（直接输出到控制台/日志）
- `stderr` 通过管道捕获，由独立任务处理
- 设置 `kill_on_drop(true)` 确保函数退出时子进程被清理
- 启动后通过 `send_sub_process_info` 上报子进程 PID 信息

### 步骤七：动态点位更新（PointsUpdater）

启动 `PointsUpdater` 后台任务，实现运行时点位列表的动态更新：

- **触发条件**：`update_mode` 不为 `None` 时启用
- **更新模式**：`Append`（仅新增点位）或 `Update`（全量覆盖）
- **更新间隔**：默认 600 秒（10 分钟），可通过 DSN 参数配置
- **更新来源**：根据 `PointsMode` 决定
  - `ByCommand`：重新执行 `taosx-opc points` 获取最新点位
  - `ByCsv`：重新读取 CSV 文件
- **更新方式**：对比当前点位列表和新列表的差异，更新 `collect.toml` 配置文件，taosx-opc 进程会自动重载

### 步骤八：stderr 日志采集

启动独立的 tokio 任务读取子进程的 stderr 输出：

1. **写入日志文件**：通过 `RollingFileAppender` 写入 `{LOG_DIR}/opc-{task_id}-{job_id}.log`
2. **panic 检测**：如果日志行包含 `"panic"` 关键字，存入环形缓冲区（容量 2），用于子进程异常退出时的错误报告
3. **重连事件转发**：如果日志行包含 `"[RECONNECT]"`，通过 `TaskNotifySender` 转发给 Explorer，显示为任务活动

### 步骤九：事件循环

使用 `tokio::select!` 同时等待三个事件，任一触发即退出：

| 事件 | 触发条件 | 处理 |
|------|---------|------|
| 子进程退出 | `child.wait()` 返回 | 执行 `safe_exit!` 清理资源；非成功退出附带 panic 缓冲区内容报错 |
| IPC 写入错误 | `ipc_handler.recv_error()` 收到消息 | 终止子进程，报告写入错误 |
| 任务取消 | `cancel.cancelled()` 触发 | 优雅退出 |

#### `safe_exit!` 宏

`safe_exit!` 是一个内联宏，执行以下清理操作：

1. 使用 `terminate_timeout(2s)` 优雅终止子进程
2. 关闭 IPC handler（等待所有 IPC 写入完成）
3. 关闭 TLS 临时文件
4. 释放 IPC 端口回端口池
5. 取消 `PointsUpdater` 后台任务

## 数据流全景

```
                       opc_to_taos (本函数)
                             │
                ┌────────────┼────────────┐
                │            │            │
                ▼            ▼            ▼
        PointsUpdater   taosx-opc    stderr 日志
        (动态更新点位)   (子进程)     (日志采集)
                             │
                             │ IPC (TCP Socket)
                             ▼
                     IpcStreamWorker
                             │
              ┌──────────────┤
              ▼              ▼
  handle_point_message_init  数据消费循环
  (一次性建表初始化)            │
                              ▼
                    consume_point_record
                    (逐批写入 TDengine)
```

## 与其他文档的关系

| 文档 | 对应阶段 |
|------|---------|
| 本文档 (`opc_to_taos.md`) | 任务顶层入口，生命周期管理 |
| [`handle-point-message-init.md`](./handle-point-message-init.md) | IPC 通道建立后的表结构初始化 |
| [`consume_point_record.md`](./consume_point_record.md) | 数据消费阶段的写入与异常处理 |

## 错误处理

| 场景 | 行为 |
|------|------|
| 目标 DSN 缺少数据库名 | 立即 bail |
| 端口池无可用端口 | 立即报错 |
| OPC 配置解析失败 | 立即报错 |
| IPC 通道创建失败 | 立即报错 |
| taosx-opc 子进程启动失败 | 立即报错 |
| taosx-opc 子进程异常退出 | 报错并附带 panic 信息 |
| taosx-opc 子进程被信号杀死 | 报错（exit code = 0 也视为异常） |
| IPC 写入线程 panic | 终止子进程并报错 |
| 任务被用户取消 | 优雅退出，返回 Ok |

## 注意事项

1. **子进程正常退出也视为错误**：OPC 采集是持续运行的任务，子进程正常退出（exit code = 0）意味着被信号杀死，同样会报错 `"OPC process was killed by signal"`。
2. **Agent 模式与本地模式**：两者的主要区别在于 IPC 通道的建立方式。Agent 模式下数据通过 Arrow Flight RPC 转发到远端 taosx 写入；本地模式下 taosx-opc 直连本地 TDengine。
3. **配置文件持久化**：`collect.toml` 写入磁盘而非通过管道传递，是因为 `PointsUpdater` 需要在运行时修改该文件以实现动态点位更新。
4. **端口自动回收**：`ipc_port` 为 RAII 资源，`safe_exit!` 宏执行后或函数退出时，端口自动归还端口池。
