# `handle_point_message_init` 设计文档

> 源码位置：`taosx-core/src/plugins/sink/mod.rs`

## 概述

`handle_point_message_init` 是 OPC 类数据源（OPC UA、OPC DA、KingHistorian、Pspace）在数据写入前的**表结构初始化**函数。它根据 `PointModelConfig` 配置，在 TDengine 中完成建表准备工作，确保后续数据写入时目标表已就绪。

## 函数签名

```rust
pub async fn handle_point_message_init(
    config: &PointModelConfig,
    taos: &Taos,
) -> anyhow::Result<()>
```

### 参数

| 参数     | 类型                | 说明                                                                   |
| -------- | ------------------- | ---------------------------------------------------------------------- |
| `config` | `&PointModelConfig` | 点位模型配置，包含点位映射、表配置、Object Node 配置等全部建表所需信息 |
| `taos`   | `&Taos`             | TDengine 连接实例                                                      |

### `PointModelConfig` 关键字段

```rust
pub struct PointModelConfig {
    pub source_type: SourceType,                                    // 数据源类型 (OPCUA/OPCDA/KingHistorian/Pspace)
    pub point_config_map: LinkedHashMap<String, PointConfig>,       // key: point_id
    pub table_config_map: LinkedHashMap<String, TableConfig>,       // key: point_id
    pub node_config_map: Option<LinkedHashMap<String, ObjectNodeConfig>>,  // key: node_id (Object Nodes)
}
```

## 功能详解

该函数依次执行以下三个步骤：

### 步骤一：删除被禁用的子表（Drop Disabled Tables）

遍历 `point_config_map` 中的所有点位，检查对应的 `TableConfig.enabled` 字段：

- 若 `enabled == Some(0)`，表示该点位已被用户禁用
- 执行 `DROP TABLE IF EXISTS \`{tbname}\`` 删除对应子表
- 表名取自 `PointConfig.code` 字段

这一步确保用户在 UI 上禁用某个点位后，其对应的 TDengine 子表会被清理掉。

### 步骤二：创建 Variable Node 的超级表和子表（Create Tables for Variable Nodes）

调用 `config.to_create_table_sqls()` 生成建表 SQL，该方法内部分两步：

1. **创建超级表**（`to_stable_sqls`）：
   - 格式：`CREATE TABLE IF NOT EXISTS \`{stable}\` ({cols}) TAGS({tags})`
   - 每种超级表名只生成一条 SQL（去重）
   - 跳过 stable 名称仍为表达式模板（未替换）的点位
   - 跳过列类型为动态（`type` 为 `None`）的点位

2. **创建子表**（`to_table_sqls`）：
   - 格式：`CREATE TABLE IF NOT EXISTS \`{tbname}\` USING \`{stable}\` ({tag_names}) TAGS({tag_values})`
   - 支持批量建表，多个子表段拼入同一条 SQL，上限 1 MiB
   - 跳过 stable、tag_values、tag_configs 缺失的点位

建表失败不会中断流程，仅打印 `warn` 级别日志后继续。

### 步骤三：创建 Object Node 的超级表和子表（Create Tables for Object Nodes）

当 `node_config_map` 存在时，为 OPC Object Node（非变量节点，如文件夹、设备对象等）创建存储表：

1. **创建固定超级表**：

   ```sql
   CREATE STABLE IF NOT EXISTS opc_object(
       ts TIMESTAMP, _null INT
   ) TAGS(
       name VARCHAR(1024),
       `BrowseName` VARCHAR(1024),
       `DisplayName` VARCHAR(1024),
       `Description` VARCHAR(1024),
       `Path` VARCHAR(1024)
   )
   ```

2. **为每个 Object Node 创建子表**：
   - 子表名根据数据源类型使用不同模板：
     - OPC UA: `t_{ns}_{id#/_}`
     - OPC DA / KingHistorian: `t_{tagname}`
     - Pspace: `t_{point_id}`
   - 通过 `generate_tbname_from_pattern` 函数将模板替换为实际值
   - Tag 值取自 `ObjectNodeConfig` 的 `name`、`browse_name`、`display_name`、`description`、`path` 字段

同样，建表失败仅 warn 不中断。

## 调用时机

### 在数据流中的位置

```
Agent 发起 IPC 连接
       │
       ▼
ipc_stream_writer() 启动
       │
       ├── 1. 构建 IpcStreamWorker
       │       └── 从 IPC schema metadata 中的 "config" 字段
       │           反序列化出 PointModelConfig
       │
       ├── 2. handle_lush_message_init()   ← Lush 模式建表（如适用）
       │
       ├── 3. handle_point_message_init()  ← ★ 本函数：OPC 点位模式建表
       │
       ├── 4. 开始消费 IPC 数据流
       │       └── consume_point_record() 处理每条数据并写入 TDengine
       │
       └── 5. 流结束 / 取消
```

### 调用位置

- **文件**：`src/serve/rpc/put.rs` → `ipc_stream_writer()` 函数
- **触发条件**：`worker.opc_model_config()` 返回 `Some`，即 IPC Schema 的 metadata 中包含有效的 `"config"` 字段（JSON 格式的 `PointModelConfig`）
- **时序**：在 IPC 通道建立后、数据流消费循环开始前执行，属于**一次性初始化**操作

## 错误处理策略

| 步骤                        | 错误行为                                                           |
| --------------------------- | ------------------------------------------------------------------ |
| Drop disabled tables        | **中断**（`?` 传播错误），因为找不到 point_id 的配置说明数据不一致 |
| Create Variable Node tables | **继续**（warn 日志），单条建表失败不影响其他表                    |
| Create Object Node tables   | **继续**（warn 日志），同上                                        |
| 生成 Object Node SQL        | **中断**（`?` 传播错误），因为无法生成 SQL 说明配置有根本性问题    |
