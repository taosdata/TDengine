# Parquet 数据源 - FS

## 1. 修订记录

| 编写日期 | 发布日期 | 版本 | 修订人 | 主要修改内容 |
| --- | --- | --- | --- | --- |
| 2026-01-13 | 2026-01-13 | 1.0 | 霍琳贺 | 初始版本 |

## 2. 背景

随着大数据技术的普及，Apache Parquet 格式作为列式存储格式被广泛应用于数据湖、数据仓库等场景。许多企业积累了大量 Parquet 格式的历史数据，需要将这些数据迁移到 TDengine 以支持时序数据分析。
目前 taosX 已支持 CSV、ORC 等数据源，但缺少对 Parquet 格式的支持。本特性旨在填补这一空白，为用户提供便捷的 Parquet 数据导入能力。
目标：
1. 实现高性能的 Parquet 文件读取和数据导入
2. 提供与现有数据源一致的配置和使用方式
3. 支持列投影等性能优化特性
4. 确保数据导入的可靠性和稳定性
需求：
1. 

## 3. 定义

- **Parquet**: Apache Parquet 是一种列式存储格式，支持高效的数据压缩和编码方案
- **DSN (Data Source Name)**: 数据源名称，用于配置数据源连接的 URI 格式字符串
- **Record Batch**: Apache Arrow 的数据批次结构，用于高效的批量数据传输
- **Projection**: 列投影，只读取需要的列，减少 I/O 和内存开销
- **Backpressure**: 背压控制，通过限制未处理批次数量来防止内存溢出

## 4. 行为说明

### 4.1 DSN 配置格式

Parquet 数据源使用以下 DSN 格式：
```plaintext
parquet:<file_path>[,<file_path2>...]?[batch_size=<size>][&projection=<columns>][&unprocessed_batches=<count>]
```

#### 4.1.1 参数说明

| 参数名称 | 类型 | 必填 | 默认值 | 说明 |
| --- | --- | --- | --- | --- |
| file_path | string | 是 | - | Parquet 文件路径，支持多个文件用逗号分隔 |
| batch_size | integer | 否 | 1000 | 每批读取的行数 |
| projection | string | 否 | all | 列投影，可以是列名或列索引（从0开始） |
| unprocessed_batches | integer | 否 | 64 | 允许的最大未处理批次数，用于背压控制 |

#### 4.1.2 配置示例

1. **基本配置** - 读取单个 Parquet 文件：
```plaintext
parquet:/data/sensors.parquet
```

1. **多文件配置** - 读取多个 Parquet 文件：
```plaintext
parquet:/data/sensors_2024_01.parquet,/data/sensors_2024_02.parquet,/data/sensors_2024_03.parquet
```

1. **自定义批量大小**：
```plaintext
parquet:/data/sensors.parquet?batch_size=5000
```

1. **列投影（按列名）**：
```plaintext
parquet:/data/sensors.parquet?projection=ts,temperature,humidity
```

1. **列投影（按索引）**：
```plaintext
parquet:/data/sensors.parquet?projection=0,2,5
```

1. **完整配置示例**：
```plaintext
parquet:/data/sensors.parquet?batch_size=2000&projection=ts,temperature,humidity&unprocessed_batches=100
```

### 4.2 任务配置示例

在 taosX 任务配置中使用 Parquet 数据源：
```json
{
  "name": "import_parquet_data",
  "from": "parquet:/data/sensors.parquet?batch_size=1000",
  "to": "taos://localhost:6030/test_db",
  "parser": { ... },
}
```

### 4.3 数据类型映射

Parquet 数据类型自动映射到 TDengine 数据类型：

| Parquet 类型 | TDengine 类型 |
| --- | --- |
| BOOLEAN | BOOL |
| INT32 | INT |
| INT64 | BIGINT |
| FLOAT | FLOAT |
| DOUBLE | DOUBLE |
| BYTE_ARRAY (UTF8) | NCHAR |
| BYTE_ARRAY (Binary) | BINARY |
| INT96 (Timestamp) | TIMESTAMP |

### 4.4 错误处理

#### 4.4.1 文件访问错误

当文件不存在或无权限访问时：
```plaintext
Error: open parquet file error: /data/sensors.parquet: No such file or directory (os error 2)
```

#### 4.4.2 格式错误

当文件不是有效的 Parquet 格式时：
```plaintext
Error: build parquet reader error: Parquet error: Invalid Parquet file
```

#### 4.4.3 列投影错误

当指定的列不存在时，系统会自动过滤不存在的列，只读取存在的列。

### 4.5 日志输出

系统会输出以下关键日志：
```plaintext
INFO Parquet to taos, from: parquet:/data/sensors.parquet, to: taos://localhost:6030/test_db
INFO parquet task completed successfully
ERROR read parquet error: <error details>
```

## 5. 性能

### 5.1 性能优化策略

1. **批量处理**: 默认每批处理 1000 行，可通过 `batch_size` 参数调整
2. **列投影**: 支持只读取需要的列，减少 I/O 和内存使用
3. **并发读取**: 多个 Parquet 文件并发读取，充分利用多核 CPU
4. **零拷贝**: 使用 Apache Arrow 格式，减少数据拷贝
5. **背压控制**: 通过 `unprocessed_batches` 参数限制内存使用
6. **异步 I/O**: 使用 tokio 异步运行时，提高并发性能

### 5.2 性能预期

- 单文件读取速度：取决于磁盘 I/O 和文件大小
- 内存使用：batch_size × unprocessed_batches × 每行大小
- 并发性能：随文件数量线性扩展

## 6. 安全

### 6.1 文件访问安全

1. **路径验证**: 验证文件路径的合法性，防止路径遍历攻击
2. **权限检查**: 遵守操作系统的文件权限控制
3. **错误处理**: 文件访问失败时提供明确的错误信息，但不泄露敏感路径信息

### 6.2 资源限制

1. **内存限制**: 通过 `unprocessed_batches` 参数限制内存使用
2. **并发限制**: 使用 tokio 运行时的默认并发限制
3. **任务取消**: 支持任务取消机制，防止资源泄漏

### 6.3 数据完整性

1. 使用 Parquet 格式内置的校验和机制
2. 读取失败时提供详细的错误日志
3. 支持任务失败通知机制

## 7. 兼容性

无破坏性变更。本特性为新增功能，不影响现有功能。

## 8. 运维

### 8.1 部署要求

无特殊要求，与现有 taosX 部署方式一致。

### 8.2 监控和日志

- 使用标准的 tracing 日志框架
- 支持任务状态通知机制
- 错误时提供详细的错误上下文

### 8.3 故障排查

1. 检查文件路径是否正确
2. 检查文件权限
3. 查看日志中的详细错误信息
4. 验证 Parquet 文件格式是否正确

## 9. 使用场景

### 9.1 场景 1: 历史数据迁移

用户有大量历史传感器数据存储在 Parquet 文件中，需要迁移到 TDengine：
```bash
taosx run -f "parquet:/data/sensors/*.parquet?batch_size=5000" \
  -t "taos://localhost:6030/iot_db" \
  -p "@test.parser.json"
```

### 9.2 场景 2: 数据湖数据导入

从数据湖中导入特定时间范围的数据：
```bash
taosx run -f "parquet:/datalake/2024/01/*.parquet,/datalake/2024/02/*.parquet" \
  -t "taos://localhost:6030/analytics_db" \
  -p "@test.parser.json"
```

### 9.3 场景 3: 选择性列导入

只导入需要的列以优化性能：
```json {wrap}
taosx run \
 -f "parquet:/data/full_data.parquet?projection=ts,device_id,temperature,pressure" \
 -t "taos://localhost:6030/iot_db" \
 -p "@test.parser.json"
```

### 9.4 场景 4: 大文件批量导入

处理大文件时调整批量大小和背压控制：
```bash
taosx run \
 -f "parquet:/data/large_file.parquet?batch_size=10000&unprocessed_batches=50" \
 -t "taos://localhost:6030/big_data_db" \
 -p "@test.parser.json"
```

## 10. 约束和限制

### 10.1 约束

1. Parquet 文件必须是有效的 Apache Parquet 格式
2. 文件必须有读取权限
3. 数据类型必须能够映射到 TDengine 支持的类型

### 10.2 限制

1. 不支持嵌套的复杂类型（如 STRUCT、LIST、MAP）
2. 不支持通配符路径匹配（需要明确指定文件路径）
3. 列投影按索引指定时，索引从 0 开始
4. 单个任务的文件数量建议不超过 100 个

## 11. 常见错误和排查

### 11.1 错误 1: 文件不存在

**错误信息**:
```plaintext
Error: open parquet file error: /data/sensors.parquet: No such file or directory
```

**排查方法**:
1. 检查文件路径是否正确
2. 确认文件是否存在
3. 检查是否有拼写错误

### 11.2 错误 2: 权限不足

**错误信息**:
```plaintext
Error: open parquet file error: /data/sensors.parquet: Permission denied
```

**排查方法**:
1. 检查文件权限
2. 确认运行 taosX 的用户是否有读取权限
3. 使用 `ls -l` 查看文件权限

### 11.3 错误 3: 无效的 Parquet 文件

**错误信息**:
```plaintext
Error: build parquet reader error: Parquet error: Invalid Parquet file
```

**排查方法**:
1. 使用 Parquet 工具验证文件格式
2. 检查文件是否损坏
3. 确认文件确实是 Parquet 格式

### 11.4 错误 4: 内存不足

**症状**: 任务运行缓慢或系统内存耗尽
**排查方法**:
1. 减小 `batch_size` 参数
2. 减小 `unprocessed_batches` 参数
3. 分批处理大文件

## 12. 可观测性

对 taos shell、taos Explorer、TDinsight 等组件无影响。
本特性仅涉及数据导入任务，不影响查询和可视化组件。

## 13. 安装和卸载

无特殊要求。Parquet 数据源模块随 taosX 一起编译和部署，无需额外安装。
卸载 taosX 时，Parquet 数据源模块也会一并卸载。

## 14. 文档

需要更新以下文档：
- taosX 数据源列表
- 添加 Parquet 数据源使用说明
- 添加配置参数说明和示例

## 15. 参考文档

- Apache Parquet 格式规范: https://parquet.apache.org/docs/
- Apache Arrow 文档: https://arrow.apache.org/docs/
- Rust Parquet 库文档: https://docs.rs/parquet/

## 16. 附录

### 16.1 实现说明

#### 16.1.1 核心组件

1. **source-parquet crate**: 独立的 Parquet 数据源模块
  - `config.rs`: DSN 配置解析
  - `lib.rs`: 主要逻辑实现
1. **task 模块集成**: 在 `crates/task/src/lib.rs` 中集成 Parquet 数据源路由

#### 16.1.2 关键技术点

1. **同步 I/O + tokio::spawn_blocking**: Parquet 读取使用同步 I/O，通过 `spawn_blocking` 在异步环境中执行
2. **Arrow Record Batch**: 使用 Arrow 格式作为中间数据格式，实现零拷贝传输
3. **flume channel**: 使用异步 channel 进行生产者-消费者模式的数据传输
4. **任务取消**: 使用 CancellationToken 实现优雅的任务取消

#### 16.1.3 数据流

```plaintext
Parquet File(s) 
  → ParquetRecordBatchReader (同步读取)
  → spawn_blocking (异步化)
  → flume::Sender<Result<RecordBatch>> (channel)
  → channel_based_transformer (数据转换)
  → TDengine (写入)
```
