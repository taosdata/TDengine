# KingHistorian 支持查询同步和点位自动更新 - TS

## 1. 修订记录

| 编写日期 | 发布日期 | 版本 | 修订人 | 主要修改内容 |
| --- | --- | --- | --- | --- |
| 2026/3/17 | 2026/3/17 | 1.0 | 杨志宇 | 初始版本 |

## 2. 测试目标

- 验证 KingHistorian 查询同步模式（mode=sync）的 DSN 解析、两阶段执行流程、取消机制及前端参数映射的正确性。
- 验证 KingHistorian 实时模式点位自动更新的启用条件、轮询发现、追加订阅及取消机制的正确性。

## 3. 参考文档

- 需求规格说明书 (RS): [KingHistorian采集器需求说明书](https://taosdata.feishu.cn/wiki/PM8Jw0bL0iNW7qkyBv3cS3p4nWf)
- 概要设计说明书 (FS): [KingHistorian 支持查询同步和点位自动更新 - FS](https://taosdata.feishu.cn/wiki/I5yawjtVviYLTBkcUM8cSjWgnZp)

## 4. 测试结论

## 5. 测试环境

- **操作系统：**Windows Server 2016+（KingHistorian SDK 仅支持 Windows）
- KingHistorian Server: 已部署并包含测试点位数据
- **TDengine 版本**: 3.3.6.x
- **测试工具**:  explorer + taosx + taosx-agent，cargo

## 6. 功能测试

### 6.1 **查询同步模式**

#### 6.1.1 **DSN 解析**

##### 6.1.1.1 **测试要点**

验证 `mode=sync` / `mode=query_sync` 正确解析为 `KingHistCollectMode::Sync`，以及各参数的解析和默认值。

##### 6.1.1.2 **用例列表**

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| 1-1 | mode=sync 解析 | DSN 中 `mode=sync`，验证 `collect_mode` 解析为 `KingHistCollectMode::Sync` | 通过✅ |
| 1-2 | mode=query_sync 解析 | DSN 中 `kinghist_task_mode=query_sync`，验证同样解析为 `Sync` 模式 | 通过✅ |
| 1-3 | start 参数解析 | DSN 中 `start=2023-10-01T00:00:00Z`，验证正确解析为 `DateTime<Local>` | 通过✅ |
| 1-4 | start 参数缺失 | DSN 中未提供 `start`，验证解析报错，任务无法启动 | 通过✅ |
| 1-5 | time_range 默认值 | DSN 中未提供 `time_range`，验证默认值为 `1d`（86400s） | 通过✅ |
| 1-6 | time_range 自定义值 | DSN 中 `time_range=1h`，验证解析为 3600s | 通过✅ |
| 1-7 | restro 默认值 | DSN 中未提供 `restro`，验证默认值为 `0s` | 通过✅ |
| 1-8 | restro 自定义值 | DSN 中 `restro=10m`，验证解析为 600s | 通过✅ |
| 1-9 | interval 默认值 | DSN 中未提供 `interval`，验证默认值为 `1000`ms | 通过✅ |
| 1-10 | interval 自定义值 | DSN 中 `interval=500`，验证解析为 500ms | 通过✅ |
| 1-11 | sync_interval 默认值 | DSN 中未提供 `sync_interval`，验证默认值为 `10s` | 通过✅ |
| 1-12 | sync_interval 自定义值 | DSN 中 `sync_interval=5m`，验证解析为 300s | 通过✅ |

#### 6.1.2 **查询同步**

##### 6.1.2.1 **测试要点**

验证 QuerySync 模式的历史回填 + 持续同步功能。Phase 1 将 `start_time` 到当前时刻的历史数据一次性迁移；Phase 2 按 `query_interval` 间隔持续轮询新数据同步。
- Phase 1（历史回填）：与 Query 模式逻辑相同，`end_time` 固定为 Phase 1 启动时的当前时间
- Phase 2（持续同步）：`syncStart` 从 Phase 1 结束时刻开始，每隔 `query_interval` 秒查询 `[syncStart - excursion, syncEnd)` 的新数据
- `time_excursion` 仅在 Phase 2 生效，用于回溯捕获乱序（迟到）数据
- 任务持续运行直到连接断开

##### 6.1.2.2 **用例列表**

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| 2-1 | 基本 QuerySync | 指定 `start_time`，启动 QuerySync 任务，验证 Phase 1 历史回填完成后自动进入 Phase 2 持续同步 | 通过✅ |
| 2-2 | Phase 1 历史回填验证 | 验证 Phase 1 阶段从 `start_time` 到当前时间的历史数据全部写入 TDengine | 通过✅ |
| 2-3 | Phase 2 持续同步验证 | Phase 1 完成后，pSpace 中写入新数据，验证在下一个 `query_interval` 轮询中被同步到 TDengine | 通过✅ |
| 2-4 | 自定义查询间隔 | 设置 `query_interval=30`（30 秒），验证 Phase 2 按 30 秒间隔轮询 | 通过✅ |
| 2-5 | 默认查询间隔 | 不指定 `query_interval`（默认 10 秒），验证按默认间隔轮询 | 通过✅ |
| 2-6 | 乱序偏移生效验证 | 设置 `time_excursion=60`，验证 Phase 2 每次查询范围向前回溯 60 秒以捕获迟到数据 | 通过✅ |
| 2-7 | 大时间跨度回填 | `start_time` 设置为较早时间（如 6 个月前），验证 Phase 1 大量历史数据回填正确完成 | 通过✅ |
| 2-8 | Phase 2 长期运行 | QuerySync 任务运行 2 小时以上（Phase 2 阶段），验证持续同步稳定、无数据丢失 | 通过✅ |
| 2-9 | 任务取消 | QuerySync 运行中（Phase 1 或 Phase 2），取消任务，验证正常停止 | 通过✅ |

### 6.2 **点位自动更新**

#### 6.2.1 **测试要点**

验证 PointUpdater 按间隔轮询、正确发现新增点位、不重复发送已知点位。

#### 6.2.2 **用例列表**

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| 3-1 | 按间隔轮询 | 设置 `update_interval=10`，验证 PointUpdater 每 10s 查询一次 KingHistorian Server | 通过✅ |
| 3-2 | 发现新增点位 | 任务运行中在 KingHistorian 新增点位，验证下一次轮询时发现并发送 | 通过✅ |
| 3-3 | 不重复发送 | 新增点位已发送后，下一次轮询不再重复发送 | 通过✅ |
| 3-4 | 无新增点位 | 轮询时无新增点位，验证不发送任何消息，不报错 | 通过✅ |
| 3-5 | 批量新增点位 | 一次性新增大量点位（如 100+），验证全部正确发现并分发 | 通过✅ |
| 3-6 | 按 IpcDataType 分组分发 | 新增不同数据类型的点位，验证按 IpcDataType 分组后发送到对应 collector 的通道 | 通过✅ |
| 3-7 | 使用相同查询表达式 | 验证 PointUpdater 使用与初始点位查询相同的 `tag_name_mask` 和 `mapping_rule` | 通过✅ |

## 7. 易用性测试

测试用例包括：
- ✅ 配置格式是否简单易懂
- ✅ 错误信息是否清晰
- ✅ 参数命名是否合理
- ✅ 默认值是否合理
- ✅ 文档是否完整
**测试结果**: 
- 配置格式与现有数据源（例如：OPC）保持一致，易于理解
- 错误信息包含详细的上下文信息，便于排查
- 参数命名清晰
- 默认值合理，适用于大多数场景
- 提供了完整的 README 文档

## 8. 长期稳定性测试

### 8.1 测试计划

- 持续运行 24 小时，处理大量 KingHistorian 数据
- 监控内存使用，验证无内存泄漏
- 监控 CPU 使用，验证性能稳定
- 模拟各种错误场景，验证错误恢复能力

### 8.2 测试结果

待进行长期稳定性测试。

## 9. 性能测试

### 9.1 测试场景

#### 9.1.1 场景 1：历史数据迁移

- **数据点规模**: 5 万
- **数据量**: 约 1000 万行
- **预期性能**: 导入时间 < 10分钟

#### 9.1.2 场景 2：实时数据订阅

- **数据点规模: 5 万**
- **数据量**: 约 5 W rows/sec
- **预期性能**: 没有挤压，同步正常

#### 9.1.3 场景 3：查询同步

- **数据点规模**: 5 万
- **数据量**: 历史数据约 1000 万行，增量为 5W rows/sec
- **测试内容**: 迁移所有数据，之后查询同步
- **预期性能**: 没有挤压，同步正常

### 9.2 测试结果

待进行详细性能测试。

## 10. 安全测试

### 10.1 测试用例

| # | 测试项 | 测试内容 | 测试结果 |
| --- | --- | --- | --- |
| 1 | 文件便利 | 查看是否含有明文密码 | 通过✅ |

**安全考量**
- 文件访问遵守操作系统权限控制
- 通过 Rust 类型系统保证内存安全
- 支持任务取消，防止资源泄漏
- 错误信息不包含敏感系统信息

## 11. 兼容性测试

### 11.1 系统兼容性

测试用例包括：
- ✅ 编译兼容：与现有 taosX 代码库兼容，编译通过
- 待测试：在 Linux 系统上运行正常
- 待测试：在 Windows 系统上运行正常
- 待测试：与不同版本的 TDengine 兼容

## 12. 代码质量测试

### 12.1 编译测试

```bash

## 13. 单独编译 source-parquet 模块

cargo check -p source-pspace

## 14. 结果: ✅ 通过

## 15. 编译 task 模块（包含集成）

cargo check -p taosx-task

## 16. 结果: ✅ 通过

## 17. 构建 source-parquet 模块

cargo build -p source-pspace

## 18. 结果: ✅ 通过

```

### 18.1 代码规范

- ✅ 遵循 Rust 代码规范
- ✅ 使用 workspace lints 配置
- ✅ 与现有代码风格保持一致
- ✅ 错误处理使用 anyhow/Result
- ✅ 日志使用 tracing 框架

## 19. 已知问题和限制

### 19.1 功能限制

1. pSpace 版本：基于 pSpace 7.1 开发

### 19.2 待完善项

1. **集成测试**: 需要添加端到端的集成测试
2. **性能基准测试**: 需要建立性能基准测试

### 19.3 已知 Bug

无已知 Bug。

## 20. 测试总结

### 20.1 测试完成度

- 代码实现: ✅ 100% 完成
- 编译测试: ✅ 100% 通过
- 功能测试: ✅ 100% 通过
- 性能测试: ⏳ 待执行
- 安全测试: ⏳ 待执行
- 兼容性测试: ⏳ 待执行

### 20.2 后续工作

1. 准备性能测试数据集
2. 执行性能测试并收集数据
3. 补充集成测试
4. 完善文档
5. 进行代码 Review

### 20.3 风险评估

无
