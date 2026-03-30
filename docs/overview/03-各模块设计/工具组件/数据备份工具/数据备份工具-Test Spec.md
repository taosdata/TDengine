# 数据备份工具-Test Spec

## 1. **修订记录**

| 日期 | 版本 | 作者 | 备忘 |
| --- | --- | --- | --- |
| 2025-01-20 | 1.0 | 陈浩然 | 第一版定稿 |
| 2026-01-05 | 1.1 | 王旭 | 完善测试用例 |

## 2. **测试目标**

1. 功能测试：验证 taosdump 工具所有命令行参数和功能按规格正常工作。
2. 性能测试：验证数据导出性能不低于 200 万数据点/秒，导入性能不低于 100 万数据点/秒。
3. 安全性测试：验证凭据保护、传输安全、路径权限等安全需求。
4. 稳定性测试：验证长时间运行无内存泄漏，多实例运行互不影响，异常情况下工具行为可控。
5. 兼容性测试：验证版本兼容性，包括低版本导出数据高版本导入，高版本 taosdump 能导出低版本 TDengine 数据。

## 3. **测试范围**

1. 功能测试：覆盖所有连接功能、备份功能、还原功能和通用选项，包括参数校验、边界条件、错误处理。
2. 性能测试：大数据量导出导入性能，多线程并发性能，不同数据批大小对性能的影响。
3. 安全性测试：敏感信息保护（密码/Token）、路径安全、输入校验、日志脱敏。
4. 稳定性测试：长时间运行（24 小时以上）稳定性，多实例并发运行，异常中断恢复。
5. 兼容性测试：跨版本数据兼容性，不同 TDengine 版本（2.x， 3.x）兼容性，命令行参数向后兼容。

## 4. **测试结论**

1. 功能测试：通过
2. 性能测试：通过
3. 安全性测试：通过
4. 稳定性测试：通过
5. 兼容性测试：通过

## 5. **已知问题和限制**

1. 导出功能无断点续导功能，一旦中断需重新开始。
2. 数据库乱序严重情况下，导出性能会下降。
3. Windows 系统暂不支持 lzma 编码格式。

## 6. **测试环境**

| 系统 | IP | 部署 | CPU | 内存 | 硬盘 |
| --- | --- | --- | --- | --- | --- |
| CentOS 7.9 | 192.168.1.100 | TDengine 3.0 单节点 | 8 核 | 32GB | SSD 500GB |
| Ubuntu 20.04 | 192.168.1.101 | TDengine 3.0 集群（3 节点） | 16 核 | 64GB | SSD 1TB |
| Windows Server 2019 | 192.168.1.102 | TDengine 3.0 单节点 | 4 核 | 16GB | SSD 256GB |

## 7. **测试用例**

### 7.1 **功能测试**

| 测试类型 | **测试项** | 测试内容 | 预期结果 | **对应测试例** |
| --- | --- | --- | --- | --- |
| 连接功能 | 连接本地部署 TDengine | 使用-h/-P/-u/-p 参数连接本地 TDengine | 连接成功，可执行后续操作 | test_taosdump_basic.py |
| 连接功能 | 连接 TDengine Cloud | 使用-C/--cloud 或-X/--dsn 参数连接 Cloud 服务 | 连接成功，支持 Cloud DSN 格式 | test_taosdump_commandline.py |
| 连接功能 | 连接驱动选择 | 使用-Z/--driver 指定 Native 或 WebSocket 驱动 | 按指定驱动连接，默认 Native | test_taosdump_commandline.py |
| 连接功能 | 连接失败重试配置 | 使用-k/--retry-count 设置重试次数 | 连接失败时按指定次数重试 | test_taosdump_except.py |
| 连接功能 | 重试间隔设置 | 使用-z/--retry-sleep-ms 设置重试间隔 | 重试间隔符合设置值 | test_taosdump_except.py |
| 连接功能 | WebSocket 超时配置 | 使用-t/--timeout 设置超时时间 | WebSocket 连接超时时间正确生效 | test_taosdump_commandline.py |
| 数据备份 | 备份所有数据库 | 使用-A/--all-databases 备份所有数据库 | 所有数据库（包括系统库）被正确备份 | test_taosdump_basic.py |
| 数据备份 | 备份指定数据库 | 使用-D/--databases 或默认参数指定数据库 | 指定数据库被正确备份，支持逗号分隔多库 | test_taosdump_basic.py |
| 数据备份 | 备份指定表 | 直接指定数据库名和表名 | 指定表（超级表、普通表、子表）被正确备份 | test_taosdump_basic.py |
| 数据备份 | 设置输出目录 | 使用-o/--outpath 指定输出目录 | 数据正确输出到指定目录，目录为空则成功 | test_taosdump_basic.py |
| 数据备份 | 设置备份时间段 | 使用-S/--start-time 和-E/--end-time | 只备份指定时间段数据，时间格式 ISO8601/RFC3339 | test_taosdump_basic.py |
| 数据备份 | 设置批大小 | 使用-B/--data-batch 设置批大小 | 备份批大小正确生效，默认 16384 | test_taosdump_commandline.py |
| 数据备份 | 备份线程数 | 使用-T/--thread-num 设置线程数 | 按指定线程数并发备份，默认 8 | test_taosdump_commandline.py |
| 数据备份 | 仅备份 schema | 使用-s/--schemaonly 只备份结构 | 只备份数据库和表结构，不包含数据 | test_taosdump_basic.py |
| 数据备份 | 不使用反引号 | 使用-n/--no-escape 禁用反引号 | SQL 语句中表名不使用反引号 | test_taosdump_basic.py |
| 数据备份 | 点号换为下划线 | 使用-Q/--dot-replace 替换点号 | 元数据名称中点号替换为下划线 | test_taosdump_basic.py |
| 数据备份 | 备份时排除数据库属性 | 使用-N/--without-property 排除属性 | 备份文件不包含数据库属性信息 | test_taosdump_basic.py |
| 数据备份 | Avro 编码格式选择 | 使用-d/--avro-codec 选择编码器 | 支持 null/deflate/snappy/lzma 格式 | test_taosdump_commandline.py |
| 数据备份 | 数据库名转义字符支持 | 使用-e/--escape-character 转义 | 特殊数据库名正确转义处理 | test_taosdump_basic.py |
| 数据备份 | 松散模式 | 使用-L/--loose-mode 启用宽松模式 | 表名和列名仅含字母数字时提升效率 | test_taosdump_basic.py |
| 数据还原 | 还原功能 | 使用-i/--inpath 指定备份目录 | 数据正确还原到目标数据库 | test_taosdump_basic.py |
| 数据还原 | 还原线程数 | 使用-T/--thread-num 设置还原线程 | 按指定线程数并发还原 | test_taosdump_commandline.py |
| 数据还原 | 还原时改库名 | 使用-W/--rename 修改数据库名 | 还原过程中数据库名被正确修改 | test_taosdump_basic.py |
| 数据还原 | 备份文件完整性检查 | 使用-I/--inspect 检查备份文件 | 输出备份文件概要信息，检查文件完整性 | test_taosdump_basic.py |
| 通用选项 | 导入/导出结果文件路径 | 使用-r/--resultFile 指定结果文件 | 备份还原结果输出到指定文件 | test_taosdump_commandline.py |
| 通用选项 | 日志文件路径 | 指定日志文件输出路径 | 日志正确输出到指定文件 | test_taosdump_commandline.py |
| 通用选项 | taos.cfg 配置位置 | 使用-c/--config-dir 指定配置目录 | Native 连接时使用指定配置文件 | test_taosdump_commandline.py |
| 通用选项 | 调试模式 | 使用-g/--debug 启用调试模式 | 输出更详细的调试信息 | test_taosdump_commandline.py |
| 通用选项 | 帮助信息 | 使用-？/--help/--usage | 打印完整的帮助信息 | test_taosdump_commandline.py |
| 通用选项 | 显示版本号 | 使用-V/--version | 打印版本号和编译信息 | test_taosdump_commandline.py |
| 数据类型 | 所有数据类型支持 | 测试所有 TDengine 数据类型备份还原 | TIMESTAMP， INT， BOOL， TINYINT， SMALLINT， BIGINT， FLOAT， DOUBLE， BINARY， NCHAR， UNSIGNED 等 | test_taosdump_datatypes.py |
| 特殊功能 | 虚拟表支持 | 测试虚拟表（vtable）备份还原 | 虚拟表结构正确备份还原 | test_taosdump_basic.py |
| 异常处理 | 错误参数处理 | 测试各种错误参数组合 | 给出明确的错误提示，不崩溃 | test_taosdump_except.py |
| 异常处理 | 网络异常处理 | 模拟网络断开等异常 | 工具正确处理异常，给出错误信息 | test_taosdump_except.py |
| 权限测试 | 权限验证 | 测试不同用户权限下的备份还原 | 权限不足时正确拒绝操作 | test_taosdump_privilege.py |
| Schema 变更 | 表结构变更处理 | 测试备份后 schema 变更再还原 | 正确处理 schema 变更场景 | test_taosdump_schema_change.py |
| 主键功能 | 主键表支持 | 测试主键表的备份还原 | 主键表数据正确备份还原 | test_taosdump_primarykey.py |
| 时间精度 | 不同时间精度支持 | 测试不同时间精度数据备份还原 | 支持毫秒、微秒、纳秒精度 | test_taosdump_precision.py |

### 7.2 **性能测试**

| **测试项** | 测试内容 | 预期结果 | **对应测试例** |
| --- | --- | --- | --- |
| 大数据量导出性能 | 1 亿数据点（4 个 VNODE，8 线程，1 万子表每表 1 万行）导出 | 导出速度不低于 200 万数据点/秒 | 手动测试 |
| 大数据量导入性能 | 1 亿数据点导入测试 | 导入速度不低于 100 万数据点/秒 | 手动测试 |
| 多线程并发性能 | 不同线程数（1，4，8，16，32）对性能的影响 | 线程数增加性能提升，但存在合理上限 | 手动测试 |
| 批大小性能影响 | 不同批大小（1024,4096,16384,65536）对性能影响 | 适当增大批大小可提升性能，但受 WAL 限制 | 手动测试 |
| 内存使用监控 | 长时间运行内存使用情况 | 内存使用平稳，无持续增长（内存泄漏） | 手动测试 |

### 7.3 **安全性测试**

| **测试项** | 测试内容 | 预期结果 | **对应测试例** |
| --- | --- | --- | --- |
| 密码隐藏测试 | 密码在命令行历史、日志中的显示 | 密码不回显、不记录明文 | test_taosdump_privilege.py |
| 路径权限测试 | 测试无权限目录、非法路径穿越 | 拒绝非法路径，输出目录权限校验 | test_taosdump_except.py |
| 输入注入防护 | 特殊字符数据库名、表名测试 | SQL 注入被正确防护，特殊字符正确处理 | test_taosdump_except.py |
| 日志脱敏测试 | 检查日志文件中敏感信息 | token、密码等敏感信息脱敏处理 | test_taosdump_privilege.py |
| 文件权限设置 | 备份文件权限检查 | 输出文件权限设为 0600，保护数据安全 | test_taosdump_basic.py |

### 7.4 **稳定性测试**

| **测试项** | 测试内容 | 预期结果 | **对应测试例** |
| --- | --- | --- | --- |
| 长时间运行测试 | 连续运行 24 小时以上备份还原操作 | 无崩溃、无内存泄漏、功能正常 | 稳定性测试脚本 |
| 多实例并发测试 | 同时运行多个 taosdump 实例 | 实例间互不影响，各自完成操作 | test_taosdump_basic.py |
| 异常中断恢复 | 在备份/还原过程中强制中断 | 工具正确处理信号，清理临时资源 | test_taosdump_except.py |
| 资源使用监控 | 监控 CPU、内存、文件描述符使用 | 资源使用在合理范围内，无资源泄漏 | 手动测试 |
| 大压力测试 | 高并发、大数据量持续压力测试 | 系统稳定，响应时间在可接受范围 | 手动测试 |

### 7.5 **兼容性测试**

| **测试项** | 测试内容 | 预期结果 | **对应测试例** |
| --- | --- | --- | --- |
| 跨版本数据导入 | 低版本 taosdump 导出数据，高版本导入 | 数据正确导入，无兼容性问题 | test_taosdump_compa.py |
| 跨版本数据导出 | 高版本 taosdump 导出低版本 TDengine 数据 | 数据正确导出，格式兼容 | test_taosdump_compa.py |
| 命令行向后兼容 | 测试旧版本命令行参数在新版本中支持 | 原有参数继续支持，不破坏兼容性 | test_taosdump_commandline.py |
| 不同 TDengine 版本 | 测试 2.x 和 3.x 版本 TDengine 兼容性 | 工具适配不同版本引擎接口 | test_taosdump_compa.py |
| 不同操作系统 | Linux/Windows 环境下功能一致性 | 功能一致，平台特定问题被正确处理 | 手动测试 |

## 8. **测试计划**

1. 总计：7 人天
2. 测试环境准备：0.5 人天（包括 TDengine 部署、测试数据准备）
3. 功能测试执行：3 人天（覆盖所有功能测试用例）
4. 性能测试执行：1 人天（大数据量性能测试）
5. 安全稳定性测试：1 人天（安全测试、长时间运行测试）
6. 兼容性测试：0.5 人天（跨版本、跨平台测试）
7. 测试总结与报告：1 人天（整理测试结果，编写测试报告）

## 9. **风险评估**

1. **性能风险**：实际性能可能受硬件、网络、数据分布影响，需在不同环境中验证。
2. **兼容性风险**：不同 TDengine 版本间可能存在细微差异，需充分测试。
3. **安全风险**：安全需求可能随产品演进变化，需定期 review 安全测试用例。
4. **资源风险**：大数据量测试需要充足存储空间和内存，需提前准备。
5. **时间风险**：复杂 bug 修复可能影响测试进度，需预留缓冲时间。

## 10. **参考文档**

1. 数据备份工具-Requirement Spec
2. 数据备份工具-Function Spec
3. 数据备份工具-Design Spec
4. [TDengine 官方文档](https://docs.taosdata.com/)
