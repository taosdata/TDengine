# 基准测试工具-Test Spec

## 1. **修订记录**

| 日期 | 版本 | 作者 | 备忘 |
| --- | --- | --- | --- |
| 2025-01-21 | 1.0 | 陈浩然 | 第一版定稿 |
| 2026-01-10 | 1.1 | 王旭 | 测试用例整理 |

## 2. **测试目标**

1. 功能测试：验证基准测试工具（taosBenchmark）的所有功能需求是否按照规格说明实现。
2. 性能测试：验证工具在压力下的性能表现，包括吞吐量、延迟等指标。
3. 安全性测试：验证工具在异常输入、认证失败等场景下的行为，确保无安全漏洞。
4. 稳定性测试：验证工具在长时间、高负载运行下的稳定性，确保无内存泄漏和崩溃。
5. 兼容性测试：验证工具与不同版本TDengine的兼容性，以及命令行与JSON配置文件的兼容性。

## 3. **测试范围**

1. 功能测试：覆盖所有需求规格中定义的功能，包括连接方式、写入性能、查询性能、订阅性能等。
2. 性能测试：测试工具在最大配置下的资源消耗，以及各功能模块的性能指标。
3. 安全性测试：输入验证、认证失败处理、SQL注入防护等。
4. 稳定性测试：工具在7*24小时连续运行、高并发场景下的稳定性。
5. 兼容性测试：与TDengine 3.0版本及之前版本的兼容性，不同操作系统平台的兼容性。

## 4. **测试结论**

1. 功能测试：通过
2. 性能测试：通过
3. 安全性测试：通过
4. 稳定性测试：通过
5. 兼容性测试：通过

## 5. **已知问题和限制**

无

## 6. **测试环境**

| 系统 | IP | 部署 | CPU | 内存 | 硬盘 |
| --- | --- | --- | --- | --- | --- |
| CentOS 7.9 | 192.168.1.100 | TDengine 3.0 + taosBenchmark | 8核 | 16GB | 500GB SSD |
| Ubuntu 20.04 | 192.168.1.101 | TDengine 3.0 + taosBenchmark | 4核 | 8GB | 256GB SSD |
| Windows Server 2019 | 192.168.1.102 | TDengine 3.0 + taosBenchmark | 8核 | 16GB | 500GB SSD |

## 7. **测试用例**

### 7.1 **功能测试**

| 测试类型 | **测试项** | 测试内容 | 预期结果 | **对应测试例** |
| --- | --- | --- | --- | --- |
| 连接方式 | 原生连接 | 使用原生连接方式连接TDengine实例 | 连接成功，可正常执行后续操作 | benchmarkTest.cpp (connection相关), test_benchmark_taosc.py |
| 连接方式 | WebSocket连接 | 使用WebSocket连接方式连接TDengine实例 | 连接成功，可正常执行后续操作 | benchmarkTest.cpp (connection相关), test_benchmark_websocket.py |
| 连接方式 | REST连接 | 使用REST接口连接TDengine实例 | 连接成功，可正常执行后续操作 | test_benchmark_rest.py |
| 写入性能 | SQL写入 | 使用SQL拼接方式写入数据 | 数据写入成功，统计信息正确 | case/insertSuit1.json, benchmarkTest.cpp, test_benchmark_basic.py, test_benchmark_taosc.py |
| 写入性能 | STMT写入 | 使用参数绑定（STMT）方式写入数据 | 数据写入成功，性能优于SQL写入 | case/insertStmt2.json, test_benchmark_stmt.py |
| 写入性能 | sml(line) | 使用Schemaless的line协议写入数据 | 数据写入成功，自动建表 | case/insertMix.json, test_benchmark_sml.py |
| 写入性能 | sml(json) | 使用Schemaless的json协议写入数据 | 数据写入成功，自动建表 | case/insertMix.json, test_benchmark_sml.py |
| 写入性能 | sml(telnet) | 使用Schemaless的telnet协议写入数据 | 数据写入成功，自动建表 | case/insertMix.json, test_benchmark_sml.py |
| 写入性能 | 批写入模式 | 一张子表全部写完再写下一张子表 | 数据写入成功，顺序正确 | case/order.json, test_benchmark_basic.py |
| 写入性能 | 交叉(interlace)写入模式 | 每张子表写入指定行数后循环写入 | 数据写入成功，交叉顺序正确 | case/insertMix.json, test_benchmark_mix.py |
| 写入性能 | 统计写入耗时 | 从开始写入到完成的总耗时 | 耗时统计准确，单位秒 | 工具输出验证，test_benchmark_basic.py |
| 写入性能 | 统计写入数据量 | 写入数据的总行数 | 行数统计准确，与配置一致 | 工具输出验证，test_benchmark_basic.py |
| 写入性能 | 统计写入吞吐量 | 每秒写入的数据总行数 | 吞吐量计算正确，单位rows/s | 工具输出验证，test_benchmark_basic.py |
| 写入性能 | 统计写入请求延时分布 | 写入请求的min, max, avg, p90, p95, p99 | 延迟分布统计准确 | 工具输出验证，test_benchmark_basic.py |
| 写入性能 | 数据类型测试 | 测试各种数据类型的写入 | 所有数据类型写入成功 | test_benchmark_datatypes.py |
| 写入性能 | 标签顺序测试 | 测试标签不同顺序的写入 | 标签顺序不影响数据正确性 | test_benchmark_tag_order_sql.py, test_benchmark_tag_order_stmt.py, test_benchmark_tag_order_stmt2.py, test_benchmark_tag_order_sml.py |
| 查询性能 | 超级表查询 | 对超级表的所有子表发起并发查询 | 查询成功，返回结果正确 | case/insertQuery.json, test_benchmark_query_main.py |
| 查询性能 | 给定SQL查询 | 执行一组指定的SQL语句 | 查询成功，返回结果正确 | case/insertQuery.json, test_benchmark_query_sqlfile.py |
| 查询性能 | REST查询 | 使用REST接口进行查询 | 查询成功，返回结果正确 | test_benchmark_query_rest.py |
| 查询性能 | 统计总查询耗时 | 所有查询完成的总耗时 | 耗时统计准确，单位秒 | 工具输出验证，test_benchmark_query_main.py |
| 查询性能 | 统计总查询次数 | 完成的查询请求次数 | 次数统计准确，与配置一致 | 工具输出验证，test_benchmark_query_main.py |
| 查询性能 | 统计总QPS | 每秒完成的查询个数 | QPS计算正确，单位次/秒 | 工具输出验证，test_benchmark_query_main.py |
| 查询性能 | 统计查询延时分布 | 查询请求的min, max, avg, p90, p95, p99 | 延迟分布统计准确 | 工具输出验证，test_benchmark_query_main.py |
| 订阅性能 | 订阅topic名 | 设置订阅的topic名称 | 订阅创建成功，可接收消息 | case/insertQuery.json (订阅相关), test_benchmark_tmq.py |
| 订阅性能 | 订阅HOST/PORT | 设置订阅主机位置和端口 | 订阅连接成功 | case/insertQuery.json (订阅相关), test_benchmark_tmq.py |
| 订阅性能 | 用户名/密码 | 设置订阅连接认证信息 | 认证成功，订阅建立 | case/insertQuery.json (订阅相关), test_benchmark_tmq.py |
| 订阅性能 | 消费线程数 | 设置消费线程数 | 消费线程按配置启动 | case/insertQuery.json (订阅相关), test_benchmark_tmq.py |
| 订阅性能 | 消费组ID | 设置消费组ID | 消费组ID生效 | case/insertQuery.json (订阅相关), test_benchmark_tmq.py |
| 订阅性能 | 消费组模式 | 设置共享消费或独立消费模式 | 消费模式按预期工作 | case/insertQuery.json (订阅相关), test_benchmark_tmq.py |
| 订阅性能 | 创建模式 | 设置顺序创建或并发创建消费者 | 消费者创建模式正确 | case/insertQuery.json (订阅相关), test_benchmark_tmq.py |
| 订阅性能 | 消息最大延时 | 设置消费等待最大延时 | 超时后无消息返回 | case/insertQuery.json (订阅相关), test_benchmark_tmq.py |
| 订阅性能 | 客户端ID | 设置消费者客户端ID | 客户端ID生效 | case/insertQuery.json (订阅相关), test_benchmark_tmq.py |
| 订阅性能 | 自动提交 | 设置自动或手动提交消费消息 | 提交模式按配置工作 | case/insertQuery.json (订阅相关), test_benchmark_tmq.py |
| 订阅性能 | 订阅TOPIC列表 | 创建并订阅多个TOPIC | 多个TOPIC订阅成功 | case/insertQuery.json (订阅相关), test_benchmark_tmq.py |
| 订阅性能 | 统计线程消费消息块个数 | 输出每个线程收到的消息块个数 | 统计信息准确 | 工具输出验证，test_benchmark_tmq.py |
| 订阅性能 | 统计线程消费消息行数 | 输出每个线程收到的消息行数 | 统计信息准确 | 工具输出验证，test_benchmark_tmq.py |
| 订阅性能 | 统计整体消费消息块个数 | 输出总计收到的消息块个数 | 统计信息准确 | 工具输出验证，test_benchmark_tmq.py |
| 订阅性能 | 统计整体消费消息行数 | 输出总计收到的消息行数 | 统计信息准确 | 工具输出验证，test_benchmark_tmq.py |
| 兼容性 | 命令行参数兼容 | 使用原有命令行参数执行工具 | 参数解析正确，功能正常 | benchmarkTest.cpp (参数解析测试), test_benchmark_commandline.py |
| 兼容性 | JSON配置文件兼容 | 使用原有JSON配置文件执行工具 | 配置读取正确，功能正常 | case/*.json 文件测试, test_benchmark_basic.py |
| 易用性 | 命令行参数简洁表意 | 检查参数命名是否简洁易懂 | 参数命名符合规范 | 代码审查 |
| 易用性 | JSON配置文件分层清晰 | 检查JSON配置文件结构是否清晰 | 配置文件结构合理 | 代码审查 |
| 其他功能 | 异常处理 | 测试工具在异常输入下的行为 | 工具正确处理异常，无崩溃 | test_benchmark_except.py |
| 其他功能 | 连接模式测试 | 测试不同连接模式 | 各种连接模式工作正常 | test_benchmark_conn_mode.py |
| 其他功能 | 混合功能测试 | 测试混合操作场景 | 混合操作正常执行 | test_benchmark_mix.py |
| 其他功能 | Bug回归测试 | 测试历史Bug修复 | Bug不再重现 | test_benchmark_bugs.py |
| 其他功能 | CSV文件测试 | 测试CSV文件导入导出 | CSV文件处理正确 | test_benchmark_with_csv.py |
| 其他功能 | 网站相关测试 | 测试网站相关功能 | 网站功能正常 | test_benchmark_website.py |
| 运维需求 | 内存泄漏检查 | 长时间运行后检查内存使用 | 无内存泄漏，内存使用稳定 | valgrind内存检测 |
| 测试需求 | 调试模式 | 启用调试模式输出详细信息 | 调试信息输出完整，便于问题定位 | 工具调试模式验证 |

### 7.2 **性能测试**

| **测试项** | 测试内容 | 预期结果 | **对应测试例** |
| --- | --- | --- | --- |
| 写入吞吐量 | 使用最大配置进行写入压力测试 | 吞吐量达到预期指标，资源消耗在合理范围 | case/insertPressIO.json, test_benchmark_basic.py, test_benchmark_mix.py |
| 查询QPS | 并发查询压力测试 | QPS达到预期指标，查询延迟可控 | case/insertQuery.json, test_benchmark_query_main.py, test_benchmark_query_sqlfile.py |
| 订阅消费速度 | 高频率数据产生下的订阅消费测试 | 消费速度能跟上数据产生速度，无消息堆积 | case/insertQuery.json (订阅相关), test_benchmark_tmq.py |
| 工具框架资源消耗 | 监控工具进程的CPU、内存使用 | 资源消耗在合理范围，无异常增长 | 系统监控工具 |
| 多线程并发性能 | 使用多线程进行混合操作（写入+查询+订阅） | 各线程协调工作，无死锁，性能指标达标 | case/insertMix.json, test_benchmark_mix.py |

### 7.3 **安全性测试**

| **测试项** | 测试内容 | 预期结果 | **对应测试例** |
| --- | --- | --- | --- |
| 认证失败处理 | 使用错误用户名/密码连接 | 连接失败，返回明确的错误信息 | 手动测试，test_benchmark_except.py |
| SQL注入防护 | 在参数中注入SQL特殊字符 | 数据被正确转义或拒绝，无安全漏洞 | 手动测试，test_benchmark_except.py |
| 输入验证 | 输入超长字符串、非法数值等异常参数 | 参数验证正确，返回错误信息，无崩溃 | benchmarkTest.cpp (异常输入测试)，test_benchmark_except.py |
| 配置文件权限 | 检查配置文件权限设置 | 敏感信息（如密码）不应明文存储或权限过松 | 安全扫描 |
| 网络传输安全 | 检查数据传输是否加密（如WebSocket TLS） | 支持加密传输，防止窃听 | 配置验证，test_benchmark_websocket.py |

### 7.4 **稳定性测试**

| **测试项** | 测试内容 | 预期结果 | **对应测试例** |
| --- | --- | --- | --- |
| 长时间运行 | 连续运行24小时，执行混合操作 | 无内存泄漏，无崩溃，性能稳定 | case/insertMix.json (长时间运行), test_benchmark_mix.py |
| 高负载压力 | 在极限配置下持续运行1小时 | 系统稳定，无异常退出 | case/insertPressIO.json, test_benchmark_basic.py, test_benchmark_mix.py |
| 异常中断恢复 | 在运行中强制终止进程，然后重新启动 | 工具能正常启动，无残留状态影响 | 手动测试 |
| 数据库异常处理 | 模拟数据库服务重启、网络中断等场景 | 工具能检测到错误并给出相应处理（重连、退出等） | 手动测试 |
| 资源耗尽场景 | 模拟磁盘满、内存不足等场景 | 工具能优雅降级或退出，无崩溃 | 手动测试 |

### 7.5 **兼容性测试**

| **测试项** | 测试内容 | 预期结果 | **对应测试例** |
| --- | --- | --- | --- |
| TDengine版本兼容 | 分别连接TDengine 2.x和3.x版本 | 工具功能正常，无版本不兼容错误 | 不同版本TDengine部署测试，test_benchmark_basic.py |
| 操作系统兼容 | 在CentOS、Ubuntu、Windows等系统上运行 | 工具编译、运行正常 | 跨平台编译测试，test_benchmark_basic.py |
| 编译器兼容 | 使用gcc、clang等不同编译器编译 | 编译通过，无警告，功能正常 | 多编译器构建测试 |
| 依赖库版本兼容 | 使用不同版本的jansson、curl等依赖库 | 工具功能正常，无链接或运行时错误 | 依赖库版本测试 |
| 命令行与JSON配置优先级 | 同时提供命令行参数和JSON配置文件 | 命令行参数优先级高于配置文件，符合文档说明 | 手动测试，test_benchmark_commandline.py, test_benchmark_basic.py |

## 8. **测试计划**

1. 合计：9 人天
2. 测试环境搭建：0.5 人天
3. 功能测试执行：3 人天
4. 性能测试执行：2 人天
5. 安全性、稳定性、兼容性测试：2 人天
6. 测试结果分析与报告编写：1.5 人天

## 9. **风险评估**

| 风险描述 | 可能影响 | 缓解措施 |
| --- | --- | --- |
| TDengine版本更新导致接口变化 | 工具兼容性失效 | 及时跟进TDengine版本变更，更新工具代码 |
| 测试环境配置复杂 | 测试进度延迟 | 提前准备自动化部署脚本，简化环境搭建 |
| 性能测试结果波动大 | 性能指标难以评估 | 多次测试取平均值，控制环境变量一致 |
| 内存泄漏难以定位 | 稳定性问题 | 使用valgrind等工具定期检测，代码review |
| 多线程并发死锁 | 工具卡死 | 代码review，使用线程安全数据结构，增加超时机制 |

## 10. **参考文档**

1. 基准测试工具-Requirement Spec
2. 基准测试工具-Function Spec
3. 基准测试工具-Design Spec
4. [taosBenchmark用户手册](https://docs.tdengine.com/reference/taosbenchmark)
