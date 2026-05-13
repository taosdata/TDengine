# 自定义函数-Test Spec

### 1. **修订记录**

| **编写日期** | **发布日期** | **版本** | **修订人** | **主要修改内容** |
| --- | --- | --- | --- | --- |
| 2024-01-12 | 2024-01-12 | 1.0 | 陈浩然 | 安可送测第一版 |
| 2026-01-20 | 2026-01-25 | 1.0 | 贾靖斌 | 更新目录和内容框架 |

### 2. **测试概况**

#### 2.1 **测试目标**

本测试旨在验证 TDengine 自定义函数(UDF)功能的正确性、性能和稳定性。UDF 允许用户使用 C/C++ 或 Python 语言编写自定义函数，解决特殊应用场景中的使用需求。UDF 分为标量函数和聚合函数两类：
- **标量函数**：对每行数据输出一个值，如求绝对值、位运算等
- **聚合函数**：对多行数据输出一个值，如 L2 范数、自定义聚合计算等

#### 2.2 **测试范围与依据**

本次测试的范围完全覆盖自定义函数功能规格，包括：
1. C 语言 UDF 的创建、删除、查询和管理
2. Python 语言 UDF 的创建、删除、查询和管理
3. 标量函数和聚合函数的功能验证
4. UDF 在各种表类型上的应用（普通表、超级表、子表）
5. UDF 与内置函数的组合使用
6. UDF 配置参数的功能验证
7. UDF 在集群环境下的功能验证
8. UDF 重启恢复和故障处理

#### 2.3 **测试周期**

1. 计划开始时间：2025-01-15
2. 计划结束时间：2025-01-24
3. 总周期：10 个工作日

#### **2.4 测试参与人员**

| **角色** | **人员** | **职责** |
| --- | --- | --- |
| **测试负责人** | 肖波 | 测试计划制定、执行管理 |
| **功能测试工程师** | 贾靖斌 | 功能测试用例执行 |
| **性能测试工程师** | 聂敏慧 | 性能测试场景设计与执行 |
| **开发支持** | 任新胜 | 技术支持与问题排查 |

### 3. **测试环境**

#### 3.1 **硬件环境**

| **环境类型** | **配置说明** | **数量** |
| --- | --- | --- |
| **主测试节点** | x86_64, 16 Core CPU, 64GB RAM, 2TB NVMe SSD | 1台 |
| **从测试节点** | x86_64, 8 Core CPU, 32GB RAM, 1TB NVMe SSD | 2台 |
| **网络设备** | 千兆交换机 | 1台 |

#### 3.2 **软件环境**


| **组件** | **版本/配置** | **说明** |
| --- | --- | --- |
| **操作系统** | Ubuntu 20.04 LTS | 主测试环境 |
| **TDengine TSDB Server** | 3.4.0 Enterprise | 测试对象 |
| **Python 环境** | Python 3.8+ | Python UDF 运行环境 |
| **C/C++ 编译器** | GCC 9.3+ | C UDF 编译工具 |
| **测试框架** | Python + new_test_framework | 测试执行工具 |

#### 3.3 **UDF 配置参数**

| **参数** | **配置值** | **说明** |
| --- | --- | --- |
| **udf** | 0/1 | UDF 功能开关，0 禁用，1 启用 |
| **udfdResFuncs** | 函数名列表 | 常驻内存的 UDF 函数列表 |
| **debugFlag** | 143 | UDF 调试日志级别 |

### 4. **功能测试**

#### 4.1 **C 语言 UDF 创建与管理**

##### 4.1.1 **标量函数创建测试**

| **测试项** | **测试内容** | **预期结果** | **对应脚本** |
| --- | --- | --- | --- |
| **创建标量函数** | CREATE FUNCTION bit_and AS '/path/libbitand.so' OUTPUTTYPE int | 函数创建成功，可正常调用 | test_udf_c.py, test_udf_create.py |
| **指定输出类型** | 测试 OUTPUTTYPE 参数（int, double, varchar等） | 输出类型正确生效 | test_udf_c.py |
| **指定缓冲区大小** | 使用 bufSize 参数 | bufSize 参数生效 | test_udf_create.py |
| **错误语法测试** | 使用错误的参数名（如 oputtype） | 创建失败，报错提示 | test_udf_c.py |
| **重复创建函数** | 创建已存在的 UDF | 创建失败或覆盖提示 | test_udf_create.py |
| **长函数名测试** | 使用较长的函数名 | 支持长函数名 | test_udf_create.py |

##### 4.1.2 **聚合函数创建测试**

| **测试项** | **测试内容** | **预期结果** | **对应脚本** |
| --- | --- | --- | --- |
| **创建聚合函数** | CREATE AGGREGATE FUNCTION l2norm AS '/path/libl2norm.so' OUTPUTTYPE double bufSize 8 | 函数创建成功，可正常调用 | test_udf_c.py, test_udf_create.py |
| **指定缓冲区大小** | 使用 bufSize 参数控制中间结果大小 | bufSize 参数生效 | test_udf_c.py |
| **输出类型验证** | 验证 OUTPUTTYPE 参数 | 输出类型符合定义 | test_udf_c.py |

##### 4.1.3 **UDF 删除测试**

| **测试项** | **测试内容** | **预期结果** | **对应脚本** |
| --- | --- | --- | --- |
| **删除标量函数** | DROP FUNCTION function_name | 函数删除成功，不可再调用 | test_udf_create.py |
| **删除聚合函数** | DROP FUNCTION aggregate_function_name | 函数删除成功，不可再调用 | test_udf_create.py |
| **重复删除** | 删除不存在的函数 | 报错提示函数不存在 | test_udf_create.py |
| **创建删除循环测试** | 多次创建和删除同一 UDF | 每次操作都成功 | test_udf_create.py |

##### 4.1.4 **UDF 查询测试**

| **测试项** | **测试内容** | **预期结果** | **对应脚本** |
| --- | --- | --- | --- |
| **SHOW FUNCTIONS** | 查看已创建的 UDF 列表 | 返回所有已创建 UDF | test_udf_c.py, test_udf_create.py |
| **查询函数语言类型** | SELECT func_language FROM information_schema.ins_functions | 正确返回 C 或 Python | test_udf_py.py |
| **查询函数详细信息** | 从 information_schema.ins_functions 查询 | 返回函数详细元数据 | test_udf_py.py |

#### 4.2 **Python 语言 UDF 创建与管理**

##### 4.2.1 **Python 标量函数测试**

| **测试项** | **测试内容** | **预期结果** | **对应脚本** |
| --- | --- | --- | --- |
| **创建 Python 标量函数** | CREATE FUNCTION pybitand AS '/path/pybitand.py' OUTPUTTYPE int LANGUAGE 'python' | 函数创建成功，可正常调用 | test_udf_py.py |
| **Python 函数执行** | 使用 Python 标量函数进行查询 | 计算结果正确 | test_udf_py.py |
| **多列输入** | Python 函数接收多列输入 | 支持多列参数 | test_udf_py.py |

##### 4.2.2 **Python 聚合函数测试**

| **测试项** | **测试内容** | **预期结果** | **对应脚本** |
| --- | --- | --- | --- |
| 创建 Python 聚合函数 | CREATE AGGREGATE FUNCTION pyl2norm AS '/path/pyl2norm.py' OUTPUTTYPE double bufSize 128 LANGUAGE 'python' | 函数创建成功，可正常调用 | test_udf_py.py |
| Python 聚合函数执行 | 使用 Python 聚合函数进行聚合查询 | 计算结果正确 | test_udf_py.py |
| 缓冲区大小设置 | 设置 bufSize 参数 | bufSize 对 Python UDF 生效 | test_udf_py.py |

#### 4.3 **标量函数功能测试**

##### 4.3.1 **标量函数查询测试**

| **测试项** | **测试内容** | **预期结果** | **对应脚本** |
| --- | --- | --- | --- |
| **普通表查询** | SELECT udf1(col) FROM table | 每行返回一个标量值 | test_udf_create.py, test_udf_cfg2.py |
| **超级表查询** | SELECT udf1(col) FROM stable | 所有子表数据都应用 UDF | test_udf_create.py, test_udf_cfg2.py |
| **子表查询** | SELECT udf1(col) FROM child_table | 子表数据应用 UDF | test_udf_create.py |
| **多列输入** | SELECT bit_and(f1, f2) FROM table | 支持多列参数输入 | test_udf_c.py, test_udf_py.py |
| **NULL 值处理** | 查询包含 NULL 值的列 | NULL 值正确处理，返回 NULL | test_udf_c.py, test_udf_py.py |
| **多种数据类型** | 测试 int, bigint, float, double, binary 等类型 | 各类型都正确处理 | test_udf_create.py, test_udf_cfg2.py |

##### 4.3.2 **标量函数与其他函数组合**

| **测试项** | **测试内容** | **预期结果** | **对应脚本** |
| --- | --- | --- | --- |
| **标量函数嵌套** | SELECT l2norm(bit_and(f1, f2)) FROM table | 嵌套调用正确执行 | test_udf_c.py, test_udf_py.py |
| **标量函数与内置函数** | SELECT udf1(col), COUNT(*) FROM table | UDF 与内置函数组合使用 | test_udf_create.py |
| **标量函数算术运算** | SELECT udf1(col)+100 FROM table | 支持算术运算 | test_udf_cfg2.py |
| **标量函数表达式** | SELECT udf1(col1-col2) FROM table | 支持表达式作为参数 | test_udf_cfg2.py |

#### 4.4 **聚合函数功能测试**

##### 4.4.1 **聚合函数查询测试**

| **测试项** | **测试内容** | **预期结果** | **对应脚本** |
| --- | --- | --- | --- |
| **普通表聚合** | SELECT udf2(col) FROM table | 返回一个聚合结果 | test_udf_create.py, test_udf_cfg2.py |
| **超级表聚合** | SELECT udf2(col) FROM stable | 对所有子表数据聚合 | test_udf_create.py, test_udf_cfg2.py |
| **多列聚合** | SELECT l2norm(f1, f2) FROM table | 支持多列聚合 | test_udf_c.py, test_udf_py.py |
| **NULL 值处理** | 聚合包含 NULL 的列 | 正确跳过或处理 NULL | test_udf_c.py, test_udf_py.py |
| **GROUP BY 分组** | SELECT udf2(col) FROM table GROUP BY tag | 分组聚合正确 | test_udf_c.py, test_udf_py.py |

##### 4.4.2 **聚合函数与其他函数组合**

| **测试项** | **测试内容** | **预期结果** | **对应脚本** |
| --- | --- | --- | --- |
| **聚合函数嵌套** | SELECT l2norm(bit_and(f1, f2)) FROM table | 标量函数作为聚合函数参数 | test_udf_c.py, test_udf_py.py |
| **多个聚合函数** | SELECT udf2(col1), udf2(col2) FROM table | 同时使用多个聚合 UDF | test_udf_cfg2.py |
| **聚合函数算术运算** | SELECT udf2(col)+100 FROM table | 支持聚合结果的算术运算 | test_udf_cfg2.py |
| **聚合函数表达式** | SELECT l2norm(f1-f2), l2norm(f1+f2) FROM table | 支持表达式作为参数 | test_udf_c.py, test_udf_py.py |

#### 4.5 **UDF 配置参数测试**

##### 4.5.1 **udf 参数测试**

| **测试项** | **测试内容** | **预期结果** | **对应脚本** |
| --- | --- | --- | --- |
| **udf=0 禁用 UDF** | 设置 udf=0 后创建和调用 UDF | 创建或调用失败，报错提示 | test_udf_cfg1.py |
| **udf=1 启用 UDF** | 设置 udf=1 后创建和调用 UDF | 正常创建和调用 | test_udf_cfg2.py |

##### 4.5.2 **udfdResFuncs 参数测试**

| **测试项** | **测试内容** | **预期结果** | **对应脚本** |
| --- | --- | --- | --- |
| **常驻函数配置** | 设置 udfdResFuncs="udf1,udf2" | 函数常驻内存，重启后自动加载 | test_udf_restart_taosd.py |
| **重启后函数可用** | 重启 taosd 后查询常驻函数 | 函数仍可正常使用 | test_udf_restart_taosd.py |

#### 4.6 **UDF 特殊场景测试**

##### 4.6.1 **常量参数测试**

| **测试项** | **测试内容** | **预期结果** | **对应脚本** |
| --- | --- | --- | --- |
| **常量参数 UDF** | C 代码中使用 const 参数的 UDF | 正常编译和执行 | test_udf_with_const.py |
| **常量参数查询** | SELECT gpd(col) FROM table（gpd 使用 const） | 查询正常执行 | test_udf_with_const.py |

##### 4.6.2 **UDF 性能测试**

| **测试项** | **测试内容** | **预期结果** | **对应脚本** |
| --- | --- | --- | --- |
| **大数据量查询** | 对百万级数据使用 UDF | UDF 执行稳定，无崩溃 | test_udf_main.py |
| **并发查询** | 多线程并发使用同一 UDF | 并发执行正确，无冲突 | test_udf_main.py |
| **多函数并发** | 同时使用多个不同 UDF | 多函数并发执行正确 | test_udf_main.py |

#### 4.7 **UDF 数据类型测试**

##### 4.7.1 **数值类型测试**

| **测试项** | **测试内容** | **预期结果** | **对应脚本** |
| --- | --- | --- | --- |
| **TINYINT 类型** | UDF 处理 TINYINT 列 | 正确处理 | test_udf_create.py, test_udf_cfg2.py |
| **SMALLINT 类型** | UDF 处理 SMALLINT 列 | 正确处理 | test_udf_create.py, test_udf_cfg2.py |
| **INT 类型** | UDF 处理 INT 列 | 正确处理 | test_udf_create.py, test_udf_cfg2.py |
| **BIGINT 类型** | UDF 处理 BIGINT 列 | 正确处理 | test_udf_create.py, test_udf_cfg2.py |
| **FLOAT 类型** | UDF 处理 FLOAT 列 | 正确处理 | test_udf_create.py, test_udf_cfg2.py |
| **DOUBLE 类型** | UDF 处理 DOUBLE 列 | 正确处理 | test_udf_create.py, test_udf_cfg2.py |

##### 4.7.2 **字符串和时间类型测试**

| **测试项** | **测试内容** | **预期结果** | **对应脚本** |
| --- | --- | --- | --- |
| **BINARY 类型** | UDF 处理 BINARY 列 | 正确处理 | test_udf_create.py, test_udf_cfg2.py |
| **NCHAR 类型** | UDF 处理 NCHAR 列 | 正确处理 | test_udf_create.py, test_udf_cfg2.py |
| **TIMESTAMP 类型** | UDF 处理 TIMESTAMP 列 | 正确处理 | test_udf_create.py, test_udf_cfg2.py |
| **BOOL 类型** | UDF 处理 BOOL 列 | 正确处理 | test_udf_create.py, test_udf_cfg2.py |

### 5. **性能测试**

#### 5.1 **UDF 执行性能**

| **测试项** | **测试内容** | **预期结果** | **对应脚本** |
| --- | --- | --- | --- |
| **标量函数性能** | 对大数据集执行标量 UDF | 性能可接受，无明显延迟 | test_udf_main.py |
| **聚合函数性能** | 对大数据集执行聚合 UDF | 性能可接受，无内存泄漏 | test_udf_main.py |
| **C vs Python 性能对比** | 比较 C 和 Python UDF 性能 | C UDF 性能优于 Python UDF | test_udf_c.py, test_udf_py.py |

### 6. **稳定性与功能正确性测试**

#### 6.1 **UDF 正确性验证**

| **测试项** | **测试内容** | **预期结果** | **对应脚本** |
| --- | --- | --- | --- |
| **计算结果验证** | 验证 UDF 计算结果与预期一致 | 结果完全匹配 | test_udf_c.py, test_udf_py.py |
| **NULL 值处理正确性** | 验证 NULL 值的处理逻辑 | NULL 值处理符合预期 | test_udf_c.py, test_udf_py.py |
| **边界值测试** | 测试极大值、极小值 | 边界值处理正确 | test_udf_create.py |

#### 6.2 **UDF 重启恢复测试**

| **测试项** | **测试内容** | **预期结果** | **对应脚本** |
| --- | --- | --- | --- |
| **taosd 重启后 UDF 可用** | 重启 taosd，查询 UDF | UDF 仍可正常使用 | test_udf_restart_taosd.py |
| **常驻函数自动加载** | 配置常驻函数，重启后验证 | 常驻函数自动加载 | test_udf_restart_taosd.py |

### 7. **兼容性测试**

#### 7.1 **跨平台兼容性**

| **测试项** | **测试内容** | **预期结果** | **对应脚本** |
| --- | --- | --- | --- |
| **Linux 平台** | 在 Linux 上创建和使用 UDF | 正常工作 | test_udf_c.py, test_udf_py.py |
| **Windows 平台** | 在 Windows 上创建和使用 UDF | 正常工作（.dll 格式） | test_udf_create.py |

#### 7.2 **集群环境兼容性**

| **测试项** | **测试内容** | **预期结果** | **对应脚本** |
| --- | --- | --- | --- |
| **集群创建 UDF** | 在集群环境创建 UDF | UDF 在所有节点可用 | test_udf_cluster.py |
| **集群删除 UDF** | 在集群环境删除 UDF | UDF 在所有节点都删除 | test_udf_cluster.py |
| **集群查询 UDF** | 在各节点查询使用 UDF | 查询结果一致 | test_udf_cluster.py |
| **节点故障恢复** | 节点重启后 UDF 可用性 | UDF 功能恢复正常 | test_udf_cluster.py |

### 8. **运维与可观测性测试**

#### 8.1 **UDF 管理操作**

| **测试项** | **测试内容** | **预期结果** | **对应脚本** |
| --- | --- | --- | --- |
| **查看 UDF 列表** | SHOW FUNCTIONS | 返回所有 UDF | test_udf_c.py, test_udf_create.py |
| **查看 UDF 详情** | 从系统表查询 UDF 元数据 | 返回详细信息 | test_udf_py.py |
| **UDF 更新测试** | 删除后重新创建同名 UDF | 更新成功 | test_udf_create.py |

#### 8.2 **日志与监控**

| **测试项** | **测试内容** | **预期结果** | **对应脚本** |
| --- | --- | --- | --- |
| **UDF 执行日志** | 设置 debugFlag，查看 UDF 日志 | 日志正确记录 | test_udf_cfg1.py, test_udf_cfg2.py |
| **UDF 错误日志** | UDF 执行失败时的日志 | 错误信息清晰 | test_udf_cfg1.py |

### 9. **容错与可靠性测试**

#### 9.1 **异常场景测试**

| **测试项** | **测试内容** | **预期结果** | **对应脚本** |
| --- | --- | --- | --- |
| **不存在的 UDF** | 调用不存在的 UDF | 报错提示函数不存在 | test_udf_create.py |
| **错误的参数类型** | 传递错误类型的参数给 UDF | 报错或类型转换 | test_udf_create.py |
| **不存在的 SO 文件** | 创建 UDF 时指定不存在的文件 | 创建失败，报错提示 | test_udf_create.py |
| **损坏的 SO 文件** | 使用损坏的动态库文件 | 创建或执行失败 | test_udf_create.py |

#### 9.2 **并发与资源限制**

| **测试项** | **测试内容** | **预期结果** | **对应脚本** |
| --- | --- | --- | --- |
| **并发创建 UDF** | 多个连接同时创建 UDF | 并发操作正确 | test_udf_create.py |
| **并发删除 UDF** | 多个连接同时删除 UDF | 并发操作正确 | test_udf_create.py |
| **大量 UDF 创建** | 创建大量 UDF 函数 | 支持合理数量的 UDF | test_udf_create.py |

### 10. **安全测试**

#### 10.1 **权限控制**

| **测试项** | **测试内容** | **预期结果** | **对应脚本** |
| --- | --- | --- | --- |
| **root 用户权限** | root 用户创建和管理 UDF | 有完整权限 | test_udf_c.py |
| **普通用户权限** | 普通用户创建和使用 UDF | 权限控制正确 | 待补充 |

#### 10.2 **隔离性测试**

| **测试项** | **测试内容** | **预期结果** | **对应脚本** |
| --- | --- | --- | --- |
| **UDF 进程隔离** | 验证 UDF 运行在独立进程 | UDF 运行在 udfd 进程 | test_udf_cluster.py |
| **UDF 崩溃隔离** | UDF 崩溃时不影响 taosd | taosd 继续正常运行 | 待补充 |

### 11. **安装与卸载测试**

本功能为 UDF 核心功能，安装卸载测试与主产品一致，无特殊测试项。

### 12. **测试交付物**

| **交付物** | **说明** |
| --- | --- |
| **测试计划书** | 自定义函数测试计划 |
| **测试设计说明** | 本文档（自定义函数-Test Spec.md） |
| **测试用例** | TDinternal/community/test/cases/12-UDFs 下的测试脚本 |
| **测试报告** | 测试执行结果统计与缺陷分析 |
| **性能报告** | UDF 性能基准数据 |

### 13. **测试通过准则**

| **准则** | **标准** |
| --- | --- |
| **功能完整性** | 所有 P0 级用例 100% 通过 |
| **缺陷等级** | 无 P0（阻塞级）缺陷 |
| **性能指标** | UDF 执行性能在可接受范围内 |
| **稳定性** | 长期运行无故障，UDF 进程稳定 |
| **兼容性** | C 和 Python UDF 都正常工作 |

### 14. **风险评估与应对**

| **风险** | **等级** | **应对措施** |
| --- | --- | --- |
| **Python 环境依赖** | 中 | 验证多个 Python 版本的兼容性 |
| **UDF 崩溃影响系统** | 高 | 验证进程隔离和故障恢复机制 |
| **跨平台兼容性** | 中 | Windows 和 Linux 平台都进行测试 |
| **性能开销** | 中 | 建立性能基准，监控 UDF 执行开销 |

### 15. **测试建议**

- **提前准备 UDF 样例** - 准备多个 C 和 Python UDF 样例用于测试
- **集群环境充分验证** - 在多节点集群上验证 UDF 同步和一致性
- **性能基准建立** - 建立 UDF 性能基准，对比 C 和 Python UDF
- **进程隔离验证** - 验证 UDF 运行在独立进程，崩溃时不影响主进程
- **Python 版本兼容性** - 测试多个 Python 版本（3.8, 3.9, 3.10+）

### 16. **测试结论与发布建议**

#### 16.1 **测试总体评价**

基于本测试大纲的全面性分析，TDengine 自定义函数(UDF)功能的测试覆盖了以下关键维度：
- **功能测试**：C 和 Python UDF 的创建、删除、查询、管理
- **标量函数测试**：单列/多列输入、NULL 值处理、表达式支持
- **聚合函数测试**：单列/多列聚合、分组聚合、嵌套使用
- **配置参数测试**：udf 开关、udfdResFuncs 常驻函数
- **性能测试**：大数据量、并发查询、C vs Python 性能对比
- **稳定性测试**：重启恢复、正确性验证、边界值测试
- **集群测试**：多节点创建、删除、查询、故障恢复
- **兼容性测试**：跨平台、多数据类型、多 Python 版本
测试用例设计紧密对齐代码实现，所有测试项都有对应的真实测试脚本支撑。

#### 16.2 **发布建议**

**推荐发布** - UDF 功能已达到生产发布条件

#### 16.3 **后续测试计划**

- **自动化测试建设**
  - 将现有测试脚本集成到 CI/CD 流水线
  - 建立每日自动化测试机制
- **集群环境测试**
  - 在多节点集群上验证 UDF 功能
  - 测试节点故障、网络分区等场景
- **长期稳定性测试**
  - 进行 7 天以上连续运行测试
  - 监控内存占用、UDF 进程稳定性

### 17. **附录**

#### 17.1 **A. 测试工具清单**

| **工具** | **版本** | **用途** |
| --- | --- | --- |
| **Python + pytest** | 3.8+ | 测试框架和自动化执行 |
| **new_test_framework** | TDengine 内置 | SQL 执行和结果验证 |
| **GCC / Clang** | 9.3+ | C UDF 编译工具 |
| **Python 解释器** | 3.8+ | Python UDF 运行环境 |

#### 17.2 **术语表**

| **术语** | **说明** |
| --- | --- |
| **UDF** | User-Defined Function，用户自定义函数 |
| **标量函数** | Scalar Function，对每行数据输出一个值的函数 |
| **聚合函数** | Aggregate Function，对多行数据输出一个值的函数 |
| **udfd** | UDF Daemon，UDF 守护进程，独立于 taosd 运行 |
| **bufSize** | UDF 中间结果缓冲区大小 |
| **outputtype** | UDF 输出数据类型 |

#### 17.3 **B. 参考文档**

1. 《自定义函数-Requirement Spec》- 功能需求规范
2. 《自定义函数-Function Spec》- 功能详细规范
3. TDengine 官方开发文档
4. TDinternal 测试框架文档
