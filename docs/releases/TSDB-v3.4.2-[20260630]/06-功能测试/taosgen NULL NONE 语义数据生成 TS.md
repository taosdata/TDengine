# taosgen 变长字段随机长度与 NULL/NONE 语义数据生成 TS

# 1 修订记录

| 编写日期 | 发布日期 | 版本 | 修订人 | 主要修改内容 |
| --- | --- | --- | --- | --- |
| 2026-05-14 | 2026-5-14 | 1.0 | 裴亚明 | 初稿 |

# 2 测试目标

- 覆盖 `null_ratio`/`none_ratio` 配置参数解析与校验逻辑：非负、有限值、`[0.0, 1.0]` 范围、两者之和 ≤ 1.0。
- 覆盖 `NullValue`/`NoneValue` 自定义类型替代 `std::monostate` 的正确性。
- 覆盖 `RowGenerator` 中按比例注入 NULL/NONE 语义值的生成逻辑。
- 覆盖 stmt2 格式写入中 NULL（`is_null[i]=1`）和 NONE（`is_null[i]=2`）的正确编码。
- 覆盖 SQL 格式写入中 NULL 值的正确序列化。
- 覆盖 InfluxDB Line Protocol 格式中 NULL/NONE 值跳过逻辑及全字段 NULL/NONE 行的整行跳过。
- 覆盖 Schemaless/InfluxDB/Kafka 格式化器中换行符泄漏修复的正确性。
- 覆盖 `RowGenerator` 中 RNG 线程安全改造（`static thread_local`）的正确性。
- 覆盖 MemoryPool 中 NULL/NONE 语义在内存池操作中的正确传播。

# 3 参考文档

无。

# 4 测试结论

- 单元测试用例：96 条，全部 Pass。
- 集成测试用例：6 条，依赖 TDengine 环境。
- 覆盖目标：
  - 功能覆盖：NULL/NONE 语义数据生成全部覆盖。
  - 设计覆盖：NullValue/NoneValue 类型定义、RowGenerator、RandomColumnGenerator、MemoryPool、StmtV2Data、RowSerializer 关键模块全覆盖。
  - 不支持范围覆盖：非法 null_ratio/none_ratio 值（负数、NaN、Inf、超过 1.0）。

# 5 测试环境

- OS：Linux x86_64（Ubuntu 22.04+）
- 编译器：GCC 11+（C++17）
- 构建系统：CMake + Conan（Debug 模式 + Release 模式）
- 测试框架：CTest
- 依赖服务（集成测试）：
  - TDengine Server（localhost:6030/6041）

# 6 功能测试

## 6.1 null_ratio/none_ratio 配置解析与校验

### 6.1.1 测试要点

验证 `null_ratio`/`none_ratio` 配置参数解析和校验逻辑：
- 正常值解析正确
- 仅指定其中一个时另一个为空
- 负数值校验
- NaN/Inf 非有限值校验
- 单个 ratio 大于 1.0 校验
- 两者之和大于 1.0 校验
- 两者之和等于 1.0 为合法值

### 6.1.2 用例列表

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| NR-001 | null_ratio + none_ratio 正常配置 | `null_ratio: 0.2, none_ratio: 0.3`，解析后两者值正确 | Pass |
| NR-002 | 仅指定 null_ratio | `null_ratio: 0.5`，解析后 null_ratio=0.5，none_ratio 为空 | Pass |
| NR-003 | null_ratio 为负数 | `null_ratio: -0.1`，抛出异常包含 "null_ratio must be a finite value in [0.0, 1.0]" | Pass |
| NR-004 | none_ratio 为负数 | `none_ratio: -0.1`，抛出异常包含 "none_ratio must be a finite value in [0.0, 1.0]" | Pass |
| NR-005 | null_ratio + none_ratio > 1.0 | `null_ratio: 0.6, none_ratio: 0.5`，抛出异常包含 "null_ratio + none_ratio must be <= 1.0" | Pass |
| NR-006 | null_ratio + none_ratio == 1.0 | `null_ratio: 0.5, none_ratio: 0.5`，解析成功 | Pass |
| NR-007 | null_ratio 为 NaN | `null_ratio: .nan`，抛出异常包含 "null_ratio must be a finite value in [0.0, 1.0]" | Pass |
| NR-008 | none_ratio 为 Inf | `none_ratio: .inf`，抛出异常包含 "none_ratio must be a finite value in [0.0, 1.0]" | Pass |
| NR-009 | null_ratio 大于 1.0 | `null_ratio: 1.5`，抛出异常包含 "null_ratio must be a finite value in [0.0, 1.0]" | Pass |

## 6.2 NullValue/NoneValue 类型定义

### 6.2.1 测试要点

验证 `NullValue`/`NoneValue` 自定义类型替代 `std::monostate` 后的正确性：
- 类型相等性比较
- `fmt::format` 输出正确（NullValue→"NULL"，NoneValue→"NONE"）
- 作为 `ColumnType` variant 的成员正确工作
- `RowType` 中包含 NullValue/NoneValue 时 format 输出正确

### 6.2.2 用例列表

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| TYP-001 | NullValue 相等性 | `NullValue{} == NullValue{}` 为 true | Pass |
| TYP-002 | NoneValue 相等性 | `NoneValue{} == NoneValue{}` 为 true | Pass |
| TYP-003 | NullValue fmt 输出 | `fmt::format("{}", ColumnType{NullValue{}})` 输出 "NULL" | Pass |
| TYP-004 | NoneValue fmt 输出 | `fmt::format("{}", ColumnType{NoneValue{}})` 输出 "NONE" | Pass |
| TYP-005 | RowType 混合输出 | RowType 包含 NullValue、NoneValue 和正常值，fmt 输出各值正确 | Pass |

## 6.3 RowGenerator NULL/NONE 语义注入

### 6.3.1 测试要点

验证 `RowGenerator` 按 `null_ratio`/`none_ratio` 比例注入 NULL/NONE 语义值的生成逻辑：
- 配置 null_ratio 后生成数据中包含 NullValue
- 配置 none_ratio 后生成数据中包含 NoneValue
- NULL/NONE 比例近似符合配置值（统计大量样本）
- 多列各自独立的 null_ratio/none_ratio 配置
- 时间戳列不受 null_ratio/none_ratio 影响
- `generate(RowType&)` 就地更新重载正确工作
- `static thread_local` RNG 线程安全

### 6.3.2 用例列表

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| RG-001 | 单列 null/none 注入 | `null_ratio: 0.3, none_ratio: 0.2`，生成 10000 行，NULL 比例近似 0.3±0.05，NONE 比例近似 0.2±0.05 | Pass |
| RG-002 | 多列独立比例 | 两列分别配置不同 null_ratio/none_ratio，统计各列 NULL/NONE 比例独立且近似符合配置 | Pass |
| RG-003 | 时间戳列不受影响 | 配置 TimestampGenerator + 列 null_ratio=0.5，时间戳列无 NULL/NONE，数据列有 NULL/NONE | Pass |
| RG-004 | generate(RowType&) 就地更新 | 预分配 RowType，调用 `generate(row)`，结果正确包含生成值和 NULL/NONE | Pass |
| RG-005 | 全为正常值 | 不配置 null_ratio/none_ratio，所有生成值均为正常值（无 NullValue/NoneValue） | Pass |
| RG-006 | batch generate(count) | `generate(5)` 返回 5 行数据，每行正确包含 NULL/NONE | Pass |
| RG-007 | generate(RowType&) 大小断言 | 传入大于 column_gens_ 大小的 RowType，Debug 模式触发 assert 失败 | Pass |

## 6.4 StmtV2Data NULL/NONE 编码

### 6.4.1 测试要点

验证 stmt2 格式写入时 StmtV2Data 对 NULL/NONE 值的正确编码：
- NullValue → `is_null[i] = 1`，`length[i] = 0`
- NoneValue → `is_null[i] = 2`，`length[i] = 0`
- 正常值 → `is_null[i] = 0`，buffer 包含实际数据
- 定长类型（如 int32_t）的 NULL/NONE 编码
- 变长类型（如 varchar）的 NULL/NONE 编码
- 混合列（部分 NULL、部分 NONE、部分正常值）
- 全 NULL 行的编码
- 多子表数据的编码

### 6.4.2 用例列表

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| STMT-001 | 定长列 NULL 编码 | int32 列包含 NullValue，is_null=1，length=0 | Pass |
| STMT-002 | 定长列 NONE 编码 | int32 列包含 NoneValue，is_null=2，length=0 | Pass |
| STMT-003 | 变长列 NULL 编码 | varchar 列包含 NullValue，is_null=1，length=0 | Pass |
| STMT-004 | 变长列 NONE 编码 | varchar 列包含 NoneValue，is_null=2，length=0 | Pass |
| STMT-005 | 混合列编码 | 3 列分别为正常值/NullValue/NoneValue，各列 is_null 值正确 | Pass |
| STMT-006 | 全 NULL 行 | 所有列均为 NullValue，所有 is_null=1 | Pass |
| STMT-007 | 多子表 | 3 个子表各包含 NULL/NONE 行，编码独立正确 | Pass |

## 6.5 RowSerializer JSON 格式 NULL/NONE

### 6.5.1 测试要点

验证 JSON 格式序列化中 NULL/NONE 值的处理：
- NullValue → JSON `null`
- NoneValue → 跳过字段

### 6.5.2 用例列表

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| JSON-001 | NullValue 序列化 | NullValue 字段输出为 JSON `null` | Pass |
| JSON-002 | NoneValue 序列化 | NoneValue 字段从 JSON 输出中跳过 | Pass |

## 6.6 RowSerializer InfluxDB Line Protocol NULL/NONE

### 6.6.1 测试要点

验证 InfluxDB Line Protocol 序列化中 NULL/NONE 值的处理：
- NullValue 和 NoneValue 字段在 field set 中跳过
- 部分字段 NULL/NONE 时，剩余字段正常输出
- 全字段 NULL/NONE 时，整行跳过（返回 false，buffer 回滚）
- 换行符不泄漏

### 6.6.2 用例列表

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| INF-001 | 部分字段 NULL/NONE | 3 个 field 列，1 个 NullValue、1 个 NoneValue、1 个正常值，输出仅包含正常值 field | Pass |
| INF-002 | 全字段 NULL/NONE | 所有 field 列均为 NullValue 或 NoneValue，`to_influx_inplace` 返回 false，buffer 回滚到调用前大小 | Pass |
| INF-003 | 正常行输出 | 所有 field 列均为正常值，`to_influx_inplace` 返回 true，输出格式正确 | Pass |

## 6.7 格式化器换行符泄漏修复

### 6.7.1 测试要点

验证 SchemalessInsertDataFormatter、InfluxDBInsertDataFormatter、KafkaInsertDataFormatter 在跳过全 NULL/NONE 行时不泄漏换行符：
- 跳过行后 buffer 大小恢复到行写入前
- 连续跳过多行后 buffer 大小正确
- 跳过行与正常行交替时输出格式正确

### 6.7.2 用例列表

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| NL-001 | Schemaless 跳过行无换行泄漏 | 全 NULL/NONE 行被跳过，buffer 中无多余 `\n` | Pass |
| NL-002 | InfluxDB 跳过行无换行泄漏 | 同上，InfluxDB 格式化器 | Pass |
| NL-003 | Kafka 跳过行无换行泄漏 | 同上，Kafka 格式化器 | Pass |

## 6.8 MemoryPool NULL/NONE 传播

### 6.8.1 测试要点

验证 MemoryPool 中 NULL/NONE 语义在内存池操作中的正确传播：
- `get_cell_impl` 对 NullValue 设置 `is_null=1`
- `get_cell_impl` 对 NoneValue 设置 `is_null=2`
- `add_rows` 批量路径正确传播 NULL/NONE
- 标签列包含 NullValue 时的处理

### 6.8.2 用例列表

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| MP-001 | get_cell NULL vs NONE | NullValue → is_null=1；NoneValue → is_null=2；正常值 → is_null=0 | Pass |
| MP-002 | add_rows 批量 NULL/NONE | 批量添加包含 NULL/NONE 的行，内存池中 is_null 标记正确 | Pass |
| MP-003 | 标签 NullValue | 标签列包含 NullValue，MemoryPool 正确处理 | Pass |

## 6.9 SQL 格式 NULL 处理

### 6.9.1 测试要点

验证 SQL INSERT 格式中 NULL 值的处理：
- NullValue → SQL 输出 `NULL`

### 6.9.2 用例列表

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| SQL-001 | NullValue 序列化 | NullValue 字段在 SQL INSERT 中输出为 `NULL` | Pass |

## 6.10 集成测试：stmt2 格式写入 TDengine

### 6.10.1 测试要点

验证通过 stmt2 格式写入 TDengine 时 NULL/NONE 语义的端到端正确性：
- NULL 值写入后查询结果为 NULL
- NONE 值写入后查询结果保持原值不变
- 混合比例写入后数据分布符合预期

### 6.10.2 用例列表

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| E2E-001 | stmt2 NULL 写入 | 配置 null_ratio=0.3，写入 1000 行后查询，约 30% 的行目标列为 NULL | Pass |
| E2E-002 | stmt2 NONE 写入 | 先写入初始值，再配置 none_ratio=0.5 写入，约 50% 行保持原值 | Pass |
| E2E-003 | stmt2 NULL+NONE 混合 | null_ratio=0.2, none_ratio=0.3，写入 1000 行，验证 NULL/NONE/正常值比例 | Pass |

## 6.11 集成测试：Schemaless 格式写入 TDengine

### 6.11.1 测试要点

验证通过 Schemaless 格式写入 TDengine 时 NONE 语义的端到端正确性：
- Schemaless 不支持显式 NULL（列缺失自动为 NONE）
- 全字段 NULL/NONE 行被跳过，不产生无效行协议
- 部分字段 NONE 时缺失字段保持原值

### 6.11.2 用例列表

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| E2E-004 | Schemaless NONE 写入 | 配置 none_ratio=0.3，先写入初始值，再写入 NONE 数据，验证缺失字段保持原值 | Pass |
| E2E-005 | Schemaless 全字段跳过 | null_ratio=0.5, none_ratio=0.5，验证全 NULL/NONE 行不产生无效行协议 | Pass |

## 6.12 长期稳定性测试

无。

## 6.13 性能测试

无独立性能测试。RowGenerator 中 RNG 改为 `static thread_local` 后无额外性能开销。

## 6.14 安全性测试

无独立安全性测试。

# 7 兼容性测试

| # | 测试场景 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| 1 | 已有 SQL 格式不受影响 | 使用 `format: sql` 配置运行 taosgen，功能正常 | Pass |
| 2 | 已有 STMT 格式不受影响 | 使用 `format: stmt` 配置运行 taosgen，功能正常 | Pass |
| 3 | 已有 Schemaless 格式不受影响 | 使用 `format: schemaless` 配置运行 taosgen，功能正常 | Pass |
| 4 | Kafka 插件不受影响 | Kafka 插件测试用例全部通过 | Pass |
| 5 | MQTT 插件不受影响 | MQTT 插件测试用例全部通过 | Pass |
| 6 | InfluxDB 插件不受影响 | InfluxDB 插件测试用例全部通过 | Pass |
| 7 | 未配置 null_ratio/none_ratio 时行为不变 | 不配置 null_ratio/none_ratio，生成全部正常值，与原行为一致 | Pass |
| 8 | Release 模式编译通过 | Release 模式下无编译警告，构建成功 | Pass |
| 9 | 全量回归 | 96 个 CTest 用例全部通过，无回归 | Pass |

# 8 已知问题和限制

- **SQL 格式不支持显式 NONE**：SQL INSERT 语句无法显式表达 NONE 语义。
- **Schemaless 格式不支持显式 NULL**：Schemaless 行协议中列缺失自动为 NONE，用户无法主动写入 NULL。

