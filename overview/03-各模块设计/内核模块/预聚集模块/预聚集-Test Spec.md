# 预聚集-Test Spec

### 1. **修订记录**

| **编写日期** | **发布日期** | **版本** | **修订人** | **主要修改内容** |
| --- | --- | --- | --- | --- |
| 2024-01-15 | 2024-01-15 | 1.0 | 贾靖斌 | 安可送测第一版 |
| 2026-02-01 | 2026-02-01 | 1.1 | 贾靖斌 | 更新目录和内容框架 |

### 2. **测试概况**

#### 2.1 **测试目标**

本测试旨在验证 TDengine 预聚集(TSMA/RSMA)功能的正确性、性能和稳定性。预聚集包括两类实现：
- **TSMA (Time-Range Small Materialized Aggregates)**：基于时间窗口的预计算聚集，用于加速聚合查询
- **RSMA (Roll-up Small Materialized Aggregates)**：降采样预聚集，自动存储降采样数据并删除原始数据

#### 2.2 **测试范围与依据**

本次测试的范围完全覆盖预聚集功能规格，包括：
1. TSMA 的创建、删除、查看和查询优化
2. TSMA 的递归创建和大窗口聚集
3. RSMA 的创建、修改、删除和手动重算
4. 多级存储下的 RSMA 数据分层
5. 预聚集计算的正确性和性能验证

#### 2.3 **测试周期**

1. 计划开始时间：2024-01-15
2. 计划结束时间：2024-01-30
3. 总周期：15 个工作日

#### 2.4 **测试参与人员**

| **角色** | **人员** | **职责** |
| --- | --- | --- |
| **测试负责人** | 肖波 | 测试计划制定、执行管理 |
| **功能测试工程师** | 贾靖斌 | 功能测试用例执行 |
| **性能测试工程师** | 贾靖斌 | 性能测试场景设计与执行 |

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
| **测试框架** | Python + Tsim自动化框架 | 测试执行工具 |
| **监控工具** | Grafana + Prometheus | 性能监控 |

#### 3.3 **存储配置**

| **组件** | **配置** | **说明** |
| --- | --- | --- |
| **本地存储** | NVMe SSD | 热数据存储（keep 0） |
| **对象存储** | MinIO (S3兼容) | 冷数据存储（keep 1/2） |
| **文件系统** | EXT4 | 数据目录文件系统 |

### 4. **功能测试**

#### 4.1 **TSMA 创建与基础操作**

##### 4.1.1 **TSMA 创建语法测试**

| **测试项** | **测试内容** | **预期结果** | **对应脚本** |
| --- | --- | --- | --- |
| **基本语法创建** | CREATE TSMA ... FUNCTION(...) INTERVAL(...) | 创建成功，TSMA 可用 | test_tsma.py (tsma_create_tsma_on_stable) |
| **支持的聚集函数** | avg, min, max, sum, first, last, count 函数 | 函数正确应用 | test_tsma.py (tsma_create_tsma_on_stable) |
| **非法时间间隔** | INTERVAL 设置超出允许范围 (>1h 或<1ms) | 创建报错，提示间隔非法 | test_tsma.py (tsma_create_tsma_on_stable) |
| **超级表创建** | 在超级表上创建 TSMA | 创建成功，应用于所有子表 | test_tsma.py (tsma_create_tsma_on_stable) |
| **普通表创建** | 在普通表上创建 TSMA | 创建成功，仅对该表生效 | test_tsma.py (tsma_create_tsma_on_norm_table) |
| **子表拒绝** | 在子表上尝试创建 TSMA | 报错，不支持子表 | test_tsma.py (tsma_create_tsma_on_child_table) |
| **长 TSMA 名称** | TSMA 名称长度测试 | 支持长名称 | test_tsma.py (tsma_long_tsma_name) |

##### 4.1.2 **TSMA 递归创建测试**

| **测试项** | **测试内容** | **预期结果** | **对应脚本** |
| --- | --- | --- | --- |
| **递归创建基本语法** | CREATE RECURSIVE TSMA ... ON existing_tsma INTERVAL(...) | 创建成功，基于已有 TSMA 计算 | test_tsma.py (tsma_create_recursive_tsma) |
| **多级递归创建** | 基于一个 TSMA 创建多层递归 TSMA | 多级递归创建成功 | test_tsma.py (tsma_create_recursive_tsma) |
| **递归后查询使用** | 创建递归 TSMA 后执行查询 | 查询能正确使用递归 TSMA | test_tsma.py (tsma_recursive_tsma) |

##### 4.1.3 **TSMA 列数限制测试**

| **测试项** | **测试内容** | **预期结果** | **对应脚本** |
| --- | --- | --- | --- |
| **列数限制验证** | 创建超过列数限制的 TSMA | 报错"Too Many Columns" | test_tsma.py (tsma_create_tsma_maxlist_function) |

##### 4.1.4 **TSMA 删除与修改测试**

| **测试项** | **测试内容** | **预期结果** | **对应脚本** |
| --- | --- | --- | --- |
| **删除基本语法** | DROP TSMA tsma_name | 删除成功，TSMA 不可用 | test_tsma.py (tsma_drop_tsma) |
| **删除有依赖的 TSMA** | 删除被递归 TSMA 依赖的基础 TSMA | 报错，提示有依赖关系 | test_tsma.py (tsma_drop_tsma) |

##### 4.1.5 **TSMA 查看命令测试**

| **测试项** | **测试内容** | **预期结果** | **对应脚本** |
| --- | --- | --- | --- |
| **库内名字查看** | 查询库内创建的不同名称 TSMA | 能正确区分不同名称 | test_tsma.py (tsma_create_diffrent_tsma_name) |
| **长子表名称** | 长子表名称的 TSMA 查询 | 正常工作 | test_tsma.py (tsma_long_ctb_name) |

#### 4.2 **TSMA 计算与存储测试**

##### 4.2.1 **TSMA 历史数据处理**

| **测试项** | **测试内容** | **预期结果** | **对应脚本** |
| --- | --- | --- | --- |
| **创建时计算历史数据** | 表中存在历史数据时创建 TSMA | 自动计算历史数据 | test_tsma.py (tsma_query_with_tsma) |
| **大数据量计算** | 创建 TSMA 时处理大量历史数据 | 计算成功 | test_sma_basic.py (test_sma_basic) |

##### 4.2.2 **TSMA 数据写入**

| **测试项** | **测试内容** | **预期结果** | **对应脚本** |
| --- | --- | --- | --- |
| **插入新数据** | 在 TSMA 创建后插入新数据 | 新数据自动包含在 TSMA 中 | test_tsma.py (tsma_ins_tsma) |
| **修改 tag 值** | 修改超级表子表的 tag 值 | tag 修改后 TSMA 继续计算 | test_tsma.py (tsma_alter_tag_val) |

##### 4.2.3 **TSMA 配置参数**

| **测试项** | **测试内容** | **预期结果** | **对应脚本** |
| --- | --- | --- | --- |
| **querySmaOptimize 参数** | 控制 TSMA 查询优化开关 | 参数开关生效 | test_tsma.py (tsma_skip_tsma_hint) |

#### 4.3 **TSMA 查询优化测试**

##### 4.3.1 **TSMA 查询功能**

| **测试项** | **测试内容** | **预期结果** | **对应脚本** |
| --- | --- | --- | --- |
| **简单聚集查询** | SELECT COUNT(*), SUM(col) FROM stb | 使用 TSMA，结果准确 | test_tsma.py (tsma_query_tsma_all) |
| **INTERVAL 窗口查询** | SELECT AVG(col) FROM stb INTERVAL(1h) | 使用 TSMA，时间窗口结果正确 | test_tsma.py (tsma_query_with_tsma_interval) |
| **按 tbname 分组** | SELECT COUNT(*) FROM stb GROUP BY tbname | TSMA 查询分组结果正确 | test_tsma.py (tsma_query_with_tsma_agg_group_by_tbname) |
| **按 tag 分组** | SELECT COUNT(*) FROM stb GROUP BY tag | TSMA 查询分组结果正确 | test_tsma.py (tsma_query_with_tsma_interval_partition_by_col) |
| **聚集函数组合** | 同一查询中多个不同的聚集函数 | 能正确计算所有函数 | test_tsma.py (tsma_query_with_tsma_agg) |
| **子查询中的 TSMA** | SELECT * FROM (SELECT COUNT(*) FROM stb) | 子查询中能使用 TSMA | test_tsma.py |
| **递归 TSMA 使用** | 基于一个 TSMA 创建的递归 TSMA 查询 | 查询结果准确 | test_tsma.py (tsma_recursive_tsma) |

##### 4.3.2 **TSMA 查询禁用**

| **测试项** | **测试内容** | **预期结果** | **对应脚本** |
| --- | --- | --- | --- |
| **禁用 TSMA 优化** | 设置 querySmaOptimize=0 | 查询使用原始数据 | test_tsma.py (tsma_skip_tsma_hint) |
| **动态启用/禁用** | 在查询时动态切换 querySmaOptimize | 参数立即生效 | test_tsma.py (tsma_skip_tsma_hint) |

#### 4.4 **RSMA/RETENTION 创建与管理**

##### 4.4.1 **RETENTION 创建**

| **测试项** | **测试内容** | **预期结果** | **对应脚本** |
| --- | --- | --- | --- |
| **单级 RETENTION** | 创建数据库 RETENTIONS -:1d | 创建成功，单级保留配置生效 | test_create_retentions.py (check_create_databases) |
| **多级 RETENTION** | 创建数据库 RETENTIONS -:1d,2m:2d,3h:3d | 创建成功，多级降采样配置生效 | test_create_retentions.py (check_create_databases) |
| **聚集函数支持** | avg, min, max, sum, first, last | 创建超级表时支持这些函数 | test_create_retentions.py (create_stable_sql_current) |

##### 4.4.2 **RETENTION 修改**

| **测试项** | **测试内容** | **预期结果** | **对应脚本** |
| --- | --- | --- | --- |
| **无法修改 RETENTION** | ALTER DATABASE ... RETENTIONS | 修改不支持或有严格限制 | test_create_retentions.py (alter_database_sql_err) |

#### 4.5 **RSMA/RETENTION 数据处理**

| **测试项** | **测试内容** | **预期结果** | **对应脚本** |
| --- | --- | --- | --- |
| **数据自动保留** | 数据按 RETENTION 配置自动保留 | 热数据按配置时长保留 | test_create_retentions.py (test_create_retentions) |
| **多级降采样执行** | 多级 RETENTION 配置自动降采样 | 各级别按规则自动处理 | test_create_retentions.py (test_create_retentions) |

### 5. **性能测试**

#### 5.1 **TSMA 性能对比**

| **测试项** | **测试内容** | **预期结果** | **对应脚本** |
| --- | --- | --- | --- |
| **查询性能对比** | TSMA 查询 vs 原始数据查询 | TSMA 查询性能显著提升 | test_sma_basic.py (checkPerformance) |

### 6. **稳定性与功能正确性测试**

#### 6.1 **功能正确性测试**

| **测试项** | **测试内容** | **预期结果** | **对应脚本** |
| --- | --- | --- | --- |
| **聚集函数正确性** | min, max, sum, avg, first, last 等函数计算 | 计算结果准确 | test_sma_basic.py (checkCorrentFun) |
| **查询结果验证** | 使用 TSMA 的查询结果与原始数据一致 | 结果完全相同 | test_sma_basic.py (checkCorrentSum) |

#### 6.2 **多表操作测试**

| **测试项** | **测试内容** | **预期结果** | **对应脚本** |
| --- | --- | --- | --- |
| **单表查询** | 普通表上的 TSMA 查询 | 正常工作 | test_tsma.py (tsma_query_child_table) |
| **超级表查询** | 超级表及其子表的 TSMA 查询 | 聚集所有子表，结果正确 | test_tsma.py (tsma_query_with_tsma) |
| **UNION 查询** | 多个子表的 UNION 查询使用 TSMA | 结果正确 | test_tsma.py (tsma_union) |

### 7. **兼容性测试**

#### 7.1 **表类型兼容性**

| **测试项** | **测试内容** | **预期结果** | **对应脚本** |
| --- | --- | --- | --- |
| **超级表 TSMA** | 在超级表上创建 TSMA | 应用于所有子表 | test_tsma.py (tsma_create_tsma_on_stable) |
| **普通表 TSMA** | 在普通表上创建 TSMA | 正常工作 | test_tsma.py (tsma_create_tsma_on_norm_table) |
| **子表拒绝** | 在子表上创建 TSMA | 系统拒绝 | test_tsma.py (tsma_create_tsma_on_child_table) |

### 8. **运维与可观测性测试**

#### 8.1 **TSMA 管理**

| **测试项** | **测试内容** | **预期结果** | **对应脚本** |
| --- | --- | --- | --- |
| **创建与删除** | TSMA 创建和删除操作 | 创建和删除都成功 | test_tsma.py (tsma_create_and_drop_tsma) |
| **表结构变更** | 修改表中的列名或类型 | 影响 TSMA 或者 TSMA 需要先删除 | test_tsma.py (tsma_modify_col_name_value) |
| **tag 列变更** | 添加新 tag 列或修改 tag 值 | TSMA 继续工作 | test_tsma.py (tsma_add_tag_col) |

#### 8.2 **查询验证**

| **测试项** | **测试内容** | **预期结果** | **对应脚本** |
| --- | --- | --- | --- |
| **EXPLAIN 分析** | EXPLAIN 命令查看 TSMA 使用情况 | 能显示是否使用了 TSMA | test_tsma.py (check_explain) |
| **查询结果一致性** | 相同查询多次执行结果一致 | 结果完全相同 | test_tsma.py (check_sql) |

### 9. **容错与可靠性测试**

#### 9.1 **删除依赖关系检查**

| **测试项** | **测试内容** | **预期结果** | **对应脚本** |
| --- | --- | --- | --- |
| **删除被依赖的 TSMA** | 删除被递归 TSMA 依赖的基础 TSMA | 报错，提示有依赖关系，不允许删除 | test_tsma.py (tsma_drop_tsma) |
| **先删依赖再删基础** | 先删除递归 TSMA，再删除基础 TSMA | 都删除成功 | test_tsma.py (tsma_drop_tsma) |

#### 9.2 **表结构变更影响**

| **测试项** | **测试内容** | **预期结果** | **对应脚本** |
| --- | --- | --- | --- |
| **删除被使用的列** | 删除被 TSMA 使用的列 | 报错，要求先删除 TSMA | test_tsma.py (tsma_tb_ddl_with_created_tsma) |
| **修改被使用列的类型** | 修改 TSMA 中使用的列的数据类型 | 列无法修改，需要先删除 TSMA | test_tsma.py |
| **添加新列** | 向表添加新列 | 添加成功，新列不包含在现有 TSMA 中 | test_tsma.py |
| **修改列名** | 修改表中的列名 | 修改失败或 TSMA 失效 | test_tsma.py (tsma_modify_col_name_value) |

#### 9.3 **数据操作的 TSMA 影响**

| **测试项** | **测试内容** | **预期结果** | **对应脚本** |
| --- | --- | --- | --- |
| **INSERT 新数据** | 在 TSMA 创建后插入新数据 | 新数据自动包含在 TSMA 计算中 | test_tsma.py (tsma_ins_tsma) |
| **修改 tag 值** | 修改超级表子表的 tag 值 | tag 修改后 TSMA 继续计算 | test_tsma.py (tsma_alter_tag_val) |
| **添加新 tag 列** | 向超级表添加新的 tag 列 | 新 tag 不影响已有 TSMA | test_tsma.py (tsma_add_tag_col) |

#### 9.4 **递归 TSMA 的数据一致性**

| **测试项** | **测试内容** | **预期结果** | **对应脚本** |
| --- | --- | --- | --- |
| **递归 TSMA 查询一致性** | 基于基础 TSMA 创建的递归 TSMA 查询结果 | 递归 TSMA 结果与直接聚集一致 | test_tsma.py (tsma_recursive_tsma) |
| **多级递归查询** | 创建多级递归 TSMA（3+ 级）并查询 | 多级递归结果准确 | test_tsma.py (tsma_recursive_tsma) |

#### 9.5 **TSMA 查询优化降级**

| **测试项** | **测试内容** | **预期结果** | **对应脚本** |
| --- | --- | --- | --- |
| **禁用 TSMA 优化** | 设置 querySmaOptimize=0 禁用 TSMA 查询优化 | 查询使用原始数据，不使用 TSMA | test_tsma.py (tsma_skip_tsma_hint) |
| **动态启用禁用切换** | 在查询时动态切换 querySmaOptimize | 参数立即生效 | test_tsma.py (tsma_skip_tsma_hint) |

#### 9.6 **子表和普通表的区别处理**

| **测试项** | **测试内容** | **预期结果** | **对应脚本** |
| --- | --- | --- | --- |
| **子表上的 TSMA 拒绝** | 尝试在子表上创建 TSMA | 系统拒绝，报错提示 | test_tsma.py (tsma_create_tsma_on_child_table) |
| **普通表 TSMA** | 在普通表上创建 TSMA | 创建成功，查询可使用 | test_tsma.py (tsma_create_tsma_on_norm_table) |
| **超级表 TSMA** | 在超级表上创建 TSMA | 创建成功，应用于所有子表 | test_tsma.py (tsma_create_tsma_on_stable) |

#### 9.7 **TSMA 对查询性能的影响**

| **测试项** | **测试内容** | **预期结果** | **对应脚本** |
| --- | --- | --- | --- |
| **INTERVAL 查询性能** | INTERVAL 窗口查询使用 TSMA | 性能明显提升 | test_tsma.py (tsma_query_with_tsma_interval) |
| **GROUP BY 查询性能** | 按 tbname/tag 分组查询使用 TSMA | TSMA 查询比原始数据快 | test_tsma.py (tsma_query_with_tsma_agg_group_by_tbname) |
| **聚集函数查询** | 多个聚集函数的复合查询 | 能正确使用 TSMA 计算 | test_tsma.py (tsma_query_with_tsma_agg) |

### 10. **安装与卸载测试**

本功能为预聚集核心功能，安装卸载测试与主产品一致，无特殊测试项。

### 11. **测试交付物**

| **交付物** | **说明** |
| --- | --- |
| 测试设计说明 | 本文档（预聚集-Test Spec.md） |
| 测试用例 | TDinternal/community/test/cases/19-TSMAs 下的测试脚本 |

### 12. **测试通过准则**

| **准则** | **标准** |
| --- | --- |
| **功能完整性** | 所有用例 100% 通过 |
| **缺陷等级** | 无 P0（阻塞级）缺陷 |
| **性能指标** | TSMA 查询性能提升 >5 倍 |
| **稳定性** | 长期运行无故障，无内存泄漏 |
| **兼容性** | 与现有功能无冲突 |

### 13. **风险评估与应对**

| **风险** | **等级** | **应对措施** |
| --- | --- | --- |
| **TSMA 计算延迟过长** | 中 | 建立性能监控，设置 maxTsmaCalcDelay 阈值 |
| **递归 TSMA 复杂度** | 中 | 严格验证窗口倍数关系 |
| **集群环境兼容性** | 中 | 集群环境专项测试 |
| **大数据量处理** | 低 | test_sma_basic.py 已验证 TB 级数据 |

### 14. **缺陷跟踪与管理**

缺陷管理遵循 TDengine 标准流程：
- 缺陷优先级：P0（阻塞）、P1（主要）、P2（次要）、P3（建议）
- 跟踪工具：内部缺陷管理系统
- 解决目标：P0 级 100% 解决，P1 级 ≥90% 解决

### 15. **测试建议**

- **提前建立性能基线** - 在稳定版本上建立 TSMA 查询性能基准
- **集群环境充分验证** - 在多节点集群上验证功能正确性
- **大数据量压力测试** - 使用 taosBenchmark 进行大规模数据测试
- **长期稳定性验证** - 进行 7 天以上的持续运行测试
- **缓存与压缩配合测试** - 验证与其他存储优化功能的兼容性

### 16. **测试结论与发布建议**

#### 16.1 **测试总体评价**

基于本测试大纲的全面性分析，TDengine 预聚集(TSMA/RSMA)功能的测试覆盖了以下关键维度：
- **功能测试**：TSMA 创建、删除、递归创建、查询优化等核心功能
- **RETENTION 功能**：数据库级别的多级保留策略和自动降采样
- **性能测试**：TSMA 查询性能与原始数据的对比
- **稳定性测试**：函数正确性、多表操作、表结构变更影响
- **兼容性测试**：超级表、普通表、子表的 TSMA 支持
- **运维测试**：TSMA 管理、表结构变更、查询结果一致性验证

#### 16.2 **发布建议**

**推荐发布** - 预聚集功能已达到生产发布条件
- 前置条件：P0 级缺陷全部解决，性能指标确认无误，集群环境兼容性验证完成
- 适用场景：所有核心功能通过测试，性能指标达到或超过预期

#### 16.3 **后续测试计划**

1. **自动化测试建设**
- 将现有测试脚本集成到 CI/CD 流水线
- 建立每日自动化测试机制
1. **集群环境测试**
- 在多节点集群上验证 TSMA/RETENTION 功能
- 测试数据副本、故障转移等集群特性
1. **长期稳定性测试**
- 进行 7 天以上连续运行测试
- 监控内存占用、性能衰减等指标
1. **用户场景模拟**
- 基于真实业务场景设计压力测试
- 验证大规模数据、高并发查询场景

### 17. **附录**

#### 17.1 **A. 测试工具清单**

| **工具** | **版本** | **用途** |
| --- | --- | --- |
| **Python + pytest** | 3.8+ | 测试框架和自动化执行 |
| **new_test_framework** | TDengine 内置 | SQL 执行和结果验证 |
| **EXPLAIN 命令** | 内置 | TSMA 使用情况分析 |
| **Tsim 框架** | TDengine 内置 | 集成测试支持 |
| **taosBenchmark** | TDengine 工具 | 性能基准测试 |
| **Grafana + Prometheus** | 监控工具 | 性能监控与可视化 |

#### 17.2 **术语表**

| **术语** | **说明** |
| --- | --- |
| **TSMA** | Time-Range Small Materialized Aggregates，基于时间窗口的预计算聚集 |
| **RSMA** | Roll-up Small Materialized Aggregates，降采样预聚集 |
| **RETENTION** | 数据保留策略，定义数据在不同存储级别的保留时间 |
| **SMA** | Small Materialized Aggregates，小型物化聚集 |
| **maxTsmaNum** | 集群内最多能创建的 TSMA 个数，默认 8 |
| **maxTsmaCalcDelay** | TSMA 计算可接受的最大延迟时间，默认 600 秒 |
| **querySmaOptimize** | TSMA 查询优化开关，控制是否使用 TSMA 进行查询加速 |

#### 17.3 **B. 参考文档**

1. 《预聚集-Requirement Spec》- 功能需求规范
2. 《预聚集-Function Spec》- 功能详细规范
3. TDengine 官方开发文档


###
