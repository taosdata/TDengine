# taosX 集成测试重构计划

## 1. 现状分析

### 当前测试结构
```
tests/
├── e2e/                    # Python E2E 测试
│   ├── test_function/      # 各数据源功能测试
│   │   ├── kafka_test.py
│   │   ├── mqtt_test.py
│   │   ├── opcua_test.py
│   │   ├── mysql_test.py
│   │   ├── oracle_test.py
│   │   └── ...
│   ├── config/            # 环境配置
│   └── pytest.ini         # pytest 配置
├── kafka/                 # Rust 集成测试
├── mqtt/
├── opc/
├── pi/
└── performance/           # 性能测试
```

### 现存问题

1. **组织混乱**：
   - Python E2E 测试和 Rust 集成测试分离
   - 无统一的数据源依赖管理
   - 无法单独运行某个数据源的测试

2. **执行困难**：
   - 需要手动配置多个第三方数据源
   - 缺乏数据源可用性检查机制
   - 测试间依赖关系不清晰

3. **维护成本高**：
   - 测试配置分散在多个文件
   - 缺乏统一的测试标签体系
   - 难以进行增量测试

## 2. 重构目标

### 核心目标
- ✅ 统一测试组织结构，按数据源分类
- ✅ 实现测试的独立性和可选择性
- ✅ 通过 cargo make 管理所有测试任务
- ✅ 自动检测和跳过不可用的数据源
- ✅ 提供清晰的测试报告和覆盖率

### 非功能目标
- 📊 集成覆盖率收集（Rust + Python）
- 🚀 支持并行测试执行
- 🔄 CI/CD 友好
- 📝 完善的测试文档

## 3. 重构方案

### 3.1 新的目录结构

```
tests/
├── integration/              # 所有集成测试入口
│   ├── core/                 # 核心功能测试（无外部依赖）
│   │   ├── tmq/
│   │   ├── backup/
│   │   └── replication/
│   │
│   ├── datasources/          # 数据源集成测试（Rust）
│   │   ├── kafka/
│   │   │   ├── mod.rs
│   │   │   ├── basic.rs
│   │   │   └── advanced.rs
│   │   ├── mysql/
│   │   ├── oracle/
│   │   ├── mqtt/
│   │   ├── mongodb/
│   │   ├── postgres/
│   │   ├── mssql/
│   │   ├── opcua/
│   │   ├── opcda/
│   │   ├── pi/
│   │   └── historian/
│   │
│   ├── e2e/                  # Python E2E 场景测试
│   │   ├── scenarios/
│   │   │   ├── kafka/
│   │   │   ├── mysql/
│   │   │   └── ...
│   │   └── pyproject.toml
│   │
│   └── common/               # 测试辅助工具
│       ├── fixtures.rs
│       ├── helpers.rs
│       └── data_generator.rs
│
├── performance/              # 性能测试
│   ├── throughput/
│   └── latency/
│
└── Cargo.toml                # 测试工作空间配置
```

### 3.2 测试分类标签系统

#### Rust 测试标签（使用 cfg 和自定义属性）

```rust
// 核心测试（无外部依赖）
#[test]
#[cfg_attr(not(feature = "integration"), ignore)]
fn test_core_functionality() {}

// 数据源测试（需要特定数据源）
#[test]
#[cfg(feature = "test-kafka")]
fn test_kafka_integration() {}

#[test]
#[cfg(feature = "test-mysql")]
fn test_mysql_integration() {}

// 工业协议测试（需要特殊环境）
#[test]
#[cfg(all(feature = "test-opcua", target_os = "linux"))]
fn test_opcua_integration() {}
```

#### Python 测试标签（使用 pytest.mark）

```python
# 按数据源分类
@pytest.mark.kafka
@pytest.mark.sanity
def test_kafka_basic():
    pass

@pytest.mark.mysql
@pytest.mark.performance
def test_mysql_throughput():
    pass

# 按测试级别分类
@pytest.mark.smoke      # 冒烟测试（最基础）
@pytest.mark.sanity     # 正常功能测试
@pytest.mark.regression # 回归测试
@pytest.mark.stress     # 压力测试
```

### 3.3 Cargo.toml 配置

在 `tests/Cargo.toml` 中添加：

```toml
[package]
name = "taosx-integration-tests"
version = "0.1.0"
edition = "2021"
publish = false

[features]
default = []
# 核心测试
integration = []

# 数据源测试
test-kafka = []
test-mysql = []
test-oracle = []
test-postgres = []
test-mongodb = []
test-mssql = []
test-mqtt = []

# 工业协议
test-opcua = []
test-opcda = []
test-pi = []
test-historian = []

# 测试组合
test-databases = ["test-mysql", "test-oracle", "test-postgres", "test-mongodb", "test-mssql"]
test-message-queues = ["test-kafka", "test-mqtt"]
test-industrial = ["test-opcua", "test-opcda", "test-pi", "test-historian"]
test-all-datasources = ["test-databases", "test-message-queues", "test-industrial"]

[dependencies]
taosx-core = { path = "../taosx-core" }
taosx-task = { path = "../crates/task" }
taos = { workspace = true }
tokio = { workspace = true }
anyhow = { workspace = true }
```

### 3.4 cargo make 任务定义

在 `Makefile.toml` 中添加：

```toml
# ==================== 核心测试任务 ====================

[tasks.test-core]
description = "运行核心功能测试（无外部依赖）"
command = "cargo"
args = [
    "nextest", "run",
    "--workspace",
    "--exclude", "taosx-integration-tests",
    "-E", "not test(/_with_datasource$/)"
]

# ==================== 数据源测试任务 ====================

[tasks.test-datasource-kafka]
description = "测试 Kafka 集成"
env = { "RUST_LOG" = "debug" }
command = "cargo"
args = [
    "nextest", "run",
    "-p", "taosx-integration-tests",
    "--features", "test-kafka",
    "-E", "test(/kafka/)"
]

[tasks.test-datasource-mysql]
description = "测试 MySQL 集成"
command = "cargo"
args = [
    "nextest", "run",
    "-p", "taosx-integration-tests",
    "--features", "test-mysql",
    "-E", "test(/mysql/)"
]

[tasks.test-datasource-oracle]
description = "测试 Oracle 集成"
command = "cargo"
args = [
    "nextest", "run",
    "-p", "taosx-integration-tests",
    "--features", "test-oracle",
    "-E", "test(/oracle/)"
]

[tasks.test-datasource-postgres]
description = "测试 PostgreSQL 集成"
command = "cargo"
args = [
    "nextest", "run",
    "-p", "taosx-integration-tests",
    "--features", "test-postgres",
    "-E", "test(/postgres/)"
]

[tasks.test-datasource-mongodb]
description = "测试 MongoDB 集成"
command = "cargo"
args = [
    "nextest", "run",
    "-p", "taosx-integration-tests",
    "--features", "test-mongodb",
    "-E", "test(/mongodb/)"
]

[tasks.test-datasource-mqtt]
description = "测试 MQTT 集成"
command = "cargo"
args = [
    "nextest", "run",
    "-p", "taosx-integration-tests",
    "--features", "test-mqtt",
    "-E", "test(/mqtt/)"
]

# ==================== 更多数据源测试任务 ====================

[tasks.test-datasource-opcua]
description = "测试 OPC-UA 集成"
command = "cargo"
args = [
    "nextest", "run",
    "-p", "taosx-integration-tests",
    "--features", "test-opcua",
    "-E", "test(/opcua/)"
]

[tasks.test-datasource-pi]
description = "测试 PI System 集成"
command = "cargo"
args = [
    "nextest", "run",
    "-p", "taosx-integration-tests",
    "--features", "test-pi",
    "-E", "test(/pi/)"
]

# ==================== 测试组合任务 ====================

[tasks.test-all-relational-db]
description = "测试所有关系型数据库数据源"
command = "cargo"
args = [
    "nextest", "run",
    "-p", "taosx-integration-tests",
    "--features", "test-relational-db",
    "-E", "test(/(mysql|oracle|postgres|mssql)/)"
]

[tasks.test-all-nosql-db]
description = "测试所有 NoSQL 数据库数据源"
command = "cargo"
args = [
    "nextest", "run",
    "-p", "taosx-integration-tests",
    "--features", "test-nosql-db",
    "-E", "test(/mongodb/)"
]

[tasks.test-all-message-queue]
description = "测试所有消息队列数据源"
command = "cargo"
args = [
    "nextest", "run",
    "-p", "taosx-integration-tests",
    "--features", "test-message-queue",
    "-E", "test(/(kafka|mqtt)/)"
]

[tasks.test-all-industrial-protocol]
description = "测试所有工业协议数据源"
command = "cargo"
args = [
    "nextest", "run",
    "-p", "taosx-integration-tests",
    "--features", "test-industrial-protocol",
    "-E", "test(/(opcua|opcda|pi|historian)/)"
]

# ==================== Python E2E 测试任务 ====================

[tasks.e2e-setup]
description = "设置 E2E 测试环境"
script = """
cd tests/integration/e2e
poetry install
"""

[tasks.e2e-kafka]
description = "运行 Kafka E2E 测试"
dependencies = ["e2e-setup"]
script = """
cd tests/integration/e2e
poetry run pytest -m kafka --tb=short
"""

[tasks.e2e-mysql]
description = "运行 MySQL E2E 测试"
dependencies = ["e2e-setup"]
script = """
cd tests/integration/e2e
poetry run pytest -m mysql --tb=short
"""

[tasks.e2e-sanity]
description = "运行冒烟测试"
dependencies = ["e2e-setup"]
script = """
cd tests/integration/e2e
poetry run pytest -m smoke --tb=short
"""

[tasks.e2e-all]
description = "运行所有 E2E 测试"
dependencies = ["e2e-setup"]
script = """
cd tests/integration/e2e
poetry run pytest -m "not (slow or manual)" --tb=short
"""

# ==================== 便捷组合任务 ====================

[tasks.test-quick]
description = "快速测试（核心 + 冒烟）"
dependencies = ["test-core", "e2e-sanity"]

[tasks.test-integration-all]
description = "运行所有集成测试"
command = "cargo"
args = [
    "nextest", "run",
    "-p", "taosx-integration-tests",
    "--features", "test-all-datasources",
    "--no-fail-fast"
]

# ==================== 测试报告和覆盖率 ====================

[tasks.test-integration-with-coverage]
description = "运行集成测试并收集覆盖率"
dependencies = ["install-llvm-cov", "install-nextest"]
command = "cargo"
args = [
    "llvm-cov",
    "--lcov",
    "--output-path", "target/integration-coverage.lcov",
    "nextest",
    "-p", "taosx-integration-tests",
    "--features", "test-all-datasources",
]

[tasks.test-report]
description = "生成测试报告"
script = """
echo "=== Test Summary ==="
cargo nextest list -p taosx-integration-tests
echo ""
echo "Run specific datasource test with:"
echo "  cargo make test-datasource-<name>"
echo ""
echo "Available datasources: kafka, mysql, oracle, postgres, mongodb, mqtt, opcua, pi"
"""

# ==================== 数据源健康检查 ====================

[tasks.check-datasources]
description = "检查数据源可用性"
script = """
#!/bin/bash
echo "Checking datasource availability..."

# Kafka
if nc -z localhost 9092 2>/dev/null; then
    echo "✓ Kafka available on localhost:9092"
else
    echo "✗ Kafka not available"
fi

# MySQL
if nc -z localhost 3306 2>/dev/null; then
    echo "✓ MySQL available on localhost:3306"
else
    echo "✗ MySQL not available"
fi

# Oracle
if nc -z localhost 1521 2>/dev/null; then
    echo "✓ Oracle available on localhost:1521"
else
    echo "✗ Oracle not available"
fi

# Add more checks as needed...
"""
```

### 3.5 pytest.ini 配置更新

```ini
[pytest]
# 数据源标签
markers =
    kafka: Kafka integration tests
    mysql: MySQL integration tests
    oracle: Oracle integration tests
    postgres: PostgreSQL integration tests
    mongodb: MongoDB integration tests
    mssql: SQL Server integration tests
    mqtt: MQTT integration tests
    opcua: OPC-UA integration tests
    opcda: OPC-DA integration tests
    pi: PI System integration tests
    historian: Historian integration tests
    
    # 测试级别
    smoke: Smoke tests (fastest, most critical)
    sanity: Sanity tests (normal functionality)
    regression: Regression tests
    performance: Performance tests
    stress: Stress tests
    
    # 其他分类
    slow: Slow running tests
    manual: Manual tests (require special setup)
    windows_only: Windows platform only
    linux_only: Linux platform only

# 超时配置
timeout = 300
timeout_method = thread

# 日志配置
log_cli = True
log_cli_level = INFO
log_file = ../logs/integration_tests.log
log_file_level = DEBUG
```

## 4. 实施步骤

### Phase 1: 基础架构搭建（Week 1）
1. ✅ 创建新的目录结构
2. ✅ 配置 Cargo.toml features
3. ✅ 更新 Makefile.toml 任务定义
4. ✅ 更新 pytest.ini 配置

### Phase 2: 测试迁移（Week 2-3）
1. 迁移 Kafka 测试作为示例
   - Rust 集成测试
   - Python E2E 测试
2. 验证新结构可行性
3. 编写迁移指南

### Phase 3: 批量迁移（Week 4-6）
1. 按数据源类型迁移测试
   - 关系型数据库（MySQL, Oracle, PostgreSQL, SQL Server）
   - NoSQL 数据库（MongoDB）
   - 消息队列（Kafka, MQTT）
   - 工业协议（OPC-UA/DA, PI, Historian）
2. 更新测试用例标签
3. 清理旧测试代码

### Phase 4: 完善和优化（Week 7-8）
1. 实现数据源健康检查
2. 优化测试并行执行
3. 集成 CI/CD pipeline
4. 编写测试文档

### Phase 5: 验证和发布（Week 9）
1. 完整的测试运行验证
2. 性能基准测试
3. 文档审查
4. 团队培训

## 5. 使用示例

### 开发者日常使用

```bash
# 1. 运行核心测试（快速反馈）
cargo make test-core

# 2. 开发 Kafka 连接器时
cargo make test-datasource-kafka

# 3. 运行某个数据源的完整测试（Rust + Python）
cargo make test-datasource-mysql
cargo make e2e-mysql

# 4. 检查数据源可用性
cargo make check-datasources

# 5. 运行快速冒烟测试
cargo make test-quick

# 6. 查看可用的测试任务
cargo make test-report
```

### CI/CD 使用

```bash
# PR 检查：核心测试 + 冒烟测试
cargo make test-quick

# 每日构建：所有数据库测试
cargo make test-all-databases

# 发布前：完整测试
cargo make test-integration-all
cargo make e2e-all

# 带覆盖率的完整测试
cargo make test-integration-with-coverage
```

### Python E2E 测试

```bash
# 按标签运行
cd tests/integration/e2e
poetry run pytest -m kafka           # Kafka 测试
poetry run pytest -m "mysql or oracle"  # 多个数据源
poetry run pytest -m smoke           # 冒烟测试

# 按文件运行
poetry run pytest scenarios/kafka/test_basic.py

# 按测试名运行
poetry run pytest -k "test_kafka_basic"
```

## 6. 优势和收益

### 开发效率提升
- ⚡ 快速定位到特定数据源的测试
- 🎯 只运行相关的测试，节省时间
- 📋 清晰的测试组织，易于导航

### 测试质量提升
- 🔒 独立的测试环境，减少干扰
- 🏷️ 标准化的标签体系
- 📊 更好的测试覆盖率跟踪

### 维护成本降低
- 📝 统一的配置管理
- 🔄 易于添加新的数据源测试
- 🛠️ 简化的 CI/CD 集成

### 团队协作改善
- 📖 清晰的文档和示例
- 🤝 降低新成员学习成本
- 🎓 标准化的测试实践

## 7. 风险和缓解措施

### 风险1：迁移过程中测试覆盖率下降
**缓解**：
- 保留旧测试直到新测试验证通过
- 并行运行新旧测试对比结果
- 逐步迁移，每个数据源单独验证

### 风险2：Feature 标志过多导致组合爆炸
**缓解**：
- 使用 feature 组合简化配置
- 提供预定义的测试场景
- 文档化常用组合

### 风险3：CI/CD 流水线调整成本
**缓解**：
- 提供向后兼容的任务名称
- 分阶段迁移 CI 配置
- 详细的迁移指南

## 8. 未来展望

### 短期目标（3个月）
- 完成所有测试迁移
- 稳定新的测试架构
- 培训团队成员

### 中期目标（6个月）
- 实现测试容器化
- 自动化测试环境搭建
- 集成性能基准测试

### 长期目标（12个月）
- 智能测试选择（基于代码变更）
- 测试结果分析和可视化
- 持续优化测试效率

## 9. 参考资源

- [Cargo Features 文档](https://doc.rust-lang.org/cargo/reference/features.html)
- [cargo-nextest 文档](https://nexte.st/)
- [pytest 标签文档](https://docs.pytest.org/en/stable/how-to/mark.html)
- [cargo-make 文档](https://github.com/sagiegurari/cargo-make)

---

**文档版本**: v1.0  
**创建日期**: 2025-12-24  
**负责人**: TaosX 团队  
**审核状态**: 待审核
