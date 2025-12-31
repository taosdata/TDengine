# 集成测试重构 - 快速开始指南

## 快速索引

- [新用户快速上手](#新用户快速上手)
- [开发者常用命令](#开发者常用命令)
- [迁移现有测试](#迁移现有测试)
- [FAQ](#faq)

## 新用户快速上手

### 1. 安装依赖

```bash
# 安装 Rust 测试工具
cargo install cargo-nextest cargo-make

# 安装 Python 测试环境
cd tests/integration/e2e
poetry install
```

### 2. 检查数据源状态

```bash
# 查看哪些数据源可用
cargo make check-datasources
```

### 3. 运行你的第一个测试

```bash
# 运行核心测试（不需要外部依赖）
cargo make test-core

# 运行冒烟测试
cargo make e2e-sanity
```

## 开发者常用命令

### 日常开发场景

#### 场景1: 开发 Kafka 连接器

```bash
# 1. 检查 Kafka 是否可用
nc -z localhost 9092

# 2. 运行 Kafka 相关的 Rust 测试
cargo make test-datasource-kafka

# 3. 运行 Kafka E2E 测试
cargo make e2e-kafka

# 4. 运行特定的测试用例
cd tests/integration/e2e
poetry run pytest -m kafka -k "test_basic"
```

#### 场景2: 修复 MySQL 连接器 Bug

```bash
# 1. 运行 MySQL 测试查看问题
cargo make test-datasource-mysql

# 2. 修改代码...

# 3. 只运行失败的测试
cargo nextest run -p taosx-integration-tests --features test-mysql --failing-first

# 4. 验证修复
cargo make test-datasource-mysql
cargo make e2e-mysql
```

#### 场景3: 添加新的 PostgreSQL 功能

```bash
# 1. 添加测试用例到 tests/integration/datasources/postgres/

# 2. 运行 PostgreSQL 测试
cargo make test-datasource-postgres

# 3. 查看测试覆盖率
cargo make test-integration-with-coverage

# 4. 生成覆盖率报告
cargo llvm-cov report --html
```

### 测试组合场景

```bash
# 按数据源类型测试
cargo make test-all-relational-db         # 关系型数据库
cargo make test-all-nosql-db              # NoSQL 数据库
cargo make test-all-message-queue         # 消息队列
cargo make test-all-industrial-protocol   # 工业协议

# 运行完整的集成测试套件
cargo make test-integration-all
```

### CI/CD 场景

```bash
# PR 验证（快速）
cargo make test-quick

# 每日构建（中等）
cargo make test-all-relational-db
cargo make test-all-message-queue

# 发布前验证（完整）
cargo make test-integration-all
cargo make e2e-all
```

## 迁移现有测试

### 步骤1: 确定测试类型

判断你的测试属于哪一类：

```
核心功能测试     → tests/integration/core/
数据源集成测试   → tests/integration/datasources/<datasource>/
  - 关系型数据库: mysql, oracle, postgres, mssql
  - NoSQL数据库:  mongodb
  - 消息队列:     kafka, mqtt
  - 工业协议:     opcua, opcda, pi, historian
性能测试        → tests/performance/
```

### 步骤2: 迁移 Rust 测试

**Before** (旧结构):
```rust
// tests/kafka/basic_test.rs
#[tokio::test]
async fn test_kafka_basic() {
    // test code
}
```

**After** (新结构):
```rust
// tests/integration/datasources/kafka/basic.rs
#[cfg(feature = "test-kafka")]
mod tests {
    use super::*;
    
    #[tokio::test]
    async fn test_kafka_basic() {
        // test code
    }
}
```

### 步骤3: 迁移 Python 测试

**Before** (旧结构):
```python
# tests/e2e/test_function/kafka_test.py
def test_kafka_basic():
    pass
```

**After** (新结构):
```python
# tests/integration/e2e/scenarios/kafka/test_basic.py
import pytest

@pytest.mark.kafka
@pytest.mark.sanity
def test_kafka_basic():
    pass
```

### 步骤4: 添加测试标签

```python
# 根据测试特性添加合适的标签

# 基础功能测试
@pytest.mark.kafka
@pytest.mark.sanity

# 性能测试
@pytest.mark.kafka
@pytest.mark.performance

# 需要特殊环境的测试
@pytest.mark.kafka
@pytest.mark.manual
@pytest.mark.slow

# 平台特定测试
@pytest.mark.windows_only
@pytest.mark.opcda
```

### 步骤5: 更新 Cargo.toml

在 `tests/Cargo.toml` 中确保 feature 已定义：

```toml
[features]
test-kafka = []
test-mysql = []
# ... 其他数据源
```

### 步骤6: 验证迁移

```bash
# 验证 Rust 测试
cargo make test-datasource-kafka

# 验证 Python 测试
cd tests/integration/e2e
poetry run pytest -m kafka -v

# 验证完整流程
cargo make test-quick
```

## 测试标签使用指南

### Rust Feature 标签

```rust
// 单个数据源
#[cfg(feature = "test-kafka")]

// 多个数据源
#[cfg(any(feature = "test-kafka", feature = "test-mqtt"))]

// 平台特定
#[cfg(all(feature = "test-pi", target_os = "windows"))]

// 排除某些情况
#[cfg(not(feature = "test-slow"))]
```

### Python pytest 标签

```python
# 单个标签
@pytest.mark.kafka

# 多个标签
@pytest.mark.kafka
@pytest.mark.sanity
@pytest.mark.performance

# 条件跳过
@pytest.mark.skipif(
    os.environ.get("KAFKA_HOST") is None,
    reason="Kafka not configured"
)

# 预期失败
@pytest.mark.xfail(
    reason="Known issue #1234",
    run=True
)
```

### 运行带标签的测试

```bash
# Rust - 通过 feature
cargo nextest run --features test-kafka

# Python - 通过 marker
poetry run pytest -m kafka
poetry run pytest -m "kafka and sanity"
poetry run pytest -m "kafka or mqtt"
poetry run pytest -m "not slow"
```

## 故障排查

### 问题1: 测试找不到数据源

**症状**: 测试失败，提示无法连接到数据源

**解决**:
```bash
# 1. 检查数据源是否运行
cargo make check-datasources

# 2. 检查环境变量
echo $KAFKA_HOST
echo $MYSQL_HOST

# 3. 手动测试连接
nc -z localhost 9092  # Kafka
mysql -h localhost -P 3306  # MySQL
```

### 问题2: Feature 未启用

**症状**: 测试被跳过或不运行

**解决**:
```bash
# 确保使用正确的 feature
cargo nextest run --features test-kafka

# 或使用 cargo make 任务
cargo make test-datasource-kafka
```

### 问题3: Python 依赖问题

**症状**: ImportError 或 ModuleNotFoundError

**解决**:
```bash
cd tests/integration/e2e

# 重新安装依赖
poetry install --no-root

# 验证环境
poetry run python -c "import pytest; print(pytest.__version__)"

# 激活 shell 直接运行
poetry shell
pytest --version
```

### 问题4: 测试超时

**症状**: 测试运行超过预期时间

**解决**:
```python
# 增加超时时间
@pytest.mark.timeout(600)  # 10 minutes
def test_slow_operation():
    pass
```

```bash
# 或使用命令行参数
poetry run pytest --timeout=600
```

## 性能优化技巧

### 1. 并行执行测试

```bash
# Rust 测试并行
cargo nextest run -j 8 --features test-kafka

# Python 测试并行 (需要 pytest-xdist)
poetry run pytest -n 4 -m kafka
```

### 2. 只运行失败的测试

```bash
# Rust
cargo nextest run --failing-first

# Python
poetry run pytest --lf  # last failed
poetry run pytest --ff  # failed first
```

### 3. 缓存测试结果

```bash
# Rust - nextest 自动缓存
cargo nextest run --features test-kafka

# Python - 使用 cache
poetry run pytest --cache-show
poetry run pytest --cache-clear
```

### 4. 分片测试（CI 环境）

```bash
# 将测试分成 4 个分片，运行第 1 个
poetry run pytest --splits 4 --group 1

# 在不同的 CI job 中运行不同的分片
# Job 1: --splits 4 --group 1
# Job 2: --splits 4 --group 2
# Job 3: --splits 4 --group 3
# Job 4: --splits 4 --group 4
```

## 最佳实践

### 1. 测试命名规范

```rust
// Rust - 描述性命名
#[test]
fn test_kafka_basic_consume_produce() {}

#[test]
fn test_kafka_error_invalid_broker() {}

#[test]
fn test_kafka_perf_high_throughput() {}
```

```python
# Python - 使用前缀和后缀
def test_kafka_basic_connection():
    """测试 Kafka 基本连接"""
    
def test_kafka_sanity_consume_messages():
    """验证 Kafka 消息消费"""
    
def test_kafka_perf_throughput_1m_messages():
    """性能测试：100万消息吞吐量"""
```

### 2. 测试组织

```
✅ 好的做法：
- 按功能模块组织（basic, advanced, error_handling）
- 每个文件专注单一职责
- 使用 fixture 共享设置代码

❌ 不好的做法：
- 所有测试放在一个文件
- 测试间有依赖关系
- 硬编码配置值
```

### 3. 测试隔离

```rust
// 使用独立的测试数据
#[tokio::test]
async fn test_kafka_topic_isolation() {
    let topic = format!("test-{}", uuid::Uuid::new_v4());
    // 使用 topic 进行测试
    // 测试结束后清理
}
```

### 4. 错误消息

```rust
// 提供清晰的错误信息
assert_eq!(
    actual, expected,
    "Kafka message count mismatch. Expected {} messages but got {}. \
     Check broker logs at /var/log/kafka/",
    expected, actual
);
```

## FAQ

### Q: 我应该使用 Rust 测试还是 Python 测试？

**A**: 两者结合使用：
- **Rust 测试**: 单元测试、集成测试、性能关键路径
- **Python 测试**: E2E 场景测试、复杂的多步骤流程、用户场景验证

### Q: 如何添加新的数据源测试？

**A**: 
1. 在 `tests/Cargo.toml` 添加 feature
2. 创建 `tests/integration/datasources/<source>/` 目录
3. 在 `Makefile.toml` 添加 `test-datasource-<source>` 任务
4. 添加 Python E2E 测试和标签

### Q: CI 应该运行哪些测试？

**A**:
- **PR 检查**: `cargo make test-quick`
- **每日构建**: `cargo make test-all-relational-db`
- **发布前**: `cargo make test-integration-all`

### Q: 测试数据如何管理？

**A**:
- 小数据：直接在测试代码中
- 中等数据：使用 `tests/integration/common/fixtures/`
- 大数据：生成或从外部加载

### Q: 如何调试失败的测试？

**A**:
```bash
# Rust - 显示详细输出
RUST_LOG=debug cargo nextest run <test-name> --no-capture

# Python - 详细模式
poetry run pytest -vv -s <test-file>::<test-name>

# 使用调试器
poetry run pytest --pdb <test-file>
```

## 相关文档

- [完整重构计划](TEST_REFACTORING_PLAN.md)
- [测试贡献指南](CONTRIBUTING.md#testing)
- [CI/CD 配置](.github/workflows/pr-ci.yaml)

---

**文档更新**: 2025-12-24  
**问题反馈**: 请在 GitHub Issues 提交
