# 测试覆盖率使用指南

本文档说明如何使用 nextest 和 llvm-cov 进行集成测试和生成覆盖率报告。

## 快速开始

### 1. 运行测试并生成覆盖率数据

```bash
# 运行特定数据源测试（例如 Kafka）
cargo make test-datasource-kafka

# 运行所有测试
cargo make test-all-datasources

# 运行框架测试
cargo make test-integration-lib
```

每个测试任务都会自动：
- 使用 **nextest** 运行测试（更快的并行执行）
- 使用 **llvm-cov** 生成覆盖率数据
- 输出 lcov 格式报告到 `target/integration-{name}-lcov.info`

### 2. 查看覆盖率报告

**方式 1：生成 HTML 报告（推荐）**

```bash
# 生成 HTML 覆盖率报告
cargo make test-coverage-html

# 报告位置：target/llvm-cov/html/index.html
# 在浏览器中打开
firefox target/llvm-cov/html/index.html
```

**方式 2：查看 lcov 文件**

```bash
# 查看生成的 lcov 文件
ls -lh target/integration-*.info

# 示例输出：
# -rw-r--r-- 1 user user 123K Dec 24 16:06 target/integration-kafka-lcov.info
# -rw-r--r-- 1 user user  45K Dec 24 16:08 target/integration-mysql-lcov.info
```

## 工具介绍

### nextest

**nextest** 是一个现代化的 Rust 测试运行器，相比 `cargo test` 有以下优势：

- ⚡ **更快的执行**：智能并行化和测试调度
- 📊 **更好的输出**：彩色、结构化的测试结果
- 🔄 **失败重试**：支持测试失败自动重试
- 🎯 **更精确的过滤**：更灵活的测试选择

**常用参数**：
```bash
cargo nextest run                    # 运行所有测试
cargo nextest run --features kafka   # 运行特定 feature 的测试
cargo nextest run test_name          # 运行特定测试
cargo nextest list                   # 列出所有测试
```

### llvm-cov

**llvm-cov** 是基于 LLVM 的代码覆盖率工具，特点：

- 🎯 **准确的覆盖率**：基于编译器插桩，精确到行级别
- 📄 **多种输出格式**：lcov、HTML、JSON 等
- 🔗 **CI/CD 集成**：lcov 格式可直接用于 Codecov 等服务
- 🚀 **性能优化**：增量覆盖率收集

**输出格式**：
- **lcov** (`--lcov`)：适合 CI/CD 和工具集成
- **HTML** (`--html`)：适合本地查看，可视化覆盖率

## 集成测试任务列表

### 数据源测试

| 命令 | 数据源 | 生成的 lcov 文件 |
|------|--------|------------------|
| `cargo make test-datasource-kafka` | Kafka | `target/integration-kafka-lcov.info` |
| `cargo make test-datasource-mysql` | MySQL | `target/integration-mysql-lcov.info` |
| `cargo make test-datasource-oracle` | Oracle | `target/integration-oracle-lcov.info` |
| `cargo make test-datasource-postgres` | PostgreSQL | `target/integration-postgres-lcov.info` |
| `cargo make test-datasource-mongodb` | MongoDB | `target/integration-mongodb-lcov.info` |
| `cargo make test-datasource-mssql` | MSSQL | `target/integration-mssql-lcov.info` |
| `cargo make test-datasource-mqtt` | MQTT | `target/integration-mqtt-lcov.info` |
| `cargo make test-datasource-opcua` | OPC-UA | `target/integration-opcua-lcov.info` |
| `cargo make test-datasource-opcda` | OPC-DA | `target/integration-opcda-lcov.info` |
| `cargo make test-datasource-pi` | PI System | `target/integration-pi-lcov.info` |
| `cargo make test-datasource-historian` | Historian | `target/integration-historian-lcov.info` |

### 分组测试

| 命令 | 包含的数据源 | 生成的 lcov 文件 |
|------|--------------|------------------|
| `cargo make test-relational-db` | MySQL, Oracle, PostgreSQL, MSSQL | `target/integration-relational-db-lcov.info` |
| `cargo make test-nosql-db` | MongoDB | `target/integration-nosql-db-lcov.info` |
| `cargo make test-message-queue` | Kafka, MQTT | `target/integration-message-queue-lcov.info` |
| `cargo make test-industrial-protocol` | OPC-UA, OPC-DA, PI, Historian | `target/integration-industrial-protocol-lcov.info` |
| `cargo make test-all-datasources` | 所有数据源 | `target/integration-all-datasources-lcov.info` |

### 其他测试

| 命令 | 描述 | 生成的 lcov 文件 |
|------|------|------------------|
| `cargo make test-integration-lib` | 测试框架初始化 | `target/integration-lib-lcov.info` |
| `cargo make test-core` | 核心功能测试 | `target/integration-core-lcov.info` |
| `cargo make test-e2e` | 端到端场景测试 | `target/integration-e2e-lcov.info` |

## 典型工作流

### 开发新测试

```bash
# 1. 编写测试代码
# 2. 检查编译
cargo make test-integration-check

# 3. 运行测试
cargo make test-datasource-kafka

# 4. 查看覆盖率
cargo make test-coverage-html

# 5. 在浏览器中打开报告
firefox target/llvm-cov/html/index.html
```

### CI/CD 集成

```bash
# 运行所有测试并生成 lcov 报告
cargo make test-all-datasources

# 上传 lcov 报告到 Codecov
codecov -f target/integration-all-datasources-lcov.info
```

### 调试失败的测试

```bash
# 运行特定测试并显示详细输出
cargo nextest run -p taosx-integration-tests --features test-kafka test_kafka_connection

# 运行测试并保持失败时的环境
cargo nextest run --no-fail-fast

# 查看测试列表
cargo nextest list --features test-kafka
```

## 覆盖率目标

根据项目要求，集成测试的覆盖率目标：

- **新增代码**：≥ 80% 行覆盖率
- **核心模块**：≥ 90% 行覆盖率
- **整体项目**：≥ 75% 行覆盖率

## 常见问题

### Q: 为什么 lcov 文件是空的？

A: 这是正常的，llvm-cov 生成的 lcov 文件可能很小或为空。使用 `cargo make test-coverage-html` 生成 HTML 报告可以查看实际覆盖率。

### Q: 如何查看特定文件的覆盖率？

A: 生成 HTML 报告后，在浏览器中可以导航到具体文件查看行级覆盖率：

```bash
cargo make test-coverage-html
firefox target/llvm-cov/html/index.html
# 点击文件名查看详细覆盖率
```

### Q: 如何合并多个测试的覆盖率数据？

A: llvm-cov 会自动合并同一次运行的覆盖率数据。如果需要合并不同测试运行的数据，使用 `llvm-cov report`：

```bash
# 运行多个测试
cargo make test-datasource-kafka
cargo make test-datasource-mysql

# 生成合并后的报告
cargo make test-coverage-html
```

### Q: nextest 和 cargo test 有什么区别？

A: 主要区别：
- **nextest**：并行执行更快，输出更清晰，支持重试
- **cargo test**：Rust 内置，简单直接

选择 nextest 是为了提高开发效率和 CI 速度。

## 相关文档

- [TEST_QUICKSTART.md](TEST_QUICKSTART.md) - 快速上手指南
- [TEST_REFACTORING_PLAN.md](TEST_REFACTORING_PLAN.md) - 测试重构计划
- [TEST_MIGRATION_EXAMPLE.md](TEST_MIGRATION_EXAMPLE.md) - Kafka 测试迁移示例
- [PHASE_1_COMPLETION_REPORT.md](PHASE_1_COMPLETION_REPORT.md) - Phase 1 完成报告

## 获取帮助

```bash
# 显示所有集成测试命令
cargo make test-integration-help

# nextest 帮助
cargo nextest --help

# llvm-cov 帮助
cargo llvm-cov --help
```

---

**最后更新**: 2024-12-24  
**维护者**: taosX 团队
