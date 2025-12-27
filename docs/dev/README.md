# taosX 开发文档

本目录包含 taosX 项目的开发相关文档。

## 📚 文档索引

### 测试重构相关

| 文档 | 描述 | 适合人群 |
|------|------|---------|
| [TEST_REFACTORING_SUMMARY.md](TEST_REFACTORING_SUMMARY.md) | **项目概览** - 执行摘要、时间线、团队协作 | 项目管理、全体成员 |
| [TEST_REFACTORING_PLAN.md](TEST_REFACTORING_PLAN.md) | **技术方案** - 完整架构设计和实施细节 | 架构师、技术负责人 |
| [TEST_QUICKSTART.md](TEST_QUICKSTART.md) | **快速上手** - 常用命令和开发场景 | 所有开发者 |
| [COVERAGE_USAGE.md](COVERAGE_USAGE.md) | **覆盖率工具** - nextest 和 llvm-cov 使用指南 | 所有开发者 |
| [NEXTEST_LLVM_COV_INTEGRATION.md](NEXTEST_LLVM_COV_INTEGRATION.md) | **工具集成报告** - nextest 和 llvm-cov 集成完成报告 | 技术负责人、开发者 |
| [TEST_MIGRATION_EXAMPLE.md](TEST_MIGRATION_EXAMPLE.md) | **迁移示例** - Kafka 测试迁移完整案例 | 实施开发者 |
| [PHASE_1_COMPLETION_REPORT.md](PHASE_1_COMPLETION_REPORT.md) | **Phase 1 完成报告** - 基础设施实施和验证结果 | 所有成员 |

### 阅读建议

#### 🚀 新成员快速上手
1. 先读 [TEST_REFACTORING_SUMMARY.md](TEST_REFACTORING_SUMMARY.md) 了解项目背景和目标
2. 再看 [TEST_QUICKSTART.md](TEST_QUICKSTART.md) 学习常用命令
3. 查看 [COVERAGE_USAGE.md](COVERAGE_USAGE.md) 了解测试和覆盖率工具
4. 需要时参考 [TEST_MIGRATION_EXAMPLE.md](TEST_MIGRATION_EXAMPLE.md)

#### 🏗️ 技术评审
1. 阅读 [TEST_REFACTORING_PLAN.md](TEST_REFACTORING_PLAN.md) 了解完整技术方案
2. 参考 [TEST_REFACTORING_SUMMARY.md](TEST_REFACTORING_SUMMARY.md) 查看实施计划

#### 💻 日常开发
- 快速查询：[TEST_QUICKSTART.md](TEST_QUICKSTART.md) 的命令速查表
- 覆盖率工具：[COVERAGE_USAGE.md](COVERAGE_USAGE.md) 的 nextest 和 llvm-cov 指南
- 迁移工作：参考 [TEST_MIGRATION_EXAMPLE.md](TEST_MIGRATION_EXAMPLE.md)

## 🔗 相关资源

- [项目主 README](../../README.md) - 项目总体介绍
- [AI 编码指南](../../.github/copilot-instructions.md) - AI 助手使用指南
- [贡献指南](../../CONTRIBUTING.md) - 如何参与项目开发

## 📝 文档维护

如需更新文档，请：
1. 直接编辑对应的 Markdown 文件
2. 提交 PR 并说明修改原因
3. 确保所有交叉引用链接正确

---

**最后更新**: 2025-12-24  
**维护者**: taosX 团队
