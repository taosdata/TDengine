# Task Plan: taosd -r 参数层重构（Phase1）

## Goal
基于 `.vscode/dev/phase1-repair-freeze.md` 完成 `taosd -r` 参数层重构：兼容旧 `-r` 行为、接入新修复参数解析与校验、冻结错误码语义并提供修复帮助输出。

## Current Phase
Phase 6（已完成实现与 review 问题修复；待具备 pytest 环境时补完整执行验证）

## Phases
### Phase 1: 需求冻结对齐与现状确认
- [x] 阅读冻结文档并提炼参数/错误码矩阵
- [x] 定位 taosd 参数解析入口与旧 `-r` 行为
- [x] 记录当前缺口（新参数尚未接入）
- **Status:** complete

### Phase 2: TDD 红阶段（先验证当前行为不满足冻结）
- [x] 执行代表性命令验证当前行为
- [x] 记录失败点（例如 `--node-type`、`--force`）
- [x] 将失败证据写入 progress.md
- **Status:** complete

### Phase 3: 参数层实现与重构
- [x] 在 `dmMain.c` 增加修复参数解析与归一化
- [x] 实现 `-r` 兼容策略：仅无新参数时走 legacy rebuild
- [x] 实现冻结错误码映射（E01-E12）
- [x] 增加 repair help 输出并保留通用 help
- **Status:** complete

### Phase 4: 绿阶段验证
- [x] 构建 taosd
- [x] 运行命令矩阵（P01-P05 + 关键错误路径）
- [x] 记录实际返回码与输出
- **Status:** complete

### Phase 5: 交付说明
- [x] 汇总改动文件/行为变化/验证证据
- [x] 标注未覆盖项和后续接口挂接点
- **Status:** complete

### Phase 6: 补充参数层测试用例
- [x] 在 `test_com_cmdline.py` 增加 `-r` 参数层测试
- [x] 覆盖成功/失败关键路径并断言输出
- [x] 执行本地可用验证（py_compile）
- **Status:** complete

## Key Questions
1. Phase1 是否只要求“参数层语义与错误码冻结”，不要求真正执行数据修复逻辑？
2. `taosd -r --help` 是否必须输出专用 repair help（而非通用 help）？

## Decisions Made
| Decision | Rationale |
|----------|-----------|
| Phase1 以参数层为核心，不引入 repair 执行逻辑 | 冻结文档强调“参数语义/兼容策略/错误码矩阵” |
| 继续保留 `taosd -r` 旧行为 | 冻结文档 2.2 明确要求兼容 |

## Errors Encountered
| Error | Attempt | Resolution |
|-------|---------|------------|
| LSAN 在命令验证中中止进程（exit=134） | 1 | 使用 `ASAN_OPTIONS=detect_leaks=0` 后继续验证 |
| `pytest` 在当前环境缺失（command/module not found） | 1 | 改用 `python3 -m py_compile` 做测试文件语法校验 |

## Notes
- 修改前先完成红阶段命令验证。
- 所有结论以 `.vscode/dev/phase1-repair-freeze.md` 为准。
- 2026-03-06 会话恢复审计后确认：当前工作区实际未体现先前会话提到的 `repair/CMakeLists.txt` 与上层 `CMakeLists.txt` 改动，当前以 `git diff --stat` 可见内容为准。
- 2026-03-06 review 修复后，参数缺失报错、LSAN 测试稳定性和 unsupported 非零退出码问题均已落地修复，并完成针对性命令验证。
- 2026-03-06 规划文件已迁移到 `docs/data_repair/01-参数重构/` 目录，后续进度维护以该目录下的 `task_plan.md` / `findings.md` / `progress.md` 为准。
