# Task Plan: META 强制恢复（Phase2）

## Goal
在已有 `taosd -r` repair 参数层基础上，设计并实现 `--mode force --file-type meta` 的实际执行链路，覆盖备份目录、恢复名单与宕机场景下的可恢复性。

## Current Phase
Phase 5（实现与关键验证已完成，待收尾整理）

## Phases
### Phase 1: 需求与现状对齐
- [x] 阅读大需求、冻结文档与上一阶段留痕
- [x] 确认本次仅实现 `meta` 的强制恢复执行链路
- [x] 确认恢复名单、备份路径、宕机恢复的语义边界
- **Status:** complete

### Phase 2: 方案设计与评审
- [x] 盘点 `metaGenerateNewMeta` 现有调用链和目录切换机制
- [x] 给出 2-3 个方案选项与推荐方案
- [x] 形成设计文档并取得用户确认
- **Status:** complete

### Phase 3: 实施计划拆解
- [x] 输出实现任务拆解、涉及文件和测试路径
- [x] 保存 implementation plan 供后续会话继续
- **Status:** complete

## Key Questions
1. “恢复列表”具体指 repair 请求侧显式指定的 vnode 集合，还是系统内部需额外落盘的 in-progress/allowlist 状态？
2. `--backup-path` 是用户提供的外部备份根目录，还是 vnode 本地目录下固定结构的逻辑别名？
3. 宕机恢复的成功标准是“重启后自动收敛到一致状态”，还是“至少不破坏原 meta，允许再次执行 repair”？

## Decisions Made
| Decision | Rationale |
|----------|-----------|
| 本次范围限定为 `meta` 的 force repair 执行链路 | 用户已明确不包含 `wal/tsdb` 联动与统一编排 |
| 设计必须基于 `metaGenerateNewMeta` 现有实现增强 | 用户明确要求参考该函数调用链做增强版 |

## Notes
- 新会话恢复时，先阅读 `findings.md` 中的 `Long-Term Memory` 章节，避免重复走错测试入口或重复排查已知环境问题。
- 运行 `test_meta_force_repair.py` 时，默认使用 `cd test && ./ci/pytest.sh pytest cases/80-Components/01-Taosd/test_meta_force_repair.py`，不要默认直接调用裸 `pytest`。
- 按 `brainstorming` skill，设计未获批准前不进入实现。
- 本目录用于跨会话延续；后续进度和发现统一记录到这里。


### Phase 4: 首轮实现与验证
- [x] 新增独立 META repair 测试文件
- [x] 放开 `meta force repair` 缺省 `--vnode-id`
- [x] 将 `meta force repair` 接入 `metaOpen()` 分流
- [x] 增加外部备份目录构造与目录备份
- [x] 补充更贴近运行态的自动化验证
- **Status:** complete


### Phase 5: 收尾与交付
- [x] 固化正确测试入口与环境注意事项
- [x] 跑通 `test_meta_force_repair.py`
- [x] 跑通 phase1 参数层回归
- [ ] 整理最终交付说明
- **Status:** in_progress
