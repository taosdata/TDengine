# Task Plan: vnode repair-target CLI

## Goal
冻结并落盘 `taosd -r` 新的 vnode repair-target CLI 约定，为后续实现 `--repair-target` 多 target 模型提供可直接接手的设计和实施计划。

## Current Phase
Phase 3（parser/data-model 已落地；meta/tsdb 运行期回归仍在收敛）

## Phases
### Phase 1: 设计冻结与上下文留痕
- [x] 整理 `--repair-target` grammar
- [x] 冻结默认 strategy、冲突规则与非兼容决策
- [x] 写入正式设计文档与实现计划
- [x] 建立本目录下的 handoff 文件
- **Status:** complete

### Phase 2: 参数层 TDD 与 parser 改造
- [x] 将 `test_com_cmdline.py` 从旧参数迁移到 `--repair-target`
- [x] 在 `dmMain.c` 中实现新的 repair context 与 target 列表
- [x] 移除旧 `--file-type` / `--vnode-id` 接口
- **Status:** complete

### Phase 3: 执行侧接线
- [x] 改造 `metaOpen.c` 使用归一化 target 查询
- [x] 改造 `tsdbFS2.c` 使用归一化 target 查询
- [ ] 明确 `wal` 当前阶段的运行期边界
- [ ] 收敛 `meta` / `tsdb` 运行期测试失败
- **Status:** in_progress

### Phase 4: 验证与收尾
- [ ] 构建 `taosd`
- [ ] 回归 CLI / meta / tsdb 测试
- [ ] 更新 handoff 文件中的验证结果和剩余工作
- **Status:** pending

## Key Questions
1. `wal` 运行期在本阶段是否一起落地，还是先只冻结 CLI/parser 与 query 接口？
2. `dmMgmt.h` 新 accessor API 采用枚举接口还是字符串接口？

## Decisions Made
| Decision | Rationale |
|----------|-----------|
| 新 CLI 采用重复 `--repair-target` 的原子任务模型 | 同时支持多 file-type、多 vnode、多 tsdb fileid，且参数归属清晰 |
| `--repair-target` grammar 为 `<file-type>:<key>=<value>[:<key>=<value>]...` | 只使用一种 `:` 分隔规则，不混用逗号列表 |
| 只支持 `--mode force` + `--node-type vnode` | 其他 node type 与 mode 当前阶段不支持 |
| `meta` 默认 `from_uid`，`tsdb` 默认 `shallow_repair` | 未显式指定 strategy 时保持明确、稳定的默认行为 |
| 旧 `--file-type` / `--vnode-id` / `--replica-node` 直接非法 | 版本尚未发布，不需要兼容迁移 |
| `taosd -r` 裸启动直接报错 | 避免新旧两套 repair 入口语义并存 |

## Errors Encountered
| Error | Attempt | Resolution |
|-------|---------|------------|
| `meta` runtime backup test still times out waiting for external backup dir | 1 | 已确认 parser 通过、DB vnode 解析修正、`metaBackupCurrentMeta()` 调用已补入，但当前 pytest 场景仍未稳定通过；需继续抓 repair 进程输出 |
| `tsdb` fake-fid runtime test未按预期移除 core group | 1 | 新 `--repair-target tsdb:vnode=<id>:fileid=<id>` 语法已迁入测试，入口 smoke 未报 parser 错；需继续定位 fileset 命中/提交链路 |

## Notes
- 当前工作区已有 `source/dnode/mgmt/exe/dmMain.c` 未提交改动；后续实现前先审阅现状，再决定在其基础上修改还是拆分重构。
- 正式设计文档位于 `docs/plans/2026-03-09-vnode-repair-target-cli-design.md`。
- 正式实现计划位于 `docs/plans/2026-03-09-vnode-repair-target-cli-plan.md`。
