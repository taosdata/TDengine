# 强制修复 TSDB - 第三阶段任务计划

## Goal
- 对齐“强制修复 TSDB”需求边界、修复语义、测试范围与交付物。
- 在用户确认设计方案后，产出可执行的实现计划，支持跨会话续接。

## Scope Status
- 当前阶段：按既有设计继续实现 / 验证中
- 代码实现：已完成文件组级 repair，正在推进块级 repair 第一批
- 设计审批：已确认，沿正式设计与实施计划执行

## Phases
| Phase | Status | Description |
|---|---|---|
| P1 | completed | 阅读需求文档、前序留痕、最近提交，提炼约束 |
| P2 | completed | 逐项澄清 TSDB force repair 的目标、边界、成功标准 |
| P3 | completed | 提出 2-3 个修复方案并给出推荐 |
| P4 | completed | 形成设计文档并等待用户批准 |
| P5 | completed | 产出详细实现计划（按 skill 要求拆分） |

## Open Questions
- “第三阶段”实际目标是 `TSDB force repair`，但用户给出的留痕目录名为 `docs/data_repair/03-META_repair`，需确认目录命名是否按现状沿用。
- 强制修复的最小成功标准是“可启动/可查询/可写入/可校验通过”中的哪些组合？
- TSDB 损坏覆盖的优先文件类型与损坏模式有哪些？
- 本阶段是否仅覆盖单副本 vnode？是否明确排除 replica/copy 模式？
- 是否要求与已有 `meta force repair` 共用 CLI 语义、日志风格与备份策略？

## Errors Encountered
| Error | Attempt | Resolution |
|---|---|---|
| `printf: --: invalid option` when printing section headers in shell | 1 | 改用 `echo` 输出分隔标题 |

## Approved Design
- 用户已确认“块级扫描 + 文件组重建”为正式方案。
- 设计文档：`docs/plans/2026-03-07-tsdb-force-repair-design.md`
- 实施计划：`docs/plans/2026-03-07-tsdb-force-repair-plan.md`

## Implementation Notes
- 代码现状已明显前进于最初计划文件：`dmMain.c` 参数放行、`tsdb open fs` 分发、受影响文件组备份、`stt`/`head`/`data` 缺失规则、`size_mismatch_core`、以及 staged manifest crash-safe 提交均已落地。
- 当前续做重点已切到：块级坏块识别、有效块保留、基于有效块重建 `head/data/sma`，并继续沿用现有 pytest 入口与第三阶段留痕。
