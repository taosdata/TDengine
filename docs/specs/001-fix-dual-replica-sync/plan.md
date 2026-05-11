# Implementation Plan: 双副本恢复同步阻塞修复

**Branch**: `001-fix-dual-replica-sync` | **Date**: 2026-04-10 | **Spec**: `/root/github/taosdata/TDinternal/specs/001-fix-dual-replica-sync/spec.md`
**Input**: Feature specification from `/specs/001-fix-dual-replica-sync/spec.md`

**Note**: This template is filled in by the `/speckit.plan` command. See `.specify/templates/plan-template.md` for the execution workflow.

## Summary

修复双副本在降级写入后恢复阶段的长时间不可用问题：
1) 将同步判定从“仅看 leader 自身”改为“leader 条件 + follower 追赶进度（lag 阈值）”；
2) 避免在 follower 未追上时提前退出 `ASSIGNED_LEADER`；
3) 引入可观测性与可配置阈值（`syncLogLagThreshold`、`syncCatchupLogIntervalMs`）；
4) 在恢复为正常双副本时保持 Raft 安全（term 递增并经选举回到 LEADER）。

## Technical Context

**Language/Version**: C (TDengine server-side core; CMake-based native build)  
**Primary Dependencies**: TDengine internal sync/raft modules, mnode arbiter workflow, vnode sync server handlers  
**Storage**: WAL + snapshot replication state (vnode), SDB metadata state (mnode arbiter)  
**Testing**: C/C++ native test pipeline (ctest where applicable) + `community/tests/system-test` Python-based system tests  
**Target Platform**: Linux server cluster deployment
**Project Type**: Distributed database server (internal replication and metadata coordination)  
**Performance Goals**: Recovery期间写入中断不超过30秒；恢复前写吞吐不低于单副本基线80%；同步误判率0%  
**Constraints**: 仅改动 sync + mnode/vnode 协调逻辑；不破坏3副本/单副本行为；保持Raft安全语义  
**Scale/Scope**: 面向2副本vgroup在大数据追赶（GB到100GB量级）下的恢复流程

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-check after Phase 1 design.*

`.specify/memory/constitution.md` 当前仍为模板占位内容，未定义可执行治理原则与硬性 gate。

Gate结论（Phase 0前）：
- G1 需求完整性：PASS（spec已无 NEEDS CLARIFICATION）
- G2 可测量目标：PASS（SC-001..SC-006均可验证）
- G3 架构边界：PASS（限定在 sync/mnode/vnode 相关模块）
- G4 兼容性与安全：PASS（明确不影响3副本/单副本，保留Raft选举恢复）

Post-Design复核要求：
- 设计产物必须覆盖 FR-001..FR-009 与 SC-001..SC-006 的映射关系
- 所有新增配置项需给出默认值、生效方式与回滚策略

Post-Design复核结论（Phase 1后）：
- D1 产物覆盖性：PASS（`research.md`、`data-model.md`、`contracts/internal-sync-check-contract.md`、`quickstart.md` 已生成）
- D2 需求映射性：PASS（设计聚焦 FR-001..FR-009；验收步骤覆盖 SC-001..SC-006）
- D3 配置治理：PASS（明确 `syncLogLagThreshold` 与 `syncCatchupLogIntervalMs` 默认值和验证路径）

## Project Structure

### Documentation (this feature)

```text
specs/001-fix-dual-replica-sync/
├── plan.md              # This file (/speckit.plan command output)
├── research.md          # Phase 0 output (/speckit.plan command)
├── data-model.md        # Phase 1 output (/speckit.plan command)
├── quickstart.md        # Phase 1 output (/speckit.plan command)
├── contracts/           # Phase 1 output (/speckit.plan command)
└── tasks.md             # Phase 2 output (/speckit.tasks command - NOT created by /speckit.plan)
```

### Source Code (repository root)

```text
community/source/libs/sync/
├── inc/
└── src/
    ├── syncMain.c
    ├── syncPipeline.c
    ├── syncReplication.c
    ├── syncCommit.c
    └── syncSnapshot.c

community/source/dnode/mnode/impl/src/
└── mndArbGroup.c

community/source/dnode/vnode/src/vnd/
└── vnodeSvr.c

community/tests/
├── system-test/
└── pytest/
```

**Structure Decision**: 采用单仓库原生服务端结构，在既有 sync/mnode/vnode 路径上进行最小范围修改，并通过 community 的系统测试入口验证行为。

## Complexity Tracking

> **Fill ONLY if Constitution Check has violations that must be justified**

| Violation | Why Needed | Simpler Alternative Rejected Because |
|-----------|------------|-------------------------------------|
| None | N/A | N/A |
