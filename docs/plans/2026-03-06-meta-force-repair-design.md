# META Force Repair Design

**Date:** 2026-03-06

**Status:** Approved by user during interactive design review

## Goal
在已有 `taosd -r` repair 参数层基础上，为 `--mode force --file-type meta` 增加真实执行能力；执行位置内嵌到现有 vnode 并行 open 流程，在 `metaOpen()` 阶段按 vnode 决定是否执行增强版 `metaGenerateNewMeta()`。

## Scope
- 仅覆盖 `file-type=meta`
- 仅覆盖 `mode=force`
- 覆盖指定 `--vnode-id` 与“未指定即全量 vnode”两种选择逻辑
- 覆盖外部备份目录策略
- 覆盖恢复过程中宕机后的本地目录状态收敛

## Non-Goals
- 不实现 `wal` / `tsdb` repair 执行链路
- 不实现 `replica` / `copy` 模式
- 不实现外部备份目录自动回滚
- 不引入独立 repair context/manifest/状态文件
- 不重构 vnode 并行 open 模型

## Existing Baseline
- `source/dnode/mgmt/exe/dmMain.c` 已实现 repair 参数解析，并能识别 `-r --mode force --file-type meta ...`
- 旧 `taosd -r` 行为通过全局 `generateNewMeta` 控制，在 `source/dnode/vnode/src/meta/metaOpen.c` 中触发 `metaGenerateNewMeta()`
- `metaOpen()` 已处理 `meta` / `meta_tmp` / `meta_bak` 三目录组合下的状态收敛
- `metaGenerateNewMeta()` 已具备：扫描旧 meta、生成 `meta_tmp`、切换为新 `meta` 的能力

## Agreed Design
### 1. Control Flow
- `dmMain.c` 继续负责 repair 参数解析与模式判定
- 对于 `-r --mode force --file-type meta`：不进入集中式 repair 执行器
- vnode 启动仍保持现有并行 open 流程
- `metaOpen()` 在每个 vnode 上独立判断“当前 vnode 是否需要 meta force repair”
- 命中 repair 的 vnode 执行增强版 `metaGenerateNewMeta()`
- 未命中的 vnode 保持现有 `metaOpen()` 行为

### 2. Vnode Selection Semantics
- 指定 `--vnode-id`：仅这些 vnode 执行 meta force repair
- 未指定 `--vnode-id`：当前 dnode 下全部 vnode 执行 meta force repair
- 不引入额外恢复名单文件
- 不引入 repair 持久化上下文

### 3. Backup Semantics
- `--backup-path` 指定时，使用用户提供目录作为外部备份根目录
- 未指定时，默认根目录为 `/tmp`
- 实际备份目录格式固定为：`<backup_path>/taos_backup_YYYYMMDD`
- 每个 vnode 的旧 `meta` 在正式切换前都要备份到该目录下的独立子目录
- 本地 `meta_bak` 仍保留，作为当前内部切换协议的一部分
- 外部备份目录只用于兜底留痕，不参与自动回切逻辑

### 4. Crash Recovery Semantics
- 恢复过程中宕机后的自动收敛仍依赖 `metaOpen()` 现有 `meta/meta_tmp/meta_bak` 三目录状态机
- 不改变现有本地目录状态机语义
- 外部备份目录不参与启动恢复判断
- 因此即使 repair 中断，也应满足：
  - 不破坏既有三目录收敛逻辑
  - 旧 meta 仍在外部备份目录中可追溯

## Recommended Code Shape
### `source/dnode/mgmt/exe/dmMain.c`
- 在现有 repair 参数结构基础上补充“是否 meta force repair”、“vnode 命中判断”、“默认备份根目录”所需最小数据
- 保持 legacy `-r` 与新 repair 参数流分离
- 提供可被 vnode/meta 层读取的最小判断入口

### `source/dnode/vnode/src/meta/metaOpen.c`
- 增加一个轻量判断函数，例如：
  - 当前进程是否处于 `meta force repair`
  - 当前 vnode 是否命中 repair 范围
- 对命中 vnode，在 `metaOpen()` 成功打开旧 meta 后调用增强版 `metaGenerateNewMeta()`
- 在增强版 `metaGenerateNewMeta()` 中新增：
  - 生成外部备份目录路径
  - 备份当前 vnode 的旧 meta 到外部目录
  - 继续复用本地 `meta_tmp` / `meta_bak` 原子切换流程

## Testing Strategy
- 不再把 META force repair 测试混入 `test/cases/80-Components/01-Taosd/test_com_cmdline.py`
- 新增独立测试文件承载本次需求测试
- 新测试应至少覆盖：
  - 指定 `--vnode-id` 时只命中目标 vnode
  - 未指定 `--vnode-id` 时全部 vnode 命中
  - 指定 `--backup-path` 时使用外部目录
  - 未指定 `--backup-path` 时落到 `/tmp/taos_backup_YYYYMMDD`
  - 命中 vnode 在 `metaOpen()` 时走恢复增强链路
  - 非命中 vnode 保持原 open 逻辑
  - 中断后重启仍不破坏本地三目录状态收敛

## Risks
- 旧 `generateNewMeta` 与新 `meta force repair` 条件可能相互串扰，需要明确互斥条件
- “未指定 `--vnode-id` 即全量 vnode” 可能扩大影响面，日志与测试要足够明确
- 外部备份目录与本地 `meta_bak` 职责需严格区分，避免误接入自动回切

## Deferred Items
- 基于外部备份目录的一键回滚
- 更通用的 repair framework/context
- `wal` / `tsdb` repair 统一编排
