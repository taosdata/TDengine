# TDengine 数据修复工具任务总计划（`taosd -r` 扩展）

## 1. 目标
- 在现有 `taosd -r` 基础上实现“数据修复工具”能力，覆盖需求文档中的 `vnode + (wal|tsdb|meta)` 场景。
- 支持三种模式：`force`（单副本自救）、`replica`（副本恢复）、`copy`（跨节点文件拷贝）。
- 所有任务都拆分成单次约 30-60 分钟可完成的小任务，方便中断续跑。
- 构建会话恢复机制：任何 session 中断后，都能从磁盘计划文件恢复上下文，不需要重新分析。

## 1.1 术语统一（跨 Session 关键约定）
- 本项目统一使用：`WAL`、`META`、`TSDB`。
- 这里的 `META` 指“时序数据的元数据”，之前文档里提到的 `TDB` 在本项目语义上等价于 `META`。
- 对外帮助文案与计划文档统一写 `meta`，不再使用 `tdb`。
- 为兼容历史输入，当前 CLI 解析仍接受 `--file-type=tdb`，内部会映射为 `META`。

## 2. 范围与非范围

### 范围内
- `--node-type=vnode` 的修复链路。
- `--file-type=wal|tsdb|meta`。
- `--vnode-id` 过滤多个 vnode。
- `--backup-path`、修复日志、进度输出、异常中止。

### 非范围（当前需求明确不做）
- `mnode/dnode/snode` 文件级修复实现。
- `BSE` 等 vnode 其他文件类型修复。

## 3. 当前状态（2026-03-03）
- 当前阶段：`P0` 已完成（需求/代码勘察与任务拆解）。
- 当前执行阶段：`P3` 进行中（`force + wal`）。
- 当前可执行入口：`T3.3`。
- 当前阻塞：无。

## 4. 阶段里程碑
| Phase | 名称 | 目标 | 退出条件 | 状态 |
|---|---|---|---|---|
| P0 | 需求与代码基线 | 识别现有能力与缺口，确定方案 | 设计文档 + 任务拆解落盘 | completed |
| P1 | CLI 与参数校验 | 新命令参数可解析/校验/报错 | `taosd -r --help` 显示新参数，校验单测通过 | completed |
| P2 | 修复编排框架 | vnode 级任务调度、预检、备份、日志、状态文件 | 可执行空跑并输出进度/摘要 | completed |
| P3 | `force + wal` | 基于现有 WAL 修复能力交付 MVP | WAL 损坏样例可修复并产出日志 | in_progress |
| P4 | `force + tsdb` | 交付 TSDB 块级修复编排 | TSDB 损坏样例修复后可启动/查询 | pending |
| P5 | `force + meta` | 交付 META 修复 + 反向推导链路 | 元数据损坏样例可恢复可用子集 | pending |
| P6 | `replica` 模式 | 触发副本全量同步恢复 | 多副本损坏节点可自动拉起恢复 | pending |
| P7 | `copy` 模式 | 从指定副本节点拷贝文件恢复 | 大文件场景可快速恢复并校验权限 | pending |
| P8 | 验证与发布准备 | 系统测试矩阵、文档、回归、发布清单 | 用例通过，文档可交付 | pending |

## 5. 1 小时任务拆解（执行队列）
| ID | Phase | 任务 | 估时 | 前置 | 产出 | 状态 |
|---|---|---|---|---|---|---|
| T1.1 | P1 | 定义修复参数结构体与枚举（node/file/mode/vnodeList） | 45m | - | 参数模型头文件与解析入口 | completed |
| T1.2 | P1 | 扩展 `dmMain.c` 参数解析支持 `--node-type/--file-type/--vnode-id` | 60m | T1.1 | 新参数可进入模型 | completed |
| T1.3 | P1 | 扩展 `--backup-path/--mode/--replica-node` 解析 | 45m | T1.2 | 全参数解析打通 | completed |
| T1.4 | P1 | 参数组合校验与错误码映射 | 60m | T1.3 | 非法组合拒绝执行 | completed |
| T1.5 | P1 | `--help` 文案更新与示例命令校对 | 30m | T1.4 | 帮助文本与需求一致 | completed |
| T1.6 | P1 | 新增参数解析单测（建议放 `source/common/test/commonTests.cpp`） | 60m | T1.4 | parser/validator 单测 | completed |
| T2.1 | P2 | 设计修复运行时上下文（repair session） | 45m | T1.4 | `SRepairCtx` + 初始化逻辑 | completed |
| T2.2 | P2 | vnode 过滤器：从 vnode list 里选出目标 `vnode-id` | 45m | T2.1 | 精准作用范围 | completed |
| T2.3 | P2 | 预检：参数、路径、磁盘空间、目标文件存在性 | 60m | T2.2 | 失败即中止并记录原因 | completed |
| T2.4 | P2 | 备份管理器（按 vnode+时间戳目录） | 60m | T2.3 | `backup/` 目录结构稳定 | completed |
| T2.5 | P2 | 修复日志与状态文件（`repair.log` + `repair.state.json`） | 60m | T2.4 | 会话可追踪 | completed |
| T2.6 | P2 | 进度输出（每 N 秒）与最终摘要输出 | 45m | T2.5 | 控制台进度 + 结果摘要 | completed |
| T2.7 | P2 | 会话恢复能力：读取 `repair.state.json` 续跑未完成步骤 | 60m | T2.5 | 中断后可继续 | completed |
| T3.1 | P3 | `force+wal` 调度器：接入 `walCheckAndRepair*` 流程 | 45m | T2.6 | 每 vnode WAL 修复入口 | completed |
| T3.2 | P3 | WAL 修复前备份与失败回滚保护 | 45m | T3.1 | 安全防护 | completed |
| T3.3 | P3 | WAL 修复明细记录（损坏区段、重建 idx 条目数） | 60m | T3.1 | 可审计日志 | pending |
| T3.4 | P3 | `wal_test` 扩展：损坏样例自动化验证 | 60m | T3.1 | 回归测试 | pending |
| T4.1 | P4 | TSDB 文件枚举与完整性扫描器封装 | 60m | T2.6 | `.data/.head/.sma/.stt` 扫描结果 | pending |
| T4.2 | P4 | TSDB 可恢复块提取与损坏块定位输出 | 60m | T4.1 | 结构化损坏报告 | pending |
| T4.3 | P4 | TSDB 文件重建流程（先 MVP：保留有效块） | 60m | T4.2 | 可重建输出目录 | pending |
| T4.4 | P4 | TSDB 修复结果验证（启动 + 查询可用） | 45m | T4.3 | 可用性验收 | pending |
| T4.5 | P4 | TSDB 场景系统测试脚本补齐 | 60m | T4.4 | 自动化脚本 | pending |
| T5.1 | P5 | META 元数据解析器稳定化（结构/标签/索引） | 60m | T2.6 | 可读取元数据快照 | pending |
| T5.2 | P5 | WAL/TSDB 反向推导元数据规则实现（第一批规则） | 60m | T5.1 | 推导器 MVP | pending |
| T5.3 | P5 | 缺失元数据标记与“不可推导”日志输出 | 45m | T5.2 | 风险透明 | pending |
| T5.4 | P5 | 重建 META 并切换生效（含备份目录） | 60m | T5.3 | META 修复闭环 | pending |
| T5.5 | P5 | META 修复测试：部分损坏/完全损坏双场景 | 60m | T5.4 | 可复现测试 | pending |
| T6.1 | P6 | `mode=replica` 指令接入与分支调度 | 30m | T2.6 | replica 模式可选通 | pending |
| T6.2 | P6 | 本地坏副本降级动作（不可用标记 + 版本/任期策略） | 60m | T6.1 | 触发全量同步 | pending |
| T6.3 | P6 | 与现有 restore/vgroup 逻辑联动验证 | 60m | T6.2 | 多副本恢复成功 | pending |
| T6.4 | P6 | replica 模式失败保护与回滚语义 | 45m | T6.3 | 不产生二次破坏 | pending |
| T7.1 | P7 | `--replica-node` 解析与目标合法性校验 | 45m | T1.4 | copy 模式参数完备 | pending |
| T7.2 | P7 | 远端拷贝抽象层（先本地 mock） | 60m | T7.1 | 可测试接口 | pending |
| T7.3 | P7 | SSH/SCP 实现并接入 copy 模式 | 60m | T7.2 | 远端拷贝可执行 | pending |
| T7.4 | P7 | 覆盖写入后的权限/owner 修复逻辑 | 45m | T7.3 | 权限一致性 | pending |
| T7.5 | P7 | copy 模式一致性校验与异常中断处理 | 60m | T7.4 | 可控失败行为 | pending |
| T8.1 | P8 | 损坏数据生成器（WAL/META/TSDB）自动脚本化 | 60m | T3.4,T4.5,T5.5 | 可复现实验数据 | pending |
| T8.2 | P8 | 三模式系统测试矩阵与验收脚本 | 60m | T8.1 | 验收流水线 | pending |
| T8.3 | P8 | 文档更新（中英）与运维手册示例 | 60m | T8.2 | 可发布文档 | pending |
| T8.4 | P8 | 发布前回归与风险清单签出 | 45m | T8.3 | 发布 gate 通过 | pending |

## 6. 中断恢复机制（开发过程）
1. 先执行 `git status --short` 确认工作区状态。
2. 顺序阅读 `task_plan.md`、`findings.md`、`progress.md`（本目录）。
3. 在任务表中定位：
   - 优先 `status=in_progress` 的任务；
   - 若无 in_progress，则取第一个 pending 任务。
4. 在开始编码前，把该任务状态改为 `in_progress`，并在 `progress.md` 追加一条日志。
5. 每完成一个任务，必须：
   - 更新任务状态为 `completed`；
   - 在 `progress.md` 记录变更文件、测试命令、结果；
   - 在 `findings.md` 补充任何新发现（尤其是失败原因）。
6. 若任务失败 3 次，记录失败尝试并升级到“待决策项”，不要盲目重复。
7. 每次对外汇报必须包含进度条，格式：
   - `进度: <percent>% [<bar>] <done>/<total>`
   - 计算口径：`done=completed`，`total=completed+in_progress+pending`（基于本文件任务表）。

## 7. 错误记录
| 时间 | 任务 | 错误 | 尝试次数 | 处理方式 | 结果 |
|---|---|---|---|---|---|
| 2026-03-03 | 文档读取 | 需求文件名与路径不完全匹配（有空格） | 1 | 先枚举目录后精确读取 | resolved |
| 2026-03-03 | T1.1 测试验证 | 并行执行 build 与 test 导致测试先于新二进制完成，出现伪失败 | 1 | 改为顺序执行：先 build 再 test | resolved |
| 2026-03-03 | T1.1 测试验证 | `LeakSanitizer` 在当前 ptrace 环境下运行失败 | 1 | `ctest` 时增加 `ASAN_OPTIONS=detect_leaks=0` | resolved |
| 2026-03-03 | T2.3 运行验证 | 使用 `-o /tmp` 启动时触发 `osDir.c:taosMulModeMkDir` ASan 越界（历史问题） | 1 | 改为 `-o /tmp/taoslog` 继续验证预检路径 | resolved |
