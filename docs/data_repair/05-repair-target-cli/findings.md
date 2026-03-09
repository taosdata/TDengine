# Findings & Decisions

## Requirements
- 新 repair CLI 不再使用 `--file-type`、`--vnode-id` 这类单 profile 参数。
- 一次 `taosd -r` 启动可声明多个 repair target。
- 只有 `--node-type vnode` 时 `--repair-target` 才生效。
- 只有 `--mode force` 支持新接口。
- `backup-path` 对整次 repair 启动全局唯一。

## Agreed CLI
示例：

```bash
taosd -r \
  --mode force \
  --node-type vnode \
  --backup-path /tmp/repair-bak \
  --repair-target meta:vnode=3 \
  --repair-target meta:vnode=4:strategy=from_redo \
  --repair-target tsdb:vnode=5:fileid=1809 \
  --repair-target tsdb:vnode=5:fileid=1810:strategy=deep_repair \
  --repair-target wal:vnode=3
```

## Repair Target Grammar
- 语法：`<file-type>:<key>=<value>[:<key>=<value>]...`
- `file-type` 只允许：`meta`、`tsdb`、`wal`
- key 顺序允许任意，但建议文档统一顺序
- 同一条 target 内 key 不可重复

## File-Type Rules
### meta
- 必填：`vnode`
- 可选：`strategy`
- 默认 strategy：`from_uid`
- 合法 strategy：`from_uid`、`from_redo`

### tsdb
- 必填：`vnode`、`fileid`
- 可选：`strategy`
- 默认 strategy：`shallow_repair`
- 合法 strategy：`shallow_repair`、`deep_repair`

### wal
- 必填：`vnode`
- 当前不允许：`strategy`

## Duplicate Detection
- `meta` 唯一键：`type + vnode`
- `wal` 唯一键：`type + vnode`
- `tsdb` 唯一键：`type + vnode + fileid`

命中同一 repair object 的多条 target 直接报错，不做 merge。

## Validation Policy
- 全局前置条件在命令行解析阶段校验：
  - 必须有 `-r`
  - 必须有 `--mode force`
  - 必须有 `--node-type vnode`
  - 必须至少有一个 `--repair-target`
- 单条 target 的 file-type、key、必填字段、strategy 枚举值也都在命令行解析阶段校验
- 默认 strategy 在命令行解析阶段补齐

## Compatibility Decision
- `--file-type`、`--vnode-id`、`--replica-node` 直接按非法选项处理
- `taosd -r` 裸启动直接报错
- 不保留旧行为兼容

## Runtime Decision
- target 之间没有顺序语义
- 实际修复仍由各模块在 open 时检查是否命中
- `dmMain.c` 负责解析和归一化
- `meta` / `tsdb` / `wal` 执行侧只查询归一化 target，不再解析原始 CLI 字符串

## Code Impact
- `source/dnode/mgmt/exe/dmMain.c`
- `include/common/dmRepair.h`
- `source/dnode/mgmt/node_mgmt/inc/dmMgmt.h`
- `source/dnode/vnode/src/vnd/vnodeRepair.c`
- `source/dnode/vnode/src/meta/metaOpen.c`
- `source/dnode/vnode/src/tsdb/tsdbFS2.c`
- `test/cases/80-Components/01-Taosd/test_com_cmdline.py`
- `test/cases/80-Components/01-Taosd/test_meta_force_repair.py`
- `test/cases/80-Components/01-Taosd/test_tsdb_force_repair.py`

## Current Workspace Audit
- `git status --short` 显示当前工作区已有 `source/dnode/mgmt/exe/dmMain.c` 未提交改动。
- 本阶段仅新增文档，未修改现有实现代码。

## Implementation Update (2026-03-09)
- `dmMain.c` 已从单 profile 模型切换到 `SArray<SDmRepairTarget>` 模型。
- 新增公共头 `include/common/dmRepair.h`，承载 repair target 枚举、结构体和 accessor 原型。
- `test_com_cmdline.py` 已切到 `--repair-target` 语法，目标用例本地通过。
- `metaOpen.c` 已改为扫描归一化 target 列表匹配 vnode，并使用 CLI 提供的 strategy。
- `tsdbFS2.c` 已改为按 `vnode + fileid` 匹配 target，而不再读取全局 `--vnode-id` 字符串。
- `test_meta_force_repair.py` 与 `test_tsdb_force_repair.py` 已迁移到新语法。

## Runtime Findings
- `metaBackupCurrentMeta()` 在旧实现里存在但未被 `metaForceRepair()` 调用；本轮已补入调用。
- `meta` 的 parser/syntax 用例通过，但真实 backup 用例在当前 pytest 环境下仍未生成外部 backup 目录，后续需要继续抓 repair 进程输出确认卡点。
- `meta` 测试原先通过“扫描 vnode 目录取第一个 vnode”来选 repair 目标，这对新精确 target 语义不可靠；本轮已改为按 `information_schema.ins_tables` / `show <db>.vgroups` 解析真实 vnode id。
- `tsdb` 的入口 smoke 用例在当前环境中因为缺少 manifest-matched real fileset 被 skip，没有暴露 parser 错误。
- `tsdb` 的 fake-fid 用例已成功进入新语法路径，但 `current.json` 尚未按预期移除目标 core group，后续需要沿 `tsdbDispatchForceRepair -> tsdbRepairAnalyzeFileSet -> tsdbRepairCommitStagedCurrent` 继续排查。

## Resources
- `docs/plans/2026-03-09-vnode-repair-target-cli-design.md`
- `docs/plans/2026-03-09-vnode-repair-target-cli-plan.md`
- `source/dnode/mgmt/exe/dmMain.c`
- `source/dnode/vnode/src/meta/metaOpen.c`
- `source/dnode/vnode/src/tsdb/tsdbFS2.c`
