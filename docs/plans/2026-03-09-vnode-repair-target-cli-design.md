# Vnode Repair Target CLI Design

## Goal
为 `taosd -r` 定义一套新的 repair CLI 接口，使一次启动可以声明多个 vnode repair target，并分别为 `meta`、`tsdb`、`wal` 携带各自的修复配置。

## Scope
- 仅覆盖 `--mode force`
- 仅覆盖 `--node-type vnode`
- 仅覆盖 repair CLI 参数层、归一化模型与执行侧查询接口
- 允许一次命令声明多个 repair target

## Out of Scope
- `mnode`、`snode`、`dnode` 等非 `vnode` repair
- `replica`、`copy` mode
- 旧接口兼容
- target 之间的执行顺序控制
- `wal` 的具体 repair strategy 设计

## CLI Contract
示例命令：

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

## Global Rules
- `-r` 表示进入 repair 模式。
- 使用 `--repair-target` 时，必须同时提供 `--mode force` 和 `--node-type vnode`。
- `taosd -r` 裸启动直接报错。
- `--node-type` 非 `vnode` 时直接报错；当前阶段不支持其他 node type。
- `--backup-path` 是全局唯一参数，作用于本次 repair 启动，不属于某个 target。
- 旧参数 `--file-type`、`--vnode-id`、`--replica-node` 直接按非法选项处理。

## Repair Target Grammar
`--repair-target` 的值采用如下语法：

```text
<file-type>:<key>=<value>[:<key>=<value>]...
```

规则：
- `<file-type>` 必须是第一个 segment。
- 当前只允许 `meta`、`tsdb`、`wal`。
- 后续 segment 必须是 `key=value` 形式。
- 同一条 target 内 key 不可重复。
- key 顺序不影响语义，但文档示例统一使用推荐顺序。

推荐写法：
- `meta:vnode=<id>:strategy=<strategy>`
- `tsdb:vnode=<id>:fileid=<id>:strategy=<strategy>`
- `wal:vnode=<id>`

## File-Type Schema
### meta
- 必填：`vnode`
- 可选：`strategy`
- 默认 strategy：`from_uid`
- 合法 strategy：
  - `from_uid`
  - `from_redo`

### tsdb
- 必填：`vnode`
- 必填：`fileid`
- 可选：`strategy`
- 默认 strategy：`shallow_repair`
- 合法 strategy：
  - `shallow_repair`
  - `deep_repair`

约束：
- 当前阶段 `fileid` 必填，不允许省略为“整个 vnode TSDB”。

### wal
- 必填：`vnode`
- 当前不允许：`strategy`

如果用户传入 `wal:vnode=3:strategy=scan`，命令行解析阶段直接报错。

## Conflict Rules
- 单条 target 内 key 重复，直接报错。
- 未知 file type、未知 key、缺少必填 key，直接报错。
- strategy 非法值，在命令行解析阶段直接报错。
- 同一 repair object 不允许重复定义。

唯一键定义：
- `meta`: `type + vnode`
- `wal`: `type + vnode`
- `tsdb`: `type + vnode + fileid`

重复示例：
- `meta:vnode=3` 和 `meta:vnode=3:strategy=from_redo` 冲突
- `wal:vnode=3` 和 `wal:vnode=3` 冲突
- `tsdb:vnode=5:fileid=1809` 和 `tsdb:fileid=1809:vnode=5:strategy=deep_repair` 冲突

## Error Handling
全局前置条件错误优先于单条 target 校验：
- `--repair-target` 必须与 `-r` 一起使用
- `--repair-target` 要求 `--mode force`
- `--repair-target` 当前只支持 `--node-type vnode`

单条 target 报错格式建议统一为：

```text
invalid '--repair-target <raw>': <reason>
```

示例：
- `invalid '--repair-target meta': missing required key 'vnode'`
- `invalid '--repair-target foo:vnode=3': unknown file type 'foo'`
- `invalid '--repair-target meta:vnode=3:vnode=4': duplicated key 'vnode'`
- `invalid '--repair-target wal:vnode=3:strategy=scan': key 'strategy' is not supported for file type 'wal' in current phase`
- `duplicated repair target for tsdb vnode 5 fileid 1809`

## Normalized Runtime Model
`dmMain.c` 不再维护单一 `fileType/vnodeId/mode` repair profile，而是维护：
- 一个全局 repair context
- 一个 repair target 数组

建议归一化后的字段包括：
- global:
  - `mode`
  - `nodeType`
  - `backupPath`
- target:
  - `fileType`
  - `vnodeId`
  - `fileId`
  - `strategy`

默认 strategy 在命令行解析阶段补齐：
- `meta` 缺省为 `from_uid`
- `tsdb` 缺省为 `shallow_repair`

执行侧只查询归一化后的 target，不再解析原始字符串，也不再在模块内决定默认 strategy。

## Runtime Semantics
- `--repair-target` 只声明“修谁”和“怎么修”。
- 它不表达执行顺序。
- 真实执行仍由各模块在 open 时判断“当前 vnode / file set 是否命中某个 target”，命中后走对应 repair 分支。
- `meta`、`tsdb`、`wal` 之间默认认为互不影响，不引入额外顺序约束。

## Compatibility Decision
- 新接口发布前，不保留旧参数兼容。
- 旧参数 `--file-type`、`--vnode-id`、`--replica-node` 直接视为非法选项。
- 不输出“该参数已移除”的迁移提示。

## Implementation Impact
至少会影响以下路径：
- `source/dnode/mgmt/exe/dmMain.c`
- `source/dnode/mgmt/node_mgmt/inc/dmMgmt.h`
- `source/dnode/vnode/src/vnd/vnodeRepair.c`
- `source/dnode/vnode/src/meta/metaOpen.c`
- `source/dnode/vnode/src/tsdb/tsdbFS2.c`
- `test/cases/80-Components/01-Taosd/test_com_cmdline.py`
- `test/cases/80-Components/01-Taosd/test_meta_force_repair.py`
- `test/cases/80-Components/01-Taosd/test_tsdb_force_repair.py`

## Delivery Notes
- 当前文档只冻结 CLI 与内部建模约定，不包含具体代码实现。
- `wal` 的 grammar 已冻结，但其实际 repair 执行能力仍需后续实现工作补齐。
