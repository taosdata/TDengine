# 数据修复工具 FS

## 1. 修订记录

| 编写日期 | 发布日期 | 版本 | 修订人 | 主要修改内容 |
| --- | --- | --- | --- | --- |
| 2026-03-12 | 2026-03-13 | 3.4.1.0 | @程洪泽 | taosd 本地修复模式 Functional Spec，覆盖 repair-target grammar、meta/tsdb/wal 修复行为、限制与运维要求 |

## 2. 背景

现有修复入口以零散参数为主，难以表达“一次启动同时修复多个本地对象”的需求，也不利于对不同文件类型施加不同修复策略。数据修复功能的目标是把本地修复能力统一收敛到 `taosd -r` 模式下，以显式的 `--repair-target` 描述修复对象，先支持 vnode 范围内的 `meta`、`tsdb` 和 `wal` 三类目标，并把用户可见的 grammar、默认策略和限制固定下来。
本次改动是“离线、本地、启动期”的修复能力增强，不引入新的 SQL 接口，不改变在线写入流程。

## 3. 定义

| 术语 | 定义 |
| --- | --- |
| 本地修复模式 | 通过 `taosd -r` 启动的离线修复流程，仅在启动期执行 |
| repair target | 一个显式声明的修复对象，语法为 `<file-type>:<key>=<value>[:<key>=<value>]...` |
| vnode | 当前阶段唯一支持的 `--node-type`，表示 vnode 级别本地数据对象 |
| meta repair | 对 vnode meta 目录进行重建的修复流程 |
| tsdb repair | 对某个 vnode 下指定 `fileid` 的单个 TSDB fileset，或 `fileid=*` 命中的全部 fileset 执行检查、删除或重建 |
| wal repair | 对指定 vnode 的 WAL 损坏目录执行按修复模式触发的清理路径 |
| force mode | 当前阶段唯一支持的 `--mode`，表示尽可能利用本地已有信息进行强制修复 |
| fileid | TSDB fileset 标识，当前阶段对 `tsdb` target 为必填字段；可取单个 fileset ID，或 `*` 表示该 vnode 下全部 fileset |

## 4. 行为说明

### 4.1 总体入口

使用方式如下：
```bash
taosd -r --mode force --node-type vnode [--backup-path <path>] \
  --repair-target <target> [--repair-target <target>]...
```

行为约束如下：
- `-r` 进入本地修复模式。
- 当前仅支持 `--mode force`。
- 当前仅支持 `--node-type vnode`。
- 至少需要一个 `--repair-target`。
- `--backup-path` 是本次修复启动的全局参数，不归属于单个 target。
- 一次启动允许同时声明多个 target。
如果使用 `taosd -r --help`，需要输出 repair 模式专用帮助，而不是普通启动帮助。

### 4.2 repair-target grammar

每个 `--repair-target` 的语法如下：
```plaintext
<file-type>:<key>=<value>[:<key>=<value>]...
```

固定规则如下：
- `<file-type>` 必须是第一个 segment。
- 当前支持的 file type 为 `meta`、`tsdb`、`wal`。
- `key=value` 顺序不影响语义。
- 同一 target 内 key 不能重复。
- 多个 target 不能命中同一个修复对象，否则直接报错。
- 对 `tsdb` 来说，`fileid=*` 表示命中该 vnode 下全部 fileset，且不能与同一 vnode 下任意显式 `fileid=<fid>` target 共存。
当前支持的 target 如下：

| 文件类型 | 必填字段 | 可选字段 | 默认策略 | 支持的策略 |
| --- | --- | --- | --- | --- |
| `meta` | `vnode` | `strategy` | `from_uid` | `from_uid`、`from_redo` |
| `tsdb` | `vnode`、`fileid` | `strategy` | `drop_invalid_only` | `drop_invalid_only`、`head_only_rebuild`、`full_rebuild` |
| `wal` | `vnode` | 无 | 无 | 无 |

### 4.3 meta 修复行为

meta 修复由 `meta:vnode=<id>[:strategy=...]` 触发。
用户可见行为如下：
- 默认策略为 `from_uid`。
- 可显式指定 `strategy=from_redo`。
- 修复发生在 vnode 打开流程中。
- 若指定 `--backup-path`，会在 `${backup-path}/taos_backup_YYYYMMDD/vnode<id>/meta/` 下生成备份目录。
- 未指定 `--backup-path` 时，代码路径允许回退到临时目录作为备份根目录。
示例：
```bash
taosd -r --mode force --node-type vnode \
  --repair-target meta:vnode=3
```

```bash
taosd -r --mode force --node-type vnode \
  --repair-target meta:vnode=3:strategy=from_redo
```

### 4.4 TSDB 修复行为

TSDB 修复由 `tsdb:vnode=<id>:fileid=<fid|*>[:strategy=...]` 触发。
- `fileid` 为必填。
- `fileid=<fid>` 表示只修复一个显式声明的 fileset。
- `fileid=*` 表示修复该 vnode 下全部 fileset。
- 同一个 vnode 内，`fileid=*` 不能与显式 `fileid=<fid>` target 混用。
- 默认策略为 `drop_invalid_only`。
- `drop_invalid_only` 只处理显式缺失文件这类明显损坏，不主动处理 `current.json` 与磁盘文件 size mismatch 的场景。
- `head_only_rebuild` 会对有效 core block 做 deep scan，只重建 `.head`；保留 `.data`；当 `.sma` 元数据不可复用时删除 `.sma`。
- `full_rebuild` 会对有效 core block 做 deep scan，并重建完整 core 数据。
- 健康 fileset 在 repair 流程下应保持无变更。
- 深度修复时可能产生日志记录，例如 `action=rebuild_core_group`、`action=drop_core_group`、`reason=missing_stt` 等。
示例：
```bash
taosd -r --mode force --node-type vnode \
  --repair-target tsdb:vnode=5:fileid=1809
```

```bash
taosd -r --mode force --node-type vnode \
  --repair-target tsdb:vnode=5:fileid=1809:strategy=head_only_rebuild
```

```bash
taosd -r --mode force --node-type vnode \
  --repair-target tsdb:vnode=5:fileid=1809:strategy=full_rebuild
```

```bash
taosd -r --mode force --node-type vnode \
  --repair-target 'tsdb:vnode=5:fileid=*'
```

### 4.5 WAL 修复行为

WAL 修复由 `wal:vnode=<id>` 触发。
当前阶段的用户可见行为较收敛：
- 不支持 `strategy`。
- 不引入新的 WAL repair grammar。
- 在 repair 模式下，目标 vnode 的 WAL 检查会复用“损坏即重命名/清理”的现有修复路径，而不是新增重建策略。
示例：
```bash
taosd -r --mode force --node-type vnode \
  --repair-target wal:vnode=6
```

### 4.6 多 target 组合

一次启动可同时声明多个 target：
```bash
taosd -r --mode force --node-type vnode --backup-path /tmp/repair-bak \
  --repair-target meta:vnode=3 \
  --repair-target tsdb:vnode=5:fileid=1809 \
  --repair-target wal:vnode=6
```

组合规则如下：
- 同类 target 可以存在多条，但不能重复命中同一个对象。
- `meta` 与 `wal` 的粒度是 vnode。
- `tsdb` 的粒度是 `vnode + fileid`；其中 `fileid=*` 的粒度是“该 vnode 下全部 fileset”。

### 4.7 出错处理

代表性报错契约如下：
- 未携带 `-r` 却传入 `--repair-target`：`'--repair-target' must be used with '-r'`
- `-r` 但缺少 `--mode`：`missing '--mode'`
- 缺少 `--node-type`：`missing '--node-type'`
- `--node-type` 不是 `vnode`：`currently only supports '--node-type vnode'`
- 缺少 `--repair-target`：`missing '--repair-target'`
- 未知 file type：`unknown file type 'foo'`
- target 中重复 key：`duplicated key 'vnode'`
- `wal` 使用 strategy：`key 'strategy' is not supported for file type 'wal' in current phase`
- `tsdb` 未指定 `fileid`：`missing required key 'fileid'`
- 非法 strategy：`invalid strategy 'foo' for file type 'meta'`、`invalid strategy 'deep_repair' for file type 'tsdb'`
- 重复 target：`duplicated repair target for meta vnode 3`、`duplicated repair target for tsdb vnode 5 fileid 1809`
- 旧参数继续使用时：`invalid option`
- `tsdb` 中 `fileid=*` 与显式 fileset 混用：`fileid=* overlaps existing tsdb repair targets for vnode 5`

## 5. 性能

本需求不改变正常在线读写路径，对常规启动性能没有新增影响；影响只发生在显式进入 repair 模式时。
性能影响分层如下：
- `drop_invalid_only` 成本最低，主要做显式坏文件检测。
- `head_only_rebuild` 需要扫描有效 core block 并重写 `.head`，启动期 IO 和耗时高于默认策略。
- `full_rebuild` 成本最高，需要重建完整 core 数据，适合显式处理更复杂的损坏。

## 6. 安全

本需求的安全设计点如下：
- 修复流程必须由本地运维显式使用 `taosd -r` 触发，不会在正常启动中自动进入。
- 当前仅支持 vnode 范围，不扩大到更高层级的远程或集群控制面。
- `--backup-path` 允许在执行破坏性修复前保留备份副本，便于回滚和取证。
- 不引入新的认证、鉴权或网络暴露面。

## 7. 兼容性

本需求对旧行为存在接口级不兼容：
- 旧的 `--file-type`、`--vnode-id`、`--replica-node` 已从这套 repair CLI 中移除。
- 新 CLI 统一收敛到 `--repair-target` grammar。
除此之外：
- 不新增 SQL 行为变化。
- 不改变客户端协议。
- 不改变正常启动时的产品行为。

## 8. 运维

运维侧需要关注以下事项：
- 修复是离线动作，应在目标节点停服或明确进入本地 repair 启动流程后执行。
- 需要提前确认 `vnode` 和 `fileid` 等目标信息；如果使用 `fileid=*`，需确认该 vnode 下全部 fileset 都在本次修复范围内，避免误修复。
- 若指定 `--backup-path`，需预留足够磁盘空间。
- 若需要处理 size mismatch 场景，必须显式选择 `head_only_rebuild` 或 `full_rebuild`。
- 修复后应重新正常启动 `taosd`，并验证数据可读可写。

## 9. 使用场景

典型 use case 如下：
- 单个 vnode 的 meta 文件损坏，需要通过 `from_uid` 或 `from_redo` 重建 meta。
- 单个 TSDB fileset 出现缺失 `.head`、`.data`、`.stt` 等局部文件损坏，需要离线处理。
- TSDB core 文件存在 size mismatch，需要通过 deep repair 策略显式重建。
- WAL 元信息或日志完整性异常，需要在 repair 模式下对指定 vnode 执行清理式修复。
- 一个节点同时存在多类局部损坏，需要一次启动中声明多个 target 统一处理。

## 10. 约束和限制

约束：
- 当前只支持 `--mode force`。
- 当前只支持 `--node-type vnode`。
- `tsdb` target 必须显式指定 `fileid`，可取单个 fileset ID 或 `*`。
限制：
- `wal` 当前不支持 `strategy`。
- 同一个 vnode 内，`fileid=*` 不能与显式 `fileid=<fid>` target 混用。
- `drop_invalid_only` 不处理 size mismatch，需要显式改用 `head_only_rebuild` 或 `full_rebuild`。

## 11. 常见错误和排查

| 错误现象 | 原因 | 排查/处理建议 |
| --- | --- | --- |
| `missing '--mode'` | 进入 `-r` 但未声明 mode | 补齐 `--mode force` |
| `missing '--node-type'` | 未声明 node type | 补齐 `--node-type vnode` |
| `currently only supports '--node-type vnode'` | 使用了 `mnode` 等未支持类型 | 当前阶段只能修复 vnode |
| `missing '--repair-target'` | 进入 repair 模式但未声明目标 | 至少补一个 `--repair-target` |
| `unknown file type` | target file type 非 `meta/tsdb/wal` | 修正 grammar |
| `missing required key 'fileid'` | `tsdb` target 缺失 fileid | 补齐 `fileid=<fid>` 或 `fileid=*` |
| `fileid=* overlaps existing tsdb repair targets` | 同一 vnode 内混用了 wildcard 和显式 fileset target | 拆分为单独一次 `fileid=*` 修复，或只保留显式 fileset target |
| `invalid strategy` | strategy 名称错误 | 使用文档中公开的 strategy 名称 |
| `duplicated repair target` | 多条 target 命中了同一对象 | 去重后重试 |
| 旧参数报 `invalid option` | 仍使用 legacy repair 参数 | 改为 `--repair-target` 写法 |

## 12. 可观测性

可观测性变化如下：
- `taosd -r --help` 会输出 repair mode 专用 usage。
- CLI/日志中会出现 repair 调度痕迹，例如 `tsdb force repair dispatch`。
- 带备份路径运行时，可在备份目录下观察到 `taos_backup_YYYYMMDD/vnodeX/...` 结构。
- 某些 TSDB repair 场景会产出 `repair.log`，记录 `fid`、`action`、`reason` 等字段。
本需求不涉及 taos shell、taos Explorer、TDinsight 的交互界面变化。

## 13. 安装和卸载

无特殊安装和卸载要求。
本需求不新增安装包组件，不修改用户安装/卸载脚本接口。

## 14. 文档

需要修改官网文档和产品参考文档。
本功能已同步修改以下文档：
- `docs/zh/14-reference/01-components/01-taosd.md`
- `docs/en/14-reference/01-components/01-taosd.md`
- `docs/zh/08-operation/05-maintenance.md`
- `docs/en/08-operation/04-maintenance.md`
  
如企业版文档单独维护 repair CLI 章节，也需要同步更新 grammar、示例和限制说明。

## 15. 参考文档

- PR: https://github.com/taosdata/TDengine/pull/34753
- `docs/zh/14-reference/01-components/01-taosd.md`
- `docs/zh/08-operation/05-maintenance.md`
- `test/cases/80-Components/01-Taosd/test_com_cmdline.py`
- `test/cases/80-Components/01-Taosd/test_meta_force_repair.py`
- `test/cases/80-Components/01-Taosd/test_tsdb_force_repair.py`

## 16. 附录

### 16.1 策略与效果对照

| 类型 | 策略 | 预期效果 |
| --- | --- | --- |
| `meta` | `from_uid` | 以 UID 扫描结果重建 meta |
| `meta` | `from_redo` | 以 redo 信息重建 meta |
| `tsdb` | `drop_invalid_only` | 删除显式失效对象，不做深度 size mismatch 扫描 |
| `tsdb` | `head_only_rebuild` | 基于有效 block 重建 `.head`，必要时移除 `.sma` |
| `tsdb` | `full_rebuild` | 基于有效 block 重建完整 core 数据 |
| `wal` | 无 | 复用现有 WAL corruption 清理式修复路径 |
