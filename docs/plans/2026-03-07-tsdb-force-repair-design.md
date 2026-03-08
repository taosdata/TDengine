# TSDB Force Repair Design

## Goal
为 `taosd -r --node-type vnode --file-type tsdb --mode force` 提供单副本 vnode 的 TSDB 强制修复能力；在保证 vnode 可启动的前提下，按保守模式尽可能恢复可确认有效的数据。

## Scope
- 仅覆盖：`node-type=vnode`、`file-type=tsdb`、`mode=force`
- 仅覆盖单副本 TSDB 强制修复
- 不包含：`wal`、`tdb/meta`、`replica`、`copy`、其他 node type

## Success Criteria
- 修复后目标 vnode 可以正常启动
- 在保守模式下尽可能恢复数据
- 对无法确认正确的数据块直接丢弃
- 对完全不可恢复的文件组允许摘除

## Architecture
- `dmMain.c` 仅负责 repair 参数校验与分流放行，不承载 TSDB repair 细节。
- TSDB force repair 逻辑下沉到 `source/dnode/vnode/src/tsdb/`。
- repair 总体采用“扫描旧文件组 -> 判定有效块/有效文件 -> 生成新文件组 -> 更新 current.json”的模式，不做 inplace patch。

## Input View
- 以 `current.json` 的 `fset` 为主视图逐个文件组扫描。
- 不在 `current.json` 中引用的残留文件不参与恢复，仍按垃圾文件处理。
- `current.json` 用于定义“应该有哪些文件组，以及每个文件组里有哪些文件和描述信息”；它不是数据有效性的充分条件。

## File-Set Semantics
- `stt` 是自描述单元。
  - 若 `stt` 缺失或损坏，可单独从文件组摘除。
- `head/data/sma` 是联动核心单元。
  - 若 `head` 或 `data` 缺失，则联动摘除 `head/data/sma`。
  - `sma` 视为派生信息，优先重建，而非原样抢救。
- 文件组最终仅允许三种结果：
  - 原样保留
  - 部分重建
  - 整组摘除

## Affected File-Set Rules
文件组被判为受影响，满足任一条件即可：
- `current.json` 描述的关键文件不存在
- 文件名 / 层级 / 块边界与描述信息不一致
- 文件可打开但关键结构无法完整解析
- 某些块能读到，但块级校验、长度、偏移、版本区间或关联关系不成立

## Block Retention Rules
仅当一个块同时满足以下条件时才允许保留：
- 块头可完整解析
- 块长度、偏移、边界合法，不越界
- 块体可完整读取
- 块校验信息若存在则校验通过
- 块对应的版本 / 时间范围 / 表标识等元数据自洽
- 与同文件组所需关联信息不冲突

任一条件不满足，则该块直接判坏并摘除。

## Repair Flow
1. 命中 repair 模式与目标 vnode
2. 读取 `current.json` 并逐组扫描
3. 判定健康文件组与受影响文件组
4. 仅对受影响文件组执行备份
5. 对受影响文件组执行保守扫描与重建决策
6. 为“部分重建”的文件组输出新的修复产物
7. 对“整组摘除”的文件组从新视图中移除
8. 统一生成新的 `current.json`
9. 记录 repair 日志，并确保同一 vnode 在本次启动内不重复 repair
10. 重新打开 / 校验 TSDB，确认 vnode 可启动

## Rebuild Strategy
- 不做 inplace truncate / patch
- 统一使用“读旧块 -> 筛有效块 -> 写新文件组”的模型

### head/data/sma
- 从旧 `head/data` 中枚举候选块
- 验证块在 `data` 中的可读性、边界、校验和元数据自洽性
- 仅将有效块写入新的 `data`
- 基于有效块生成新的 `head`
- 基于最终保留块重建 `sma`

### stt
- 单独按文件或块扫描
- 能完整解析且自洽时保留
- 有问题时直接从结果文件组中摘除

### tomb
- 原则上执行保守扫描
- 能确认完整有效则带入结果
- 若第一版实现复杂度过高，允许按“能稳妥保留则保留，否则摘除并记录”收敛

## Backup Strategy
- 明确不做整个 TSDB 目录全量备份
- 仅备份受影响文件组
- 每个受影响文件组的备份单元必须包含：
  - 原始物理文件
  - 该文件组在 `current.json` 中对应的描述信息
  - repair 过程与结果记录

建议目录形态：
- `backup_root/vnode<V>/tsdb/fid_<fid>/original/...`
- `backup_root/vnode<V>/tsdb/fid_<fid>/manifest.json`
- `backup_root/vnode<V>/tsdb/fid_<fid>/repair.log`

## Logging
日志需回答三个问题：
- 哪些文件组有问题
- 为什么这么处理
- 最终保住了多少

第一版优先记录：
- repair 目标参数
- 文件组级最终决策：`kept` / `rebuilt` / `dropped`
- 判坏原因
- 块级统计：原始块数 / 保留块数 / 丢弃块数
- `sma` / `stt` / `tomb` 的保留或摘除结果
- 新 `current.json` 的最终文件组信息
- 启动校验结果

## Testing Scope
### 参数与分流回归
- `tsdb force repair` 进入真实执行路径
- 不影响 phase1 CLI 校验
- 不影响 phase2 `meta force repair`

### 文件组级 E2E
- `stt` 缺失时，仅摘除 `stt`
- `head` 缺失时，联动摘除 `head/data/sma`
- `data` 缺失时，联动摘除 `head/data/sma`
- 某文件组整体不可恢复时，整组摘除，其他文件组保留

### 块级 E2E
- 人工制造某个 `data` 块损坏，只摘除坏块
- `head` 中部分索引项损坏，仅保留有效块对应索引
- `sma` 损坏但 `head/data` 可恢复时，重建或降级后仍可启动

## First-Version Delivery Boundaries
- 优先实现 `head/data/sma` 主链路
- `stt` 先按“能稳妥保留则保留，否则摘除”处理
- `tomb` 第一版可按相同保守原则收敛，不追求最细粒度恢复
