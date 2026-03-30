# 备份超出WAL_RETENTION_PERIOD时间范围的数据 - FS

## 1. 背景

针对 [备份超出WAL_RETENTION_PERIOD时间范围的数据](https://taosdata.feishu.cn/wiki/X4oWw0pbNiw28QkIY95cWD3sncg)需求，我们需要考虑 WAL 删除时无法订阅和备份完整数据的情况，为此制定此文档。

## 2. 变更历史

注：版本变更规则，初始版本为 0.1，中间若经过几次较大修改要增加版本号为 0.2， 0.3，最后定稿时的版本号为 1.0，以下为示例

| 日期 | 版本 | 负责人 | 主要修改内容 |
| --- | --- | --- | --- |
| 2025/03/06 | 0.1 | 霍琳贺 | 创建 |
|  |  |  |  |
|  |  |  |  |
|  |  |  |  |

## 3. 定义

- 备份：此处备份指通过 TMQ 数据订阅机制进行数据库或超级表增量备份的场景。
- WAL：Write-Ahead-Log，TMQ 数据订阅中读取 WAL 中的消息给消费者。
- WAL_RETENTION_PERIOD：控制消费者可订阅的数据范围。
- 订阅或备份数据不完整的场景：
  - WAL  删除与订阅是隔离的，消费者如果未消费完 WAL 中所有数据，当 WAL 删除后未消费的消息就订阅不到了（虽然实际仍然在数据库中存储）。

## 4. 行为说明

### 4.1 基本原则

TSDB 中最早的 Offset 记为 `tsdb_earlist`，最新的记录记为 `tsdb_latest`。
WAL 中最早的 Offset 记为 `wal_earlist`，最新的记录记为 `wal_latest`。
根据配置的不同，消费者启动订阅可以从 TSDB 或 WAL 的任意起止位置开始，或者指定每个 VGROUP 的 Offset 消费。
消费者启动订阅后，每次 Poll 新的数据都会有一个 Offset，记为 `current_offset`，此时 WAL 中的起始位置仍记为 `wal_earlist`。
如果 `current_offset` < `wal_earlist`，则从 TSDB 中订阅`[current_offset,wal_earlist)` 之间的数据。
如果 `current_offset` >= `wal_earlist`，则从 WAL 中继续订阅数据。

### 4.2 订阅添加参数 `fallback.snapshot.enable`

`fallback.snapshot.enable` 为 false，则当 WAL 删除后，则不会再从 TSDB 中读取差异数据，直接跳到新的 WAL 起始位置继续订阅，即：与现在的行为一致。
此参数在 `experimental.snapshot.enable` 为 `true` 时不生效。

### 4.3 COMPACT

每次 COMPACT 时，记录其影响的 snapshort version 位置：`[compact_start, compact_end)`。
当订阅的 `current_offset` 在此位置外，对订阅无影响。
当订阅的 `current_offset` 在此位置范围时，下次订阅从 `compact_start` 位置开始，重新订阅数据。

## 5. 性能

此修改对性能无影响。

## 6. 兼容性

已考虑兼容性，无影响。

## 7. 运维

## 8. 使用场景

### 8.1 当 WAL 删除后仍然能够按顺序订阅到数据

此修改将对所有订阅生效。

### 8.2 taosX 增量备份

taosX 增量备份启动对  WAL 时长的依赖就不再成立，大大降低运维复杂度。

## 9. 约束和限制

- COMPACT 后，可能订阅到重复的数据。

## 10. 常见错误和排查

无。

## 11. 可观测性

无。

## 12. 安装和卸载

无。

## 13. 文档

无。

## 14. 参考文档

无。

## 15. 附录

无。
