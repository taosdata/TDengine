# 消费端可控的 WAL 保留机制 FS

## 1. 修订记录

| 编写日期 | 发布日期 | 版本 | 修订人 | 主要修改内容 |
| --- | --- | --- | --- | --- |
| 2025-10-24 | - | 0.1 | 关胜亮 | 新建 |
| 2025-11-07 | 2025-11-13 | 0.2 | 鲍之骁 | 根据实现调整文档细节 |

## 2. 背景

在当前 TDengine 的设计中，WAL 文件会根据数据库的 `WAL_RETENTION_PERIOD`参数设置的时间间隔进行清理。如果一个数据订阅的进度缓慢，超过此时间间隔其尚未消费的 WAL 文件有可能会被系统自动删除。当订阅进度恢复时，由于部分数据对应的 WAL 文件已丢失，会导致消费中断或数据丢失，影响服务的可靠性。
JIRA: [TS-7539](https://jira.taosdata.com:18080/browse/TS-7539)

## 3. 定义

1. WAL：一种数据库容错机制，数据写入时会先写入日志文件，再写入数据库数据文件。
2. WAL_RETENTION_PERIOD​：数据库配置参数，WAL 日志文件需额外保留的最大时长。
3. Consumer Offset​：消费者在某个 Topic 的某个分片（vnode）上的消费位移。
4. WAL Version：与每个 WAL 文件或记录关联的、单调递增的序列号，用于标识数据在 WAL 中的位置和顺序。

## 4. 行为说明

本特性旨在通过引入一种由消费端进度控制的 WAL 保留机制来解决此问题，确保订阅暂停期间，其所依赖的 WAL 文件不会被自动清理，从而保证数据被至少消费一次。

### 4.1 新增消费者配置参数

在创建 TMQ 消费者时，增加一个名为 `enable.wal.``marker`的配置参数。
1. 定义​：布尔型参数，控制消费者是否在提交 offset 时，自动提交其已消费数据对应的 WAL Version。
2. 有效值​：`true`，`false`。
3. 缺省值​：建议为 `false`，建议仅在双活场景中使用。

### 4.2 提交 offset 时的行为变更

当消费者调用 `tmq_commit_sync`（或其异步版本）时，或者自动提交时，若 `enable.wal.marker`为 `true`，则提交操作不仅在内部记录消费位移，还会将此位移对应的 WAL Version 一并提交给对应的 vnode。

### 4.3 Vnode 对 WAL 版本号的持久化与同步

1. vnode 在接收到消费者提交的 WAL Version 后，为了保证没有被消费的 WAL log 不被删除, 会将其记录在 vnode 的元数据中，简称 WAL Keep Version。
2. WAL Keep Version 每个 vnode 中保存一个值，不会按照不同的 topic 保存多个值
3. WAL Keep  Version 的修改会通过 mnode 中的事务机制分发给各个节点，保证其功能的高可用。

### 4.4 WAL 清理逻辑的增强

1. vnode 在执行基于 `WAL_RETENTION_PERIOD`的例行 WAL 文件清理时，其清理逻辑需进行修改。
2. 新的清理规则​：仅能删除 WAL Keep Version 之前的 WAL 文件，即使某个 WAL 文件的保存时间已超过 `WAL_RETENTION_PERIOD`。

### 4.5 新增 `TRIM DATABASE`命令

引入一个新的 SQL 命令，手动清理过期的 WAL 文件。
1. 语法​：`TRIM DATABASE [db_name] WAL;`
2. 说明​：此命令会强制清理指定数据库下所有已过期（即超过 `WAL_RETENTION_PERIOD`）的 WAL 文件。该命令的执行将忽略 WAL Keep Version 所施加的保留限制，适用于需要紧急释放磁盘空间或进行特殊维护的场景。
3. 示例​
```sql {wrap}
TRIM DATABASE my_meter_db WAL;
```

### 4.6 调整 `SHOW VGroups`命令

调整 Show VGroups 命令的列，增加当前的 WAL Keep Version。
1. WAL Keep Version
2. WAL Keep Time：版本号对应的 WAL 生成时间

## 5. 性能

1. 写入性能​：无显著影响。
2. 查询性能​：无影响。
3. 订阅性能：WAL 版本号的提交是跟随 offset 提交异步进行的，不影响性能。
4. 存储空间​：由于 WAL 文件的保留时间可能因消费者暂停而延长，在极端情况下可能会占用更多的磁盘空间。这是用空间换取数据可靠性的权衡。

## 6. 安全

1. 不涉及新的安全风险或引入新的安全接口。WAL 版本号的提交和 vnode 间的同步复用现有的数据复制通道和安全机制。
2. Trim Database 的执行权限需要在 [访问控制](https://taosdata.feishu.cn/wiki/Y12Ywd797ieHBBkVZsqcpsRgnAg) 中考虑，属于系统管理级别

## 7. 兼容性

1. 向后兼容​：新参数 `enable.wal.``marker`默认为 `false`。现有应用程序无需修改代码，其行为不变。
2. 向前兼容​：无兼容性问题。
3. 在 3.3.6 分支上开发

## 8. 运维

1. 磁盘空间管理​：应关注因消费者长时间离线导致的 WAL 文件积压问题，并知晓可使用 `TRIM DATABASE`命令进行强制清理。

## 9. 使用场景

双活场景，参见 [双活设计文档](https://taosdata.feishu.cn/wiki/E9NmwBfIbiTA5bkq8kScFX0yn8c?from=space_search)。

## 10. 约束和限制

无

## 11. 常见错误和排查

1. 磁盘空间不足告警​：若收到此类告警，需检查是否有消费者滞后，并评估使用 `TRIM DATABASE`的必要性。

## 12. 可观测性

1. 在 `taos shell`中，增强 了`SHOW VGROUPS`类命令的输出。
2. 在 `TDinsight`的数据库监控页面上，可增加 WAL 实际保留大小/时间与配置的 `WAL_RETENTION_PERIOD`的对比图表。在本功能发布后，为交付部门创建相关任务。

## 13. 安装和卸载

无。

## 14. 文档

需更新官网文档，包括
1. `enable.wal.marker`参数的作用和配置方法。
2. `Trim database`参数的用法。

## 15. 参考文档

无

## 16. 附录

无。
