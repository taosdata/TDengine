# 强制制定指定双副本 Leader FS

## 1. 背景

JIRA：[TS-5805](https://jira.taosdata.com:18080/browse/TS-5805)
在双副本场景中，两个节点都停止后只启动一个节点时，无法选出 assigned leader，为了能够让双副本继续运行，本次修改增加 SQL 命令强制设置 assigned leader。

## 2. 变更历史

| 日期 | 版本 | 负责人 | 主要修改内容 |
| --- | --- | --- | --- |
| 2025/2/13 | 0.1 | 陈东明 |  |

## 3. 定义

无

## 4. 行为说明

增加一个新的命令
```bash
assign leader force
```

同时停止 2 个节点，然后重启其中一个节点时，调用 show vgroups 命令后，双副本的 vgroup 都会处在 candidate 状态。此时执行 show arbgroups，可以看到 vgroups 的 is_sync 处在 false 状态，因此，arbitrator 不会将处在 candidate 状态的节点设置为 assigned leader。通过新增 assign leader force 命令，给该 vnode 节点发送消息，强制将这个 vnode 设置为 assigned leader，使 vgroup 可以继续工作。

## 5. 性能

无

## 6. 兼容性

无

## 7. 运维

无

## 8. 使用场景

同时停止 2 个节点，但是随后只启动其中 1 个节点，使用 assign leader force，让双副本手工进入到 assigned leader 状态。

## 9. 约束和限制

Assign leader force 仅在 一个条件下可以使用：“同时停止 2 个节点，在保持磁盘文件未有任何改变的情况下启动其中 1 个节点，并且第二节点在后续也在保持磁盘文件未有改变的情况下重新启动”，这种情况下，在 2 个节点停止前，raft 保证数据已经写入到 2 个节点的 wal，所以在重启其中一个节点，并且将这个节点强制设置 assigned leader 后，即使后续另外一个节点启动起来，raft 仍然可以保证数据的完整，没有数据冲突。
在其他原因导致 vgroup 的 2 个副本处在 is_sync=false 的状态下，此时 2 个副本处在不同步状态，在这种状态下强制设置 assigned leader，可能会导致数据丢失，甚至 2 个副本数据冲突，导致后续 vgroup 无法再继续工作。

## 10. 常见错误和排查

无

## 11. 可观测性

无

## 12. 安装和卸载

无

## 13. 文档

由于这个命令的使用限制，所以这个命令不写入到文档中，作为内部的一个运维命令。

## 14. 参考文档

## 15. 附录
