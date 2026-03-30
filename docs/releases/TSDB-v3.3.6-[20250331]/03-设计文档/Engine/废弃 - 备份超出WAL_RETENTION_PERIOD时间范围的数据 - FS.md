# 废弃 - 备份超出WAL_RETENTION_PERIOD时间范围的数据 - FS

## 1. 背景

针对 [备份超出WAL_RETENTION_PERIOD时间范围的数据](https://taosdata.feishu.cn/wiki/X4oWw0pbNiw28QkIY95cWD3sncg)需求，我们需要考虑 WAL 删除时无法订阅和备份完整数据的情况，为此制定此文档。

## 2. 变更历史

注：版本变更规则，初始版本为 0.1，中间若经过几次较大修改要增加版本号为 0.2， 0.3，最后定稿时的版本号为 1.0，以下为示例

| 日期 | 版本 | 负责人 | 主要修改内容 |
| --- | --- | --- | --- |
| 2025/03/05 | 0.1 | 霍琳贺 | 创建 |
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

### 4.1 订阅参数 `wal.keepalive.timeout` 

在 TMQ 数据订阅配置中增加参数 `wal.keepalive.timeout` ，其值必须为整数，单位为秒（s）。用法如下：
```c {wrap}
tmq_conf_set(conf, "wal.keepalive.timeout", "60");
```

默认值为** -1**，行为与之前保持一致，订阅与 WAL 无关，WAL 删除操作正常执行。**用户输入必须大于等于 0 。**
当数值为** 0** 时，表示在订阅过程中，不删除 WAL。在正常关闭订阅或异常断开时，可恢复执行删除 WAL。
当数值 S **大于 0** 时，表示在消费者组订阅者退出后 S 秒后，如果仍未恢复订阅，则可执行删除。如果订阅者在该时间内恢复订阅，则按照新订阅规则执行。
对于同一 Topic 同一 VNODE 中不同消费者组使用不同的 `wal.keepalive.timeout` 时，取最大值。 

### 4.2 添加 SQL 语句清除 `wal.keepalive.timeout` 

为避免不合理的订阅参数导致 WAL 保留时间太长，管理员可使用 SQL 语句清除 `wal.keepalive.timeout` 。
```sql {wrap}
alter consumer group <groupid> on <topic> 'wal.keepalive.timeout' '-1';
```

删除主题和消费者组的 SQL 语句 `drop consumer group` 和 `drop topic` 需要清除 `wal.keepalive.timeout` 设置。

### 4.3 Explorer 添加参数 `wal.keepalive.timeout` 

对 DataIn - TDengine 3.0 和数据备份添加参数 `wal.keepalive.timeout` 。
其他参数为备份程序内部使用，不对外暴露。

### 4.4 taosX 增量备份方案的优化

当前 taosX 仅使用一个 group.id 进行消费。一个可能的优化点是将备份拆分为历史数据和实时数据：
1. 首先启动 snapshot=false,auto.reset.offset=earliest 的订阅，备份实时数据。
2. 然后启动 snapshot=true,auto.reset.offset=earliest 的订阅，备份历史数据，直到订阅到 WAL 的起始为止。
增量备份的优化不是必要的，可延后安排。

## 5. 性能

此修改对性能无影响。

## 6. 兼容性

已考虑兼容性，无影响。

## 7. 运维

### 7.1 备份任务

旧的备份任务需要在新版本 taosX 启动后，默认启用 `wal.keepalive.timeout=0` 。用户需要按照实际情况配置 `wal.keepalive.timeout` 参数，以保障在 taosX 应用与 TDengine 集群连接中断期间，WAL 不删除。

## 8. 使用场景

### 8.1 `wal.keepalive.timeout` 对所有订阅生效

消费者可按需修改 `wal.keepalive.timeout` ，以使数据订阅的数据完整性得到改善。

### 8.2 taosX 增量备份

taosX 增量备份启动时，默认启用 `wal.keepalive.timeout`，以避免在增量备份过程中 WAL 在未订阅完毕前删除。

## 9. 约束和限制

无。

## 10. 常见错误和排查

无。

## 11. 可观测性

无。

## 12. 安装和卸载

无。

## 13. 文档

- 添加 Explorer 文档，描述 `wal.keepalive.timeout` 功能和影响。

## 14. 参考文档

无。

## 15. 附录

无。
