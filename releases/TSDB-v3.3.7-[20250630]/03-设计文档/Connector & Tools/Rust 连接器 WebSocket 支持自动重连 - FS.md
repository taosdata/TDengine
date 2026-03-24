# Rust 连接器 WebSocket 支持自动重连 - FS

## 1. 背景

TD-34374

## 2. 变更历史

| 日期 | 版本 | 负责人 | 主要修改内容 |
| --- | --- | --- | --- |
| 2025/05/08 | 0.1 | 郭振伟 | 编写文档 |
| 2025/05/13 | 1.0 | 郭振伟 | 根据 review 意见修改文档 |

## 3. 定义

自动重连：系统在网络连接异常中断后，自动尝试重新建立连接，并在连接恢复后执行必要的状态同步与任务续传的完成过程。不仅包括网络层的恢复，还包括重建连接后的业务连续性保障动作。

## 4. 行为说明

默认开启自动重连。

### 4.1 DSN 参数

| 参数 | 定义与说明 | 有效范围 | 默认值 |
| --- | --- | --- | --- |
| conn_retries | 原 DSN 参数定义了连接建立失败时的最大重试次数，现重新定义为连接建立和自动重连失败时的最大重试次数。 | >= 0 | 5 |
| retry_backoff_ms | 基础重连等待时间。此值是初始退避值，并且会随着每次失败的请求呈指数级增长，直至达到 retry_backoff_max_ms 的值。 | >= 0 | 200 |
| retry_backoff_max_ms | 最大重连等待时间。发送请求失败时，等待的最长时间。为了防止所有客户端在重试时同步，将对退避时间应用一个 0.2 倍的随机抖动，使退避时间介于计算值上下 20% 之间。如果将 retry_backoff_ms 设置为高于 retry_backoff_max_ms，则 retry_backoff_max_ms 将从一开始就用作恒定退避时间。 | >= 0 | 2000 (2 second) |

示例：
```plaintext {wrap}
ws://localhost:6041,localhost:16041,localhost:26041?conn_retries=3&retry_backoff_ms=200&retry_backoff_max_ms=2000
```

### 4.2 自动重连行为说明

| 自动重连行为说明 | 备注 |
| --- | --- |
| 写入 | 恢复 WebSocket 连接，重发请求。 |
| 查询 | 恢复 WebSocket 连接，对于没使用 ResultSet 的重发请求。 |
| 恢复 WebSocket 连接，对于没使用 ResultSet 的重发请求。 |  |
| 恢复 WebSocket 连接，重发请求。 | 对于 InfluxDB 行协议，在执行时可能会遇到幂等性问题。 |
| 恢复 WebSocket 连接，poll 从最后一次提交的 offset / auto.offset.reset 位置继续拉取消息。 | poll 在执行时可能会遇到消息重复消费问题。 |

## 5. 性能

关于性能测试，请参考 [Rust 连接器 WebSocket 支持自动重连 - TS](https://taosdata.feishu.cn/wiki/ZHNKwMrwPiL4ufkuh7GcKby5n1f) 文档 8.4 章节相关内容。

## 6. 兼容性

不引入破坏性修改。

## 7. 运维

无。

## 8. 使用场景

1. 执行 SQL 自动重连
2. 参数绑定自动重连
3. 无模式写入自动重连
4. 数据订阅自动重连

## 9. 约束和限制

约束：无。
限制：
- 不处理超时情况，因为无法区分是真正的连接断开还是慢 SQL 导致的超时。
- 自动重连成功后，因 `use db` 导致的 SQL 执行错误，不进行处理。

## 10. 常见错误和排查

无。

## 11. 可观测性

在日志中打印自动重连的相关信息。

## 12. 安装和卸载

无。

## 13. 文档

需要修改官网文档。

## 14. 参考文档

[连接器自动重连](https://taosdata.feishu.cn/wiki/COb9whcCgiIkjNk1KMUcyfFlnLh)

## 15. 附录

无。
