# Opcda 重连优化 FS

## 1. 修订记录

| 编写日期 | 发布日期 | 版本 | 修订人 | 主要修改内容 |
| --- | --- | --- | --- | --- |
| 2025-12-04 | 2025-12-04 | 0.1 | 谭雪峰 | 编写文档 |
| 2025-12-05 | 2025-12-05 | 1.0 | 谭雪峰 | 添加 failed_reads_to_force_reconnect |

## 2. 背景

opcDA 重连时如果添加点位失败会直接跳过此点位，导致有些点位一直不采集。此优化增加重连次数、重连间隔、重新添加点位尝试次数、重新添加点位间隔。

## 3. 定义

无。

## 4. 行为说明

1. opc-da 启动时如果添加点位失败只会跳过此点位，此行为不变
2. 重连后重新添加点位失败将跳过此点位，此行为变更为重连后添加点位失败将重试，每次间隔 500 ms 尝试 100 次，如果都失败将退出，由 taosx 再拉起，重试次数和间隔可配置
3. 重连检查为采集时触发，采集点位失败将检测连接状态，如果获取ServerState成功并且状态不为 OPCRunning（1） OPCNoconfig（3） 和 OPCTest（5）则认为连接无异常不重连，否则进行重连，重连将尝试 100 次，每次间隔 1 秒，都失败时将退出，重连次数和重连间隔可配置
4. 累计读取失败 50 次将强制进行重连
5. 增加配置控制重连和重新添加点位
   - reconnect_times：重连尝试次数，不设置默认 100
   - reconnect_interval：重连间隔，单位毫秒，不设置默认 1000
   - add_tag_retry_times：重连后重新添加点位尝试次数，不设置默认 100
   - add_tag_retry_interval：重新添加点位失败时下次重试间隔，单位毫秒，不设置默认 500
   - failed_reads_to_force_reconnect：累计读取失败达到次数将强制重连，不设置默认 50
  样例
  ```json
  [connect.da]
  server = "Matrikon.OPC.Simulation.1"
  nodes = ["localhost"]
  reconnect_times = 100
  reconnect_interval = 1000
  add_tag_retry_times = 100
  add_tag_retry_interval = 500
  failed_reads_to_force_reconnect = 50
  ```

1. Explorer 添加界面配置以上参数
2. Taosx-opc 日志中如果包含 `[RECONNECT]` 则需要 Explorer 进行展示

## 5. 性能

无。只有连接异常时会触发重连

## 6. 安全

不涉及

## 7. 兼容性

无。

## 8. 运维

无。

## 9. 使用场景

Opc da 连接器由于网络中断或 opc server 重启导致重连

## 10. 约束和限制

约束：如果重连一直失败或添加点位也失败将崩溃退出，后续由 taosx 拉起

## 11. 常见错误和排查

数据点一直不采集，检查任务是否有过因为添加点位而重启，opc 启动日志中是否有 opcda add tag error 错误

## 12. 可观测性

启动时会打印日志
```json
opcda connect with reconnect_times:%d,reconnect_interval:%s,add_tag_retry_times:%d,add_tag_retry_interval:%s
```

重连开始会打印
```json
[RECONNECT] Trying to reconnect.
```

重连成功会打印
```json
[RECONNECT] Reconnection successful after %d attempts, total cost time: %d us.
```

失败会打印
```json
// 重新添加点位失败
[RECONNECT] Reconnection failed after %d attempts, due to re-adding tags failed, total cost time: %d us.
// 重连失败
[RECONNECT] Reconnection failed after %d attempts, total cost time: %d us.
```

重新添加点位失败额外打印失败点位
```json
[RECONNECT] Could not re-add tag: %s
```


## 13. 安装和卸载

无

## 14. 文档

不需要

## 15. 参考文档

## 16. 附录
