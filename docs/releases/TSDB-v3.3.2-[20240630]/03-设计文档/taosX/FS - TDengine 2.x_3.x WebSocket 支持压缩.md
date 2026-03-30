# FS - TDengine 2.x/3.x WebSocket 支持压缩

## 1. 背景

本文中所说的“压缩”，是 WebSocket Compression Extension（即 permessage-deflate extension， 见 [IETF RFC 7692](https://datatracker.ietf.org/doc/html/rfc7692) ），该扩展向 WebSocket 协议添加了压缩功能，对 WebSocket 连接中每一条消息进行压缩后传输，从而一定程度上降低数据交换时对网络带宽的要求。但压缩会增加服务端和客户端的 CPU 压力，因此，我们不会默认启用 WebSocket 压缩支持。在本文档中，对 TDengine 2.x/3.x 数据源增加一个 `compression` 参数，以显示地启用 WebSocket 传输压缩。

## 2. 变更历史

| 日期 | 版本 | 负责人 | 主要修改内容 |
| --- | --- | --- | --- |
| 2024/06/18 | 0.1 | @霍琳贺 | 初稿，数据源压缩参数 |
| 2024/06/20 | 0.2 | @霍琳贺 | 与 @顾香 沟通，压缩参数统一配置到”高级选项“中 |
|  |  |  |  |

## 3. 定义

- WebSocket 压缩：特指 WebSocket "permessage-deflate" Extension， 见 [IETF RFC 7692](https://datatracker.ietf.org/doc/html/rfc7692) 。
- TDengine 2.x 数据源：使用查询从 TDengine旧版本中拉取数据，其 DSN 标识为 `taos`。
- TDengine 3.x 数据源：特指使用数据订阅方式拉取数据的 TDengine 数据源（3.0 及以上版本）,其 DSN 标识为 `tmq`。

## 4. 行为说明

### 4.1 数据源 DSN

`taos`/`tmq` DSN 使用 WebSocket 连接时，新增 `bool` 类型参数 `compression`，用于指示是否启用压缩，无此参数时不启用压缩。使用原生连接时，`compression` 参数不生效，也不会报错。
示例如下：
```plaintext

## 5. 查询/写入启用 WebSocket 压缩

taos+ws://localhost:6041/db1?compression

## 6. TMQ 启用压缩

tmq+ws://root:taosdata@localhost:6041/db1?compression
```

在 taosx run 命令行模式下，数据源和目标端都支持压缩参数：
```bash

## 7. 数据订阅源/目标均使用 WebSocket 连接并启用压缩

taosx run \
  -f "tmq+ws://root:taosdata@localhost:6041/db1?compression" \
  -t "taos+ws://root:password@hostname:6041/db2?compression"

## 8. 数据迁移源/目标均使用 WebSocket 连接，源端启用压缩，目标端不启用压缩

taosx run \
  -f "taos+ws://root:taosdata@hostname:6041/db1?compression" \
  -t "taos+ws://root:password@localhost:6041/db2"

## 9. 数据迁移源端使用原生连接，目标端使用 WebSocket 连接并启用压缩

taosx runf \
  -f "taos:///db1" \
  -t "taos+wss://cloud.tdengine.com/db2?token=abcde&compression"
```

### 9.1 Explorer Data In

数据源 TDengine 2.x 和  TDengine 3.x 添加参数 `compression`：
![](./images/img_GQ2Wb55MBoqTj3xp1igc2oSEn2g.png)

TDengine 3.x 增加 `compression` 效果示例：
![](./images/img_Pby0bdQcNo0LT2xOVdZcxyzdnWc.png)

提示信息如下：
- Enable WebSocket compression to reduce network bandwidth consumption.
- 启用 WebSocket 压缩支持，以降低网络带宽占用。
TDengine 2.x 数据源参考 TDengine 3.x 参数添加，增加到 "高级选项“ 里。为保持一致性，3.0 数据源也增加”高级选项“ 部分。

### 9.2 ~~Explorer 配置文件~~

~~Explorer 配置文件中，cluster 选项可配置为 ~~`~~http://localhost:6041?compression~~`~~ 以启用 WebSocket 压缩，在创建数据源任务时，将 ~~`~~compression~~`~~ 参数带入。~~
~~需要讨论：Explorer 通常与 taosd、taosadapter 部署在一起，出于性能考虑，这个配置建议不暴露。~~

## 10. 性能

当启用压缩时，传输性能有所降低。
以本地 taosAdapter 查询 1 亿行数据（taosBenchmark 默认数据集）为例，启用压缩前，查询耗时 12 秒，启用压缩后，查询耗时 38 秒。
```bash
TDENGINE_CLOUD_DSN="ws://localhost:6041"  ./taosBenchmark -f query.json

[06/18 10:04:21.926972] SUCC: ws://localhost:6041 conneced
[06/18 10:04:33.766508] INFO: wsFetchResult() LN907, wsFetchResult delay: 11827
complete query with 1 threads and 1 query delay avg:         11.839575s min:         11.839575s max:         11.839575s p90:         11.839575s p95:         11.839575s p99:         11.839575s SQL command: select * from meters

TDENGINE_CLOUD_DSN="ws://localhost:6041?compression"  ./taosBenchmark -f query.json
[06/18 10:11:56.174688] SUCC: ws://localhost:6041?...ompression conneced
[06/18 10:12:33.612417] INFO: wsFetchResult() LN907, wsFetchResult delay: 37427
[06/18 10:12:33.612496] INFO: thread[2] has currently completed queries: 1, QPS:   0.026711
complete query with 1 threads and 1 query delay avg:         37.437775s min:         37.437775s max:         37.437775s p90:         37.437775s p95:         37.437775s p99:         37.437775s SQL command: select * from meters

```

## 11. 兼容性

连接器客户端是自适应的，无论服务端是否支持压缩，均不会报错，连接正常。在服务端支持压缩时，压缩生效，不支持压缩时，压缩不生效，连接正常。

## 12. 运维

~~当 Explorer 配置文件需要支持写入端压缩时，需要增加说明。~~

## 13. 使用场景

### 13.1 带宽受限的源端或目标端

### 13.2 启用压缩后性能满足需求，降低带宽占用以节省成本

## 14. 约束和限制

无。

## 15. 常见错误和排查

启用压缩不增加显式错误信息。
- WebSoket Protocol Error：HTTP version must be 1.1 or higher
这个错误是 WebSocket 连接错误的一种，在 taosAdapter 启用了 SSL，但连接方式仍然使用 http:// 连接导致的，应该把连接方式改为 https:// 即可。

## 16. 可观测性

无变化。

## 17. 安装和卸载

无变化。

## 18. 文档

需要修改企业版文档以添加此功能说明，不需要修改官网文档。
