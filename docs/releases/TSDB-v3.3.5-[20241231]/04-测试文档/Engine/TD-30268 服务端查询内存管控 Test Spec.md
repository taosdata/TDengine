# TD-30268 服务端查询内存管控 Test Spec

## 1. 测试目标

- 验证服务端能够对单个或整体实施查询内存管控。

## 2. 变更历史

| Date | Version | Owner | Memo |
| --- | --- | --- | --- |
| 2024.11.11 | 0.1 | 潘魏 | 初始版本 |
|  |  |  |  |

## 3. 测试范围

- 验证在正确使用服务端查询内存管控时能够降低服务端 OOM 风险；
- 验证单个查询内存上限控制功能；

## 4. 测试结论

## 5. 已知问题和限制

参考 FS 说明。

## 6. 测试环境

- OS: Linux

## 7. 测试用例

| 类型 | 测试目的 | 预期结果 | 测试结果 | 用例结果 |
| --- | --- | --- | --- | --- |
| 单元测试 | 获取系统实时可用内存大小的性能 | 单次耗时与CPU消耗均很小 | 单次耗时7微秒左右，以10毫秒为间隔获取CPU占用在2%左右，可以接受 | Pass |
|  | 单节点上单个查询（含所有子查询）的内存分配大小统计正确性 | 在全功能模式下对比统计正确，简略模式下存在统计误差 | 全功能模式下对比正确，简略模式下存在误差 | Pass |
|  | 内存达到预留上限时的淘汰速度和 OOM 概率大小 | 验证写入速度、查询淘汰速度、预留大小及稳定性的关系 | 查询不会导致内存耗尽，其他内存写入速度在超过淘汰速度时及预留大小时会导致 OOM，符合预期 | Pass |
| 功能测试 | queryUseMemoryPool、minReservedMemorySize、singleQueryMaxMemorySize的正确、非法、动态配置 | 不报错、报错、不生效 | 符合预期 | Pass |
|  | 测试不开启内存管控（queryUseMemoryPool为false)时的现有功能 | 不受影响 | 在关闭内存管控的条件下跑全部 CI 用例，测试通过 | Pass |
|  | 测试开启内存管控（queryUseMemoryPool为true)且内存未达上限时的功能 | 不受影响 | 开启内存管控的条件下跑全部 CI 用例，测试通过 | Pass |
|  | 测试不开启单个查询内存上限（singleQueryMaxMemorySize为0）时单个查询到达上限场景 | 不报错 | 开启内存管控且不开启单个查询内存上限的条件下跑全部 CI 用例，测试通过 | Pass |
|  | 测试开启单个查询内存上限（singleQueryMaxMemorySize不为0）时单个查询到达上限场景 | 报错“Query memory upper limit is reached”或其他错误码 | 正常情况下报错“Query memory upper limit is reached”，但是因为有些错误码整改未到位导致有时候会返回其他错误码 | Pass |
|  | 测试开启内存管控（queryUseMemoryPool为true)且内存达到上限时的场景 | 部分查询报错"Query memory exhausted" | 到达上限时部分查询返回“Query memory exhausted” | Pass |
|  | 测试系统可用内存低于阈值时内存管控默认不启用 | 内存管控不启动 | 内存管控不启动 | Pass |
|  | 测试预留内存后可用内存低于阈值时内存管控默认不启用 | 内存管控不启动 | 内存管控不启动 | Pass |
| 性能测试 | 测试不开启内存管控（queryUseMemoryPool为false)时的性能影响 | 性能影响很小 | 用taosBenchmark测试典型聚合、投影查询，与3.0分支相比查询平均耗时有所下降，每项差别在10%左右。 | Pass |
|  | 测试开启内存管控（queryUseMemoryPool为true)且内存未达上限时的性能影响 | 性能影响很小 | 用taosBenchmark测试典型聚合、投影查询，与3.0分支相比查询平均耗时一升一降，每项差别都在10%以内。
性能监控也是有升有降，总体变化不大：
[http://192.168.0.204:3000/d/e4dca7c7-ae86-44b3-a61c-a16aefe7483b/baseline?orgId=1&refresh=1m&var-base_type=release&var-base_label=3.3.4.3&var-target_type=release&var-target_label=3.0.0.100](http://192.168.0.204:3000/d/e4dca7c7-ae86-44b3-a61c-a16aefe7483b/baseline?orgId=1&refresh=1m&var-base_type=release&var-base_label=3.3.4.3&var-target_type=release&var-target_label=3.0.0.100) | Pass |
| 稳定性测试 | 测试启用内存管控且内存到达上限时的稳定性 | 部分查询返回错误，无crash情况 | 在机器只有查询的情况下，40G内存预留8G内存场景，可以稳定运行12小时，期间频繁达到内存上限，有报错无crash；
长稳环境运行12小时没有问题； | Pass |
|  | 测试验证所有查询结束后的内存占用情况 | 无遗留查询占用内存 | 上面的稳定性运行结果后最终会到达所有查询清空的情况，快慢取决于配置“queryNoFetchTimeoutSec” | Pass |

### 7.1 兼容性

无。

## 8. 参考文档

[服务端查询内存管控](https://taosdata.feishu.cn/wiki/Y5tbwW0bwiQqdfkoYLlcVS36nfc)
