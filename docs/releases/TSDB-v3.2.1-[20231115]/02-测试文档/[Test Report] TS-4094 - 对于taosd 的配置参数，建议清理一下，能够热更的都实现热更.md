# [Test Report] TS-4094 - 对于taosd 的配置参数，建议清理一下，能够热更的都实现热更

### 1. 概述：

整理taosd热更新参数，对于能够热更新全部实现热更新。
```sql
-- CLI: for current cli
alter local 'enableScience 1';

-- SVR: for dnode 1
alter dnode 1 'mndSdbWriteDelta 40';

-- SVR: for all dnodes
alter all dnodes 'disableStream 1';
```

动态修改后，如需保证重启后生效，需**手动写入**对应配置文件中
 SVR 参数一般期望**集群中保持一致**，建议使用 `alter all dnodes` 进行修改
 *以下新增参数仅在 **企业版** 中支持动态修改

| 外部参数名称 | CLI/SVR | 值域 | 说明 |
| --- | --- | --- | --- |
| keepAliveIdle | BOTH | [1,7200000] | 连接保活时长 |
| mndSdbWriteDelta | SVR | [20, 10000] | 单个文件的日志数目阈值，到达此阈值，mnode元数据会落盘，此时将产生新的wal文件。 |
| enableWhiteList | SVR | [0,1] | 是否开启 whitelist |
| audit | SVR | [0,1] | 是否开启 audit |
| telemetryReporting | SVR | [0,1] | 是否上传 telemetry |
| cacheLazyLoadThreshold | SVR | [0, 100000] | tsdb cache lazy load 时间花费阈值 |
| queryPolicy | CLI | [1,4] | query 策略 |
| queryRspPolicy | SVR | [0,1] | query resp 策略 |
| ttlFlushThreshold | SVR | [-1, 1000000] | 最大缓存 ttl 脏表数量 |
| timeseriesThreshold | SVR | [0, 2000] | 每个 vnode 测点数变化超过 timeseriesThreshold 都会上报 |
| minDiskFreeSize | SVR | [50*1024*1024, 1024 * 1024 * 1024] | 最小磁盘可用大小，单位为B 目前不支持M/G的表达方式 |
| tmqMaxTopicNum | SVR | [1, 10000] | 订阅最多可建立的 topic 数量 |
| transPullupInterval | SVR | [1, 10000] | 当有对 mnode 操作因为错误而未执行结束时，mnode 下次发起重试的时间间隔 |
| mqRebalanceInterval | SVR | [1, 10000] | 检测rebalance的间隔时间 |
| checkpointInterval | SVR | [60, 1200] | 用来指定check point的时间间隔，应大于delete mark以保证增量check point |
| trimVDbIntervalSec | SVR | [1, 100000] | mnode 发起trim db (retention) 的间隔 |
| disableStream | SVR | [0, 1] | 禁用 stream |
| maxStreamBackendCache | SVR | [16, 1024] | 单个 vnode 上 rocksdb 的 cache 限制，到达此阈值之后，会进行 write buffer 的 flush, 可能进一步触发 rocksdb 内部的compaction. |
| numOfLogLines | BOTH | [1000, 2000000000] | 日志最大行数 |
| logKeepDays | BOTH | [-365000, 365000] | 日志保留天数 |

### 2. 测试环境：

192.168.1.63：
CPU: Intel(R) Xeon(R) CPU E5-2650 v3 @ 2.30GHz（2）40核
Mem: DDR4 16GB* 16
Disk: 895GB

### 3. 测试用例：


|  | 用例描述 | 期望结果 | 测试结果 | 备注 |
| --- | --- | --- | --- | --- |
| 删除内部参数 移除热更功能项 | 对以下参数执行“alter all dnodes ”xxxx value“； tsCompressColData tsMaxNumOfDistinctResults tsCompatibleModel tsPrintAuth tsVndCommitMaxIntervalMs tsAuditFqdn tsAuditPort tsRpcRetryLimit tsRpcRetryInterval maxShellConns | 执行命令报错 | 执行命令报错，错误信息”DB error: Invalid config option “ |  |
| 新增热更功能项 | 1. 对新增功能项，cli 执行”alter local ”xxx value“；svr 执行”alter dndoe 1 ”xxx value““或”alter all dnodes ”xxx value““ 1. 对新增功能项范围外的值执行cli 执行”alter local ”xxx value“；srv 执行”alter dndoe 1 ”xxx value““或”alter all dnodes ”xxx value““ | 1. 功能项热更完成，通过gdb命令获取当前值，并与设置值对比 1. 功能项热更失败 | 1. 热更新完成，设置值与当前值一致 1. 热更新失败，错误信息”DB error: Out of range“ | [TD-27210](https://jira.taosdata.com:18080/browse/TD-27210) [热更minDiskFreeSize变量，默认单位bytes与值范围【50MB, 1GB】不易用](https://jira.taosdata.com:18080/browse/TD-27210) [TD-27205](https://jira.taosdata.com:18080/browse/TD-27205) [热更bool类型的变量，对变量值范围没有限制](https://jira.taosdata.com:18080/browse/TD-27205) [TD-27193](https://jira.taosdata.com:18080/browse/TD-27193) ["keepAliveIdle" 客户端热更新值与设置不一致](https://jira.taosdata.com:18080/browse/TD-27193) [TD-27189](https://jira.taosdata.com:18080/browse/TD-27189) [热更新客户端变量，日志中没有更新记录](https://jira.taosdata.com:18080/browse/TD-27189) |

### 4. 总结：

1. 部分功能项热更已删除
2. 对cli和svr功能项热更的正常值、异常值测试，与预期相符
