# Flow control for multi-level storage

## 1. [JIRA](https://jira.taosdata.com:18080/browse/TD-18582)

- [TD-18582](https://jira.taosdata.com:18080/browse/TD-18582) 3.0 多级存储越级迁移支持流控

## 2. 需求来源

- 多级存储迁移会涉及文件拷贝操作, 如果不对拷贝速度进行限制, 会影响查询和写入速度.  如评论中 @李珲提到的 提到的 "会占用较多的磁盘 IO 和 CPU，如果不限制的话，可能会影响到正常的写入、查询、流计算等业务。某些情况下，流计算的算子会慢于写入，从而出现消息的累积，占用内存会越来越多，导致OOM"

## 3. 执行方式

- 手工在 taos 中输入下述命令:
```plaintext
trim database {dbName} [max_speed {speedValue}] // 单位: MB/s
```

-  max_speed 是可选参数, 不指定则不限速.
- 已经执行中的命令, 无法改变 max_speed 参数.
- 多次执行 trim database 操作时, 如果该 database 对应的 vnode 中已经有排队或者执行中的操作, 则不会生效, 即同一时刻, 一个 vnode 只能有一个 trim database 任务, 主要是防止异常情况下输入大量命令.  多次输入时, 客户端均会返回 OK, 在 taosd 日志中能看到 Vnode task already exist 的提示.

## 4. 影响范围

- 社区版本不支持多级存储, 因此, 不适用于流控场景.
- 企业版本支持多级存储, 在进行多级存储迁移时, 适用于流控场景. 具体限制如下:
```bash {wrap}
1) 在迁移期间, dnode 内部与该 database 相关的 vgroup (包括 leader/follower), 总的迁移速度不超过 max_speed MB/s (N.B. 该限速为一段时间内的近似值, 在每一秒时, 可能会有一定的波动).
```


## 5. 使用说明

- 如果集群中包含多个 database, 建议运维人员要将多个 database 的 trim database 操作分时间段进行.
- 用户在实际使用中以什么为依据来设置合理的 max_speed？
```plaintext {wrap}
1) 用户可以根据"磁盘性能/系统业务的负载" 情况评估 max_speed 的值.
2) 大多数情况下, 如果系统负载不高,一般不需要进行设置. 
3) 另外, 不建议 max_speed 设置的太低, 否则, 迁移任务耗时太久会影响 merge task, 进而影响 commit task.(任务及互斥关系的描述, 参照: https://taosdata.feishu.cn/wiki/wikcnGKPLVoRwEhZzkbqftWDcSc)
```


## 6. 限制

- 暂不支持取消 trim database 操作
- 暂不支持修改执行中的命令的 max_speed
- 暂不支持 trim database 操作可观测

## 7. 后期优化

- trim database 操作可观测.
- 支持取消执行及排队中的 trim database 操作.
- 完善客户端的响应, 例如, 执行方式中提到的, 多次输入 trim database 命令.
- 支持更加灵活的支持限速操作, 例如, 对当前正在执行的操作 max_speed 可调整, 对 dnode 中多个 DB 的速度进行整体限制.
- 支持指定操作的时间范围.
