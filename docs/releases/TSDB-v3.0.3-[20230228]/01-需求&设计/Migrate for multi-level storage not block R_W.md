# Migrate for multi-level storage not block R/W

## 1. JIRA

- [TD-18581](https://jira.taosdata.com:18080/browse/TD-18581)

## 2. 需求背景

- 企业版本支持多级存储, 迁移 (migrate) 操作目前是在写线程执行的, 因此会阻塞写入(读取已经是异步操作).
- 社区版不支持多级存储, 但是过期数据删除功能是在 trim database 命令统一完成的, 要能够正确执行.

## 3. 实现方案

- 第一步: 将迁移操作, 从写线程迁至 commit thread 执行(因此, commit thread 默认值要加大, 也可以通过numOfCommitThreads 参数指定线程数量)
- 第二步: 第一步完成后, 因为 migrate 和 commit  均会对 tsdbFS 造成修改, 所以是互斥的. 因此, 需要进一步将对 tsdbFS 造成修改的任务拆分为: commit/compact/merge/migrate task, 以实现 migrate 和 commit 可以并行执行.

## 4. 命令

- 在 taos 手工执行:
```bash
trim database {dbName} maxSpeed {speedValue};
```

- 命令执行是异步操作, 命令执行结束即返回. 暂不支持取消 trim 操作.
- 多次执行时, 如果有正在排队的 migrate 任务, 则不会重复排队, 即同一时刻, 一个 vnode 内部只会有一个 migrate 任务在排队. 

## 5. 效果及限制

-  migrate 和 commit 可以并行执行.
- 任务按优先级为: merge > commit > (compact|migrate). 因此, 如果数据写入量非常大, 落盘非常频繁,  compact/migrate 预期的执行时间会比较长.
- 目前, migrate 操作在 show vgroups 中是不可见的, 只能在日志中看到执行结果信息.
- 下列情况, 会阻塞写入(落盘):
```plaintext {wrap}
 1) 当要落盘的文件组 nSttF >= sttTrigger 时, 会由 commit task 发起 merge task 合并 stt 文件. 
 2) merge task 与 migrate/compact 是互斥的. 如果有一个很大的数据文件正在被 migrate 或者 compact, 则 merge task 会被阻塞. 
 3) 如果落盘时, nSttF 已经达到 TSDB_MAX_STT_TRIGGER, merge task 还被阻塞无法完成 stt 文件的合并, 则此时, 落盘会被阻塞.
```

- 

## 6. 后期优化

- migrate 的操作是可观测的, 例如, 进行状态和进度等.
- 支持取消未执行的 migrate 操作.
- 优化调度策略, 防止持续大流量的写入场景下, 造成 migrate 任务一直无法完成.
