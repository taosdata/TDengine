# TS-5392 SyncLogBuffer 性能测试报告

验证
TS-5392

### 1. 测试结论

功能测试通过，性能无明显下降

### 2. 开发解决方案

1. 在原有控制 SyncLogBuffer 中缓存的可销毁的消息数量的基础上，增加总大小控制参数： syncLogBufferMemoryAllowed，单位字节，默认值为内存的 1/10，取值范围: [1024*1024*100, INT64_MAX]，支持动态修改；
2. 只控制 vnode，不控制 mnode；
3. 如果某个 vnode 中该类消息总大小不超过 TSDB_SYNC_LOG_BUFFER_THRESHOLD( 5 MB)，则继续执行销毁流程。
4. tsLogBufferMemoryAllowed 不是一个绝对的控制，有可能超出，超出的总量与 vnode 数量和消息体大小有关；但是不会不可控。

### 3. 测试环境

192.168.1.96
CPU(16) Intel(R) Core(TM) i7-10700 CPU @ 2.90GHz
MEM：64G
DISK：1T

#### 3.1 3vgroup，3副本脚本

<view type="2">

  > ⚠ 嵌入文件，需在飞书中查看 (token: ItF9bLBQ8oVDWhxdXKHcV1dNnX1)

</view>

#### 3.2 taosbenchmark配置

<view type="2">

  > ⚠ 嵌入文件，需在飞书中查看 (token: HtwcbB2vQo5KYzxg7M7cmJQenoe)

</view>

#### 3.3 复现版本

3.3.2.7发布版本：
TDengine Enterprise Edition
taosd version: 3.3.2.12 compatible_version: 3.0.0.0
git: 962573eae28c78136cc6ecd5fd9df5ca7e89e476
gitOfInternal: c48f749ff14db90c200f2533baa5d643a1f9a892
build: Linux-x64 2024-09-12 10:52:43 +0800

#### 3.4 测试版本

TDengine Enterprise Edition
taosd version: 3.3.3.0.alpha compatible_version: 3.0.0.0
git: 9956c35403bf6e303565fe92f8c2d1f3e1640997
gitOfInternal: 8088da7dbbab0810626ec4724e3cd325e91d6243
build: Linux-x64 2024-09-18 09:47:16 +0800

### 4. 升级测试

升级后杀掉taosd进程重新拉起后，内存恢复正常。可以正常写入，内存不会无限制增长

### 5. 性能对比测试结果

写入性能无下降

| 版本 | vgroup | 副本数 | 写入线程数 | 单个线程写入平均速率 rows/s （稳定状态所有线程10次数据平均值） | 机器最大内存使用 | entry bytes | Mem used | Mem allowed |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 空环境 |  |  |  |  | 7.1G |  |  |  |
| main 0913 | 3 | 3 | 4 | 49.60 | 35G | 30.21 MB | 6.22 GB | 6.24 GB |
| main 0913 | 3 | 1 | 4 | 154.32 | 27G | 30.21 MB | 5.67GB | 6.24 GB |
| 3.3.2.7 | 3 | 3 | 4 | 写入线程概率崩溃，很快OOM 预估速率 53.14 | OOM | NA | NA | NA |
| 3.3.2.7 | 3 | 1 | 4 | 154.87 | OOM | NA | NA | NA |

### 6. syncLogBufferMemoryAllowed参数验证

配置支持热更新：alter all dnodes "syncLogBufferMemoryAllowed 3000000000";
已经使用的内存不会回收，杀掉进程后重新拉起，查看配置生效，内存最大使用为21G（原为35G）
dnode3/log/taosdlog.0:09/18 20:18:26.350490 00220654 E SYN vgId:10, recycle log entry. index:101, startIndex:101, until:-121, commitIndex:135, endIndex:136, term:1, entry bytes:31682618, buf bytes:823748644, **used:2978257485, allowed:3000000000**

### 7. 异常场景测试

| 场景 | 测试结果 | 备注 |
| --- | --- | --- |
| syncLogBufferMemoryAllowed配置100M，构造消息体为100M+ | 测试通过，Memory used会大于100M，属于正常情况 | - **entry**: 143.0 MB - **used**: 188.4 MB - **allowed**: 100.0 MB |
| 写入大消息时副本数1切换3，强制重启dnode1 | 测试通过，环境可以正常恢复并切换成功 | 重启及副本切换时写入线程概率性崩溃，有写入错误 |
| 3副本集群，停止写入后，同时重启所有dnode， 恢复后可以正常写入 | 测试通过 |  |
| 构造超大消息体（大于250M） | 测试未通过，出现coredump | [TD-32122](https://jira.taosdata.com:18080/browse/TD-32122) |


### 8. 发现问题&Todo

TD-32122

<task task-id="0bfdd2be-d639-4c13-9268-9f90ad51279f"/>


### 9. 测试自动化

需要构造大消息体测试，对机器有一定性能要求，暂无实现，可以考虑后续接入性能自动化
针对配置的自动化：
https://github.com/taosdata/TDengine/pull/27964，已合入CI
