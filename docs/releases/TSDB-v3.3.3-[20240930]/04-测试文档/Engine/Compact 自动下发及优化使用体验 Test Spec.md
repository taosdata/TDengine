# Compact 自动下发及优化使用体验 Test Spec

## 1. 测试目标

1. 
  TS-4994

1. 
  TD-30555

1. [TDengine 可运维观测需求](https://taosdata.feishu.cn/wiki/OrX7woLVbiGy0ekld25c141VnPf) 第 8、9、10、11 项
2. [优化 compact 使用体验](https://taosdata.feishu.cn/wiki/Q5UjwfJoeizl2gkS3iQcAic1nAd)

## 2. 变更历史

| Date | Version | Owner | Memo |
| --- | --- | --- | --- |
| 2024-08-22 | 0.1 | @贾靖斌 | New |
|  |  |  |  |

## 3. 测试范围

- 验证自动 Compact 相关参数
  - COMPACT_INTERVAL 参数测试（create/alter）
  - COMPACT_TIME_RANGE 参数测试（create/alter）
  - COMPACT_TIME_OFFSET 参数测试（create/alter）
- 验证指定 Vnode 做 Compact
  - 单/多个 vnode
  - start_opt
  - end_opt
- 验证 VNODE 中文件组状态及相关信息
  - dnode_id 正确性校验
  - db_name 正确性校验
  - vgroup_id 正确性校验
  - fset_id 正确性校验
  - start_time 正确性校验
  - end_time 正确性校验
  - last_compact_time 正确性校验
  - compact_advice 正确性校验
  - details[含 deepscan] 正确性校验
- COMPACT 任务并发数参数测试
  - maxCompactConcurrency 参数测试（taos.cfg/alter）
  - maxCompactConcurrency, numOfCommitThreads 逻辑测试
- Compact 完成百分比及预估完成时间测试
  - Progress 正确性校验（预期不太准）
  - Remaining time 正确性校验（预期不太准）
- 历史功能回归
- 性能
  - 阻塞写入
  - 资源占用
- 稳定性
  - 无 crash/OOM/卡死等问题

## 4. 测试结论

1. 

## 5. 测试数据

1. **schema：**

|  | **type** | **count** |
| --- | --- | --- |
| **tag** | int | 1 |
| int | 2 |
| bigint | 1 |

## 6. 已知问题和限制

## 7. 测试环境

- OS：Ubuntu 20.04.2 LTS
- Env：

| **硬件环境** | **IP** | 用途 | **CPU** | **内存** | **硬盘** |
| --- | --- | --- | --- | --- | --- |
| 192.168.1.53 | taosBenchmark |
| 192.168.1.55 | taosd |
| 192.168.1.56 | taosd |
| 192.168.1.57 | taosd |

```shell
软件版本：

```


## 8. 测试用例

**测试脚本：**

### 8.1 功能


| **序号** | **测试项** | **测试点** | **测试步骤** | **期望结果** | **实际结果** |
| --- | --- | --- | --- | --- | --- |
| 1 | 功能 | COMPACT_INTERVAL 参数逻辑测试 | 1. COMPACT_INTERVAL '10m'建库并写入一定量数据（含乱序更新删除）； 1. 持续 show compacts 查询； | 应每隔 10m 自动进行一次 compact，如存在未完成的 compact 任务，不会重复下发 |  |
| 2 |  | COMPACT_INTERVAL 时间单位测试 | 1. COMPACT_INTERVAL 参数分别配置分钟、小时、天建库并写入一定量数据数据（含乱序更新删除）； 1. 持续 show compacts 查询并日志记录； 1. Taosd 日志查询； | 每隔 COMPACT_INTERVAL，在不存在未完成任务的情况下，compact 应正常下发，taosd日志中也可以查看到指定时间的 compact |  |
| 3 |  | COMPACT_TIME_RANGE 参数逻辑测试 | 1. COMPACT_INTERVAL '10m' COMPACT_TIME_RANGE '-60d,-30d' 建库，控制 duration 和写入时间范围，写入数据在 30 天以内或 60 天以上的文件组； 1. 持续 show compacts 查询； | Compact 可以下发但很快就结束~~？compact 根本不会下发？~~ |  |
| 4 |  | COMPACT_TIME_RANGE 时间单位测试 | 1. COMPACT_TIME_RANGE 参数分别配置分钟、小时、天建库并写入一定量数据（含乱序更新删除）； 1. 持续 show compacts 查询并日志记录； 1. Taosd 日志查询； | 在 compact 可以正常下发的情况下验证 COMPACT_TIME_RANGE 的每种时间单位均可生效 |  |
| 5 |  | COMPACT_TIME_OFFSET 参数逻辑测试 | 1. COMPACT_INTERVAL '10m' COMPACT_TIME_OFFSET '5h' 建库 db1，控制 duration 和写入时间范围，写入数据在 30 天以内或 60 天以上的文件组； 1. 持续 show compacts 查询； 1. COMPACT_INTERVAL '10m' COMPACT_TIME_RANGE '-60d,-30d' COMPACT_TIME_OFFSET '5h' 建库 db2； 1. 持续 show compacts 查询； | 1. 每次 Compact 应偏移 5h 开始 1. COMPACT_TIME_OFFSET对COMPACT_TIME_RANGE应不生效 |  |
| 6 |  | COMPACT_INTERVAL 默认值测试 | 不加任何参数建库 db1，COMPACT_TIME_RANGE '-60d,-30d' COMPACT_TIME_OFFSET '5h' 建库 db2，写入一定量数据（含乱序更新删除）； | 不会开始自动 compact |  |
| 7 |  | COMPACT_TIME_RANGE 默认值测试 | 1. COMPACT_INTERVAL '10m'建库并写入一定量数据（含乱序更新删除）； 1. 持续 show compacts 查询； | Compact 的文件组位于 [-keep2, -duration] 之间 |  |
| 8 |  | COMPACT_TIME_OFFSET 默认值测试 | 1. COMPACT_INTERVAL '10m'建库并写入一定量数据（含乱序更新删除）； 1. 持续 show compacts 查询； | 不会偏移 |  |
| 9 |  | COMPACT_INTERVAL 参数边界（[10m, keep2]）测试 | 1. keep2 配置为 10d COMPACT_INTERVAL 分别配置为 -1s 和 10d+1m 建库； | 应报错越界（负数报语法错误） | Pass |
| 10 |  | COMPACT_TIME_RANGE 参数边界（[-keep2, -duration]）测试 | 1. keep2 配置为 10d COMPACT_TIME_RANGE 配置为 1d，COMPACT_TIME_RANGE 分别配置为 -11d-1m、-1d+1m、0d 建库； | 应报错越界 | Pass |
| 11 |  | COMPACT_TIME_OFFSET 参数边界（[0,23]）测试 | 1. COMPACT_TIME_OFFSET 分别配置为 -1 和 24 建库； | 应报错越界（负数报语法错误） | Pass |
| 12 |  | 参数查询测试 | show create database db\G; | 应可以完整打印出所使用的compact 参数 |  |
| 13 |  | kill compact 测试 | 1. COMPACT_INTERVAL '10m' 建库并写入一定量数据（含乱序更新删除）； 1. 某次 compact 进行过程中 kill compact； 1. 持续 show compacts 查询； | 3. 自动开始的 compact 可以被 kill； 1. 被 kill 的 compact 不会影响后续的自动 compact |  |
| 14 |  | Compact 参数 alter 测试 | 1. COMPACT_INTERVAL '10m' COMPACT_TIME_RANGE '-60m,-30m' COMPACT_TIME_OFFSET '5m' 建库 db1 并控制 duration 和写入时间范围，写入数据在 30 天以内或 60 天-90天的文件组；； 1. ALTER DATABASE db1 COMPACT_INTERVAL '20m'; 1. 持续 show compacts 查询； 1. COMPACT_INTERVAL '10m' COMPACT_TIME_RANGE '-60d,-30d' COMPACT_TIME_OFFSET '5m' 建库 db2 并控制 duration 和写入时间范围，写入数据在 30 天以内或 60 天-90天的文件组；； 1. ALTER DATABASE db2 COMPACT_TIME_RANGE '-90d,-30d'; 1. 持续 show compacts 查询； 1. COMPACT_INTERVAL '10m' COMPACT_TIME_RANGE '-60d,-30d' COMPACT_TIME_OFFSET '5m' 建库 db3 并控制 duration 和写入时间范围，写入数据在 30 天以内或 60 天-90天的文件组；； 1. ALTER DATABASE db3 COMPACT_TIME_OFFSET '3m'; 1. 持续 show compacts 查询； 1. COMPACT_INTERVAL '10m' COMPACT_TIME_RANGE '-60d,-30d' COMPACT_TIME_OFFSET '5m' 建库 db4 并控制 duration 和写入时间范围，写入数据在 30 天以内或 60 天-90天的文件组；； 1. ALTER DATABASE db4 COMPACT_INTERVAL '5m' COMPACT_TIME_RANGE '-90d,-30d' COMPACT_TIME_OFFSET '3m'; 1. 持续 show compacts 查询； | 4. db1 每隔 5m compact 一次 7. Alter 后 60d-90d 的文件组可以被自动 compact 10. Alter 后每次 Compact 应偏移 3m 开始 13. 多个参数同时修改时可以生效 |  |
| 15 |  | compact 单个 vnode 测试 | 1. 新建 3 节点 1 副本； 1. 写入 20 亿数据（含乱序更新删除）； 1. 查看 vg 对应的 dnode，比如 vg2 在 dnode1 上； 1. COMPACT VNODES in (2)； 1. 查看 dnode1/2/3的资源占用； | 5. 应只有 dnode1 的 cpu/diskio 资源占用明显增长 | 通过 |
| 16 |  | compact 多个 vnode 测试 | 1. 新建 3 节点 1 副本； 1. 写入 20 亿数据（含乱序更新删除）； 1. 查看 vg 对应的 dnode，比如 vg2 在 dnode1 上，vg3 在 dnode2 上； 1. COMPACT VNODES in (2,3)； 1. 查看 dnode1/2/3的资源占用； | 5. dnode1、dnode2 的 cpu/diskio 资源占用明显增长 | 通过 |
| 17 |  | compact vnode + 时间区间测试 | 1. 控制 duration 1d，写入时间范围分别控制在 -2d~-1d、-5d~-3d，每天的数据量和分布大致相同； 1. Compact vnodes in [vgid1...] start with 1d+; 1. Compact vnodes in [vgid1...] end with 6d-; 1. Compact vnodes in [vgid1...] start with 2d- end with 1d-; 1. Compact vnodes in [vgid1...] start with -5d end with 3d-; | 2、3 compact 无效或很快结束 4、5 均能开始 compact，但 5 比 4 compact时间长了一倍左右 | 通过 |
| 18 |  | Compact 不在同一 db 的多个 vgroup | 1. 新建两个 db，vgroup参数设置为2，分别写入少量数据； 1. Compact 不同 db 的 vg； | 2. 报错 | 通过 |
| 19 |  | Compact 不存在的vgroup | 1. 新建db，vgroup参数设置为2，写入少量数据； 1. Compact 不存在的 vg； | 2. 报错 | 通过 |
| 20 |  | Vnode 文件组信息（dnode_id）校验 | 1. db1 写入一定量数据 1. select * from information_schema.ins_fsets [deep_scan]; | dnode_id：对应 dnode 的数据目录可以找到和 fset_id 对应的文件； | 通过 |
| 21 |  | Vnode 文件组信息（db_name）校验 | 1. db1 写入一定量数据； 1. select * from information_schema.ins_fsets [deep_scan]; | db_name：和 create db 语句对应； | 通过 |
| 22 |  | Vnode 文件组信息（vgroup_id）校验 | 1. db1 写入一定量数据； 1. select * from information_schema.ins_fsets [deep_scan]; | vgroup_id：和对应 db_name 的 show vgroups 信息对应； | 通过 |
| 23 |  | Vnode 文件组信息（fset_id）校验 | 1. db1 写入一定量数据； 1. select * from information_schema.ins_fsets [deep_scan]; | fset_id：对应 dnode 的数据目录可以找到和 fset_id 对应的文件； | 通过 |
| 24 |  | Vnode 文件组信息（start_time）校验 | 1. db1 写入一定量数据； 1. select * from information_schema.ins_fsets [deep_scan]; | start_time：和写入文件组的最小时间对应； | 通过 |
| 25 |  | Vnode 文件组信息（end_time）校验 | 1. db1 写入一定量数据； 1. select * from information_schema.ins_fsets [deep_scan]; | end_time：和写入文件组的最大时间对应； | 通过 |
| 26 |  | Vnode 文件组信息（last_compact_time）校验 | 1. db1 写入一定量非乱序数据，db3 写入一定量（30%+）乱序数据； 1. compact db1 一次 db3 两次； 1. select * from information_schema.ins_fsets [deep_scan]; | last_compact_time：和最后一次 compact 时间对应； | 通过 |
| 27 |  | Vnode 文件组信息（compact_advice）校验 | 1. db1 写入一定量非乱序数据，db2 写入一定量（20%-）乱序数据，db3 写入一定量（30%+）乱序数据，db4 控制写入数据分布在多个 stt，db5 写入非乱序数据并删除一些数据； 1. compact db1 一次 db2 两次； 1. select * from information_schema.ins_fsets [deep_scan]; | compact_advice：db1、db2 显示 NO，db2、db3、db4 显示 YES | 通过 |
| ~~28~~ |  | ~~Vnode 文件组信息（details）校验~~ | 1. ~~db1 写入一定量非乱序数据，db3 写入一定量（30%+）乱序数据；~~ 1. ~~select * from information_schema.ins_fsets [deep_scan];~~ 1. ~~compact db1 一次 db3 两次；~~ 1. ~~select * from information_schema.ins_fsets [deep_scan];~~ | 2. ~~details：对应 dnode 的数据目录可以找到对应数据文件，file_size/data_size 可以匹配~~ 4. ~~details 可以看到 compact 后的变化~~ | 删除 |
| ~~29~~ |  | ~~Vnode 文件组信息（deep scan）校验~~ | 1. ~~db1 写入一定量数据；~~ 1. ~~select * from information_schema.ins_fsets deep_scan;~~ | 1. ~~total rows：配置deep scan 可以显示，并结果正确~~ 1. ~~real rows：配置deep scan 可以显示，并结果正确~~ 1. ~~real data size：配置deep scan 可以显示，并结果正确~~ 1. ~~total blocks：配置deep scan 可以显示，并结果正确~~ 1. ~~rows P99：配置deep scan 可以显示，并结果正确~~ 1. ~~rows P95：配置deep scan 可以显示，并结果正确~~ 1. ~~rows P90：配置deep scan 可以显示，并结果正确~~ | 删除 |
| 30 |  | COMPACT 任务并发数测试（taos.cfg） | 1. maxCompactConcurrency 配置为 -1/17 启动taosd； 1. maxCompactConcurrency 配置为 0 numOfCommitThreads配置为 17 启动 taosd； 1. maxCompactConcurrency 配置为 16 numOfCommitThreads配置为 4 启动 taosd； 1. maxCompactConcurrency 配置为 16 numOfCommitThreads配置为 24 启动 taosd； 1. maxCompactConcurrency 和 numOfCommitThreads 均不配置使用默认值启动 taosd； 1. 写入 20 亿数据（含乱序更新删除）；（按上述步骤重复执行） 1. compact database db；（按上述步骤重复执行） 1. top -Hp `pidof taosd` 不断查看并发数； | 1. 参数越界应报错； 1. 最大并发数应有 17 个； 1. 最大并发数应有 4 个； 1. 最大并发数应有 16 个； 1. 最大并发数应有 2 个； | 通过 |
| 31 |  | COMPACT 任务并发数测试（alter） | 1. maxCompactConcurrency 和 numOfCommitThreads 均不配置使用默认值启动 taosd； 1. 写入 20 亿数据（含乱序更新删除）； 1. alter dnode `id` maxCompactConcurrency 8； 1. compact database db； 1. top -Hp `pidof taosd` 不断查看并发数； | 5. 最大并发数应有 4 个，和 numOfCommitThreads 默认值相同； | 通过 |
| 32 |  | show compacts 文件大小百分比测试 | 1. 控制 duration 1d，写入时间范围分别控制在 -2d~-1d、-5d~-3d，每天的数据量和分布大致相同； 1. Compact database db1 start with -2d end with -1d; 1. Compact database db1 start with -5d end with -3d; | 2 的文件进度百分比应比 3 快一倍 | 可以显示，但是异步调用，进度比较不准确 |
| 33 |  | show compacts 预估完成时间测试 | 1. 控制 duration 1d，写入时间范围分别控制在 -2d~-1d、-5d~-3d，每天的数据量和分布大致相同； 1. Compact database db1 start with -2d end with -1d; 1. Compact database db1 start with -5d end with -3d; | 2 的预估完成时间应比 3 快一倍 | 同 32 |
| 34 | 性能 | 自动 compact 时写入阻塞情况 | 1. 构建自动 compact 和写入文件组有交集的场景，测试写入阻塞情况； | 自动 Compact 撞到正在写入的文件组时，可能会阻塞写入 |  |
| 35 |  | 自动 compact 资源占用情况 | 1. 持续写入，测试自动 compact 资源占用情况； | 自动 Compact cpu/内存/io 等资源占用符合预期 |  |
| 36 | 稳定性 | 读写同步进行，多次自动 compact，长期运行测试稳定性 | 读写同步进行，覆盖 compact 的文件组，多次自动 compact，测试长稳情况 | 读写同步，自动 Compact 长期执行，无 crash/OOM/卡死 等现象 |  |
|  |  |  |  |  |  |
|  |  |  |  |  |  |
|  |  |  |  |  |  |


### 8.2 性能

分别在阻塞/不阻塞写入的情况下，验证 compact 性能情况：

|  |
|  |
| Compact 前 | Compact 后 | Compact 前 | Compact 后 | CPU | MEM | DISKIO |
| 阻塞写入 |  |  |  |  |  |  |  |  |
|  |  |  |  |  |  |  |  |  |
| 非阻塞写入 |  |  |  |  |  |  |  |  |
|  |  |  |  |  |  |  |  |  |

### 8.3 稳定性

百万子表、百亿数据规模以上长时间进行自动 compact，且 compact 过程中混合读写、流、订阅、dnode 重启等操作，无 crash/OOM/卡死等现象。

| **副本数** | **CPU 资源图** | **内存资源图** |
| --- | --- | --- |
| 1 |  |  |
| 3 |  |  |

## 9. Jira

| **Jira** | **描述** | **状态** | **备注** |
| --- | --- | --- | --- |
|  |  |  |  |
|  |  |  |  |
|  |  |  |  |
|  |  |  |  |
