# 限制QUEUE的积压的大小，并在入口处进行控制， 用以限制多副本恢复时候占用的总体内存大小 Test Spec

## 1. 测试目标

验证，解决在3节点3副本环境，巨大数据量在节点重启同步数据过程中内存占用过高的问题
TD-31403

本次修复不会对造成regressino问题
TD-30736

## 2. 变更历史

| Date | Version | Owner | Memo |
| --- | --- | --- | --- |
| 2024.8.14 | 1.0.0 | 翟坤 | 创建初版测试用例 |
| 2024.8.23 | 2.0.0 | 翟坤 | 补充测试报告 |

## 3. 测试结论

测试通过
- 通过将最新3.0、3.1和main分支与v3.3.2.7版本相同场景验证，修复后内存使用量从修复前的134G降低到最高55G，达到了内存控制的预期目标
- 通过修改rpcQueueMemoryAllowed的值可控制数据同步场景内存占用的大小，rpcQueueMemoryAllowed越小，占用内存的总量就越少
- 回归测试通过
  TD-30736

## 4. 已知问题和限制

无 

## 5. 测试资源及环境

| 节点/FQDN | 角色 | 地址 | 单机系统资源 | 备注 |
| --- | --- | --- | --- | --- |
| u1-43 | TDengine 服务端-master节点 | 192.168.1.43 | 集群firstEp |
| u1-58 | TDengine 服务端-master节点 | 192.168.1.58 | 集群secondEp |
| u1-61 | TDengine 服务端-master节点 | 192.168.1.61 | 集群slave节点 |

## 6. 测试用例

### 6.1 json文件

<view type="2">

  > ⚠ 嵌入文件，需在飞书中查看 (token: ZaXcbdcigoPkowxsScUcakeJnkc)

</view>

<view type="2">

  > ⚠ 嵌入文件，需在飞书中查看 (token: Ln4UbYUy1oJeQqxIRU0ch9p7nAf)

</view>


### 6.2 测试数据

| 测试场景 | 测试步骤 | 测试版本 | rpcQueueMemoryAllowed | u1-61的内存消耗（M） | 节点恢复完成时间（s） | 预期结果 | 测试结果 | 备注 |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 3副本、32vgroup，超级表用meter表结构 | 1. 关闭u-61节点的dnode服务
1. 写入32亿数据
2. 重新启动dnode服务
3. 记录重启节点的内存消耗和同步完成时间 | v3.3.2.7 | default（物理内存的10%，约25G） | (stmt)120G-6.33G=113.67G
(stmt)143G-7.16G=134G(blade) | 2024.8.15 (stmt)16:32-16:42=10min
(stmt)17:17-17:29=12min | 内存占用量非常大，但测试环境中内存比较多，应该不会出现oom |  | 复现问题场景 |
|  |  | 最新3.0分支 | default（物理内存的10%，约25G） | (stmt)56.3G-4.81G=51.49G
(taosc)59.67G-4G=55.67G | 2024.8.15
(stmt)19:02-19:12=10min
2024.8.22
(taosc)20.:14-20:24=10min | 1. 内存消耗较修复前有大幅下降
1. 数据同步时间与修复前比有所增加，但具体耗时需看具体结果 | Pass | 内存持续增长到最大值51G后，不会继续增长 |
|  |  |  | 1024*1024*1000 | (stmt)21.29G-4.86G=16.43G
(taosc)20.18G-4.66G=15.52G | 2024.8.15
(stmt)20:15-20:36=21min
2024.8.22
(taosc)10:47-23:07=20min |  | Pass | 1.rpcQueueMemoryAllowed的最小值100M，写入会报错，修改为1G
2.内存从4.86G升到9.3G，需要等待很久才能恢复到4.8G |
|  |  | 最新3.1分支 | default（物理内存的10%，约25G） | (stmt)38G-4.75G=33.25G | 2024.8.16
(stmt)16:02-16.19=17min |  | Pass | 最高占用内存33G，但很快内存下降到10G，最后16:26恢复到4G |
|  |  |  | 1024*1024*1000 | (stmt)11.34G-3.9G=7.44G | 2024.8.20
(stmt)9:07-9:23=16min |  | Pass | rpcQueueMemoryAllowed的最小值100M，写入会报错，修改为1G |
|  |  | 最新main分支 | default（物理内存的10%，约25G） | (stmt)57G-4G=53G | 2024.8.20
(stmt)9:00-9:30=10min |  | Pass | [节点重启同步数据过程中出现core](https://jira.taosdata.com:18080/browse/TD-31576)（已解决） |
|  |  |  | 1024*1024*1000 | (taosc)25.55G-4G=21.55G | (taosc)10:57-11:17=20min |  | Pass | rpcQueueMemoryAllowed的最小值100M，写入会报错，修改为1G |
|  | 验证jira：[https://jira.taosdata.com:18080/browse/TD-30736](https://jira.taosdata.com:18080/browse/TD-30736)
1. 配置多级存储，单副本写入15亿行数据
2. 修改replica为2
3. 修改replica为3

【多级存储配置】
43:
datadir /data1/taos_data 0 1 0
datadir /data3/taos_data 0 0 0
datadir /data4/taos_data 0 0 1

58:
datadir /data1/taos_data 0 1 0
datadir /data3/taos_data 1 0 0
datadir /data4/taos_data 1 0 1

61:
datadir /data1/taos_data 0 1 0
datadir /data3/taos_data 1 0 0
datadir /data4/taos_data 2 0 1 | 最新3.0分支 | default（物理内存的10%，约25G） | replica 1 to replica 2: 约使用0.5G内存
replica 1 to replica 3: 约使用0.5G内存 | replica 1 to replica 2: 10min
replica 1 to replica 3: 16min | 1.副本分裂数据同步成功完成
2.在副本分裂数据同步过程中不会出现Out of memory in rpc queue
3.内存消耗在合理范围内
4.db工作正常，数据同步可以完成 | Pass |  |
|  |  | 最新3.1分支 | default（物理内存的10%，约25G） | replica 1 to replica 3: 最高的一个节点内存从3.91G升到5.21G | replica 1 to replica 3:10min |  | Pass | 3.1分支不支持2副本，只验证1到3副本 |
|  |  | 最新main分支 | default（物理内存的10%，约25G） | replica 1 to replica 2: 内存无明显上升
replica 1 to replica 3: 内存无明显上升 | replica 1 to replica 2: 12min
replica 1 to replica 3: 14min |  | Pass |  |

## 7. 相关文档

TD-31403
