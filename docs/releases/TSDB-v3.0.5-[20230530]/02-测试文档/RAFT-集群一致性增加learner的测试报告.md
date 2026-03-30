# RAFT-集群一致性增加learner的测试报告

## 一、测试概述

相关链接：
[RAFT Learner功能测试目标](https://taosdata.feishu.cn/wiki/wikcnZevRU8ppHpFkYMg7yJYb5g) 
在多副本添加节点时，增加 Learner 的状态

### 二. 软硬件环境

### **2.1 硬件环境**

| **硬件环境** | **IP** | 用途 | CPU | **内存** | **硬盘** |
| --- | --- | --- | --- | --- | --- |
| **服务端** | 192.168.1.86 |  | Intel(R) Core(TM) i7-10700 CPU @ 2.90GHz 16 核 | 64G | (SSD)PERC H730 Mini 446G*2 |

### **2.2 软件环境**

| **软件环境(3.0分支)** | **IP** | **运行目录** | **脚本及配置** | **commitID** |
| --- | --- | --- | --- | --- |
| **TDengine** | 192.168.1.86 | /home/chr/TDengine | 默认 |  |

## 三、测试方案

### 3.1 测试工具

| **测试工具** | **描述** | **脚本/配置文件** |
| --- | --- | --- |
|  | **测试主程序，部署测试环境，建库、建表、写入、查询、确认结果等** |  |

### 3.2 写入schema

| **Type（全类型）** | **TINYINT、SMALLINT、INT、BIGINT、UTINYINT、USMALLINT、UINT、UBIGINT、FLOAT、DOUBLE、VARCHAR（256）、NCHAR（256）、BOOL** |
| --- | --- |
| **tag_count** | **各1列** |
| **column_count** | **各1列** |

### 3.3 覆盖范围

该功能应是一个全覆盖的功能，不再根据具体模块细分测试；

### 3.4 测试用例

> ⚠ 嵌入思维笔记，需在飞书中查看 (token: VRjxbecNcmS9Zgnviy0cV9X4nHb)

#### Mnode测试用例

持续建库建表，create mnode，drop mnode 循环三次，最终都能成功创建库和表。

#### Vnode测试用例

 基础环境 6 节点 3mnode
1. 建好库表，配置3replica，持续写入数据，100table*100rows，写入过程，同步执行 `alter database db0_0 replica 1` ，最终写入数据量正确，且 vgroups 显示都是 leader。再 sleep 5s，执行`alter database db0_0 replica 3 `，最后选主成功。（TDengine/tests/system-test/6-cluster/manually-test/6dnode3mnodeInsertLessDataAlterRep3to1to3.py）
这个测试需要检测的时间足够大，才能正确。目前是 选主120s ，vgroups 是 4。不放入 ci
1. 建好库表，配置1replica，持续写入数据，1000 table*100rows，写入过程中，执行 `alter database db0_0 replica 3`，再同步重启各个节点的过程中， 最终能写入数据，但是数据的量不保证，且 选主成功（vgroups 显示都是 一个leader俩个 follower）。测试用例（TDengine/tests/system-test/6-cluster/manually-test/6dnode3mnodeInsertDatarRebootAlterRep1-3.py）
这个测试实际测试下，发现重启过程中，写入数据会失败，所以不检验数据插入数据量，这个测试目的是为了测试重启过程中的选主是否正常。测试需要检测的时间足够大。 放入 ci。

1. 建好库表，配置3replica，写入数据完成后，alter 表的 schema 信息，然后重启节点，重启第三个节点时，`alter database db0_0 replica 1 `，最终修改的数据列正常，且 vgroups 显示都是 leader。6dnode3mnodeInsertDataRebootModifyMetaAlterRep3to1.py
2. 建好库表，配置1replica，写入数据完成后，alter 表的 schema 信息，然后重启节点，重启第三个节点时，`alter database db0_0 replica 3 `，最终修改的数据列正常，，且 vgroups 显示都是 leader。6dnode3mnodeInsertDataRebootModifyMetaAlterRep1to3.py
这 两个是校验 schema 信息变化，副本切换后schema 是否正常，

1. 使用 taosBenchmark 测试，replica=1，10000*10000。全程插入，无限重试，插入数据的过程中，`alter database db0_0 replica 3 `，然后重启节点，跟用例 1 相似。但是这个是插入失败后重试。预期结果是数据插入全部成功，数量符合预期，选举成功（vgroups 显示都是 一个leader俩个 follower）。

## 四、测试问题点

无

## 五、测试结论

5 个测试用例均通过。
目前测试， replica 1---->3 的时候，数据是一个数据库， 一张表一条数据，4 个 vnode，耗时 45s。
跟东明确认：整个过程需要选主，数据复制，重启 vnode 过程。选主时间每次 3s，总共 4 * 3s * 3次。
优化 JIRA:  
TD-24129


20230714更新
目前选主时间 100-130s 左右。待优化
