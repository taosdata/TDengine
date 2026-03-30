# TD-22467 创建流计算增加option，不做update data的检查”功能的测试验证

## 一、测试概述

指定 IGNORE UPDATE 1，则不会检查数据是否是 update，对于所有数据都做增量运算，如果update数据，不会触发扫盘
相关链接：
[Stream Processing](https://taosdata.feishu.cn/wiki/wikcnnK7Gs2bWGx7tjOt1i5dcne) 
[TD-22467](https://jira.taosdata.com:18080/browse/TD-22467) [完成 “TD-22268 创建流计算增加option，不做update data的检查”功能的测试验证](https://jira.taosdata.com:18080/browse/TD-22467)

## 二. 软硬件环境

### **2.1 硬件环境**

| **硬件环境** | **IP** | 用途 | CPU | **内存** | **硬盘** |
| --- | --- | --- | --- | --- | --- |
| **服务端** | 192.168.1.60 | taosd、taostest | Intel(R) Xeon(R) CPU E5-2650 v3 @ 2.30GHz 40核 | 251G | (SSD)PERC H730 Mini 446G*2 |

### **2.2 软件环境**

| **软件环境(3.0分支)** | **IP** | **运行目录** | **脚本及配置** | **commitID** |
| --- | --- | --- | --- | --- |
| **TDengine** | 192.168.1.60 | /root/TDengine | 默认 | 05c7bc170ef290f96213892a9f6f1229987a1a5f |

## 三、测试方案

### 3.1 测试工具

| **测试工具** | **描述** | **脚本/配置文件** |
| --- | --- | --- |
| **taostest** | **测试主程序，部署测试环境，建库、建表、建流、写入、查询、确认结果等** |  |

### 3.2 写入schema

| **Type（全类型）** | **TINYINT、SMALLINT、INT、BIGINT、UTINYINT、USMALLINT、UINT、UBIGINT、FLOAT、DOUBLE、VARCHAR（256）、NCHAR（256）、BOOL** |
| --- | --- |
| **tag_count** | **各1列** |
| **column_count** | **各1列** |

### 3.3 测试用例

该功能是一个通用功能，不再根据具体模块细分测试；
测试步骤：
1. 准备数据，建库建表；
2. 建流时指定  IGNORE UPDATE 1;
3. 写入一批数据，触发流计算，查询并记录流结果为 res1；
4. 对步骤 3 中的部分可触发流计算的时间线进行 update 操作，再次查询流结果记录为 res2，批结果为 res3；
5. 校验结果： res2中，触发流的窗口数据条数应比 res1 的数量 +1，res3 应和 res2 不相等；
6. 清空数据，分别测试 IGNORE UPDATE 0 和不指定 IGNORE UPDATE 的情况，逻辑和以前一致，这里不再重复测试；

## 四、测试结论

测试完成，无遗留问题
