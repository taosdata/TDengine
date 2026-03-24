# TD-22892 流计算-pause/resume 测试报告

## 一、测试概述

相关链接：
[流式计算新功能——pause / resume](https://taosdata.feishu.cn/docx/BlKGdddeLo9Iyhxxo8kcuER7nme)[ ](https://jira.taosdata.com:18080/browse/TD-21966)
[TD-22892](https://jira.taosdata.com:18080/browse/TD-22892) [TD-21453](https://jira.taosdata.com:18080/browse/TD-21453) [TDengine-Test-202300331: pause/resume stream](https://jira.taosdata.com:18080/browse/TD-22892)

### 二. 软硬件环境

### **2.1 硬件环境**

| **硬件环境** | **IP** | 用途 | CPU | **内存** | **硬盘** |
| --- | --- | --- | --- | --- | --- |
| **服务端** | 192.168.1.60 | taosd、taostest | Intel(R) Xeon(R) CPU E5-2650 v3 @ 2.30GHz 40核 | 251G | (SSD)PERC H730 Mini 446G*2 |

### **2.2 软件环境**

| **软件环境(3.0分支)** | **IP** | **运行目录** | **脚本及配置** | **commitID** |
| --- | --- | --- | --- | --- |
| **TDengine** | 192.168.1.60 | /root/TDengine | 默认 | 3b6d196ec3761c2f5fc7fac02a0e7598e2eb5fb4 |

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

### 3.3 覆盖范围

该功能应是一个全覆盖的功能，不再根据具体模块细分测试；

### 3.4 测试用例

> ⚠ 嵌入思维笔记，需在飞书中查看 (token: BZ8ZbDMgMmfpitnywF3cNlAtnhc)

## 四、测试问题点

| JIRA编号 | 描述 | 状态 |
| --- | --- | --- |
| [TD-24167](https://jira.taosdata.com:18080/browse/TD-24167) | [resume stream后流数据未增长](https://jira.taosdata.com:18080/browse/TD-24167) | Done |
| [TD-24325](https://jira.taosdata.com:18080/browse/TD-24325) | ["resume stream ignore untreated" crash](https://jira.taosdata.com:18080/browse/TD-24325) | Done |
| [TD-24381](https://jira.taosdata.com:18080/browse/TD-24381) | [pause resume stream后有时结果不对](https://jira.taosdata.com:18080/browse/TD-24381) | Done |
| [TD-24446](https://jira.taosdata.com:18080/browse/TD-24446) | [pause/resume大数据量测试，最终结果不对](https://jira.taosdata.com:18080/browse/TD-24446) | Done |
| [TD-24447](https://jira.taosdata.com:18080/browse/TD-24447) | [频繁pause/resume 报错 "failed to put into queue:****Queue out of memoryld"](https://jira.taosdata.com:18080/browse/TD-24447) | 修复中 |
|  |  |  |

## 五、测试结论
