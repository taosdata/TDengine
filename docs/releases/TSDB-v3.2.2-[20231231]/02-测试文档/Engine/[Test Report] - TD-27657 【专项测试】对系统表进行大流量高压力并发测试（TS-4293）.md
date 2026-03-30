# [Test Report] - TD-27657 【专项测试】对系统表进行大流量高压力并发测试（TS-4293）

### 1. 概述：

[TS-4293](https://jira.taosdata.com:18080/browse/TS-4293) 场景下，大量的 show cluster alive 把服务器端打挂了，系统表设计应该是能够承载大流量高并发的访问的，一旦服务器发生过载，预期是可以启动自我保护机制，拒绝处理新的请求，维持自己的系统在一个能够处理的范围内正常工作，不应该出现卡死这样的让整个系统处于不工作的状态
本次专项测试主要是验证系统表在大压力查询下自我保护能力。验证方法是通过单独的客户端，开启100+线程通过taosadapter执行查询，查询能够正常完成，没有卡住的情况发生。

### 2. 测试环境：

192.168.1.35：
CPU: Intel(R) Xeon(R) CPU E5-2630 v2 @ 2.60GHz （2）24核
Mem: DDR3  32 GB * 2
Disk: 2792GB
192.168.0.209（taosadapter）

### 3. 测试用例：

| SQL查询 | 查询线程，每个线程查询次数 | 期望结果 | 测试结果 |
| --- | --- | --- | --- |
| show cluster; | 150, 10 | pass |
| show cluster alive; | 15, 10 | fail (已存在jira [TS-4293](https://jira.taosdata.com:18080/browse/TS-4293)) |
| show connections; | 150, 10 | pass |
| show mnodes; | 150, 10 | pass |
| show dnodes; | 150, 10 | pass |
| show qnodes; | 150, 10 | pass |
| show databases; | 150, 10 | pass |
| show create database xx; | 150, 10 | pass |
| show users; | 150, 10 | pass |
| show vnodes; | 150, 10 | pass |
| show vgroups; | 150, 10 | pass |
| show cluster variables; | 150, 10 | pass |
| show transactions; | 150, 10 | pass |
| show stables; | 150, 10 | pass |
| show tables; | 150, 10 | pass |
| show tags; | 150, 10 | pass |
| show table distributed xx; | 150, 10 | pass |
|  |  |  |  |

### 4. 总结：

经测试，通过taosadapter连接数据库并执行多线程查询系统表，只有命令“show cluster alive;” 失败，执行会卡住，其他命令均执行正常。
