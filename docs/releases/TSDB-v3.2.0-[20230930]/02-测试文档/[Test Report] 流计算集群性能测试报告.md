# [Test Report] 流计算集群性能测试报告

## 一.概述

基于本月流计算三副本优化，分别测试 trigger_mode 为 at_once 和 window_close 的下的集群性能，分别测试单副本和三副本下的写入QPS、写入Latency、StreamLatency及各组合场景下的 CPU/MEM/NET/DISK 资源占用情况。

## 二. 软硬件环境

### **1.1 硬件环境**

| **硬件环境** | **IP** | 用途 | **CPU** | **内存** | **硬盘** |
| --- | --- | --- | --- | --- | --- |
| 192.168.1.53 | taosBenchmark |
| 192.168.1.55 | dnode |
| 192.168.1.56 | dnode |
| 192.168.1.57 | dnode |

### **1.2 软件环境**

| **软件环境(main分支)** | **IP** | **运行目录** | **脚本及配置** | **commitID** |
| --- | --- | --- | --- | --- |
| **TDengine** | 192.168.1.53、55、56、57 | /root/TDengine | 默认 | b75baa5b0e6529c9f7caabb6bc3bde1b605a51f8 |

 
## 三. 测试场景

### **无跨时间线窗口聚合**

| **vgroups** | **thread_count** | **batch** | **table_count** | **row_count** | **interlace** | **timestamp_step(ms)** |
| --- | --- | --- | --- | --- | --- | --- |
| 40 | 40 | 1000 | 10000 | 10000 | 4 | 1100 |

建流语句的子查询使用 partition by tbname，每个子表会在各自的时间线分别做聚合，以不同的 trigger_mode 测试性能；
建流语句：
create stream if not exists perf_stream trigger at_once into perf_db1.output_streamtb as select _wstart as wstart, min(c1),max(c2), sum(c0), avg(c0), count(c3), first(c0), last(c1), tbname, now from perf_db1.stb partition by tbname interval(1s);

### **跨时间线窗口聚合**

| **vgroups** | **thread_count** | **batch** | **table_count** | **row_count** | **interlace** | **timestamp_step(ms)** |
| --- | --- | --- | --- | --- | --- | --- |
| 40 | 40 | 1000 | 10000 | 10000 | 4 | 1 |

建流语句的子查询不带 partition，则所有子表的数据会聚合到一条时间线，以不同的 vgroups  和 trigger_mode 测试性能；
建流语句：create stream if not exists perf_stream trigger at_once/window_close ignore expired 0  into perf_db1.output_streamtb   as select _wstart as wstart, min(c1),max(c2), sum(c0), avg(c0), count(c3), first(c0), last(c1), now  from perf_db1.stb interval($interval) ;

### 三.测试结果：

|  |
|  |
| taosBenchmark | taosd1 | taosd2 | taosd3 | taosBenchmark | taosd1 | taosd2 | taosd3 | taosBenchmark | taosd1 | taosd2 | taosd3 | taosd1 | taosd2 | taosd3 |
| NoStream | - | - | 1315819 | 29 | - | 1983 | 304 | 286 | 299 | 160 | 3112 | 2999 | 3444 | 523489 | 168135 | 166108 | 193270 | 2.06 | 1.83 | 2.23 |
| at_once | 100000 | 1339115 | 28 | 39550 | 1830 | 760 | 744 | 951 | 122 | 2531 | 2488 | 2811 | 491489 | 253975 | 261959 | 342551 | 2.06 | 2.09 | 2.56 |
| window_close | - | 1324703 | 28 | - | 1918 | 604 | 619 | 674 | 149 | 2961 | 2859 | 2976 | 513389 | 163176 | 168835 | 184583 | 2.69 | 2.75 | 2.81 |
| at_once | 10000 | 1339490 | 28 | 7.9 | 1971 | 581 | 641 | 591 | 152 | 2962 | 2599 | 2327 | 530771 | 186716 | 336729 | 258314 | 3.11 | 3.31 | 3.49 |
| window_close | 9971 | 1357954 | 27 | 12.6 | 1899 | 685 | 609 | 677 | 144 | 2756 | 2497 | 2365 | 529200 | 301657 | 178592 | 221940 | 2.75 | 2.68 | 2.88 |
| NoStream | - | - | 1041613 | 36 | - | 1481 | 829 | 850 | 808 | 156 | 5245 | 4849 | 4516 | 403570 | 466739 | 502325 | 489350 | 6.49 | 6.68 | 6.45 |
| at_once | 100000 | 1059031 | 36 | 29648 | 1258 | 1187 | 607 | 1290 | 116 | 3315 | 2282 | 3412 | 383044 | 578454 | 542134 | 605726 | 5.47 | 5.58 | 5.39 |
| window_close | - | 1016798 | 37 | - | 1402 | 1116 | 813 | 949 | 143 | 4491 | 4206 | 4194 | 384644 | 458802 | 452045 | 462686 | 7.48 | 7.07 | 7.00 |
| at_once | 10000 | 986753 | 38 | 67 | 1261 | 944 | 904 | 928 | 139 | 3913 | 3906 | 3803 | 383440 | 476989 | 581096 | 465326 | 6.44 | 6.15 | 5.79 |
| window_close | 9971 | 979381 | 38 | 45 | 1243 | 1136 | 860 | 811 | 139 | 4168 | 4171 | 3937 | 379802 | 537549 | 456074 | 449312 | 6.74 | 6.68 | 6.53 |

## 四. 测试结论

1.无论单副本还是三副本，Partition 模式流结果延迟都很高，计算性能不如 NoPartition 模式；
2.写入 QPS 和写入 Latency 在有流和无流模式相当；

## 五. 遗留问题Jira

| [TD-26616](https://jira.taosdata.com:18080/browse/TD-26616) | [流计算partition by tbname + interval模式，计算性能不及预期](https://jira.taosdata.com:18080/browse/TD-26616) |
| --- | --- |
| [TD-26644](https://jira.taosdata.com:18080/browse/TD-26644) | [评估stream tBloomFilter资源占用是否可优化](https://jira.taosdata.com:18080/browse/TD-26644) |
