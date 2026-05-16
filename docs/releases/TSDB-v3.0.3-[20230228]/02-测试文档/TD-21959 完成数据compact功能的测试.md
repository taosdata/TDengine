# TD-21959 完成数据compact功能的测试

## 一、测试概述

TDengine 在多种写入场景下，存储会导致数据存储的放大或数据文件的空洞等。一方面影响数据的存储效率，另一方面也会影响查询效率。为了解决上述问题，TDengine 需要提供一个数据的 COMPACT 功能，将存储的数据文件重新整理，删除文件空洞和无效数据，提高数据的组织度，从而提高存储和查询的效率。
相关链接：
[TD-21959](https://jira.taosdata.com:18080/browse/TD-21959) [完成数据compact功能的测试](https://jira.taosdata.com:18080/browse/TD-21959) 
[Data Compact](https://taosdata.feishu.cn/wiki/wikcnv8joh1pPYMH9GUEEZdhFFb) 

## 二. 软硬件环境

### **2.1 硬件环境**

| **硬件环境** | **IP** | 用途 | CPU | **内存** | **硬盘** |
| --- | --- | --- | --- | --- | --- |
| **服务端** | 192.168.1.60 | taosd、taostest |
|  | 192.168.1.53 |
|  | 192.168.1.55 |
|  | 192.168.1.56 |
|  | 192.168.1.57 |

### **2.2 软件环境**

| **软件环境(3.0分支)** | **IP** | **运行目录** | **脚本及配置** | **commitID** |
| --- | --- | --- | --- | --- |
| **TDengine** | 192.168.1.53、55-57、60 | /root/TDengine | 默认 | 320ad8d1f2a549d056f24b382840ec0fc0c4cde7 |

## 三、测试方案

### 3.1 测试工具

| **测试工具** | **描述** | **脚本/配置文件** |
| --- | --- | --- |
| **taostest** | **测试主程序，部署测试环境，调用taosBenchmark进行写入、查询、确认结果等** |  |
| **taosBenchmark** | **多场景写入** |  |

### 3.2 写入schema

| **Type（全类型）** | **TINYINT、SMALLINT、INT、BIGINT、UTINYINT、USMALLINT、UINT、UBIGINT、FLOAT、DOUBLE、VARCHAR（2）、NCHAR（2)、BOOL** |
| --- | --- |
| **tag_count** | **各1列** |
| **column_count** | **各1列** |

### 3.3 测试用例

> ⚠ 嵌入思维笔记，需在飞书中查看 (token: F1GhbV4RbmHwj5n7I1zc5E9LnKh)

## 四、测试问题点

| JIRA编号 | 描述 | 状态 |
| --- | --- | --- |
| [TD-22776](https://jira.taosdata.com:18080/browse/TD-22776) | [数据只在内存中时"show table distributed *" Total_Rows和文档不符](https://jira.taosdata.com:18080/browse/TD-22776) | 已修复 |
| [TD-22740](https://jira.taosdata.com:18080/browse/TD-22740) | [compact后，select count的结果比show table distributed的结果少](https://jira.taosdata.com:18080/browse/TD-22740) | 已修复 |
| [TD-22722](https://jira.taosdata.com:18080/browse/TD-22722) | [compact手册描述是同步命令，返回就结果，但实际情况是返回后，仍然在执行。](https://jira.taosdata.com:18080/browse/TD-22722) | 已修复 |
| [TD-22705](https://jira.taosdata.com:18080/browse/TD-22705) | [采用taosBenchmark插入几千万数据后，没有乱序和重复数据，进行compact，taosd crash](https://jira.taosdata.com:18080/browse/TD-22705) | 已修复 |
| [TD-22694](https://jira.taosdata.com:18080/browse/TD-22694) | [采用taosBenchmark插入几千万数据后，包括乱序和重复数据，进行compact，taosd crash](https://jira.taosdata.com:18080/browse/TD-22694) | 已修复 |
| [TD-22710](https://jira.taosdata.com:18080/browse/TD-22710) | [采用taosBenchmark插入1千多万数据后停止，先后进行flush和compact，select count结果前后不一样](https://jira.taosdata.com:18080/browse/TD-22710) | 已修复 |
| [TD-22719](https://jira.taosdata.com:18080/browse/TD-22719) | [compact后，执行查询select count的延迟，比compact前更慢了](https://jira.taosdata.com:18080/browse/TD-22719) | 已修复 |
| [TD-22823](https://jira.taosdata.com:18080/browse/TD-22823) | [流计算稳定性测试，70亿数据规模compact后一段时间OOM，且taosd无法启动](https://jira.taosdata.com:18080/browse/TD-22823) | 修复中 |
| [TD-22853](https://jira.taosdata.com:18080/browse/TD-22853) | [compact+kill taosd测试，重启后15分钟左右taod crash](https://jira.taosdata.com:18080/browse/TD-22853) | 修复中 |
|  |  |  |

## 五、测试结果

1. 基本功能验证通过；
2. 基础性能结果：

| qps | latency |
| --- | --- |
|  |  | compact前 | compact后 | compact前 | compact后 |
| 无乱序更新删除 | 1 | 1000000000 | 951698.26 | 33.0ms | 58G*1 | 24G*1 | 7.1s | 0.17s | 0.01s |
| 更新乱序删除比例均20% | 1 | 887303847 | 274153.4 | 107.3ms | 193G*1 | 38G*1 | 48.2s | 0.14s | 255.9s |
| 更新乱序删除比例均50% | 1 | 599473498 | 198483.5 | 87.1ms | 320G*1 | 28G*1 | 89.9s | 0.16s | 267.4s |
| 无乱序更新删除 | 3 | 1000000000 | 1003009.6 | 31.4ms | 58G*3 | 24G*3 | 5.9s | 0.14s | 8.7s |
| 更新乱序删除比例均20% | 3 | 887241501 | 262074.1 | 119.1ms | 193G*3 | 39G*3 | 25.7s | 0.16s | 12.6s |
| 更新乱序删除比例均50% | 3 | 599437341 | 188495.1 | 161.6ms | 321G*3 | 29G*3 | 68.8s | 0.16s | 19.8s |

1. 数据量较大时，compact耗时较长，会一定程度上阻塞写入，但不会阻塞查询；
```sql
taos> select count(*) from stb;

       count(*)        |
========================
            7398138401 |
Query OK, 1 row(s) in set (103.797658s)

taos> compact database stream_test;
Query OK, 0 row(s) affected (2922.119191s)

[02/27 16:36:57.086401] INFO: thread[37] has currently inserted rows: 186677000
[02/27 16:37:02.046108] INFO: thread[6] has currently inserted rows: 194853000
[02/27 16:44:13.586847] INFO: thread[2] has currently inserted rows: 186868000
[02/27 16:47:10.888467] INFO: thread[4] has currently inserted rows: 190667000
[02/27 16:47:31.625875] INFO: thread[21] has currently inserted rows: 184418000
[02/27 16:47:31.626424] INFO: thread[28] has currently inserted rows: 191001000
[02/27 16:50:56.944153] INFO: thread[7] has currently inserted rows: 183433000
[02/27 16:54:40.489285] INFO: thread[25] has currently inserted rows: 207290000
[02/27 16:57:12.333091] INFO: thread[37] has currently inserted rows: 186817000
[02/27 17:05:45.975068] INFO: thread[9] has currently inserted rows: 202063000
[02/27 17:08:13.511331] INFO: thread[1] has currently inserted rows: 170192000
......

taos> select count(*) from stb;
       count(*)        |
========================
            7588531000 |
Query OK, 1 row(s) in set (27.698571s)

```


1. 三副本进行大数据量稳定性测试（乱序10%），总数据量 28 亿规模时，三台环境每台磁盘占用约546G，执行compact大概用了 32 分钟，磁盘空间从 546G 降到 85G，select count(*)查询耗时从 14s 降低到 0.18s，效果非常明显；
```sql
taos> select count(*) from compact_test.meters;
       count(*)        |
========================
            2806323867 |
Query OK, 1 row(s) in set (14.108765s)

root@u1-55 ~/TDinternal/debug (main)$ du -sh /var/lib/taos
546G        /var/lib/taos
......

taos> compact database compact_test;
Query OK, 0 row(s) affected (23.964646s)

taos> select count(*) from compact_test.meters;
       count(*)        |
========================
            2806323867 |
Query OK, 1 row(s) in set (0.181542s)

root@u1-55 ~/TDinternal/debug (main)$ du -sh /var/lib/taos
85G        /var/lib/taos
......
```

1. 流计算稳定性测试，70 亿数据规模下 compact，可正常响应，但一段时间后 OOM，taosd 启动就 core 掉[TD-22823](https://jira.taosdata.com:18080/browse/TD-22823)
```sql
taos> show stables;
          stable_name           |
=================================
 stb                            |
 output_streamtb                |
Query OK, 2 row(s) in set (0.002637s)

taos> select count(*) from stb;

       count(*)        |
========================
            7398138401 |
Query OK, 1 row(s) in set (103.797658s)
taos> select count(*) from output_streamtb ;
       count(*)        |
========================
             888877000 |
             
taos> compact database stream_test;
Query OK, 0 row(s) affected (2922.119191s)

taos> select count(*) from stb;
       count(*)        |
========================
            7588531000 |
Query OK, 1 row(s) in set (27.698571s)

taos> select count(*) from output_streamtb ;
       count(*)        |
========================
             888877000 |
Query OK, 1 row(s) in set (4.405147s)


```

1. 多副本 kill 节点测试
3 副本共 140 亿数据，compact过程中 kill -9 一个节点，然后另外两个节点开始选主和同步，这时 "show transactions" 前半段 "action info" 显示 "Sync is restoring"，后半段显示 "Action in progress"，直到 compact 正常返回共用了近 4 小时；
```sql
taos> select count(*) from meters;
       count(*)        |
========================
           14371729140 |
Query OK, 1 row(s) in set (1.921609s)

taos> compact database compact_test;

Query OK, 0 row(s) affected (12938.247347s)

/*compact过程中执行*/
taos> show transactions\G;
*************************** 1.row ***************************
              id: 496
     create_time: 2023-02-28 14:44:41.971
           stage: redoAction
            oper: compact-db
              db: compact_test
          stable:
    failed_times: 1
  last_exec_time: 2023-02-28 17:40:27.513
last_action_info: action:6 code:0x111(Action in progress) msgType:vnode-compact numOfEps:3 inUse:2 ep:0-u1-57:6030 ep:1-u1-56:6030 ep:2-u1-55:6030
Query OK, 1 row(s) in set (0.003008s)

/*节点 1 restore 日志*/
taosdlog.0:02/28 17:31:34.229644 00389932 VND vgId:167, sync restore finished
/*节点 2 restore 日志*/
taosdlog.0:02/28 16:11:37.159615 00639973 VND vgId:165, sync restore finished
```

1. 磁盘占用跟踪
compact 后呈现先小幅上涨再大幅降低的趋势，测试文件：stt_data_mix_multi.json，乱序更新删除比例都是10%。
以下结果中，每30秒计算一次
<view type="2">

  > ⚠ 嵌入文件，需在飞书中查看 (token: X6UebZ6ekoTyUnxF1Mec5tJInBd)

</view>

1. 按时间段 compact (3副本结果相近)
先使用 taosBenchmark 加一定量数据，乱序20%，然后按时间段分 2 次 compact，分别记录 2 次 compact 后的磁盘占用和查询时间，并重新加数据，对比不使用时间区间的结果；
```sql
20%乱序
compact 前：

root@u1-57 ~ $ du -sh /var/lib/taos
214G        /var/lib/taos
taos> select count(*) from compact_test.meters;
       count(*)        |
========================
             810001945 |
Query OK, 1 row(s) in set (23.262678s)

taos> select first(*) from compact_test.meters;
        first(ts)        | first(c0) | first(c1) |  first(c2)  |       first(c3)       | first(c4) | first(c5) |  first(c6)  |       first(c7)       |      first(c8)       |         first(c9)         | first(c10) | first(c11) | first(c12) |
================================================================================================================================================================================================================================================
 2017-07-14 10:39:59.000 |       107 |     -3680 |   670139788 |             669003449 |        33 |     55429 |  1478221846 |            1538908190 |        8808318.00000 |      -236264498.631996989 | 5J         | 3x         |      false |
Query OK, 1 row(s) in set (45.392540s)

taos> select last(*) from compact_test.meters;
        last(ts)         | last(c0) | last(c1) |  last(c2)   |       last(c3)        | last(c4) | last(c5) |  last(c6)   |       last(c7)        |       last(c8)       |         last(c9)          | last(c10) | last(c11) | last(c12) |
=========================================================================================================================================================================================================================================
 2017-09-21 21:19:00.000 |       81 |   -20897 |  -968458724 |            -623450517 |        1 |    17217 |  1442564333 |              72012436 |      986926848.00000 |       948718105.160128951 | HB        | QT        |     false |
Query OK, 1 row(s) in set (36.174596s)

第 1 次 compact：
taos> compact database compact_test start with "2017-07-14 10:39:59.000" end with "2017-08-15 10:39:59.000";
Query OK, 0 row(s) affected (13.401298s)

root@u1-57 ~ $ du -sh /var/lib/taos
37G        /var/lib/taos

taos> select count(*) from compact_test.meters;
       count(*)        |
========================
             810001945 |
Query OK, 1 row(s) in set (0.432800s)

第 2 次 compact：
taos> compact database compact_test start with "2017-08-15 10:39:59.000" end with "2017-09-21 21:19:00.000";
Query OK, 0 row(s) affected (0.016785s)

root@u1-57 ~ $ du -sh /var/lib/taos
36G        /var/lib/taos

taos> select count(*) from compact_test.meters;
       count(*)        |
========================
             810001945 |
Query OK, 1 row(s) in set (0.108366s)

再重新加数据，不加时间区间对比测试，发现磁盘占用和查询市场基本是相同的（结果略）
```

因 taosBenchmark 的乱序实现是在 start_timestamp 之前进行插入的，所以理论上第一次 compact 就可以完成大部分的工作，而测试结果中，磁盘空间从214G->37G->36G  select count(*)查询从23s->0.4s->0.1s，也是符合预期的。

## 五、测试结论

所有测试均已完成，部分问题待修复
