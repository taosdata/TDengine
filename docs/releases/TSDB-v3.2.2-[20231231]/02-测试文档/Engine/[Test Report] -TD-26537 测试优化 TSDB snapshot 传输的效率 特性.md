# [Test Report] -TD-26537 测试优化 TSDB snapshot 传输的效率 特性

## 一. 测试结论

Redistribute 在单副本的效率从未优化的3.6MB/s 提升到优化后的 109MB/s，提升了30 倍。
单副本变成 3 副本的过程提升了 27 倍。
Redistribute 在3副本的情况下，提升了10 倍。
3.1 和 3.0 的结果基本相同。
一.概述
测试 jira： 
TD-26537

测试主要是基于快照复制的优化，wal 复制并未涉及，所以split/alter replica 3/redistribute 这三个功能项都有优化。

## 二. 软硬件环境

### **硬件环境**

| **硬件环境** | **IP** | 用途 | **CPU** | **内存** | **硬盘** |
| --- | --- | --- | --- | --- | --- |
| 192.168.1.86：6030 | Taosd taosBenchmark |
| 192.168.1.86：6040 | taosd |
| 192.168.1.86：6050 | taosd |
| 192.168.1.86：6060 | taosd |
| 192.168.1.86：6070 | taosd |
| 192.168.1.86：6080 | taosd |

### ** 软件环境**

| **软件环境(3.0分支)** | **IP** | **commitID** |
| --- | --- | --- |
| **TDinternal-3.0优化** | 192.168.1.86 | enterprise version: 3.2.2.0.alpha compatible_version: 3.0.0.0 gitinfo: 49b3d72764d38f08ff7dc6739b7ecd3ccc4e5a58 gitinfoOfInternal: 3771c690bfcc3711a2d71677c582a34111d24f04 buildInfo: Built Linux-x64 at 2023-12-14 19:19:02 +0800 |
| **TDinternal-3.0 未优化** |  | enterprise version: 3.2.2.0.alpha compatible_version: 3.0.0.0 gitinfo: 9c72ce846e63018ca210c7b2bc2827d2b77fad85 gitinfoOfInternal: 7a64f3f6d20a4e12a5a01726d45b367131a352d8 buildInfo: Built Linux-x64 at 2023-12-17 23:08:08 +0800 |
| **TDinternal-3.1 优化** |  | enterprise version: 3.1.1.0 compatible_version: 3.0.0.0 gitinfo: 4034747bbc8d32684ae61158f502a2ad8501835f gitinfoOfInternal: cc685ae2a4492168adc7c333dfc0d35f5029a0f7 buildInfo: Built Linux-x64 at 2023-12-20 17:09:27 +0800 |


## 三. 测试场景

跟本光的讨论：
之前的快照复制都是会重新读一遍数据，然后 compact 以后再重新写入。本次主要优化点是改成了按文件复制，增加snapshot 传输的多流水线。所以优化前 split 和 replica、redistribute 以后文件大小可能会变小，现在优化后应该是基本不变化的。

|  | 测试点 | 描述 |
| --- | --- | --- |
| 性能 | 计算单个 vgroup 8.8G 时迁移速度 | 单副本验证 100000张子表， 总数据条目数11,199,994,832 超级表： CREATE STABLE `meters` (`ts` TIMESTAMP, `current` FLOAT, `voltage` INT, `phase` FLOAT) TAGS (`groupid` INT, `location` VARCHAR(24)) 配置 WAL_RETENTION_PERIOD 为 0。 |
|  | 计算单副本变3副本. |  |
|  | 三副本的迁移速度 |  |
| 稳定性测试 | 长时间大数据量测试 | 将功能测试项尽可能叠加，数据量调大进行长时间压测 |


## 四.测试结果：

### 4.2 性能测试

<callout emoji="small_blue_diamond" background-color="light-orange" border-color="light-orange">
Redistribute 日志关键信息：
start（start to redistribute vgroup to dnode）
finish（vgId:*.*msgType:alter-confirm）
单副本结束标志可以按以上方法确认，redistribute 直接可以确认。
三副本也可以在 show vnodes 时迁移 vg_id 的 restored 均为 true 即可）
</callout>


| **序号** | **测试点** | **测试步骤** | 副本数 | 单 vgroups 数据量 | 未优化耗时(s) | 优化耗时(s) | 3.1 优化耗时 (s) | 优化迁移速度提升倍速 |  | **测试结果** |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 1 | 单副本单个 vnode 迁移，vnode 大小8.8G ( redistribute vgroup 6 dnode 1 ) | 1.启动 taosBenchmark 写入100亿数据（11,199,994,832）； 2.写入完成后新增一个 dnode 进行迁移; 3.记录日志时间区间； | 1 | 4.4G | 1250 (3.6MB/s) | 41 (109MB/s) | 40 | 30 |  | 耗时 41s，迁移速度约为 219M/s 未优化前。8.8G redistribute 以后变成了 4.4G，时间为1250s 。迁移速度约为 3.6MB/s，提了30 倍。 |
| 2 | 单副本变 3 副本 (alter database test replica 3;) | 把上述数据变成 3 副本 | 3 | 8.8G | 2454 | 89 | 98 | 27 |  |  |
| 3 | 三副本vnode迁移，每个 dnode 上的vnode 大小均为 8.8.G( redistribute vgroup 6 dnode 1 dnode 5 dnode 6 ;) | 1.启动 taosBenchmark 写入100亿数据； 2.写入完成后新增三个 dnode 进行迁移; 3.记录日志时间区间； | 3 | 8.8G | 2140 | 200 | 180 | 10.7 |  |  |
| 4 | 多副本 vgroups进行split变成 2 个 vgroups |  | 3 | 8.8G*3 |  | 200 | 280 |  |  | 因为数据无法缩容，只记录了第一次 split 的结果。这个结果没法反复做， |
|  |  |  |  |  |  |  |  |  |  |  |

优化前，redistribute 单副本1250s
```powershell

单副本：
taos> redistribute vgroup 5 dnode 1 ;
Query OK, 0 row(s) affected (1156.536569s)
taos> redistribute vgroup 5 dnode 2 ;
Query OK, 0 row(s) affected (2220.111111s)
taos> redistribute vgroup 5 dnode 3;
Query OK, 0 row(s) affected (1252.824095s)
taos> redistribute vgroup 5 dnode 4;
Query OK, 0 row(s) affected (1256.589435s)

多副本
taos>  redistribute vgroup 5 dnode 1 dnode 2 dnode 6 ;
Query OK, 0 row(s) affected (2158.887364s)
taos>  redistribute vgroup 6 dnode 1 dnode 5 dnode 6 ;
Query OK, 0 row(s) affected (2140.049865s)
taos>  redistribute vgroup 5 dnode 3 dnode 4 dnode 5 ;
Query OK, 0 row(s) affected (3993.130877s)
```


优化后迁移时间41s
```powershell
单副本
taos> redistribute vgroup 5 dnode 2 ;
Query OK, 0 row(s) affected (42.745725s)
taos> redistribute vgroup 5 dnode 3 ;
Query OK, 0 row(s) affected (37.544331s)
taos> redistribute vgroup 5 dnode 4 ;
Query OK, 0 row(s) affected (41.743762s)
taos> redistribute vgroup 6 dnode 1 ;
Query OK, 0 row(s) affected (48.327598s)
taos> redistribute vgroup 6 dnode 2 ;
Query OK, 0 row(s) affected (37.895450s)
taos> redistribute vgroup 6 dnode 3 ;
Query OK, 0 row(s) affected (41.889702s)
taos> redistribute vgroup 6 dnode 5 ;
Query OK, 0 row(s) affected (33.260676s)

多副本
taos>  redistribute vgroup 5 dnode 1 dnode 5 dnode 6 ;
Query OK, 0 row(s) affected (80.554197s)
taos>  redistribute vgroup 6 dnode 2 dnode 3 dnode 4 ;
Query OK, 0 row(s) affected (99.065507s)
taos>  redistribute vgroup 6 dnode 1 dnode 5 dnode 6 ;
Query OK, 0 row(s) affected (156.491141s)
taos>  redistribute vgroup 6 dnode 2 dnode 3 dnode 4 ;
Query OK, 0 row(s) affected (236.820743s)
taos>  redistribute vgroup 6 dnode 1 dnode 5 dnode 6 ;
Query OK, 0 row(s) affected (175.764851s)
taos>  redistribute vgroup 6 dnode 2 dnode 3 dnode 4 ;
Query OK, 0 row(s) affected (181.130731s)
taos>  redistribute vgroup 6 dnode 1 dnode 5 dnode 6 ;
Query OK, 0 row(s) affected (245.186735s)
taos>  redistribute vgroup 6 dnode 2 dnode 3 dnode 4 ;
Query OK, 0 row(s) affected (145.091791s)
taos>  redistribute vgroup 6 dnode 1 dnode 5 dnode 6 ;
Query OK, 0 row(s) affected (157.346295s)
taos>  redistribute vgroup 6 dnode 2 dnode 3 dnode 4 ;
Query OK, 0 row(s) affected (140.390809s)
taos>  redistribute vgroup 6 dnode 1 dnode 5 dnode 6 ;
Query OK, 0 row(s) affected (140.730716s)
taos>  redistribute vgroup 6 dnode 2 dnode 3 dnode 4 ;
Query OK, 0 row(s) affected (226.566800s)
```

这里4.4G 的是旧版本的 redistribute（带 compact 的），8.8G 是新版本的 redistribute。
```powershell
root@yw86 /home/chr/2.6/TDinternal/sim (3.0)$ du * -sh
8.0K    asan
8.8G    dnode1
6.5M    dnode2
17M     dnode3
4.4G    dnode4
11M     dnode5
2.6M    dnode6
3.1M    nohup.out
28M     psim
4.0K    tsim
```



### 4.3 稳定性测试

| 测试点 | 测试步骤 | 结果 |
| --- | --- | --- |
| 覆盖所有功能点，长时间大数据量压测 | 写入过程中组合更新、乱序、删除、stream、tmq、restart dnode等一系列操作，且往复进行，确保最终数据结果正确，且不会出现 Crash 和OOM等现象； | 暂未测试 |

## 五 . 3.1测试结果

```powershell
taos> redistribute vgroup 42 dnode 2;
Query OK, 0 row(s) affected (66.522783s)
taos> redistribute vgroup 42 dnode 3;
Query OK, 0 row(s) affected (71.736026s)
taos> redistribute vgroup 42 dnode 4;
Query OK, 0 row(s) affected (67.539320s)
taos> redistribute vgroup 42 dnode 5;
Query OK, 0 row(s) affected (66.529218s) 
split 以后的 redistribute
taos>  redistribute vgroup 43 dnode 1 ;
Query OK, 0 row(s) affected (37.314530s)
taos>  redistribute vgroup 43 dnode 2 ;
Query OK, 0 row(s) affected (37.334079s)
taos>  redistribute vgroup 43 dnode 3;
Query OK, 0 row(s) affected (41.468156s)
taos>  redistribute vgroup 43 dnode 4;
Query OK, 0 row(s) affected (41.390487s)
taos>  redistribute vgroup 43 dnode 5;
Query OK, 0 row(s) affected (41.466056s)
taos>  redistribute vgroup 43 dnode 6;
Query OK, 0 row(s) affected (37.405323s)
taos>  redistribute vgroup 43 dnode 1;
Query OK, 0 row(s) affected (41.397693s)
taos>  redistribute vgroup 43 dnode 2;
Query OK, 0 row(s) affected (48.081235s)


 redistribute vgroup 42 dnode 1 dnode 4 dnode 6;
Query OK, 0 row(s) affected (187.269504s)
taos> redistribute vgroup 42 dnode 2 dnode 3 dnode 5;
 Query OK, 0 row(s) affected (177.180801s)
 taos> redistribute vgroup 42 dnode 1 dnode 4 dnode 6;
Query OK, 0 row(s) affected (130.686560s)


```



| **序号** | **测试点** | **测试步骤** | 副本数 | 单 vgroups 数据量 | 未优化耗时(s) | 优化耗时(s) | 优化迁移速度提升倍速 |  | **测试结果** |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 1 | 单副本单个 vnode 迁移，vnode 大小8.8G ( redistribute vgroup 6 dnode 1 ) | 1.启动 taosBenchmark 写入100亿数据（11,199,994,832）； 2.写入完成后新增一个 dnode 进行迁移; 3.记录日志时间区间； | 1 | 8.8G | 1250 | 68 |  |  |  |
| 2 | 单副本变 3 副本 (alter database test replica 3;) | 把上述数据变成 3 副本 | 3 | 8.8G | 2454 | 74 |  |  |  |
| 3 | 三副本vnode迁移，每个 dnode 上的vnode 大小均为 8.8.G( redistribute vgroup 6 dnode 1 dnode 5 dnode 6 ;) | 1.启动 taosBenchmark 写入100亿数据； 2.写入完成后新增三个 dnode 进行迁移; 3.记录日志时间区间； | 3 | 8.8G | 2140 | 180 |  |  |  |
| 4 | 单个 vgroups进行split变成 2 个 vgroups |  |  | 8.8G |  |  |  |  | 因为数据无法缩容，只记录了第一次 split 的结果。猜测和三副本的迁移相似。 |
|  | 多副本 vgroups进行split变成 2 个 vgroups |  |  | 8.8G |  | 120 280 |  |  |  |
|  |  |  |  |  |  |  |  |  |  |

## 六. 问题单

| **JiraID** | **Describe** | **Status** |
| --- | --- | --- |
|  | TD-27907 |  |
|  |  |  |
|  |  |  |
|  |  |  |
|  |  |  |
|  |  |  |
|  |  |  |
|  |  |  |
