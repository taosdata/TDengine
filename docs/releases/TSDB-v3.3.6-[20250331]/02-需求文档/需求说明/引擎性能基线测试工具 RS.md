# 引擎性能基线测试工具 RS

## 1. 引言

### 1.1 术语与缩写名词

1. **进程绑定 CPU 核心**：把某个进程/线程绑定到特定的cpu核上后，该进程就会一直在此核上运行，不会再被操作系统调度到其他核上。但绑定的这个核还是可能会被调度运行其他应用程序的
2. **interlace**：交错写入模式
3. **batch**：批量写入模式

### 1.2 相关文档资料

1. [V3.3.5.0 性能测试结果](https://taosdata.feishu.cn/wiki/Yhrrwb5pEikC6DkRiy6cym8mn6g)
2. [南网数研院智能电网调度PoC验证报告](https://taosdata.feishu.cn/wiki/Wk69wkefNiZGN3kwqoecNTAannf)
3. [大集群测试方案设计 （初稿）](https://taosdata.feishu.cn/wiki/AmZTwfpzIiu2PgkBKJpcc9x3nJh)
4. [Lastrow并发查询性能分析与优化](https://taosdata.feishu.cn/wiki/XDAhwt2tYiZO5fk0bZhcLsLHnec)
5. [基线性能测试方法](https://taosdata.feishu.cn/wiki/CqyowSvW6idLYRkQI2kcsnMAnHb)

### 1.3 优先级要求

影响到 0331 版本发布，希望在 0331 前提供一个可运行版本

### 1.4 版本要求

开源，任何人都可以运行，因为会使用同样的脚本测试不同的 TDengine 版本，考虑单独的代码仓库

## 2. 变更历史

| 日期 | 版本 | 负责人 | 主要修改内容 |
| --- | --- | --- | --- |
| 2024/01/15 | 1.0 | 关胜亮 | 新建，还需进一步补充一些测试用例 |

## 3. 需求目标

### 3.1 测试目的

TDengine 拥有众多组件，相应地也衍生出了多种部署方案。以 JMeter 测试工具为例，可通过 JDBC 连接器，采用 Websocket 接口，经由 taosAdapter 来访问 taosd。
1. 测试第一阶段：聚焦点在引擎层面，借助 taosBenchmark 工具，测试 taosc 通过原生接口与 taosd 进行交互时的性能表现。待 TDengine 引擎的性能得到验证与确认后，其他组件或不同场景下可能出现的性能相关问题，便能够更加便捷、高效地进行判断与定位。
2. 测试第二阶段：逐步引入其他连接器，例如 JDBC、GO，引入 taosadapter 组件，给出各方式的衰减比例
测试目的主要包括以下几个方面：
1. 明确性能预期：让开发团队、运维团队以及业务部门等各方对系统的性能有一个清晰的预期
2. 确保性能稳定：每次版本发布时，进行可重复的测试，确保各项性能指标不出现非预期的下降

### 3.2 测试环境

在测试中，客户端与服务端以及其他组件，均部署在同一台物理机上，规避网络因素对测试结果的潜在干扰。公司已有一台用于性能测试的高性能物理机（IP 地址为 192.168.1.58），该物理机有 40 个核心机 250GB 的内存。
为了进一步确保测试过程中的资源分配合理性，客户端与服务端将被绑定至固定数量的 CPU 核心上，避免服务器与客户端之间的资源争抢情况。
在仅有 taosBenchmark 和 taosd 的场景中，taosd 绑定 16 个 CPU 核，taosBenchmark 同样绑定 16 个 CPU 核.

### 3.3 测试范围

针对社区版进行测试，原因
1. 社区版：GIT PR 切换更加方便快捷，当出现性能问题时，能够根据合并时间精确定位到引入性能缺陷的 PR
2. 企业版：影响性能的参数只有 SST Trigger，在（[TS-5918](https://jira.taosdata.com:18080/browse/TS-5918)）完成后社区版也可使用。
覆盖的测试场景如下
1. 查询：所有函数、语法
2. 写入：SQL 、STMT、STMT2 三种方法，batch、interlace 两种场景
3. 订阅：全库订阅、SQL 订阅
4. 流计算：有流计算时的写入速度和资源消耗
5. 数据同步：taosX
6. 数据源接入：Kafka、MQTT

## 4. 功能需求

### 4.1 数据模型

1. 建库参数：Replica=1，其他默认
2. 集群参数：numOfCommitThreads：4，compressMsgSize：-1，monitor：0，audit：0，其他默认
3. 表（超级表）定义：
```sql {wrap}
create stable stb (ts timestamp, c1 int, c2 int, ……） tags(t1 int, t2 varchar(10));
create table d1 using stb(1, '1');
```

1. 数据集：时间戳从 '2024-01-01 00:00:00.000' 开始，单设备单条记录间隔 60 秒，tbname 序号从 0 开始
2. taosBenchmark 参数：thread_count：32，thread_bind_vgroup：yes

### 4.2 写入场景

以 stmt、interlace=1、tables=40K、timeseries=4、vgroups=16 作为典型场景，其他维度以此为基准扩展。
1. 写入场景：batch（能拼接的最大数量），interlace（1、4、16、64、256）
2. 写入方法：sql、stmt、stmt2、sql+自动建表、stmt+自动建表、stmt2+自动建表、schemaless
3. 是否自动建表：自动建表、不自动建表
4. 子表数：2.5k、10k、40k、160k、640k
5. 时间线（列数目）：1 、4、8、64、256、1024
6. Vgroups（16 核）：1 、4、16、64
7. 列定义：bool、tinyint、smallint、int、bigint、float、double、varcchar、nchar
8. 缓存：none、last_value、last_row、both
9. STT_TRIGGER：1、2、4、8
10. WAL：默认设置、强制刷盘（WAL_LEVEL=2 and WAL_FSYNC_PERIOD=0）
11. BUFFER：16、64、256、1024
12. 压缩算法：lz4、zlib、zstd、tsz、xz、disabled
下表为写入场景的编号，约 52 项测试，用时大约 200 分钟
| 编号 | 写入模式 | 写入方法 | 子表数 | 时间线数 | VGroup 数 | 列定义 | 非默认的参数 | 备注 |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| W000 | interlace=1 | stmt | 40k | 4 | 16 | int |  |  |
| W001 | interlace=4 | stmt | 40k | 4 | 16 | int |  |  |
| W002 | interlace=16 | stmt | 40k | 4 | 16 | int |  |  |
| W003 | interlace=64 | stmt | 40k | 4 | 16 | int |  |  |
| W004 | interlace=256 | stmt | 40k | 4 | 16 | int |  |  |
| W005 | batch | stmt | 40k | 4 | 16 | int |  |  |
| W010 | interlace=1 | sql | 40k | 4 | 16 | int |  |  |
| W011 | interlace=1 | stmt | 40k | 4 | 16 | int |  | 和 W000 相同 |
| W012 | interlace=1 | stm2 | 40k | 4 | 16 | int |  |  |
| W013 | interlace=1 | sql+自动建表 | 40k | 4 | 16 | int |  |  |
| W014 | interlace=1 | stmt+自动建表 | 40k | 4 | 16 | int |  |  |
| W015 | interlace=1 | stm2+自动建表 | 40k | 4 | 16 | int |  |  |
| W020 | batch | sql | 40k | 4 | 16 | int |  |  |
| W021 | batch | stmt | 40k | 4 | 16 | int |  | 和 W000 相同 |
| W022 | batch | stm2 | 40k | 4 | 16 | int |  |  |
| W023 | batch | sql+自动建表 | 40k | 4 | 16 | int |  |  |
| W024 | batch | stmt+自动建表 | 40k | 4 | 16 | int |  |  |
| W025 | batch | stm2+自动建表 | 40k | 4 | 16 | int |  |  |
| W026 | batch | schemaless | 40k | 4 | 16 | int |  |  |
| W030 | interlace=1 | stmt | 2.5k | 4 | 16 | int |  |  |
| W031 | interlace=1 | stmt | 10k | 4 | 16 | int |  |  |
| W032 | interlace=1 | stmt | 40k | 4 | 16 | int |  | 和 W000 相同 |
| W033 | interlace=1 | stmt | 160k | 4 | 16 | int |  |  |
| W034 | interlace=1 | stmt | 640k | 4 | 16 | int |  | 根据测试耗时确定是否开展 |
| W040 | interlace=1 | stmt | 40k | 1 | 16 | int |  |  |
| W041 | interlace=1 | stmt | 40k | 4 | 16 | int |  | 和 W000 相同 |
| W042 | interlace=1 | stmt | 40k | 16 | 16 | int |  |  |
| W043 | interlace=1 | stmt | 40k | 64 | 16 | int |  |  |
| W044 | interlace=1 | stmt | 40k | 256 | 16 | int |  |  |
| W045 | interlace=1 | stmt | 40k | 1024 | 16 | int |  | 根据测试耗时确定是否开展 |
| W046 | interlace=1 | stmt | 40k | 4096 | 16 | int |  | 根据测试耗时确定是否开展 |
| W050 | interlace=1 | stmt | 40k | 4 | 1 | int |  |  |
| W051 | interlace=1 | stmt | 40k | 4 | 4 | int |  | 和 W000 相同 |
| W052 | interlace=1 | stmt | 40k | 4 | 16 | int |  |  |
| W053 | interlace=1 | stmt | 40k | 4 | 64 | int |  |  |
| W060 | interlace=1 | stmt | 40k | 4 | 16 | bool |  |  |
| W061 | interlace=1 | stmt | 40k | 4 | 16 | tinyint |  |  |
| W062 | interlace=1 | stmt | 40k | 4 | 16 | smallint |  |  |
| W063 | interlace=1 | stmt | 40k | 4 | 16 | int |  | 和 W000 相同 |
| W064 | interlace=1 | stmt | 40k | 4 | 16 | bigint |  |  |
| W065 | interlace=1 | stmt | 40k | 4 | 16 | float |  |  |
| W066 | interlace=1 | stmt | 40k | 4 | 16 | double |  |  |
| W070 | interlace=1 | stmt | 40k | 4 | 16 | int | cachemodel=none |  |
| W071 | interlace=1 | stmt | 40k | 4 | 16 | int | cachemodel=last_value |  |
| W072 | interlace=1 | stmt | 40k | 4 | 16 | int | cachemodel=last_row |  |
| W073 | interlace=1 | stmt | 40k | 4 | 16 | int | cachemodel=both | 和 W000 相同 |
| W080 | interlace=1 | stmt | 40k | 4 | 16 | int | stt_trigger=1 | 和 W000 相同 |
| W081 | interlace=1 | stmt | 40k | 4 | 16 | int | stt_trigger=2 |  |
| W082 | interlace=1 | stmt | 40k | 4 | 16 | int | stt_trigger=4 |  |
| W083 | interlace=1 | stmt | 40k | 4 | 16 | int | stt_trigger=8 |  |
| W090 | interlace=1 | stmt | 40k | 4 | 16 | int | stt_trigger=1 | 和 W000 相同 |
| W091 | interlace=1 | stmt | 40k | 4 | 16 | int | WAL_LEVEL=2 &&
WAL_FSYNC_PERIOD=0 |  |
| W100 | interlace=1 | stmt | 40k | 4 | 16 | int | buffer=16 |  |
| W101 | interlace=1 | stmt | 40k | 4 | 16 | int | buffer=64 |  |
| W102 | interlace=1 | stmt | 40k | 4 | 16 | int | buffer=256 | 和 W000 相同 |
| W103 | interlace=1 | stmt | 40k | 4 | 16 | int | buffer=1024 |  |
| W110 | interlace=1 | stmt | 40k | 4 | 16 | int | 压缩算法：lz4 | 和 W000 相同 |
| W111 | interlace=1 | stmt | 40k | 4 | 16 | int | 压缩算法：zlib |  |
| W112 | interlace=1 | stmt | 40k | 4 | 16 | int | 压缩算法：zstd |  |
| W113 | interlace=1 | stmt | 40k | 4 | 16 | int | 压缩算法：tsz |  |
| W114 | interlace=1 | stmt | 40k | 4 | 16 | int | 压缩算法：xz |  |
| W115 | interlace=1 | stmt | 40k | 4 | 16 | int | 压缩算法：disabled |  |

### 4.3 查询场景

#### 4.3.1 数据集合

为了确保多次测试的性能结果一致，用于查询性能评估的数据集在写入 TDengine 后，将被打包成压缩文件，以便后续能够多次便捷地调用。未特殊说明时，写入完成后，要执行 Flush Database 操作，以便测试过程中 taosd 能够快速启动。如下为典型的测试数据集合（限于测试时长，测试集合不易过多）。
| 编号 | 写入模式 | 子表数 | 时间线数 | VGroup 数 | 列定义 | 备注 |
| --- | --- | --- | --- | --- | --- | --- |
| D001 | interlace=1 | 40k | 4 | 1 | int |  |
| D002 | interlace=1 | 40k | 4 | 16 | float |  |
| D003 | interlace=1 | 40k | 4 | 16 | int |  |
| D004 | interlace=1 | 40k | 64 | 16 | int |  |
| D005 | interlace=1 | 40k | 512 | 16 | int |  |
| D006 | interlace=1 | 640k | 4 | 16 | int |  |
| D007 | interlace=1 | 640k | 4 | 16 | int | sst_trigger=4 |
| D008 | interlace=1 | 640k | 4 | 16 | int | 不执行 flush database |
| D009 | batch | 640k | 4 | 16 | int |  |
| D010 | batch | 640k | 4 | 16 | int | compact 后保存 |

说明：在进行查询性能测试时，之所以区分是否 flush db 操作、采用何种写入模式、STT 参数，是因为数据在内存中与在 data 文件中的差异，会导致查询时所走的路径不同，进而使得查询性能也有所区别。

#### 4.3.2 投影查询

| 编号 | 描述 | SQL |
| --- | --- | --- |
| Q0100 | 查询一个子表的所有数据 | select * from d1000 |
| Q0101 | 查询一个子表的所有数据行中的四列数据 | select ts, c1, c2, c3 from d1000 |
| Q0102 | 查询一个子表的 c1=0 的所有数据 | select * from d1000 where c1=0 |
| Q0103 | 查询某 10 个子表的所有数据 | select * from stb where t1 > 100 and t1 <= 110 |
| Q0104 | 查询某 10 个子表的某固定时间范围的所有数据 | select * from stb where t1 > 1100 and t1 <= 1110 |

#### 4.3.3 最新值查询

| 编号 | 描述 | SQL |
| --- | --- | --- |
| Q0110 | last_row 查询子表最新一行数据中的四列 | select last_row(ts, c1, c2, c3) from d1; |
| Q0111 | last_row 查询子表最新一行数据 | select last_row(*) from d1; |
| Q0112 | order by limit 方式查询子表最新一行数据 | select * from d1 order by ts limit 1 |
| Q0113 | last_row 查询超级表中某个子表的最新一行数据 | select last_row(*) from stb where tbname = 'd1'; |
| Q0114 | last_row 分组加 slimit 查询 10 个子表的最新一行数据 | select last_row(*) from stb partition by tbname slimit 10; |
| Q0115 | last 查询子表最新一行数据 | select last_row(*) from d1; |

#### 4.3.4 函数查询

对所有支持的函数进行全面测试。
| 编号 | 描述 | SQL |
| --- | --- | --- |
| Q0120 | max | select max(c1) from d1; |
| Q0121 | min | select min(c1) from d1; |
| Q0122 | count | select count(c1) from d1; |
| Q0123 | sum | select sum(c1) from d1; |
| Q0124 | avg | select avg(c1) from d1; |
| Q0125 | percentile | select percentile(c1, 0.2) from d1 |
| Q0126 | apercentile | select apercentile(c1, 50) from d1 |
| Q0127 | …… |  |
| Q0128 |  |  |

#### 4.3.5 窗口查询（待补充 SQL）

| 编号 | 描述 | SQL |
| --- | --- | --- |
| Q0130 | interval | select avg(c1) from stb interval(1h); |
|  | session |  |
|  | state |  |
|  | event |  |

#### 4.3.6 嵌套查询（待补充 SQL）

| 编号 | 描述 | SQL |
| --- | --- | --- |
| Q0140 | 按 tbname 分组后，三层嵌套 | select abs(max_a - min_a ) from (select max(a) max_a,min(b) min_a from ( select last(current) as a, avg(current) as b from test.d1 group by tbname)) |

#### 4.3.7 Group 查询（待补充 SQL）

| 编号 | 描述 | SQL |
| --- | --- | --- |
| Q0150 |  |  |

#### 4.3.8 Partition 查询（待补充 SQL）

| 编号 | 描述 | SQL |
| --- | --- | --- |
| Q0160 |  |  |

#### 4.3.9 Join 查询（待补充 SQL）

| 编号 | 描述 | SQL |
| --- | --- | --- |
| Q0170 | inner join | select * from test.d1 as t1 inner join test.d2 as t2 on t1.voltage = t2.voltage and t1.ts = t2.ts |
| Q0171 | left  join | select * from test.d1 as t1 left join test.d2 as t2 on t1.voltage = t2.voltage and t1.ts = t2.ts and t1.ts >= '2022-10-01 00:00:00.000' and t1.ts < '2022-10-01 00:00:05.000' |
| Q0172 | right  join | select * from test.d0 as t1 right join test.d1 as t2 on t1.voltage = t2.voltage and t1.ts = t2.ts and t2.ts in ( '2022-10-01 00:00:00.000','2022-10-01 00:00:30.000') |
| Q0173 | full join | select * from test.d0 as t1 full join test.d1 as t2 on t1.voltage = t2.voltage and t1.ts = t2.ts and t1.ts > '2022-10-01 00:00:00.000' and t1.ts < '2022-10-01 00:00:02.000' |

#### 4.3.10 Union 查询（待补充 SQL）

| 编号 | 描述 | SQL |
| --- | --- | --- |
| Q0180 | union all | select * from test.d1 union all select * from test.d2 union all select * from test.d3 |

#### 4.3.11 Interp 查询（待补充 SQL）

| 编号 | 描述 | SQL |
| --- | --- | --- |
| Q0190 | 子表查询，带时间范围的断面查询 | select interp(current) from d1 range('2024-01-01 10:00:01.000','2024-11-02 00:00:03.000') every(10s) fill(linear) |
| Q0191 | 子表查询，使用 near 填充方式但只取一条数据 |  |
| Q0192 | 对超级表进行查询，使用 prev last 同时填充 |  |
|  | 查询5000点一个断面在1秒内返回结果集 |  |

#### 4.3.12 TSBS 查询语句（待补充 SQL）

| 编号 | 描述 | SQL |
| --- | --- | --- |
| Q0190 | 子表查询，带时间范围的断面查询 | select interp(current) from d1 range('2024-01-01 10:00:01.000','2024-11-02 00:00:03.000') every(10s) fill(linear) |
| Q0191 | 子表查询，使用 near 填充方式但只取一条数据 |  |
| Q0192 | 对超级表进行查询，使用 prev last 同时填充 |  |

### 4.4 数据订阅（待补充）

| 编号 | 描述 | SQL |
| --- | --- | --- |
| Q0190 | 库订阅，用taosBenchmark 订阅库里的全部数据，用总数据行数除以时间统计出来吞吐量  rows/s。（一个消费者组里用和 db 相同 vgroups 个数的consumer 来订阅） | create topic topic_db as database db |
| Q0191 | 查询订阅，用taosBenchmark 订阅查询超级表的全部数据，用超级表总数据行数除以时间统计出来吞吐量  rows/s。查询不再做条件过滤，因为条件过滤是查询引擎执行的，不同的条件过滤耗时不一样，所以订阅里不再加过滤条件。（一个消费者组里用和 db 相同 vgroups 个数的consumer 来订阅） | create topic topic_query as select * from stb |

### 4.5 其他场景（待补充）

## 5. 性能需求

各个场景的总测试时间应控制在 10 小时之内。当自动化运行时，从晚上 10点开始到第二天 8 点出结果，预留工作时间进行问题排查和特定分支的性能测试任务。
写入场景建议在 4 小时内结束，查询场景在 2 小时内结束，留 4 个小时给订阅、流计算、同步、数据接入、以及各个语言连接器。

## 6. 其他需求

### 6.1 调度需求

#### 6.1.1 例行执行

分别测试 main 分支和 3.0 分支的性能，可隔天执行一次。如果性能测试的时间足够快，能在一天内执行完更好。

#### 6.1.2 自动调度

通过 Jenkins 调度，提供任务界面，输入 commitId、场景编号集合等执行信息，即可自动执行。

#### 6.1.3 手动调度

在开发人员自己指定的机器上，完成运行前的必要准备后，通过脚本输入 commitId、场景编号集合等执行信息，即可运行。

#### 6.1.4 其他说明

commitId 是一个集合，需配置各仓库的 commitId，第一阶段仅涉及 TDengine 和 taos-tools 仓库

### 6.2 界面需求

执行结果可通过两种方式查看
1. Grafana 界面
2. CSV 文件

### 6.3 场景定义

所有测试场景，可通过文本文件方式配置。可使用编号作为文件名，例如 W001.json，该文件可以使用 taosBenchmark 直接执行。
