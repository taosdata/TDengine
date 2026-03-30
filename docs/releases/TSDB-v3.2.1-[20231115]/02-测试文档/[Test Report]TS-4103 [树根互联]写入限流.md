# [Test Report]TS-4103 [树根互联]写入限流

## 1. 问题描述

树根互联写入时产生大量stt文件，影响查询和compact

## 2. 需求描述

限制stt文件产生的数量

## 3. 测试版本

3.0最新版本和3.1.1.7 企业版（3.1 分支）

## 4. 数据准备和测试场景

### 4.1 场景1: 

```sql {wrap}
建库：
CREATE DATABASE `test_10bi` BUFFER 256 CACHESIZE 1 CACHEMODEL 'none' COMP 2 DURATION 14400m WAL_FSYNC_PERIOD 3000 MAXROWS 4096 MINROWS 1000 STT_TRIGGER 2 KEEP 5256000m,5256000m,5256000m PAGES 256 PAGESIZE 4 PRECISION 'ms' REPLICA 1 WAL_LEVEL 1 VGROUPS 40 SINGLE_STABLE 0 TABLE_PREFIX 0 TABLE_SUFFIX 0 TSDB_PAGESIZE 4 WAL_RETENTION_PERIOD 3600 WAL_RETENTION_SIZE 0
```

子表：2000000，每个表500条记录。 rows: 1000000000
由于库越来越多，不太统计stt生成过程多文件个数了，昨天已经验证限流之后文件个数变少了，现在主要记录性能变化记录。

|  | 重要参数 | 插入耗时 | min/max/avg | count(*)耗时 | 备注 |
| --- | --- | --- | --- | --- | --- |
| 3.0分支 | 2328.8 s | 0.887ms 6508.9ms 23.04ms | 63.22s | 1个dnodes |
| 3.1分支 | 15791.6s | 1.596ms 2881.98ms 157.41ms | 713.377s | 6个dnodes |


### 4.2 场景2:

和场景1的最重要的区别是减少数量，方便快速出结果。
子表：2000000--》500000，每个表500条记录。 rows: 250000000。vgroups：40--〉10

|  | 重要参数 | 插入耗时 | min/max/avg | count(*)耗时 | 备注 |
| --- | --- | --- | --- | --- | --- |
| 3.0版本 | 586.5 s | 1.094ms 4461.8ms 22.904ms | 16.89s |  |
| 3.1版本 | 556.22s | 1.034ms 5414.7ms 21.98ms | 14.604s |  |


### 4.3 场景3:

数据量=场景2。区别只有3.1版本，记录不同minrow和stt_triger时的耗时。 


| 3.1版本 重要参数 | 插入耗时 | min/max/avg | count(*)耗时 | 备注 |
| --- | --- | --- | --- | --- |
|  |  |  |  |  |
| minrow=100 stt_triger=2 | 556.22s | 1.034ms 5414.7ms 21.98ms | 14.604s |  |
| minrow=100 stt_triger=4 | 598.51s | 1.019ms 6415.15ms 23.26ms | 21.589s |  |
| minrow=100 stt_triger=8 | 550.7s | 1.034ms 3249.7ms 21.526ms | 33.014s |  |


### 4.4 场景4:

数据量=场景2。区别只有3.1版本，记录启动限速和不启动限速时的耗时。

| 3.1版本 重要参数 | 插入耗时 | min/max/avg | count(*)耗时 | 备注 |
| --- | --- | --- | --- | --- |
| minrow=1000 stt_triger=2 | 3293.31s | 1.478ms 1448.7ms 130.82ms | 95.60s | 限速 db：test_10bi_hz_1000row |
| minrow=100 stt_triger=2 | 3291.48s | 1.56ms 1202.07ms 130.69ms | 138.395s | 限速 db：test_10bi_hz_100row |
| minrow=1000 stt_triger=2 | 3314.01s | 1.468ms 938.79ms 131.61ms | 97.88s | 修改代码，不限速 db：test_10bi_hz_1000row_no |
| minrow=100 stt_triger=2 | 3316.60s | 1.17ms 1726.18ms 131.79ms | 147.95s | 修改代码，不限速 db：test_10bi_hz_100row_no |


### 4.5 场景5:

数据量=场景2。区别只有3.1.6及以前的版本【未限速】和3.1.7【限制】版本。

| 3.1版本 重要参数 | 插入耗时 | min/max/avg | count(*)耗时 | 备注 |
| --- | --- | --- | --- | --- |
| minrow=1000 stt_triger=2 | 3293.31s | 1.478ms 1448.7ms 130.82ms | 95.60s | 3.1.7 限速 db：test_10bi_hz_1000row |
| minrow=100 stt_triger=2 | 3291.48s | 1.56ms 1202.07ms 130.69ms | 138.395s | 3.1.7 限速 db：test_10bi_hz_100row |
| minrow=100 stt_triger=2 | 3422.54s | 1.61ms 1381.21ms 136.05ms | 136.90s | 3.16 不限速 db：test_10bi_hz_100row_316 |
| minrow=100 stt_triger=2 | 3276.10s | 1.497ms 1446.13ms 129.94ms | 143.51s | 3.14 不限速 db：test_10bi_hz_100row_314 |
