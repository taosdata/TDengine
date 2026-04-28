# TSBS 测试报告 v3.4.0.0

这个测试是为了对比验证新版本的 timescaledb & InfluxDB

## 测试总结

1. 相较 influxDB 和 timescaleDB，TDengine 的写入性能依旧是很大优势，特别是大数据量场景（1000万）子表下，
   1. cpu 场景测试，TDengine 写入速度是 influxdb 的 32 倍，是 timescaleDB 的 5.2 倍
   1. iot 场景测试，TDengine 写入速度是 influxdb 的 2.1 倍，是 timescaleDB 的 2.8 倍
1. 相较 timescaledb，子表规模不大，TDengine 的查询性能优势没有那么明显：
   1. cpu 场景测试，包含 scale=4000 和 100，共 30 项查询中，有 9 项低于 timescaleDB，落后幅度约在 7%-35%之间。
   1. iot 场景测试，包含 scale=4000 和 100，共 24 项查询中，有 5 项低于 timescaleDB，落后幅度约在 5%-15%之间。
   1. 其余场景，查询性能优于 timescaleDB，提升约在 10%-2500% 之间。
1. 相较 influxDB，子表规模不大，TDengine 的查询性能优势还是很大，查询性能均优于 influxDB，提升约在 0-7200% 之间。

## 变更历史

| 日期 | 版本 | 负责人 | 主要修改内容 |
| --- | --- | --- | --- |
| 2026/1/22 | 1.0 | 陈浩然 | 起草 |

## 测试环境

### 测试分支和版本

| TDengine 版本 | TDengine 描述 | TimescaleDB | InfluxDB |
| --- | --- | --- | --- |
| 3.4.0.0 | TDengine TSDB Enterprise 3.4.0.0 | ver-2.13，psql：14.12 | ver-1.8.10 |

### 测试机器

使用报告里的机器：[基于 TSBS 标准数据集时序数据库 TimescaleDB、InfluxDB 与 TDengine 的性能对比测试](https://www.taosdata.com/performance-comparison-influxdb-and-timescaledb-vs-tdengine)

| 角色 | CPU | Memory | Disk |
| --- | --- | --- | --- |
| 服务器 | Intel(R) Xeon(R) CPU E5-2686 v4 @ 2.30GHz 32vCPU | 244GiB | 800G SSD，3000 IOPS. 吞吐量上限是 125 MiB/Sec |
| 客户端 | Intel(R) Xeon(R) CPU E5-2686 v4 @ 2.30GHz 32vCPU | 244GiB | 800G SSD，3000 IOPS. 吞吐量上限是 125 MiB/Sec |

### 测试服务器系统

为运行测试脚本，服务器 OS 需要是 ubuntu20.04 以上的系统。服务器系统信息如下：

```text
Linux u1-59 5.4.0-172-generic #190-Ubuntu SMP Fri Feb 2 23:24:22 UTC 2024 x86_64 x86_64 x86_64 GNU/Linux
```

### 编译选项

```bash
cmake .. -Ddisable_assert=True -DSIMD_SUPPORT=true -DCMAKE_BUILD_TYPE=Release -DBUILD_TOOLS=false -DBUILD_CONTRIB=on && make -j 10 && make install
```

## 性能统计规则

### 写入速度

写入速度的性能变化率计算公式：

$$写入速度变化率=\frac{TDengine写入速度}{otherDB写入速度} \times 100\%$$

当变化率大于 100%时，说明 TDengine 写入速度快于竞对 DB；反之意味着写入速度慢于竞对 DB。

### 查询时延

查询时延的性能变化率的计算公式：

$$查询时延变化比率=\frac{otherDB查询时延}{TDengine查询时延} \times 100\%$$

当查询时延变化率大于 1 时，说明查询性能高于竞对；当该值小于 1 时，意味着查询性能低于竞对。

### 查询 QPS

## cpu-only 测试结果

测试方法参考官网报告：[基于 TSBS 标准数据集时序数据库性能对比测试](https://www.taosdata.com/performance-comparison-influxdb-and-timescaledb-vs-tdengine)

### cpu-only 写入结果

写入结果：指标是写入速度 metrics/s，比值越大越好。

| 场景 | TDengine | InfluxDB | TDengine/InfluxDB | TimescaleDB | TDengine/TimescaleDB |
| --- | --- | --- | --- | --- | --- |
| 100 devices × 10 metrics | 13190724.53 | 4140760.04 | 318.56% | 1727014.05 | 763.79% |
| 4,000 devices × 10 metrics | 12879012.27 | 2504568.02 | 514.22% | 1409469.62 | 913.75% |
| 100,000 devices × 10 metrics | 9715350.48 | 1648090.74 | 589.49% | 1369486.5 | 709.42% |
| 1,000,000 devices × 10 metrics | 5907131.96 | 560538.24 | 1053.83% | 1323559.49 | 446.31% |
| 10,000,000 devices × 10 metrics | 5728377.49 | 174595.75 | 3280.94% | 1099911.88 | 520.80% |

### cpu-only 查询结果

指标是时延，单位：ms。比值越大越好。

#### scale=4000

| 查询分类 | 查询项 | TDengine | InfluxDB | InfluxDB/TDengine | TimescaleDB | TimescaleDB/TDengine |
| --- | --- | --- | --- | --- | --- | --- |
| Simple Rollups | single-groupby-1-1-1 | 1.7 | 1.7 | 100.00% | 1.37 | 80.59% |
| | single-groupby-1-1-12 | 2.68 | 9.16 | 341.79% | 3.51 | 130.97% |
| | single-groupby-1-8-1 | 3.81 | 4.3 | 112.86% | 2.5 | 65.62% |
| | single-groupby-5-1-1 | 1.77 | 4.39 | 248.02% | 1.66 | 93.79% |
| | single-groupby-5-1-12 | 3.71 | 33.61 | 905.93% | 4.72 | 127.22% |
| | single-groupby-5-8-1 | 4.26 | 14.07 | 330.28% | 3.58 | 84.04% |
| Aggregates | cpu-max-all-1 | 2.1 | 6.22 | 296.19% | 3.5 | 166.67% |
| | cpu-max-all-8 | 5.02 | 20.24 | 403.19% | 16.69 | 332.47% |
| Double-Rollups | double-groupby-1 | 289.4 | 2676.55 | 924.86% | 2896.27 | 1000.78% |
| | double-groupby-5 | 507.97 | 11755.45 | 2314.20% | 4856.33 | 956.03% |
| | double-groupby-all | 786.84 | 23704.11 | 3012.57% | 7501.91 | 953.42% |
| Thresholds | high-cpu-1 | 2.74 | 18.13 | 661.68% | 2.27 | 82.85% |
| | high-cpu-all | 3449.40 | 52794.17 | 1530.53% | 2732.51 | 79.22% |
| Complex Queries | groupby-orderby-limit | 327.71 | 23559.81 | 7189.23% | 8846.13 | 2699.38% |
| | lastpoint | 69.54 | 2812.41 | 4044.31% | 966.42 | 1389.73% |

#### scale=100

| 查询分类 | 查询项 | TDengine | InfluxDB | InfluxDB/TDengine | TimescaleDB | TimescaleDB/TDengine |
| --- | --- | --- | --- | --- | --- | --- |
| Simple Rollups | single-groupby-1-1-1 | 1.51 | 1.86 | 123.18% | 1.39 | 92.05% |
| | single-groupby-1-1-12 | 2.42 | 9.54 | 394.21% | 3.37 | 139.26% |
| | single-groupby-1-8-1 | 1.94 | 4.02 | 207.22% | 2.28 | 117.53% |
| | single-groupby-5-1-1 | 1.58 | 4.48 | 283.54% | 1.61 | 101.90% |
| | single-groupby-5-1-12 | 3.5 | 37.1 | 1060.00% | 4.58 | 130.86% |
| | single-groupby-5-8-1 | 2.69 | 14.12 | 524.91% | 3.35 | 124.54% |
| Aggregates | cpu-max-all-1 | 1.92 | 5.98 | 311.46% | 3.37 | 175.52% |
| | cpu-max-all-8 | 4.43 | 22.13 | 499.55% | 16.22 | 366.14% |
| Double-Rollups | double-groupby-1 | 15.99 | 78.95 | 493.75% | 85.57 | 535.15% |
| | double-groupby-5 | 33.77 | 338.71 | 1002.99% | 135.94 | 402.55% |
| | double-groupby-all | 54.89 | 639.63 | 1165.29% | 192.62 | 350.92% |
| Thresholds | high-cpu-1 | 2.51 | 13.62 | 542.63% | 2.22 | 88.45% |
| | high-cpu-all | 85.61 | 1137.81 | 1329.06% | 68.23 | 79.70% |
| Complex Queries | groupby-orderby-limit | 20.47 | 563.91 | 2754.81% | 255.27 | 1247.04% |
| | lastpoint | 5.45 | 76.06 | 1395.60% | 23.96 | 439.63% |

## IoT 测试结果

测试方法参考官网报告：[IoT 场景性能对比测试](https://www.taosdata.com/iot-performance-comparison-influxdb-and-timescaledb-vs-tdengine)

### IoT 写入测试结果

指标是写入速度 metrics/s。

| 场景 | TDengine | InfluxDB | TDengine/InfluxDB | TimescaleDB | TDengine/TimescaleDB |
| --- | --- | --- | --- | --- | --- |
| 100 devices × 10 metrics | 9084349.05 | 4187129.42 | 216.96% | 1220670.97 | 744.21% |
| 4,000 devices × 10 metrics | 8981433.71 | 2038670.65 | 440.55% | 994633.3 | 902.99% |
| 100,000 devices × 10 metrics | 5668574.97 | 1586586.88 | 357.28% | 700944.93 | 808.70% |
| 1,000,000 devices × 10 metrics | 3118216.02 | 589358.65 | 529.09% | 889245.79 | 350.66% |
| 10,000,000 devices × 10 metrics | 2343295.82 | 1099911.88 | 213.04% | 830717.38 | 282.08% |

### IoT 查询测试结果

指标是时延，单位：ms。

#### IoT scale=4000

| 查询类型 | TDengine | InfluxDB | InfluxDB/TDengine | TimescaleDB | TimescaleDB/TDengine |
| --- | --- | --- | --- | --- | --- |
| last-loc | 13.23 | 632.24 | 4778.84% | 14.3 | 108.09% |
| low-fuel | 40.72 | 514.53 | 1263.58% | 350.68 | 861.20% |
| high-load | 12.43 | 675.34 | 5433.15% | 14.05 | 113.03% |
| stationary-trucks | 28.79 | 2374.1 | 8246.27% | 105.95 | 368.01% |
| long-driving-sessions | 65.33 | 254.51 | 389.58% | 1132.76 | 1733.90% |
| long-daily-sessions | 240.04 | 1040.44 | 433.44% | 6144.4 | 2559.74% |
| avg-vs-projected-fuel-consumption | 3509.26 | 20374.16 | 580.58% | 13783.17 | 392.77% |
| avg-daily-driving-duration | 4393.04 | 34538.75 | 786.22% | 22416.7 | 510.28% |
| avg-daily-driving-session | 4177.78 | 61500.37 | 1472.08% | 23077.63 | 552.39% |
| avg-load | 369.86 | 489371.44 | 132312.62% | 7772.37 | 2101.44% |
| daily-activity | 2424.5 | 7773.78 | 320.63% | 25030.77 | 1032.41% |
| breakdown-frequency | 5507.23 | 288760.63 | 5243.30% | 19963.32 | 362.49% |

#### IoT scale=100

| 查询类型 | TDengine | InfluxDB | InfluxDB/TDengine | TimescaleDB | TimescaleDB/TDengine |
| --- | --- | --- | --- | --- | --- |
| last-loc | 1.43 | 14.87 | 1039.86% | 1.31 | 91.61% |
| low-fuel | 7.15 | 14.67 | 205.17% | 6.86 | 95.94% |
| high-load | 1.43 | 16.09 | 1125.17% | 1.32 | 92.31% |
| stationary-trucks | 4.64 | 57.01 | 1228.66% | 3.97 | 85.56% |
| long-driving-sessions | 6.17 | 10.52 | 170.50% | 63.84 | 1034.68% |
| long-daily-sessions | 15.62 | 35.58 | 227.78% | 156.32 | 1000.77% |
| avg-vs-projected-fuel-consumption | 325.94 | 525.05 | 161.09% | 375.28 | 115.14% |
| avg-daily-driving-duration | 298.61 | 741.03 | 248.16% | 395.54 | 132.46% |
| avg-daily-driving-session | 180.81 | 1320.53 | 730.34% | 424.37 | 234.70% |
| avg-load | 24.68 | 13299.12 | 53886.22% | 220.38 | 892.95% |
| daily-activity | 164.75 | 393.18 | 238.65% | 430.22 | 261.14% |
| breakdown-frequency | 434.98 | 6820.13 | 1567.92% | 397.64 | 91.42% |
