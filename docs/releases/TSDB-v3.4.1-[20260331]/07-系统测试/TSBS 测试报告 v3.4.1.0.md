# TSBS 测试报告 v3.4.1.0

> 来源：[TSBS 测试结果 v3.4.1.0](https://taosdata.feishu.cn/wiki/KhiVwUzX2iMimlkFoeIc2RxQnjc)

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
| 2026/4/27 | 1.0 | 陈浩然 | 起草 |

## 测试环境

### 测试分支和版本

| TDengine 版本 | TDengine 描述 | TimescaleDB | InfluxDB |
| --- | --- | --- | --- |
| 3.4.1.0 | TDengine TSDB Enterprise 3.4.1.0 | ver-2.13，psql：14.12 | ver-1.8.10 |

### 测试机器

使用报告里的机器：[基于 TSBS 标准数据集时序数据库 TimescaleDB、InfluxDB 与 TDengine 的性能对比测试](https://www.taosdata.com/performance-comparison-influxdb-and-timescaledb-vs-tdengine)

| | CPU | Memory | Disk |
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

| | TDengine | InfluxDB | TDengine/InfluxDB | TimescaleDB | TDengine/TimescaleDB |
| --- | --- | --- | --- | --- | --- |
| 100 devices × 10 metrics | 12900634.36 | 4168372.88 | 309.49% | 1737232.83 | 742.60% |
| 4,000 devices × 10 metrics | 12966175.26 | 2525957.94 | 513.32% | 1398111.74 | 927.41% |
| 100,000 devices × 10 metrics | 9637692.84 | 1656796.05 | 581.71% | 1359681.35 | 708.82% |
| 1,000,000 devices × 10 metrics | 5864428.34 | 565674.01 | 1036.72% | 1313468.84 | 446.48% |
| 10,000,000 devices × 10 metrics | 5758683 | 175469.41 | 3281.87% | 1106178.59 | 520.59% |

### cpu-only 查询结果

指标是时延，单位：ms。比值越大越好。

#### scale=4000

| 查询分类 | | TDengine | InfluxDB | InfluxDB/TDengine | TimescaleDB | TimescaleDB/TDengine |
| --- | --- | --- | --- | --- | --- | --- |
| Simple Rollups | single-groupby-1-1-1 | 1.63 | 1.74 | 106.75% | 1.32 | 80.98% |
| | single-groupby-1-1-12 | 2.59 | 9.35 | 361.00% | 3.67 | 141.70% |
| | single-groupby-1-8-1 | 3.95 | 4.46 | 112.91% | 2.64 | 66.84% |
| | single-groupby-5-1-1 | 1.81 | 4.58 | 253.04% | 1.58 | 87.29% |
| | single-groupby-5-1-12 | 3.54 | 32.48 | 917.51% | 4.92 | 138.98% |
| | single-groupby-5-8-1 | 4.01 | 13.63 | 339.90% | 3.7 | 92.27% |
| Aggregates | cpu-max-all-1 | 2.2 | 6.03 | 274.09% | 3.33 | 151.36% |
| | cpu-max-all-8 | 5.18 | 21.39 | 412.93% | 17.47 | 337.26% |
| Double-Rollups | double-groupby-1 | 280.94 | 2,745.76 | 977.35% | 2804.79 | 998.36% |
| | double-groupby-5 | 525.26 | 12,036.39 | 2291.51% | 4997.59 | 951.45% |
| | double-groupby-all | 826 | 24,800.87 | 3002.53% | 7749.66 | 938.22% |
| Thresholds | high-cpu-1 | 2.8 | 19.13 | 683.21% | 2.37 | 84.64% |
| | high-cpu-all | 3,570.00 | 54,355.92 | 1522.57% | 2662.02 | 74.57% |
| Complex Queries | groupby-orderby-limit | 313.56 | 24,653.82 | 7862.55% | 9291.56 | 2963.25% |
| | lastpoint | 71.52 | 2,678.06 | 3744.49% | 942.94 | 1318.43% |

#### scale=100

| 查询分类 | | TDengine | InfluxDB | InfluxDB/TDengine | TimescaleDB | TimescaleDB/TDengine |
| --- | --- | --- | --- | --- | --- | --- |
| Simple Rollups | single-groupby-1-1-1 | 1.46 | 1.93 | 132.19% | 1.34 | 91.78% |
| | single-groupby-1-1-12 | 2.34 | 9.93 | 424.36% | 3.52 | 150.43% |
| | single-groupby-1-8-1 | 2.04 | 4.11 | 201.47% | 2.37 | 116.18% |
| | single-groupby-5-1-1 | 1.62 | 4.66 | 287.65% | 1.53 | 94.44% |
| | single-groupby-5-1-12 | 3.34 | 35.94 | 1076.05% | 4.7 | 140.72% |
| | single-groupby-5-8-1 | 2.57 | 13.66 | 531.52% | 3.51 | 136.58% |
| Aggregates | cpu-max-all-1 | 1.98 | 5.69 | 287.37% | 3.21 | 162.12% |
| | cpu-max-all-8 | 4.64 | 23.39 | 504.09% | 16.72 | 360.34% |
| Double-Rollups | double-groupby-1 | 15.51 | 82.20 | 529.98% | 82.96 | 534.88% |
| | double-groupby-5 | 34.99 | 351.11 | 1003.46% | 142.39 | 406.94% |
| | double-groupby-all | 57.72 | 658.33 | 1140.56% | 201.88 | 349.76% |
| Thresholds | high-cpu-1 | 2.57 | 14.34 | 557.98% | 2.32 | 90.27% |
| | high-cpu-all | 90.01 | 1,173.06 | 1303.26% | 66.48 | 73.86% |
| Complex Queries | groupby-orderby-limit | 19.35 | 580.75 | 3001.29% | 263.62 | 1362.38% |
| | lastpoint | 5.68 | 73.33 | 1291.02% | 23.35 | 411.09% |

## IoT 测试结果

测试方法参考官网报告：[IoT 场景性能对比测试](https://www.taosdata.com/iot-performance-comparison-influxdb-and-timescaledb-vs-tdengine)

### IoT 写入测试结果

指标是写入速度 metrics/s。

| | TDengine | InfluxDB | TDengine/InfluxDB | TimescaleDB | TDengine/TimescaleDB |
| --- | --- | --- | --- | --- | --- |
| 100 devices × 10 metrics | 8743769.52 | 4328104.61 | 202.02% | 1169435.54 | 747.69% |
| 4,000 devices × 10 metrics | 8631906.35 | 2102199.04 | 410.61% | 1031110.65 | 837.15% |
| 100,000 devices × 10 metrics | 5918907.47 | 1635974.52 | 361.80% | 733643.21 | 806.78% |
| 1,000,000 devices × 10 metrics | 3211811.07 | 609201.23 | 527.22% | 851690.22 | 377.11% |
| 10,000,000 devices × 10 metrics | 2252753.45 | 1055370.41 | 213.46% | 857956.5 | 262.57% |

### IoT 查询测试结果

指标是时延，单位：ms。

#### IoT scale=4000

| 查询类型 | TDengine | InfluxDB | InfluxDB/TDengine | TimescaleDB | TimescaleDB/TDengine |
| --- | --- | --- | --- | --- | --- |
| ast-loc | 13.79 | 608.74 | 4414.36% | 13.61 | 98.69% |
| low-fuel | 42.45 | 498.11 | 1173.40% | 363.38 | 856.02% |
| high-load | 12.97 | 646.47 | 4984.35% | 13.47 | 103.86% |
| stationary-trucks | 29.72 | 2262.57 | 7612.95% | 101.58 | 341.79% |
| long-driving-sessions | 63.04 | 244.21 | 387.39% | 1169.26 | 1854.79% |
| long-daily-sessions | 232.32 | 990.59 | 426.39% | 6367.36 | 2740.77% |
| avg-vs-projected-fuel-consumption | 3355.17 | 19661.35 | 586.00% | 13220.86 | 394.04% |
| avg-daily-driving-duration | 4544.93 | 33088.91 | 728.04% | 23161.48 | 509.61% |
| avg-daily-driving-session | 4355.28 | 64124.47 | 1472.34% | 23980.67 | 550.61% |
| avg-load | 382.34 | 512697.63 | 134094.69% | 8131.06 | 2126.66% |
| daily-activity | 2514.51 | 7523.46 | 299.20% | 23955.6 | 952.69% |
| breakdown-frequency | 5251.91 | 299183.63 | 5696.66% | 19160.5 | 364.83% |

#### IoT scale=100

| 查询类型 | TDengine | InfluxDB | InfluxDB/TDengine | TimescaleDB | TimescaleDB/TDengine |
| --- | --- | --- | --- | --- | --- |
| last-loc | 1.36 | 14.29 | 1050.74% | 1.37 | 100.74% |
| low-fuel | 7.44 | 14.09 | 189.38% | 6.57 | 88.31% |
| high-load | 1.49 | 16.88 | 1132.89% | 1.37 | 91.95% |
| stationary-trucks | 4.47 | 58.98 | 1319.46% | 3.82 | 85.46% |
| long-driving-sessions | 6.43 | 10.84 | 168.58% | 66.25 | 1030.33% |
| long-daily-sessions | 16.3 | 36.95 | 226.69% | 162.72 | 998.28% |
| avg-vs-projected-fuel-consumption | 335.92 | 548.94 | 163.41% | 389.5 | 115.95% |
| avg-daily-driving-duration | 311.14 | 768.1 | 246.87% | 382.12 | 122.81% |
| avg-daily-driving-session | 171.91 | 1370.41 | 797.17% | 441.68 | 256.93% |
| avg-load | 25.6 | 12738.69 | 49760.51% | 213.15 | 832.62% |
| daily-activity | 159.26 | 412.74 | 259.16% | 446.52 | 280.37% |
| breakdown-frequency | 421.88 | 7051.86 | 1671.53% | 379.91 | 90.05% |
