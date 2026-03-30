# TSBS 测试结果 v3.3.4.10.1210-aws-官网报告

## 1. 本期报告只更新 TD engine 的数据内容，竞对数据取自官网的 2023年tsbs报告。

## 2. 测试总结

1. 相较 influxDB 和 timescaleDB，TDengine 的写入性能依旧是很大优势，特别是大数据量场景（1000万）子表下，
   - cpu 场景测试，TDengine 写入速度是 influxdb 的 30.9 倍，是 timescaleDB 的 4.2 倍
   - iot 场景测试，TDengine 写入速度是 influxdb 的 26 倍，是 timescaleDB 的 1.7 倍
2. 相较 timescaledb，子表规模不大，TDengine 的查询性能优势没有那么明显：
   - iot 场景测试，包含 scale=4000 和 100，共 24 项查询中，有 6 项低于 timescaleDB，落后幅度约在 6%-13%之间。其余均为优势，提升110%-7800%。
   - cpu 场景测试，查询性能均优于 timescaleDB，提升约在 20% -3800% 之间。
3. 相较 influxDB，子表规模不大，TDengine 的查询性能优势还是很大，查询性能均优于 influxDB，提升约在 0 -6900% 之间。
  
## 3. 变更历史

| 日期 | 版本 | 负责人 | 主要修改内容 |
| --- | --- | --- | --- |
| 2024/12/27 | 1.0 | 陈浩然 | 创建 |

## 4. 测试环境

### 4.1 测试分支和版本

本次 tsbs 的版本使用支持 stmt2 的分支：feat/xftan/TD-32213-stmt2，写入使用 stmt2 写入。

| TimescaleDB | InfluxDB |
| --- | --- |
| 3.3.4.9.1210 | taosd version: 3.3.4.8.alpha compatible_version: 3.0.0.0 git: a4a8cbaba935e1a5b6d1f4093c986f9efb727ece gitOfInternal: 7be6d0e5b4601267f9160cf2ab08cdc6da14b316 build: Linux-x64 2024-12-10 02:24:49 +0000 | iot:ver-2.10 cpu:ver-2.6 | ver-1.8.10 |

### 4.2 测试机器

使用报告里的机器：[基于 TSBS 标准数据集时序数据库 TimescaleDB、InfluxDB 与 TDengine 的性能对比测试](https://www.taosdata.com/performance-comparison-influxdb-and-timescaledb-vs-tdengine)
|  | CPU | Memory | Disk |
| --- | --- | --- | --- |
| 服务器 | Intel(R) Xeon(R) CPU E5-2686 v4 @ 2.30GHz 32vCPU | 244GiB | 800G SSD，3000 IOPS. 吞吐量上限是 125 MiB/Sec |
| 客户端 | Intel(R) Xeon(R) CPU E5-2686 v4 @ 2.30GHz 32vCPU | 244GiB | 800G SSD，3000 IOPS. 吞吐量上限是 125 MiB/Sec |

### 4.3 测试服务器系统

为运行测试脚本，服务器 OS 需要是 ubuntu20.04 以上的系统。服务器系统信息如下：
Linux u1-59 5.4.0-172-generic #190-Ubuntu SMP Fri Feb 2 23:24:22 UTC 2024 x86_64 x86_64 x86_64 GNU/Linux

### 4.4 编译选项

```powershell {wrap}
cmake .. -Ddisable_assert=True -DSIMD_SUPPORT=true   -DCMAKE_BUILD_TYPE=Release -DBUILD_TOOLS=false  -DBUILD_CONTRIB=on    && make -j 10 && make install
```

## 5. 性能统计规则

### 5.1 写入速度变化

写入速度的性能变化率计算公式：
<equation>写入速度变化率=\frac{ TDengine写入速度}{otherDB 写入速度}*100\%
</equation>
当变化率大于 100%时，说明 TDengine 写入速度快 于竞对 DB；反之意味着写入速度慢于竞对 DB

### 5.2 查询时延变化

查询时延的性能变化率的计算公式：
<equation>查询时延变化比率=\frac{otherDB查询时延}{TDengine 查询时延}* 100\%
</equation>
当查询时延变化率为负值时，说明查询性能下降；当该值为正时，意味着查询性能提升

### 5.3 查询 QPS（无）

## 6. cpu-only测试结果

测试方法参考官网报告：
<!-- Unsupported block type: 999 -->
本次重跑TDengine 的结果，其余结果按着之前的测试报告。

### 6.1 cpu-only 写入结果

指标是写入速度metrics/s。该值越大，说明写入性能越好。
比值结果越大，说明性能越好。
|  | TDengine | InfluxDB | TDengine/InfluxDB | TimescaleDB | TDengine/TimescaleDB |
| --- | --- | --- | --- | --- | --- |
| 100 devices × 10 metrics | 13190724.53 | 4149000 | B2/C2 | 2135000 | B2/E2 |
| 4,000 devices × 10 metrics | 12879012.27 | 2354000 | B3/C3 | 1840000 | B3/E3 |
| 100,000 devices × 10 metrics | 9715350.48 | 1637000 | B4/C4 | 1333000 | B4/E4 |
| 1,000,000 devices × 10 metrics | 5907131.96 | 561000 | B5/C5 | 1439000 | B5/E5 |
| 10,000,000 devices × 10 metrics | 5728377.49 | 185000 | B6/C6 | 1294000 | B6/E6 |


### 6.2 cpu-only 查询结果

指标是时延，单位：ms。该值越小，说明查询性能越好。
比值结果越大，说明查询性能越好。

#### 6.2.1 scale=4000

| 查询分类 |  | TDengine | InfluxDB | InfluxDB/TDengine | TimescaleDB | TimescaleDB/TDengine |
| --- | --- | --- | --- | --- | --- | --- |
| Simple Rollups | single-groupby-1-1-1 | 1.7 | 1.71 | D2/C2 | 3.27 | F2/C2 |
|  | single-groupby-1-1-12 | 2.68 | 9.4 | D3/C3 | 5.07 | F3/C3 |
|  | single-groupby-1-8-1 | 3.81 | 4.1 | D4/C4 | 4.56 | F4/C4 |
|  | single-groupby-5-1-1 | 1.77 | 4.4 | D5/C5 | 3.34 | F5/C5 |
|  | single-groupby-5-1-12 | 3.71 | 36.43 | D6/C6 | 7.02 | F6/C6 |
|  | single-groupby-5-8-1 | 4.26 | 13.58 | D7/C7 | 9.6 | F7/C7 |
| Aggregates | cpu-max-all-1 | 2.1 | 5.86 | D8/C8 | 5.54 | F8/C8 |
|  | cpu-max-all-8 | 5.02 | 20.64 | D9/C9 | 23.72 | F9/C9 |
| Double-Rollups | double-groupby-1 | 289.4 | 2785.23 | D10/C10 | 5467.91 | F10/C10 |
|  | double-groupby-5 | 507.97 | 11702.49 | D11/C11 | 10984.63 | F11/C11 |
|  | double-groupby-all | 786.84 | 23509.02 | D12/C12 | 16660.7 | F12/C12 |
| Thresholds | high-cpu-1 | 2.74 | 17.15 | D13/C13 | 4.05 | F13/C13 |
|  | high-cpu-all | 3449.4 | 52884.94 | D14/C14 | 4328.64 | F14/C14 |
| Complex Queries | groupby-orderby-limit | 327.71 | 23169.15 | D15/C15 | 12784.92 | F15/C15 |
|  | lastpoint | 69.54 | 2808 | D16/C16 | 755.37 | F16/C16 |

#### 6.2.2 scale=100

| 查询分类 |  | TDengine | InfluxDB | InfluxDB/TDengine | TimescaleDB | TimescaleDB/TDengine |
| --- | --- | --- | --- | --- | --- | --- |
| Simple Rollups | single-groupby-1-1-1 | 1.51 | 2.01 | D2/C2 | 2.93 | F2/C2 |
|  | single-groupby-1-1-12 | 2.42 | 9.4 | D3/C3 | 4.87 | F3/C3 |
|  | single-groupby-1-8-1 | 1.94 | 3.98 | D4/C4 | 4.3 | F4/C4 |
|  | single-groupby-5-1-1 | 1.58 | 4.4 | D5/C5 | 3.19 | F5/C5 |
|  | single-groupby-5-1-12 | 3.5 | 36.77 | D6/C6 | 6.38 | F6/C6 |
|  | single-groupby-5-8-1 | 2.69 | 13.71 | D7/C7 | 5.91 | F7/C7 |
| Aggregates | cpu-max-all-1 | 1.92 | 5.92 | D8/C8 | 5.55 | F8/C8 |
|  | cpu-max-all-8 | 4.43 | 21.88 | D9/C9 | 22.83 | F9/C9 |
| Double-Rollups | double-groupby-1 | 15.99 | 78.61 | D10/C10 | 116.66 | F10/C10 |
|  | double-groupby-5 | 33.77 | 340.53 | D11/C11 | 346.48 | F11/C11 |
|  | double-groupby-all | 54.89 | 642.16 | D12/C12 | 489.04 | F12/C12 |
| Thresholds | high-cpu-1 | 2.51 | 13.51 | D13/C13 | 3.92 | F13/C13 |
|  | high-cpu-all | 85.61 | 1129.62 | D14/C14 | 104.68 | F14/C14 |
| Complex Queries | groupby-orderby-limit | 20.47 | 533.5 | D15/C15 | 367.4 | F15/C15 |
|  | lastpoint | 5.45 | 74.55 | D16/C16 | 17.64 | F16/C16 |

## 7. IoT 测试结果

测试方法参考官网报告：https://www.taosdata.com/iot-performance-comparison-influxdb-and-timescaledb-vs-tdengine
只更新了 TDengine 测试数据，其余结果按着之前的测试报告结果。

### 7.1 IoT写入测试结果

指标是写入速度metrics/s。该值越大，说明写入性能越好。
比值结果越大，说明性能越好。
|  | TDengine | InfluxDB | TDengine/InfluxDB | TimescaleDB | TDengine/TimescaleDB |
| --- | --- | --- | --- | --- | --- |
| 100 devices × 10 metrics | 9084349.05 | 4245000 | B2/C2 | 2510000 | B2/E2 |
| 4,000 devices × 10 metrics | 8981433.71 | 2072000 | B3/C3 | 2113000 | B3/E3 |
| 100,000 devices × 10 metrics | 5668574.97 | 1680000 | B4/C4 | 1251000 | B4/E4 |
| 1,000,000 devices × 10 metrics | 3118216.02 | 686000 | B5/C5 | 1484000 | B5/E5 |
| 10,000,000 devices × 10 metrics | 2343295.82 | 87000 | B6/C6 | 1345000 | B6/E6 |

### 7.2 IoT查询测试结果

指标是时延，单位：ms。该值越小，说明查询性能越好。
比值结果越大，说明查询性能越好。

#### 7.2.1 scale=4000

| 查询类型 | TDengine | InfluxDB | InfluxDB/TDengine | TimescaleDB | TimescaleDB/TDengine |
| --- | --- | --- | --- | --- | --- |
| last-loc | 13.23 | 562.86 | C2/B2 | 11.77 | E2/B2 |
| low-fuel | 40.72 | 635 | C3/B3 | 416.75 | E3/B3 |
| high-load | 12.43 | 861.13 | C4/B4 | 11.62 | E4/B4 |
| stationary-trucks | 28.79 | 3156.65 | C5/B5 | 195.46 | E5/B5 |
| long-driving-sessions | 65.33 | 374.98 | C6/B6 | 2938.54 | E6/B6 |
| long-daily-sessions | 240.04 | 1439.19 | C7/B7 | 19080.95 | E7/B7 |
| avg-vs-projected-fuel-consumption | 3509.26 | 40842.05 | C8/B8 | 37127.24 | E8/B8 |
| avg-daily-driving-duration | 4393.04 | 43588.02 | C9/B9 | 73781.97 | E9/B9 |
| avg-daily-driving-session | 4177.78 | 84494.79 | C10/B10 | 80765.04 | E10/B10 |
| avg-load | 369.86 | 552493.78 | C11/B11 | 30452.26 | E11/B11 |
| daily-activity | 2424.5 | 15248.66 | C12/B12 | 79242.14 | E12/B12 |
| breakdown-frequency | 5507.23 | 288804.93 | C13/B13 | 70205.29 | E13/B13 |

#### 7.2.2 scale=100

| 查询类型 | TDengine | InfluxDB | InfluxDB/TDengine | TimescaleDB | TimescaleDB/TDengine |
| --- | --- | --- | --- | --- | --- |
| last-loc | 1.43 | 14.94 | C2/B2 | 1.35 | E2/B2 |
| low-fuel | 7.15 | 17.45 | C3/B3 | 6.74 | E3/B3 |
| high-load | 1.43 | 18.33 | C4/B4 | 1.31 | E4/B4 |
| stationary-trucks | 4.64 | 69.1 | C5/B5 | 4.02 | E5/B5 |
| long-driving-sessions | 6.17 | 13 | C6/B6 | 61.87 | E6/B6 |
| long-daily-sessions | 15.62 | 42.91 | C7/B7 | 228.38 | E7/B7 |
| avg-vs-projected-fuel-consumption | 325.94 | 1033.72 | C8/B8 | 830.79 | E8/B8 |
| avg-daily-driving-duration | 298.61 | 942.47 | C9/B9 | 1049.07 | E9/B9 |
| avg-daily-driving-session | 180.81 | 1707.27 | C10/B10 | 1066.69 | E10/B10 |
| avg-load | 24.68 | 15956.73 | C11/B11 | 487.39 | E11/B11 |
| daily-activity | 164.75 | 510.3 | C12/B12 | 1245.05 | E12/B12 |
| breakdown-frequency | 434.98 | 6953.83 | C13/B13 | 955.2 | E13/B13 |
