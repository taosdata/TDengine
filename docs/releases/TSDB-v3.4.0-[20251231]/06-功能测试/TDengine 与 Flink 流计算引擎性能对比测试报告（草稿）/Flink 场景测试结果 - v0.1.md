# Flink 场景测试结果 - v0.1

## 1. 修订记录

| 编写日期 | 发布日期 | 版本 | 修订人 | 主要修改内容 |
| --- | --- | --- | --- | --- |
| 2025-11-12 | - | 0.1 | 关胜亮 | 新建 |

## 2. 测试目标

基于 TSBS (Time Series Benchmark Suite) 场景，将 TDengine 的流计算语句按同等逻辑改写为 Apache Flink 的流计算语句。随后，对 Flink 的流计算性能进行测试与验证，评估其吞吐量、内存占用及 CPU 使用率。
1. **v0.1 阶段测试目标：**确保系统能够充分利用 CPU 和内存资源，以获取 Flink 吞吐量的初步数据，并验证测试程序本身的功能正确性与稳定性。
2. **v0.x 阶段测试目标：**通过持续调优 Flink 配置参数，探索并获取不同场景下的最优吞吐量。同时，精确记录并分析各场景下的内存占用和 CPU 消耗情况。

## 3. 参考文档

1. [Flink VS TDengine 流计算测试计划](https://taosdata.feishu.cn/wiki/As8ywKnizi7rbKkCMBvciWQGneg)
2. [流计算与 Flink 性能对比测试记录](https://taosdata.feishu.cn/wiki/E8JqwrTN1inrA8kbP7AcEAbAnCc)
3. [流计算与 Flink 性能对比测试讨论稿](https://taosdata.feishu.cn/wiki/CGS5wHpEaiDCgRkXLxbc2iLMnRs)（废弃）

## 4. 测试结论

### 4.1 吞吐量对比

Flink 在两种计算模式下的吞吐量存在显著差异，具体原因还需分析（**分析工作暂时搁置**）
1. 采用 **时间窗口计算 **时
   - 简单场景：吞吐量约为 **66 万条 / 秒**，场景 `A1-A7` `F1-F4` `F8` `T1` 
   - 多分组场景：吞吐量约为 **3 万条 / 秒，**场景 `F6-F7`
2. 通过 **MATCH_RECOGNIZE** 模拟状态与事件窗口时，可能和数据模式关系密切
   - 场景一：吞吐量约 **4 万条 / 秒，**场景 `A8-A9` `F5` `T4-T7`
   - 场景二：吞吐量约为** 725 条/秒**，场景 `T3`
   - 场景三：吞吐量约为 55** 万条 / 秒**，场景 `T2` `T8-T9`

### 4.2 Watermark 机制差异及影响

1. **生效范围不同**：Flink 的 Watermark 机制对所有输入记录**全局生效**，而 TDengine 支持按分区（`partition by tbname`）独立生效。
2. **延迟设置的折衷**：由于 TSBS 场景存在大量数据乱序，需设置较长的 Watermark 延迟（**60 分钟**）—— 若延迟低于 60 分钟，会导致窗口计算结果错误率显著上升；60 分钟是兼顾计算准确性与实用性的折衷值。
3. **对资源的影响**：该延迟设置会直接增加 Flink 的内存占用（需缓存更多窗口数据）。

### 4.3 计算触发机制对比

1. **Flink**：采用「**逐条处理 + 窗口结束输出**」模式 —— 每条记录到达后立即执行计算逻辑，但结果仅在窗口关闭时统一输出；
2. **TDengine**：采用「**逐条判断 + 窗口结束输出**」模式 —— 每条记录到达后仅触发 “窗口是否关闭” 的判断，计算与结果输出均在窗口结束时执行。

### 4.4 性能预期总结

1. 时间窗口计算场景：Flink 吞吐量预计高于 TDengine
2. MATCH_RECOGNIZE 模拟窗口或复杂查询场景：Flink 吞吐量预计低于 TDengine。

## 5. 测试方法

### 5.1 测试环境

1. **测试服务器**：192.168.1.59 
2. **操作系统**： Linux ubuntu 20
3. **硬件配置**
   - CPU: 40C Intel(R) Xeon(R) CPU E5-2620 v3 @ 2.40GHz
   - 内存  256G
   - 硬盘：500G SSD
4. **Flink 版本**
   - JAVA：openjdk 11.0.27 2025-04-15
   - Maven：Apache Maven 3.6.3
   - Flink：flink-1.17.2

### 5.2 测试数据

1. 数据模型​：TSBS 
2. 数据量：
   - Readings：10,000,000 条记录
   - Diagnostics：10,000,000 条记录
3. 数据格式：扁平化的 CSV 文件
   - Readings 样例
  ```sql
  1451606400000,72.452579999999998,68.837609999999998,255.000000000000000,0.000000000000000,181.000000000000000,70.000000000000000,25.000000000000000,truck_1,South,Albert,F-150,v1.5,2000.000000000000000,200.000000000000000,15.000000000000000
  1451606410000,72.451570000000004,68.839190000000002,259.000000000000000,0.000000000000000,180.000000000000000,72.000000000000000,27.500000000000000,truck_1,South,Albert,F-150,v1.5,2000.000000000000000,200.000000000000000,15.000000000000000
  1451606420000,72.448459999999997,68.836050000000000,253.000000000000000,0.000000000000000,184.000000000000000,75.000000000000000,28.500000000000000,truck_1,South,Albert,F-150,v1.5,2000.000000000000000,200.000000000000000,15.000000000000000
  1451606430000,72.448049999999995,68.835419999999999,260.000000000000000,0.000000000000000,186.000000000000000,74.000000000000000,27.899999999999999,truck_1,South,Albert,F-150,v1.5,2000.000000000000000,200.000000000000000,15.000000000000000
  1451606440000,72.452370000000002,68.840400000000002,265.000000000000000,3.000000000000000,186.000000000000000,74.000000000000000,30.500000000000000,truck_1,South,Albert,F-150,v1.5,2000.000000000000000,200.000000000000000,15.000000000000000
  ```

   - Diagnostics 样例
  ```sql
  1451606420000,0.900000000000000,26.000000000000000,2,truck_1,South,Albert,F-150,v1.5,2000.000000000000000,200.000000000000000,15.000000000000000
  1451606430000,0.900000000000000,41.000000000000000,0,truck_1,South,Albert,F-150,v1.5,2000.000000000000000,200.000000000000000,15.000000000000000
  1451606440000,0.900000000000000,88.000000000000000,0,truck_1,South,Albert,F-150,v1.5,2000.000000000000000,200.000000000000000,15.000000000000000
  1451606450000,NULL,552.000000000000000,0,truck_1,South,Albert,F-150,v1.5,2000.000000000000000,200.000000000000000,15.000000000000000
  1451606460000,100.900000000000000,10.000000000000000,0,truck_1,South,Albert,F-150,v1.5,2000.000000000000000,200.000000000000000,15.000000000000000
  ```

### 5.3 流计算语句

合计 26 个场景，在代码仓库中保存，参照 [链接](https://github.com/taosdata/tsbs-flink-datasource/blob/main/src/main/resources/config/default_cases.yaml)，SQL 样例如下
1. 场景 A1
```sql
    - scenarioId: "A1"
      classfication: "Summary"
      description: ""
      sql: |
          SELECT
              TUMBLE_END(ts, INTERVAL '1' HOUR) AS calculation_time,
              AVG(fuel_consumption) AS avg_fuel_consumption
          FROM
              readings
          GROUP BY
              TUMBLE(ts, INTERVAL '1' HOUR)
```

1. 场景 T3
```sql {wrap}
    - scenarioId: "T3"
      classfication: "Vehicle"
      description: ""
      sql: |
          SELECT
              name,
              slope_interval,
              (COALESCE(sum_s_fuel, 0) + COALESCE(sum_c_fuel, 0))  / (count_s + count_c) AS avg_fuel_consumption,
              (COALESCE(sum_s_velocity, 0) + COALESCE(sum_c_velocity, 0)) / (count_s + count_c) AS avg_velocity,
              segment_start,
              segment_end,
              (count_s + count_c) AS data_points
          FROM (
              SELECT
                  name,
                  ts,
                  fuel_consumption,
                  velocity,
                  grade,
                  CASE 
                      WHEN grade <= 30 THEN 0
                      WHEN grade <= 70 THEN 1
                      ELSE 2
                  END AS slope_interval
              FROM readings
          )
          MATCH_RECOGNIZE (
              PARTITION BY name
              ORDER BY ts
              MEASURES
                  FIRST(S.ts) AS segment_start,
                  LAST(E.ts) AS segment_end,
                  SUM(S.fuel_consumption) AS sum_s_fuel,
                  SUM(C.fuel_consumption) AS sum_c_fuel,
                  SUM(S.velocity) AS sum_s_velocity,
                  SUM(C.velocity) AS sum_c_velocity,
                  COUNT(S.ts) AS count_s,
                  COUNT(C.ts) AS count_c,
                  S.slope_interval AS slope_interval
              ONE ROW PER MATCH
              AFTER MATCH SKIP TO LAST E
              PATTERN (S C* E)
              DEFINE
                  S AS (S.slope_interval IS NOT NULL),
                  C AS (C.slope_interval = S.slope_interval),
                  E AS (E.slope_interval <> S.slope_interval OR E.slope_interval IS NULL)
          );

```

1. 场景 T9
```sql
    - scenarioId: "T9"
      classfication: "Vehicle"
      description: ""
      sql: |
          SELECT
              segment_start,
              segment_end,
              TIMESTAMPDIFF(SECOND, segment_start, segment_end) AS duration_seconds,
              name,
              full_tank_state
              first_current_load,
              data_points
          FROM (
              SELECT
                  ts,
                  name,
                  fuel_state,
                  current_load,
                  CASE 
                      WHEN fuel_state = 1 THEN 1
                      ELSE 0
                  END AS full_tank_state
              FROM diagnostics
          )
          MATCH_RECOGNIZE (
              PARTITION BY name
              ORDER BY ts
              MEASURES
                  FIRST(S.ts) AS segment_start,
                  LAST(E.ts) AS segment_end,
                  FIRST(S.current_load) AS first_current_load,
                  COUNT(S.ts) AS data_points,
                  S.full_tank_state as full_tank_state
              ONE ROW PER MATCH
              AFTER MATCH SKIP PAST LAST ROW
              PATTERN (S+ E)
              DEFINE
                  S AS (S.full_tank_state = 1),
                  E AS (E.full_tank_state = 0)
          );

```

### 5.4 测试流程

#### 5.4.1 部署 Flink

下载 [tsbs-flink-datasource](https://github.com/taosdata/tsbs-flink-datasource) 仓库到本地，按照 [帮助文档](https://github.com/taosdata/tsbs-flink-datasource/blob/main/README.md) 部署 Flink。在 192.168.1.59 这台测试机器上，还需要额外对 /root/flink/conf/flink-conf.yaml 进行特定的修改
```sql
jobmanager.memory.process.size: 50g
taskmanager.memory.process.size: 250g
taskmanager.numberOfTaskSlots: 64
parallelism.default: 16

rest.port: 8087
rest.address: 0.0.0.0
rest.bind-address: 0.0.0.0
env.java.opts: "--add-exports java.base/sun.net.util=ALL-UNNAMED --add-opens java.base/java.lang=ALL-UNNAMED --add-opens java.base/java.util=ALL-UNNAMED --add-opens java.base/java.io=ALL-UNNAMED --add-opens java.base/sun.net.util=ALL-UNNAMED"
```

#### 5.4.2 执行测试

1. 下载 [tsbs-flink-datasource](https://github.com/taosdata/tsbs-flink-datasource) 仓库到本地，按照 [帮助文档](https://github.com/taosdata/tsbs-flink-datasource/blob/main/README.md) 编译 tsbs-flink-datasource-1.0-SNAPSHOT.jar
2. 对 192.168.1.59 机器的特定并发度配置参照 u59_cases_cfg.yaml
   - 场景 A6、: 并发度 20（并发度为 40 时报告 rpc timeout 错误）
   - 其他场景：并发度 40
3. 执行如下脚本 
```sql {wrap}
$FLINK_HOME/bin/flink run target/tsbs-flink-datasource-1.0-SNAPSHOT.jar -d1 /root/slguan/readings.csv -d2 /root/slguan/diagnostics.csv -pc /root/tsbs-flink-datasource/src/main/resources/config/u59_cases_cfg.yaml
```

1. 查看结果
   - 在控制台可以看到输出
   - 在如下文件中，可以看到详细结果
  ```sql {wrap}
   /root/tsbs-flink-datasource/tsbs-flink-result.json
   /root/tsbs-flink-datasource/tsbs-flink-log.txt
  ```

## 6. 测试结果

```sql
| ID | Out Records | In Records  | Duration(ms) | Throughput(rec/s) |
|----|-------------|-------------|--------------|-------------------|
| A1 |           5 |    10000000 |        23858 |         419146.62 |
| A2 |      725676 |    10000000 |        25274 |         395663.53 |
| A3 |         333 |    10000000 |        17011 |         587854.92 |
| A4 |      380020 |    10000000 |        15763 |         634397.01 |
| A5 |      379963 |    10000000 |        14815 |         674991.56 |
| A6 |        3068 |    20000000 |        18370 |        1088731.63 |
| A7 |           2 |    10000000 |        12558 |         796305.14 |
| A8 |           1 |    10000000 |       170979 |          58486.71 |
| A9 |           0 |    10000000 |       171053 |          58461.41 |
| F1 |         930 |    10000000 |        15930 |         627746.39 |
| F2 |         931 |    10000000 |        13242 |         755172.93 |
| F3 |          21 |    10000000 |        13229 |         755915.04 |
| F4 |           5 |    20000000 |        23691 |         844202.44 |
| F5 |           5 |    10000000 |       172316 |          58032.92 |
| F6 |    19410334 |    10000000 |       275256 |          36329.82 |
| F7 |    29709993 |    10000000 |       396751 |          25204.73 |
| F8 |      237500 |    10000000 |        21854 |         457582.14 |
| T1 |       40005 |    10000000 |        13442 |         743936.91 |
| T2 |       28158 |    10000000 |        23570 |         424268.14 |
| T3 |      666188 |    10000000 |     13796555 |            724.82 |
| T4 |     1126105 |    10000000 |       652776 |          15319.19 |
| T5 |      822989 |    10000000 |       173284 |          57708.73 |
| T6 |      370156 |    10000000 |      1071366 |           9333.88 |
| T7 |     7504225 |    20000000 |       775793 |          25780.07 |
| T8 |       84159 |    10000000 |        30633 |         326445.34 |
| T9 |        8587 |    10000000 |        44255 |         225963.17 |
```

## 7. 后续工作计划

### 7.1 TDengine 场景测试（优先级：高）

1. **段宽军**：完成 TDengine 流计算场景的测试程序编写（含数据生成、查询提交、结果验证逻辑）
2. **段宽军**：执行 TDengine 场景测试，完成写入性能调优和计算参数调优，输出详细测试数据
3. **潘魏**：基于 TDengine 场景的测试结果，针对性优化流计算程序
4. **段宽军**：将 TDengine 场景测试整合至现有性能基准测试集合，确保可复用、可追溯

### 7.2 测试结果分析

1. 关胜亮：分析 TDengine 与 Flink 场景的测试结果可比性，若具备可比性，启动 Flink 场景测试

### 7.3 Flink 场景测试

1. **段宽军**：执行 Flink 场景测试，完成 Flink 程序参数调优（如并行度、内存）和 Flink SQL 语句优化（如窗口函数改写、Watermark 调整）
2. **段宽军**：实现 TDengine 与 Flink 测试的一键化执行，覆盖全流程：测试数据生成 → 测试环境自动部署 → 测试任务提交 → 测试结果采集 → 结果解析 → 生成测试报告（含基础图表、核心指标对比）。

### 7.4 测试报告编写

1. **潘魏**：基于上述测试结果，编写 Flink 与 TDengine 流计算性能对比测试报告（对外版本），包含测试场景说明、关键指标对比、测试结论等。
