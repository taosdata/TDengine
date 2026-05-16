---
sidebar_label: JMeter
title: 使用 JMeter 测试 TDengine 查询性能
toc_max_heading_level: 4
---

## 背景

在物联网（IoT）和工业监控场景中，时序数据库（Time Series Database）的查询性能至关重要。TDengine 作为一款高性能的时序数据库，其查询性能直接影响到实时监控、数据分析和决策效率。

本文档旨在解决以下问题：

- 如何使用标准工具（JMeter）测试 TDengine 查询性能
- 如何模拟真实业务场景进行压力测试
- 如何分析测试结果并优化数据库配置

**目标读者**：对 JMeter 和 TDengine 了解不多的开发者、测试工程师、运维人员。本文假设读者具备基本的数据库知识，但不需要深入理解 JMeter 或 TDengine。

**注意**：本文档不涉及 TDengine 写入性能测试 和 查询性能调优。

---

## 环境准备

### 环境清单与驱动下载

**需要准备的环境：**

- **JDK 8+** + **JMeter 5.6.3**：
  - JDK 推荐：JDK 8 或更高版本（JDK 8/11/17/21 均可）
  - JMeter 5.6.3 下载：[https://jmeter.apache.org/download_jmeter.cgi](https://jmeter.apache.org/download_jmeter.cgi)

> **说明**：JMeter 5.6.3 支持 Java 8 及以上版本。

- TDengine JDBC 驱动：[Maven 仓库下载最新版本 dist 包](https://mvnrepository.com/artifact/com.taosdata.jdbc/taos-jdbcdriver)
- TDengine 服务：[官方安装文档](https://docs.tdengine.com/)
- taosgen 工具（TDengine 自带，用于生成测试数据）：[taosgen 使用说明](https://docs.tdengine.com/reference/taos-data/taosgen/)

> **本文档验证环境：**
>
> - **JDK 版本**：JDK 11 (性能验证环境)
> - **JMeter 版本**：5.6.3
> - **TDengine 版本**：3.4.0.4.enterprise
> - **TDengine JDBC 驱动**：3.8.1
>

### JDBC 驱动配置

下载 TDengine JDBC 驱动 dist 包后，有两种配置方式：

- 方式 1：放到 JMeter lib/ext 目录（推荐）

  ```bash
  # JMETER_HOME 为 jmeter 安装目录，如 /opt/apache-jmeter-5.6.3/
  cp taos-jdbcdriver-3.8.1-dist.jar $JMETER_HOME/lib/ext/
  ```

- 方式 2：在 JMX 文件中指定 Classpath（不推荐，仅作为备选）

  在 TestPlan 元素中配置：

  ```xml
  <stringProp name="TestPlan.user_define_classpath">/absolute/path/to/taos-jdbcdriver-3.8.1-dist.jar</stringProp>
  ```

> **注意**：CLI 模式下强烈推荐方式 1。

### JDBC 连接方式

- **连接方式**: WebSocket 连接
- **驱动类**: `com.taosdata.jdbc.ws.WebSocketDriver`
- **URL 格式**: `jdbc:TAOS-WS://host1:port1,host2:port2,host3:port3/database?params`
- **默认端口**: 6041
- **特点**:
  - ✅ 无需安装 TDengine 客户端驱动
  - ✅ **支持多端点**（逗号分隔多个端点，连接级负载均衡）
  - ✅ 支持自动重连和故障切换
  - ✅ 跨平台兼容性好

> **说明**：多端点支持在建立连接时通过最小连接数算法选择节点，单个 JDBC 连接在其生命周期内只连接一个节点，不是请求级别的负载均衡。

**URL 示例**：

```text
# 单个端点（测试环境）
jdbc:TAOS-WS://127.0.0.1:6041/test?user=root&password=taosdata&timezone=Asia/Shanghai

# 三个端点（生产环境，推荐）⭐
jdbc:TAOS-WS://192.168.1.100:6041,192.168.1.101:6041,192.168.1.102:6041/test?user=root&password=taosdata&timezone=Asia/Shanghai
```

### 性能测试环境检查清单

性能测试前需要检查以下系统配置，避免系统限制影响测试结果：

| 检查项 | 推荐值 | 检查命令 |
|--------|--------|----------|
| 文件句柄数限制 | 65535 | `ulimit -n` |
| TCP 连接数优化 | 根据并发调整 | `sysctl net.ipv4.tcp_max_tw_buckets` |
| Swap 状态 | 关闭 | `swapon --show` 或 `free -h` |
| Swappiness 值 | 1-10（服务器） | `sysctl vm.swappiness` |

> **说明**：具体的系统配置优化方法请参考各操作系统的官方文档。TDengine 服务端配置请参考 [taosd 配置](https://docs.taosdata.com/reference/components/taosd/)。

---

## 测试数据准备

### 使用 taosgen 生成测试数据

使用 taosgen 工具生成智能电表测试数据。

#### 创建配置文件

下面是配置文件 `taosgen_meters.yaml` 样例：

```yaml
tdengine:
  dsn: taos+ws://root:taosdata@127.0.0.1:6041/test
  drop_if_exists: true
  props: precision 'ms' vgroups 32 cachemodel 'last_row'

schema:
  name: meters
  tbname:
    prefix: d
    count: 1000000
    from: 0
  columns:
    - name: ts
      type: timestamp
      start: 1704067200000  # 2024-01-01
      precision: ms
      step: 60s  # 1 分钟间隔
    - name: current
      type: float
      min: 0
      max: 100
    - name: voltage
      type: int
      expr: '220 * math.sqrt(2) * math.sin(_i)'
    - name: phase
      type: float
      min: 0
      max: 360
  tags:
    - name: location
      type: binary(24)
      values:
        - Beijing
        - Shanghai
        - Guangzhou
        - Shenzhen
        - Chengdu
    - name: group_id
      type: int
      min: 1
      max: 1000000
  generation:
    interlace: 1
    rows_per_table: 100
    rows_per_batch: 10000
    num_cached_batches: 0

jobs:
  insert:
    steps:
      - uses: tdengine/create-super-table
      - uses: tdengine/create-child-table
        with:
          batch:
            size: 100
            concurrency: 10
      - uses: tdengine/insert
        with:
          concurrency: 20
```

#### 执行数据生成

```bash
# 使用配置文件生成数据
taosgen -c taosgen_meters.yaml
```

#### 数据量说明

- 超级表：`meters`
- 子表数量：100 万个（d0 ~ d999999）
- 每子表记录数：100 条
- 总记录数：1 亿条

> **说明**：
>
> - `rows_per_batch` 是批量写入缓冲大小，用于提高写入效率，不等同于单表行数。即使 `rows_per_batch` > `rows_per_table` 也不会有问题。
> - 可根据实际需求调整配置文件中的 `count` 和 `rows_per_table` 参数。

#### 小规模体验版

如果只想快速验证功能，可修改配置为小规模数据：

```yaml
schema:
  name: meters
  tbname:
    prefix: d
    count: 10000      # 1 万子表
    from: 0
  generation:
    rows_per_table: 100
```

生成数据总量：1 万子表 × 100 行 = 100 万行，适合快速测试。

### CACHEMODEL 缓存设置

TDengine 对 `last_row` 查询有缓存机制，需要在数据库创建时显式配置。这对性能测试结果有重大影响，**建议在数据生成前就配置好**。

**CACHEMODEL 参数说明：**

| 参数值 | 说明 | 适用场景 |
|--------|------|----------|
| `none` | 不缓存（默认值） | `last_row` 查询不会命中缓存，测试真实磁盘 I/O |
| `last_row` | 缓存每个子表的最后一行数据 | 显著提升 `LAST_ROW` 查询性能 |
| `last_value` | 缓存每个列的最后一个非 NULL 值 | 提升 `LAST` 函数性能 |
| `both` | 同时启用两种缓存机制 | 最大化缓存效果 |

**配置方法：**

```sql
-- 创建数据库时指定（推荐）
CREATE DATABASE test CACHEMODEL 'last_row';

-- 修改已有数据库
ALTER DATABASE test CACHEMODEL 'last_row';

-- 查看当前配置，shell 中执行可以分号前加 \G 显示详细信息
show create database test;
```

**注意事项：**

1. **默认值是 `none`**：缓存是关闭的，需要显式配置才能启用
2. **缓存切换需谨慎**：`CACHEMODEL` 值来回切换可能导致 `last`/`last_row` 查询结果不准确
3. **推荐保持打开**：生产环境建议使用 `last_row` 或 `both` 以获得最佳性能
4. **测试影响**：
   - **使用缓存**：`last_row` 查询主要测试连接与调度能力
   - **不使用缓存（none）**：`last_row` 查询会测试磁盘随机读能力
   - **测试磁盘 I/O**：建议使用范围查询或全表扫描

**taosgen 配置示例：**

在 `taosgen_meters.yaml` 中配置数据库缓存：

```yaml
tdengine:
  dsn: taos+ws://root:taosdata@127.0.0.1:6041/test
  drop_if_exists: true
  props: precision 'ms' vgroups 32 cachemodel 'last_row'  # 启用 last_row 缓存
```

> **详细文档**：[TDengine 数据库参数配置](https://docs.taosdata.com/reference/taos-sql/database/)

---

## 极简查询测试和结果分析

### JDBC 连接配置

#### 关于 JMeter 使用方式

JMeter 支持两种运行模式：

- **GUI 模式**：用于测试计划开发和调试，不适合运行性能测试
- **CLI 模式**（推荐）：用于运行性能测试，资源占用少，性能更稳定

本文档所有测试均采用 **CLI 模式**，直接提供 JMX 配置文件供参考。如需使用 GUI 模式编辑测试计划，可启动 JMeter 后导入 JMX 文件进行可视化编辑。

创建第一个测试文件 `baseline_test.jmx`：

```xml
<?xml version="1.0" encoding="UTF-8"?>
<jmeterTestPlan version="1.2" properties="5.0" jmeter="5.6.3">
  <hashTree>
    <!-- Test Plan -->
    <TestPlan guiclass="TestPlanGui" testclass="TestPlan" testname="TDengine Baseline Test Plan">
      <stringProp name="TestPlan.comments">Baseline test for TDengine database query performance</stringProp>
      <boolProp name="TestPlan.functional_mode">false</boolProp>
      <boolProp name="TestPlan.tearDown_on_shutdown">true</boolProp>
      <boolProp name="TestPlan.serialize_threadgroups">false</boolProp>
      <elementProp name="TestPlan.user_defined_variables" elementType="Arguments" guiclass="ArgumentsPanel" testclass="Arguments" testname="User Defined Variables">
        <collectionProp name="Arguments.arguments"/>
      </elementProp>
    </TestPlan>
    <hashTree>
      <!-- Thread Group -->
      <ThreadGroup guiclass="ThreadGroupGui" testclass="ThreadGroup" testname="Thread Group">
        <stringProp name="ThreadGroup.on_sample_error">continue</stringProp>
        <intProp name="ThreadGroup.num_threads">1</intProp>
        <intProp name="ThreadGroup.ramp_time">1</intProp>
        <boolProp name="ThreadGroup.same_user_on_next_iteration">true</boolProp>
        <elementProp name="ThreadGroup.main_controller" elementType="LoopController" guiclass="LoopControlPanel" testclass="LoopController" testname="Loop Controller">
          <boolProp name="LoopController.continue_forever">false</boolProp>
          <stringProp name="LoopController.loops">100</stringProp>
        </elementProp>
        <boolProp name="ThreadGroup.scheduler">false</boolProp>
        <stringProp name="ThreadGroup.duration"></stringProp>
        <stringProp name="ThreadGroup.delay"></stringProp>
      </ThreadGroup>
      <hashTree>
        <!-- JDBC Connection -->
        <JDBCDataSource guiclass="TestBeanGUI" testclass="JDBCDataSource" testname="JDBC Connection Configuration">
          <stringProp name="dataSource">taos</stringProp>
          <stringProp name="dbUrl">jdbc:TAOS-WS://192.168.1.100:6041,192.168.1.101:6041,192.168.1.102:6041/test?timezone=Asia/Shanghai</stringProp>
          <stringProp name="driver">com.taosdata.jdbc.ws.WebSocketDriver</stringProp>
          <stringProp name="username">root</stringProp>
          <stringProp name="password">taosdata</stringProp>
          <stringProp name="poolMax">10</stringProp>
          <stringProp name="connectionAge">0</stringProp>
          <stringProp name="trimInterval">60000</stringProp>
          <boolProp name="preinit">true</boolProp>
          <intProp name="timeout">10000</intProp>
          <stringProp name="queryTimeout">60</stringProp>
          <boolProp name="keepAlive">true</boolProp>
        </JDBCDataSource>
        <hashTree/>

        <!-- JDBC Sampler -->
        <JDBCSampler guiclass="TestBeanGUI" testclass="JDBCSampler" testname="Query Latest Data">
          <stringProp name="dataSource">taos</stringProp>
          <stringProp name="queryType">Select Statement</stringProp>
          <stringProp name="query">SELECT last_row(*) FROM d1001</stringProp>
          <stringProp name="queryTimeout">60</stringProp>
          <stringProp name="resultSetHandler">Count Records</stringProp>
        </JDBCSampler>
        <hashTree/>
      </hashTree>
    </hashTree>
  </hashTree>
</jmeterTestPlan>

```

#### 关键参数说明

| 参数 | 说明 | 推荐值 | 备注 |
|------|------|--------|------|
| dbUrl | 数据库连接 URL | 修改 IP/端口/数据库名 | 支持多端点（逗号分隔） |
| driver | JDBC 驱动类 | `com.taosdata.jdbc.ws.WebSocketDriver` | 固定值 |
| poolMax | 连接池最大连接数 | 小于 200 | 最佳设置为服务端 cpu 核数两倍，一般不超过 200 |
| connectionAge | 连接最大存活时间 (毫秒) | 0 | 连接的最大存活时间，0 表示无限制。设为非零值会导致连接被定期回收，影响性能测试稳定性 |
| trimInterval | 空闲连接清理间隔 (毫秒) | 60000 | 默认 60000（1 分钟），可以设置为 0 表示不清理|
| preinit | 是否预初始化连接池 | true（性能测试推荐） | 性能测试应设为 true，避免首次查询包含连接建立时间 |
| timeout | 连接超时时间 (毫秒) | 10000 | 获取连接的超时时间（Max Wait） |
| keepAlive | 保持连接活跃 | true | 长连接模式，避免频繁建立连接 |
| query | SQL 查询语句 | - | - |
| queryTimeout | 查询超时时间 (秒) | 60 | 大查询建议 60 秒（1 分钟），简单查询可设为 5-10 秒 |

### 执行测试

**标准测试模式（生成详细报告）：**

```bash
# Set JVM memory parameters (explained below)
export JVM_ARGS="-Xms4g -Xmx4g -XX:MaxMetaspaceSize=256m -XX:+UseG1GC"

# Run test in CLI mode
jmeter -n -t baseline_test.jmx -l baseline_results.jtl

# Generate HTML report
jmeter -g baseline_results.jtl -o baseline_report/

# Open baseline_report/index.html in browser and check the result
```

**性能测试模式（高并发 QPS 测试）：**

为了避免 JMeter 的磁盘 I/O 成为瓶颈，高并发测试时应**关闭 JTL 详细记录**，只使用 summary 输出分析 QPS。

创建性能优化配置文件 `perf.properties`：

```properties
# perf.properties - JMeter 高性能测试配置
# 关闭不必要的 JTL 记录，减少磁盘 I/O

jmeter.save.saveservice.latency=false
jmeter.save.saveservice.connect_time=false
jmeter.save.saveservice.idle_time=false
sampleresult.timestamp.start=false
jmeter.save.saveservice.response_code=false
jmeter.save.saveservice.response_message=false
jmeter.save.saveservice.url=false
jmeter.save.saveservice.label=false
jmeter.save.saveservice.thread_name=false
jmeter.save.saveservice.data_type=false
jmeter.save.saveservice.bytes=false
jmeter.save.saveservice.sent_bytes=false
jmeter.save.saveservice.assertions=false
jmeter.save.saveservice.assertion_results_failure_message=false
jmeter.save.saveservice.subresults=false
jmeter.save.saveservice.thread_counts=false
jmeter.save.saveservice.autoflush=false
```

**两种高性能测试方式：**

```bash
# 方式1：使用 perf.properties（减少 JTL 写入）
jmeter -n -t baseline_test.jmx -l result.jtl -q perf.properties

# 方式2：完全不记录 JTL（推荐，只用 summary 分析 QPS）
jmeter -n -t baseline_test.jmx
```

**输出示例（summary 格式）：**

```text
summary +    500 in 00:00:01 =  500.2/s Avg:    2 Min:     1 Max:     5 Err:     0 (0.00%) Active: 4
summary +   1000 in 00:00:02 =  500.0/s Avg:    2 Min:     1 Max:     6 Err:     0 (0.00%) Active: 4
summary =   1500 in 00:00:03 =  500.0/s Avg:    2 Min:     1 Max:     6 Err:     0 (0.00%)
```

**JVM 参数说明：**

- `-Xms4g -Xmx4g`：JVM 堆内存 4GB（初始和最大值设为一致，避免内存抖动和动态调整开销）
- `-XX:MaxMetaspaceSize=256m`：元空间大小，存储类的元数据
- `-XX:+UseG1GC`：使用 G1 垃圾收集器（推荐，对延迟敏感的性能测试场景更优）
- 性能测试时建议根据并发数和测试时长适当调整（如 `-Xms8g -Xmx8g`）

> **提示**：高并发测试时，方式 2（完全不记录 JTL）能最大限度提升 JMeter 性能，避免磁盘 I/O 成为测试瓶颈。summary 输出已包含 QPS、平均响应时间、错误率等核心指标。

### 结果分析

**查看 HTML 报告**
打开 `baseline_report/index.html`，查看完整的测试报告。下面是一个样例。
 ![HTML 报告](./html-report.webp)

**关键指标说明：**

| 指标 | 含义 | 示例 | 说明 |
|------|------|------|------|
| Throughput | 吞吐量（QPS） | 1850.3/sec | 每秒处理的查询数 |
| Average | 平均响应时间 | 15.3ms | 所有请求的平均响应时间 |
| 90% Line | 90% 请求响应时间 | 78ms | 90% 的请求在此时间内完成 |
| 95% Line | 95% 请求响应时间 | 95ms | 95% 的请求在此时间内完成 |
| 99% Line | 99% 请求响应时间 | 150ms | 99% 的请求在此时间内完成 |
| Error% | 错误率 | 0.002% | 失败请求的百分比，应 < 0.1% |

> **提示**：相比 Average，百分位响应时间（90%/95%/99% Line）更能反映真实用户体验。

---

### JDBC 连接属性配置

**基础示例：设置时区**
在 JDBC URL 中添加连接属性：

```xml
<!-- 单个端点 + 时区设置 -->
<stringProp name="dbUrl">jdbc:TAOS-WS://127.0.0.1:6041/test?timezone=Asia/Shanghai</stringProp>

<!-- 三个端点 + 时区设置（推荐）⭐ -->
<stringProp name="dbUrl">jdbc:TAOS-WS://192.168.1.100:6041,192.168.1.101:6041,192.168.1.102:6041/test?timezone=Asia/Shanghai</stringProp>

<stringProp name="driver">com.taosdata.jdbc.ws.WebSocketDriver</stringProp>
```

**常用配置示例**：

| 场景 | JDBC URL 示例 |
|------|--------------|
| 测试环境（单端点） | `jdbc:TAOS-WS://localhost:6041/test?timezone=Asia/Shanghai` |
| 生产环境（三节点） | `jdbc:TAOS-WS://td1:6041,td2:6041,td3:6041/prod_db?timezone=Asia/Shanghai` |

**参考**：更多连接属性（如压缩 `enableCompression`、SSL `useSSL`、自动重连 `enableAutoReconnect` 等），请参考 [TDengine JDBC 连接器文档](https://docs.taosdata.com/reference/connector/java/#properties)

---

## CSV 参数化测试

CSV 参数化是 JMeter 中最常用且灵活的参数化方式。它通过读取外部 CSV 文件中的数据，将不同的参数值传递给测试请求，从而模拟真实业务场景中的多设备、多场景查询。

### 应用场景

**为什么需要 CSV 参数化？**

在实际业务中，我们通常需要：

- 模拟多个设备（如 100 万个智能电表）的查询
- 测试不同的时间范围查询
- 执行不同类型的聚合函数
- 避免所有请求使用相同的参数，更真实地模拟生产环境

**CSV 参数化的优势：**

- 数据准备简单：文本文件，易于编辑和维护
- 灵活性高：可随时修改 CSV 文件而无需改动 JMX
- 支持循环：设置 `recycle=true` 后可循环使用数据
- 线程共享：支持多线程共享同一数据源
- 多变量组合：一次读取多个列，实现复杂参数化

### 创建 CSV 文件

下面以多设备查询为例，给出 CSV 文件格式。

```bash
# 从超级表导出所有子表名，并用 sed 命令去掉导出结果中的引号
# CSV 文件包含表头，JMeter 通过 ignoreFirstLine=true 自动跳过
taos -s "SELECT tags tbname FROM test.meters >> './meters.csv'" && sed -i 's/"//g' meters.csv

# 仅导出部分子表进行测试
taos -s "SELECT tags tbname FROM test.meters limit 1000 >> './meters.csv'" && sed -i 's/"//g' meters.csv

# 查看前几行（包含表头）
head meters.csv
```

**输出示例：**

```text
tbname
d37
d47
d87
d130
d131
d146
d162
d197
d203
...
```

### JMX 配置

下面是 `csv_param_test.jmx` 配置样例：

```xml
<?xml version="1.0" encoding="UTF-8"?>
<jmeterTestPlan version="1.2" properties="5.0" jmeter="5.6.3">
  <hashTree>
    <!-- Test Plan -->
    <TestPlan guiclass="TestPlanGui" testclass="TestPlan" testname="TDengine Baseline Test Plan" enabled="true">
      <stringProp name="TestPlan.comments"></stringProp>
      <boolProp name="TestPlan.functional_mode">false</boolProp>
      <boolProp name="TestPlan.tearDown_on_shutdown">true</boolProp>
      <boolProp name="TestPlan.serialize_threadgroups">false</boolProp>
      <elementProp name="TestPlan.user_defined_variables" elementType="Arguments">
        <collectionProp name="Arguments.arguments"/>
      </elementProp>
    </TestPlan>
    <hashTree>
      <!-- Thread Group -->
        <ThreadGroup guiclass="ThreadGroupGui" testclass="ThreadGroup" testname="Thread Group - 5min Stress Test" enabled="true">
          <stringProp name="ThreadGroup.on_sample_error">continue</stringProp>

          <!-- LoopController: continue_forever=true -->
          <elementProp name="ThreadGroup.main_controller" elementType="LoopController">
            <boolProp name="LoopController.continue_forever">true</boolProp>
            <stringProp name="LoopController.loops">-1</stringProp> <!-- loops 不起作用 -->
          </elementProp>

          <!-- 线程数和 Ramp-up -->
          <stringProp name="ThreadGroup.num_threads">200</stringProp>
          <stringProp name="ThreadGroup.ramp_time">60</stringProp> <!-- 平滑启动60秒 -->

          <!-- Scheduler: 控制总测试时间 -->
          <boolProp name="ThreadGroup.scheduler">true</boolProp>
          <stringProp name="ThreadGroup.duration">300</stringProp> <!-- 秒，5分钟 -->
          <stringProp name="ThreadGroup.delay">0</stringProp>

          <longProp name="ThreadGroup.start_time">0</longProp>
          <longProp name="ThreadGroup.end_time">0</longProp>
        </ThreadGroup>
        <hashTree/>


        <!-- JDBC Connection Configuration -->
        <JDBCDataSource guiclass="TestBeanGUI" testclass="JDBCDataSource" testname="JDBC Connection Configuration" enabled="true">
          <stringProp name="dataSource">taos</stringProp>
          <stringProp name="dbUrl">jdbc:TAOS-WS://192.168.1.100:6041,192.168.1.101:6041,192.168.1.102:6041/test?timezone=Asia/Shanghai</stringProp>
          <stringProp name="driver">com.taosdata.jdbc.ws.WebSocketDriver</stringProp>
          <stringProp name="username">root</stringProp>
          <stringProp name="password">taosdata</stringProp>
          <stringProp name="poolMax">200</stringProp>
          <stringProp name="connectionAge">0</stringProp>
          <stringProp name="trimInterval">60000</stringProp>
          <boolProp name="preinit">true</boolProp>
          <intProp name="timeout">10000</intProp>
          <boolProp name="keepAlive">true</boolProp>
          <boolProp name="autocommit">true</boolProp>
        </JDBCDataSource>
        <hashTree/>
        
        <!-- CSV Data Set Config -->
        <CSVDataSet guiclass="TestBeanGUI" testclass="CSVDataSet" testname="CSV Data Set Config" enabled="true">
          <stringProp name="filename">meters.csv</stringProp>
          <stringProp name="variableNames">table_name</stringProp>
          <stringProp name="delimiter">,</stringProp>
          <boolProp name="ignoreFirstLine">true</boolProp>
          <boolProp name="recycle">true</boolProp>
          <stringProp name="shareMode">shareMode.all</stringProp>
        </CSVDataSet>
        <hashTree/>

        <!-- JDBC Request 使用变量 -->
        <JDBCSampler guiclass="TestBeanGUI" testclass="JDBCSampler" testname="参数化查询" enabled="true">
          <stringProp name="dataSource">taos</stringProp>
          <stringProp name="queryType">Select Statement</stringProp>
          <stringProp name="query">SELECT last_row(*) FROM ${table_name}</stringProp>
          <stringProp name="queryTimeout">10</stringProp>
        </JDBCSampler>
        <hashTree/>
      </hashTree>
    </hashTree>
  </hashTree>
</jmeterTestPlan>

```

**关键参数说明**：

| 参数 | 说明 | 推荐值 |
|------|------|--------|
| filename | CSV 文件路径 | 绝对路径或相对路径 |
| variableNames | 变量名 | 逗号分隔，对应 CSV 列 |
| delimiter | 分隔符 | `,`（逗号）或 `\t`（制表符） |
| ignoreFirstLine | 跳过第一行 | `true`（CSV 文件包含表头时使用） |
| recycle | 循环读取 | `true`（循环使用） |
| shareMode | 共享模式 | `shareMode.all`（所有线程共享） |

### 使用 CSV 参数化不同场景

#### 多设备查询

**CSV 文件（meters.csv）：**

```csv
table_name
d0
d1
d2
...
d999999
```

**JMX 中的查询：**

```xml
<stringProp name="query">SELECT last_row(*) FROM ${table_name}</stringProp>
```

**测试效果：** 循环查询不同的电表表（从 100 万子表中选取）。

#### 不同时间范围

**CSV 文件（time_ranges.csv）：**

```csv
hours
1h
6h
24h
168h
```

**JMX 中的查询：**

```xml
<stringProp name="query">SELECT * FROM d1001 WHERE ts > NOW - ${hours}</stringProp>
```

**测试效果：** 每次查询使用不同的时间范围（1 小时、6 小时、24 小时、1 周）。

#### 不同聚合类型

**CSV 文件（agg_types.csv）：**

```csv
agg_type
avg(current)
max(voltage)
min(phase)
count(*)
```

**JMX 中的查询：**

```xml
<stringProp name="query">SELECT ${agg_type} FROM d1001 WHERE ts > NOW - 1h</stringProp>
```

**测试效果：** 执行不同的聚合函数。

### 执行测试

```bash
# 性能测试（查看 summary 输出）
export JVM_ARGS="-Xms4g -Xmx4g -XX:+UseG1GC"
jmeter -n -t csv_param_test.jmx
```

### 多变量组合参数化

CSV 参数化支持同时读取多个列，实现更复杂的参数化场景。

**应用场景：**

- 同时参数化表名和分组 ID
- 参数化设备名称和查询类型
- 组合多个条件进行测试

**示例：同时参数化表名和分组 ID**：

**CSV 文件（meters_with_groups.csv）：**

```csv
table_name,group_id
d0,1
d1,1
d2,2
d3,2
...
```

**JMX 配置，只列出关键修改：**

```xml
<CSVDataSet>
  <stringProp name="variableNames">table_name,group_id</stringProp>
</CSVDataSet>

<JDBCSampler>
  <stringProp name="queryType">Select Statement</stringProp>
  <stringProp name="query">SELECT * FROM ${table_name} WHERE group_id = ${group_id} LIMIT 100</stringProp>
</JDBCSampler>
```

**注意事项：**

- 变量名顺序必须与 CSV 文件中的列顺序一致
- 多变量组合时，建议使用 `shareMode.all` 确保所有线程共享同一数据源
- 变量名区分大小写

---

## 随机参数测试

随机参数化可以模拟真实环境中不可预测的查询模式，避免所有请求都集中在相同的数据上，从而更真实地反映系统性能。

### 应用场景

- 模拟用户随机查询不同的电表
- 模拟不同的时间范围查询
- 压力测试时避免查询集中

### JMX 配置

下面给出配置文件 random_param_test.jmx 样例：

```xml
<?xml version="1.0" encoding="UTF-8"?>
<jmeterTestPlan version="1.2" properties="5.0" jmeter="5.6.3">
  <hashTree>
    <!-- Test Plan -->
    <TestPlan guiclass="TestPlanGui" testclass="TestPlan" testname="TDengine Random Param Test Plan" enabled="true">
      <stringProp name="TestPlan.comments"></stringProp>
      <boolProp name="TestPlan.functional_mode">false</boolProp>
      <boolProp name="TestPlan.tearDown_on_shutdown">true</boolProp>
      <boolProp name="TestPlan.serialize_threadgroups">false</boolProp>
      <elementProp name="TestPlan.user_defined_variables" elementType="Arguments">
        <collectionProp name="Arguments.arguments"/>
      </elementProp>
    </TestPlan>
    <hashTree>
      <!-- Thread Group -->
      <ThreadGroup guiclass="ThreadGroupGui" testclass="ThreadGroup" testname="Thread Group" enabled="true">
        <stringProp name="ThreadGroup.on_sample_error">continue</stringProp>
        <intProp name="ThreadGroup.num_threads">200</intProp>
        <intProp name="ThreadGroup.ramp_time">5</intProp>
        <elementProp name="ThreadGroup.main_controller" elementType="LoopController">
          <boolProp name="LoopController.continue_forever">false</boolProp>
          <stringProp name="LoopController.loops">10000</stringProp>
        </elementProp>
        <boolProp name="ThreadGroup.scheduler">false</boolProp>
        <longProp name="ThreadGroup.start_time">0</longProp>
        <longProp name="ThreadGroup.end_time">0</longProp>
      </ThreadGroup>
      <hashTree>
        <!-- JDBC Connection Configuration -->
        <JDBCDataSource guiclass="TestBeanGUI" testclass="JDBCDataSource" testname="JDBC Connection Configuration" enabled="true">
          <stringProp name="dataSource">taos</stringProp>
          <stringProp name="dbUrl">jdbc:TAOS-WS://192.168.1.100:6041,192.168.1.101:6041,192.168.1.102:6041/test?timezone=Asia/Shanghai</stringProp>
          <stringProp name="driver">com.taosdata.jdbc.ws.WebSocketDriver</stringProp>
          <stringProp name="username">root</stringProp>
          <stringProp name="password">taosdata</stringProp>
          <stringProp name="poolMax">200</stringProp>
          <stringProp name="connectionAge">0</stringProp>
          <stringProp name="trimInterval">60000</stringProp>
          <boolProp name="preinit">true</boolProp>
          <intProp name="timeout">10000</intProp>
          <boolProp name="keepAlive">true</boolProp>
          <boolProp name="autocommit">true</boolProp>
        </JDBCDataSource>
        <hashTree/>

        <!-- 随机表名 + 随机时间范围查询 -->
        <JDBCSampler guiclass="TestBeanGUI" testclass="JDBCSampler" testname="随机参数查询" enabled="true">
          <stringProp name="dataSource">taos</stringProp>
          <stringProp name="queryType">Select Statement</stringProp>
          <stringProp name="query">SELECT * FROM d${__Random(1001,2000)} WHERE ts > NOW - ${__Random(1,24)}h LIMIT 100</stringProp>
          <stringProp name="queryTimeout">60</stringProp>
        </JDBCSampler>
        <hashTree/>
      </hashTree>
    </hashTree>
  </hashTree>
</jmeterTestPlan>

```

### 参数修改

| 需求 | 修改方法 | 示例 |
|------|----------|------|
| 改表名随机范围 | 修改表名 __Random 参数 | `1001,2000` → `5000,10000`（随机查询更大范围的表） |
| 改时间随机范围 | 修改时间 __Random 参数 | `1,24` → `1,168`（随机查询 1 小时到 1 周） |
| 改返回记录数 | 修改 LIMIT | `LIMIT 100` → `LIMIT 1000` |

### 执行测试

```bash
# 性能测试（查看 summary 输出）
export JVM_ARGS="-Xms4g -Xmx4g -XX:+UseG1GC"
jmeter -n -t random_param_test.jmx
```

---

## 命令行属性参数化

除了 CSV 参数化和随机参数，JMeter 还支持通过**命令行属性**来参数化测试。这种方式特别适合 CLI 模式，可以避免频繁修改 JMX 文件。

### 应用场景

- 不想修改 JMX 文件就能改变测试参数
- 需要快速切换不同配置进行测试
- CI/CD 自动化测试中需要动态参数
- 批量运行多组测试

### 基本用法

#### 在 JMX 中定义属性变量

在 Test Plan 中使用 `${__P(属性名, 默认值)}` 定义属性变量。

#### JMX 文件

下面给出 param_test.jmx 配置样例：

```xml
<?xml version="1.0" encoding="UTF-8"?>
<jmeterTestPlan version="1.2" properties="5.0" jmeter="5.6.3">
  <hashTree>
    <!-- Test Plan -->
    <TestPlan guiclass="TestPlanGui" testclass="TestPlan" testname="TDengine Simple Test Plan">
      <stringProp name="TestPlan.comments"></stringProp>
      <boolProp name="TestPlan.functional_mode">false</boolProp>
      <boolProp name="TestPlan.tearDown_on_shutdown">true</boolProp>
      <boolProp name="TestPlan.serialize_threadgroups">false</boolProp>

      <!-- 用户定义变量 -->
      <elementProp name="TestPlan.user_defined_variables" elementType="Arguments" guiclass="ArgumentsPanel" testclass="Arguments" testname="User Defined Variables">
        <collectionProp name="Arguments.arguments">
          <elementProp name="THREADS" elementType="Argument">
            <stringProp name="Argument.name">THREADS</stringProp>
            <stringProp name="Argument.value">${__P(threads,10)}</stringProp>
            <stringProp name="Argument.metadata">=</stringProp>
          </elementProp>
          <elementProp name="LOOPS" elementType="Argument">
            <stringProp name="Argument.name">LOOPS</stringProp>
            <stringProp name="Argument.value">${__P(loops,100)}</stringProp>
            <stringProp name="Argument.metadata">=</stringProp>
          </elementProp>
        </collectionProp>
      </elementProp>
    </TestPlan>

    <hashTree>
      <!-- JDBC Connection Configuration -->
      <JDBCDataSource guiclass="TestBeanGUI" testclass="JDBCDataSource" testname="JDBC Connection Configuration">
        <stringProp name="dataSource">taos</stringProp>
        <stringProp name="dbUrl">jdbc:TAOS-WS://192.168.1.100:6041,192.168.1.101:6041,192.168.1.102:6041/test?timezone=Asia/Shanghai</stringProp>
        <stringProp name="driver">com.taosdata.jdbc.ws.WebSocketDriver</stringProp>
        <stringProp name="username">root</stringProp>
        <stringProp name="password">taosdata</stringProp>
        <stringProp name="poolMax">200</stringProp>
        <stringProp name="connectionAge">0</stringProp>
        <stringProp name="trimInterval">60000</stringProp>
        <boolProp name="preinit">true</boolProp>
        <intProp name="timeout">10000</intProp>
        <boolProp name="keepAlive">true</boolProp>
        <boolProp name="autocommit">true</boolProp>
      </JDBCDataSource>
      <hashTree/>

      <!-- Thread Group -->
      <ThreadGroup guiclass="ThreadGroupGui" testclass="ThreadGroup" testname="Thread Group">
        <stringProp name="ThreadGroup.on_sample_error">continue</stringProp>
        <stringProp name="ThreadGroup.num_threads">${__P(threads,10)}</stringProp>
        <elementProp name="ThreadGroup.main_controller" elementType="LoopController">
          <boolProp name="LoopController.continue_forever">false</boolProp>
          <stringProp name="LoopController.loops">${__P(loops,100)}</stringProp>
        </elementProp>
      </ThreadGroup>
      <hashTree>
        <!-- JDBC 请求 -->
        <JDBCSampler guiclass="TestBeanGUI" testclass="JDBCSampler" testname="查询">
          <stringProp name="dataSource">taos</stringProp>
          <stringProp name="queryType">Select Statement</stringProp>
          <stringProp name="query">SELECT last_row(*) FROM d${__Random(1,200000)}</stringProp>
          <stringProp name="queryTimeout">60</stringProp>
        </JDBCSampler>
        <hashTree/>
      </hashTree>

    </hashTree>
  </hashTree>
</jmeterTestPlan>

```

#### 执行不同的测试配置

```bash
# 测试1：10线程 × 100次循环（使用默认值）
jmeter -n -t param_test.jmx

# 测试2：100线程 × 20000次循环
jmeter -n -t param_test.jmx -Jthreads=100 -Jloops=20000

```

**优势：**

- ✅ 不需要修改 JMX 文件
- ✅ 同一个 JMX 可以运行多组测试

### JMeter 函数说明

| 函数 | 语法 | 说明 |
|------|------|------|
| `${__P(name, default)}` | 读取属性 | 读取 JMeter 属性，不存在则使用默认值 |
| `${__property(name, default)}` | 读取属性（同上） | 同 __P，是别名 |
| `${__threadNum}` | 线程号 | 返回当前线程号 |
| `${__machineName}` | 机器名 | 返回当前机器名 |

### 批量测试脚本示例

**脚本：batch_test.sh**：

```bash
#!/bin/bash
# 批量测试脚本

echo "=== 批量并发测试 ==="

# 定义测试配置
CONFIGS=(
  "10:100:轻负载"
  "50:100:中负载"
  "100:100:重负载"
  "200:50:压力测试"
)

# 遍历配置
for config in "${CONFIGS[@]}"; do
  IFS=':' read -r threads loops desc <<< "$config"

  echo "执行测试：$desc (${threads}线程 × ${loops}次)"

  export JVM_ARGS="-Xms8g -Xmx8g -XX:+UseG1GC"
  jmeter -n -t param_test.jmx \
    -Jthreads=$threads \
    -Jloops=$loops

  echo "完成: $desc"
  echo "---"
done

echo "=== 所有测试完成 ==="
```

**执行：**

```bash
chmod +x batch_test.sh
./batch_test.sh
```

## 并发配置测试

并发测试是性能测试的核心，用于发现系统在不同负载下的表现。

### 应用场景

- 测试系统在 10、50、100、200 线程下的表现
- 找出系统的性能瓶颈
- 确定系统能支持的最大并发数

### 使用命令行属性参数化进行并发测试

推荐使用**命令行属性参数化**的方式，使用[参数化测试](#csv-参数化测试)的 `param_test.jmx` 文件可以测试多种并发级别。

### 执行不同并发级别的测试

```bash
# 性能测试（查看 summary 输出）
export JVM_ARGS="-Xms8g -Xmx8g -XX:MaxMetaspaceSize=512m -XX:+UseG1GC"

# 测试1：10线程（使用默认值）
jmeter -n -t param_test.jmx

# 测试2：50线程
jmeter -n -t param_test.jmx -Jthreads=50

# 测试3：100线程
jmeter -n -t param_test.jmx -Jthreads=100

# 测试4：200线程
jmeter -n -t param_test.jmx -Jthreads=200
```

**或者使用批量测试脚本**:[测试脚本](#批量测试脚本示例)

### 并发参数说明

| 参数 | 说明 | 推荐值 | 注意事项 |
|------|------|--------|----------|
| num_threads | 线程数 | 10/50/100/200/500 | 逐步增加测试 |
| ramp_time | 启动时间 (秒) | 线程数/2 | 值越大，启动越慢 |
| loops | 循环次数 | 无 | 结合查询 qps 设置，与持续时间二选一 |

### 阶梯式压测

推荐使用**批量测试脚本**实现阶梯式压测，逐步增加并发数，参考[测试脚本](#批量测试脚本示例)。

**或手动执行不同并发级别：**

```bash
# 逐步增加并发，观察性能变化
for threads in 10 50 100 200; do
  echo "Testing with $threads threads..."
  jmeter -n -t param_test.jmx -Jthreads=$threads
done
```

### 结果分析

观察以下指标随并发数的变化：

```text
并发数 | 平均响应时间 | 90% 响应时间 | QPS | 错误率
-------|-------------|-------------|-----|--------
10     | 15ms        | 20ms        | 650 | 0%
50     | 45ms        | 78ms        | 1100| 0%
100    | 120ms       | 250ms       | 830 | 0.01%
200    | 450ms       | 800ms       | 440 | 5%

分析：
- 10 线程到 50 线程：性能线性增长，QPS 从 650 提升到 1100
- 100 线程：响应时间明显增加，但错误率仍很低
- 200 线程：性能明显下降，错误率升至 5%

结论：
- 推荐 50-100 线程作为日常负载
- 200 线程接近系统极限，需要优化
```

---

## 混合场景测试

真实业务中通常不是单一查询，而是多种查询混合。本场景将综合运用之前 CSV 参数化及随机函数，请确保已熟悉相关内容。

### 应用场景

模拟真实业务：2-3 种查询混合

- 70% 实时查询（查询最新读数）
- 25% 历史查询（查询历史数据）
- 5% 聚合统计（按区域统计）

### JMX 配置

下面给出 mixed_scenario_test.jmx 配置样例：

```xml
<jmeterTestPlan version="1.2" properties="5.0" jmeter="5.6.3">
  <hashTree>
    <TestPlan guiclass="TestPlanGui" testclass="TestPlan" testname="Mixed Query 5min Test" enabled="true"></TestPlan>
    <hashTree>
      <JDBCDataSource guiclass="TestBeanGUI" testclass="JDBCDataSource" testname="JDBC Connection Configuration" enabled="true">
        <stringProp name="dataSource">taos</stringProp>
        <stringProp name="dbUrl">jdbc:TAOS-WS://192.168.1.100:6041,192.168.1.101:6041,192.168.1.102:6041/test?timezone=Asia/Shanghai</stringProp>
        <stringProp name="driver">com.taosdata.jdbc.ws.WebSocketDriver</stringProp>
        <stringProp name="username">root</stringProp>
        <stringProp name="password">taosdata</stringProp>
        <stringProp name="poolMax">100</stringProp>
        <intProp name="timeout">10000</intProp>
        <boolProp name="autocommit">true</boolProp>
      </JDBCDataSource>
      <hashTree/>
      <CSVDataSet guiclass="TestBeanGUI" testclass="CSVDataSet" testname="CSV Data Set Config" enabled="true">
        <stringProp name="filename">meters.csv</stringProp>
        <stringProp name="variableNames">table_name</stringProp>
        <boolProp name="ignoreFirstLine">true</boolProp>
        <stringProp name="delimiter">,</stringProp>
        <boolProp name="recycle">true</boolProp>
        <stringProp name="shareMode">shareMode.all</stringProp>
      </CSVDataSet>
      <hashTree/>
      <ThreadGroup guiclass="ThreadGroupGui" testclass="ThreadGroup" testname="Mixed Load" enabled="true">
        <intProp name="ThreadGroup.num_threads">100</intProp>
        <intProp name="ThreadGroup.ramp_time">15</intProp>
        <boolProp name="ThreadGroup.scheduler">true</boolProp>
        <stringProp name="ThreadGroup.duration">300</stringProp> <!-- 秒，5分钟 -->
        <elementProp name="ThreadGroup.main_controller" elementType="LoopController">
          <boolProp name="LoopController.continue_forever">true</boolProp>
          <stringProp name="LoopController.loops">-1</stringProp>
        </elementProp>
      </ThreadGroup>
      <hashTree>
        <ThroughputController guiclass="ThroughputControllerGui" testclass="ThroughputController" testname="70% 实时查询" enabled="true">
          <intProp name="ThroughputController.style">1</intProp>
          <boolProp name="ThroughputController.perThread">false</boolProp>
          <stringProp name="ThroughputController.percentThroughput">70.0</stringProp>
        </ThroughputController>
        <hashTree>
          <JDBCSampler guiclass="TestBeanGUI" testclass="JDBCSampler" testname="查询最新数据" enabled="true">
            <stringProp name="dataSource">taos</stringProp>
            <stringProp name="queryType">Select Statement</stringProp>
            <stringProp name="query">SELECT last_row(current, voltage, phase) FROM ${table_name}</stringProp>
            <stringProp name="queryTimeout">60</stringProp>
          </JDBCSampler>
          <hashTree/>
        </hashTree>
        <ThroughputController guiclass="ThroughputControllerGui" testclass="ThroughputController" testname="25% 历史查询" enabled="true">
          <intProp name="ThroughputController.style">1</intProp>
          <boolProp name="ThroughputController.perThread">false</boolProp>
          <stringProp name="ThroughputController.percentThroughput">25.0</stringProp>
        </ThroughputController>
        <hashTree>
          <JDBCSampler guiclass="TestBeanGUI" testclass="JDBCSampler" testname="历史数据查询" enabled="true">
            <stringProp name="dataSource">taos</stringProp>
            <stringProp name="queryType">Select Statement</stringProp>
            <stringProp name="query">SELECT * FROM ${table_name} WHERE ts > NOW - 24h LIMIT 1000</stringProp>
            <stringProp name="queryTimeout">60</stringProp>
          </JDBCSampler>
          <hashTree/>
        </hashTree>
        <ThroughputController guiclass="ThroughputControllerGui" testclass="ThroughputController" testname="5% 聚合统计" enabled="true">
          <intProp name="ThroughputController.style">1</intProp>
          <boolProp name="ThroughputController.perThread">false</boolProp>
          <stringProp name="ThroughputController.percentThroughput">5.0</stringProp>
        </ThroughputController>
        <hashTree>
          <JDBCSampler guiclass="TestBeanGUI" testclass="JDBCSampler" testname="区域聚合" enabled="true">
            <stringProp name="dataSource">taos</stringProp>
            <stringProp name="queryType">Select Statement</stringProp>
            <stringProp name="query">SELECT location, avg(current), max(voltage)
                                    FROM meters
                                    WHERE ts > NOW - 24h and group_id = ${__Random(1,200)}
                                    PARTITION BY location</stringProp>
            <stringProp name="queryTimeout">600</stringProp>
          </JDBCSampler>
          <hashTree/>
        </hashTree>
      </hashTree>
    </hashTree>
  </hashTree>
</jmeterTestPlan>
```

### 场景配比说明

| 场景 | 线程数 | 占比 | 查询类型 | 特点 |
|------|--------|------|----------|------|
| 实时查询 | 70 | 70% | last_row 最新数据 | 简单、高频 |
| 历史查询 | 25 | 25% | 时间范围查询 | 中等复杂度 |
| 聚合统计 | 5 | 5% | 分组聚合 | 复杂、低频 |

> **重要提示**：超级表聚合查询（`PARTITION BY`）是重负载的分析型查询，会扫描大量数据，不应与实时查询（`last_row`）使用相同的 SLA 标准。在生产环境中，建议将此类查询与实时查询分开测试和评估性能指标。

### 执行测试

```bash
# 性能测试（查看 summary 输出）
export JVM_ARGS="-Xms8g -Xmx8g -XX:MaxMetaspaceSize=512m -XX:+UseG1GC"
jmeter -n -t mixed_scenario_test.jmx

# 生成测试报告
jmeter -n -t mixed_scenario_test.jmx -l result-mix.jtl -e -o mix-report
```

### 混合场景结果分析

观察不同查询类型的性能表现：

```text
场景        | 平均响应时间 | QPS   | 占比
-----------|-------------|-------|-----
实时查询    | 25ms        | 2400  | 70%
历史查询    | 180ms       | 165   | 25%
聚合统计    | 450ms       | 22    | 5%

分析：
- 实时查询性能良好，满足高并发需求
- 历史查询响应时间可接受
- 聚合统计响应时间较长，但占比低，影响有限

建议：
- 聚合统计可以考虑缓存或预计算
- 整体系统满足业务需求
```

---

## 如何判断 TDengine 是否到瓶颈

在性能测试中，正确判断 TDengine 是否已经达到性能瓶颈非常重要。本章介绍如何从 JMeter 测试结果和 TDengine 服务器指标两个方面来识别瓶颈。

### 从 JMeter 测试结果判断

**关键指标 1：QPS 增长停滞或下降**:

**正常情况**：随着并发数增加，QPS 持续增长（直到达到系统上限）

**瓶颈信号**：

- 并发数从 50 → 100 → 200，但 QPS 停滞不前
- 并发数继续增加时，QPS 反而下降

**示例**：

```text
并发线程 | QPS    | 平均响应时间
--------|--------|-------------
50      | 1500   | 33ms
100     | 2800   | 35ms    ← QPS 增长放缓
200     | 2900   | 69ms    ← QPS 几乎不变，响应时间翻倍
500     | 2500   | 200ms   ← QPS 下降，响应时间延长
```

**判断**：200 线程时已接近 TDengine 性能上限。

**关键指标 2：响应时间（延迟）激增**:

**正常情况**：90%/95%/99% 分位响应时间随并发缓慢增长

**瓶颈信号**：

- 平均响应时间突增 2-3 倍以上
- 99% 分位响应时间超过可接受阈值（如 >500ms）
- 响应时间方差增大（不稳定）

**示例**：

```text
并发线程 | 平均响应 | 90% 线 | 95% 线 | 99% 线
--------|---------|--------|--------|--------
50      | 25ms    | 40ms   | 50ms   | 80ms
100     | 30ms    | 55ms   | 70ms   | 120ms
200     | 65ms    | 150ms  | 250ms  | 500ms  ← 99% 线突增
```

**判断**：200 线程时 TDengine 已进入性能瓶颈区。

**关键指标 3：错误率上升**:

**正常情况**：错误率接近 0%

**瓶颈信号**：

- 出现查询超时错误
- 错误率 > 0.1% 且随并发增加而上升

### 从 TDengine 服务器指标判断

#### 监控工具推荐

使用 TDengine 官方监控方案：

- **taosKeeper**：指标采集器（3.x 版本内置）
- **TDinsight**：基于 Grafana 的监控仪表板

#### 关键服务器指标

1. CPU 使用率

使用 top 命令查看 taosd 和 taosAdapter cpu 占用率

**瓶颈信号**：

- taosd + taosAdapter 进程 CPU 持续 > 80%

2. 内存使用率

```bash
# 查看 taosd 和 taosAdapter 内存使用，其中 pid-taosd 和 pid-taosAdapter 是 taosd 和 taosAdapter 的进程 id
watch -n 1 "ps -p pid-taosd,pid-taosAdapter -o pid,comm,%mem,rss,vsz"

#输出样例 
    PID COMMAND         %MEM   RSS    VSZ
2410956 taosd            3.0 8086268 33347636
2411153 taosadapter      0.4 1136280 15500020

```

**指标含义**：

| 列名      | 含义           |
| ------- | ------------ |
| PID     | 进程 ID         |
| COMMAND | 进程名          |
| %MEM    | 占总内存百分比      |
| RSS     | 实际占用物理内存（KB） |
| VSZ     | 虚拟内存大小（KB）   |

3. 磁盘 I/O

```bash
# 查看磁盘 I/O 状态
iostat -x 3

```

**瓶颈信号**：

- 磁盘 I/O 使用率 > 80%
- 磁盘队列深度持续较高
- 读写速度接近硬件上限

4. 网络带宽

可用 `iftop` 或 `sar` 命令查看，查询性能一般网络带宽不是瓶颈，此处不再细述。

### 综合判断流程

1. 确认 JMeter 测试结果异常

- QPS 停滞增长
- 响应时间激增
- 错误率上升

2. 检查 TDengine 服务器资源

- CPU、内存、磁盘 I/O、网络是否接近上限

3. 定位瓶颈类型

| 瓶颈类型 | 典型表现 | 排查方向 |
|---------|---------|----------|
| CPU 瓶颈 | CPU 接近 100%，QPS 受限 | 复杂查询、聚合运算过多 |
| 内存瓶颈 | 内存接近上限，OOM | 缓存配置、结果集过大 |
| 磁盘瓶颈 | 磁盘 I/O 高，响应慢 | 数据分布、存储引擎 |
| 网络瓶颈 | 带宽接近上限 | 数据传输量、连接数 |

4. 针对性优化

- CPU：优化查询，减少计算量
- 内存：调整缓存配置
- 磁盘：优化数据分布，使用分区
- 网络：减少传输数据量，使用压缩

### 实用技巧

1. 逐步加压法

```bash
# 从小并发开始，逐步增加
for threads in 10 20 50 100 200 500; do
  echo "Testing with $threads threads..."
  jmeter -n -t test.jmx -Jthreads=$threads -l result_${threads}t.jtl
  sleep 30  # 冷却时间
done
```

观察 QPS 和响应时间的变化曲线，找到拐点。

2. 固定时长测试

```bash
# 每个并发级别运行固定时长（如 5 分钟）
jmeter -n -t test.jmx -Jthreads=100 -Jduration=300 -l result.jtl
```

避免测试过短导致结果不准确。

3. 观察稳定期

```bash
# 查看测试过程中的 QPS 变化
grep -o 'tx="[0-9]*"' result.jtl | awk -F'"' '{print $2}' | \
  awk '{print NR/60, $1}' > qps_over_time.txt
```

QPS 应该相对稳定，如果持续下降说明系统在恶化。

### 常见误区

1. 只看平均响应时间

**问题**：平均响应时间可能掩盖性能问题

**正确做法**：关注 90%/95%/99% 分位响应时间

2. 追求零错误

**问题**：在极高并发下，少量错误 (小于千分之一) 可能是正常的

**正确做法**：设定可接受的错误率阈值

3. 单次测试下结论

**问题**：单次测试可能受缓存、系统负载等因素影响

**正确做法**：多次测试取平均值，确保结果可复现

### 小结

判断 TDengine 是否到瓶颈需要综合分析：

1. **JMeter 测试结果**：QPS、响应时间、错误率
2. **服务器资源指标**：CPU、内存、磁盘 I/O、网络
3. **综合判断**：确定瓶颈类型，针对性优化

性能瓶颈是一个范围，不是单一的点。找到性能拐点后，建议在拐点之前留有 20-30% 的安全余量。

---

## 常见问题与排查

### 连接问题

1. **JDBC 驱动未找到**

- 错误信息

```text
Cannot load JDBC driver class 'com.taosdata.jdbc.ws.WebSocketDriver'
```

- 解决方案

```bash
# 方案1：将驱动放到 JMeter lib/ext 目录
cp taos-jdbcdriver-*.jar $JMETER_HOME/lib/ext/

```

2. **连接超时**

- 错误信息

```text
java.sql.SQLException: TDengine Error: connection timeout
```

- 排查步骤

```bash
# 1. 检查 TDengine 服务状态, localhost 应为实际服务端主机名
taos -h localhost -Z 1

# 2. 增加连接超时时间
# 修改 JMX 文件：
<intProp name="timeout">30000</intProp>
```

**端口说明：** WebSocket 连接使用 taosAdapter 端口（默认 6041），而非 TDengine 服务端口（6030）。

3. **文件句柄耗尽**

- 错误信息

```text
java.io.IOException: Too many open files
```

- 解决方案

```bash
# 检查当前限制
ulimit -n

# 临时调整
ulimit -n 65535

# 永久调整
echo "* soft nofile 65535" >> /etc/security/limits.conf
echo "* hard nofile 65535" >> /etc/security/limits.conf
```

### 性能问题

1. **JMeter 内存不足**

- **错误信息：**

```text
java.lang.OutOfMemoryError: Java heap space
```

- **解决方案**

```bash
# 增加 JVM 内存（已在命令中配置）
export JVM_ARGS="-Xms8g -Xmx8g -XX:MaxMetaspaceSize=512m -XX:+UseG1GC"

# 监控 JVM 内存
jconsole  # 连接到 JMeter 进程

# 如果还不够，继续增加
export JVM_ARGS="-Xms16g -Xmx16g -XX:+UseG1GC"
```

2. **连接池耗尽**

- **错误信息：**

```text
java.sql.SQLException: Timeout waiting for idle connection
```

- **解决方案**：

1. 调整连接池大小

```xml
<JDBCDataSource>
  <stringProp name="poolMax">200</stringProp>
</JDBCDataSource>
```

2. 调整 taosAdapter 配置

- `maxAsyncConcurrentLimit` 设置 C 异步方法的最大并发调用数（0 表示使用 CPU 核心数）
- `maxSyncConcurrentLimit` 设置 C 同步方法的最大并发调用数（0 表示使用 CPU 核心数）。

3. **查询超时**

- **错误信息：**

```text
java.sql.SQLException: TDengine Error: query timeout
```

- **解决方案：**

```xml
<!-- 1. 增加 queryTimeout -->
<JDBCSampler>
  <stringProp name="queryTimeout">60</stringProp>  <!-- 大查询建议 60 秒 -->
</JDBCSampler>

<!-- 2. 优化 SQL 查询 -->
<!-- 添加 LIMIT -->
<stringProp name="query">SELECT * FROM d1001 LIMIT 100</stringProp>

<!-- 3. 添加时间范围条件 -->
<stringProp name="query">SELECT * FROM meters WHERE ts > NOW - 24h PARTITION BY location</stringProp>
```

---

## 参考资料

- [JMeter CLI 模式文档](https://jmeter.apache.org/usermanual/get-started.html#non_gui)
- [JMeter JDBC Connection Configuration 官方文档](https://jmeter.apache.org/usermanual/component_reference.html#JDBC_Connection_Configuration)
- [TDengine JDBC 驱动下载](https://mvnrepository.com/artifact/com.taosdata.jdbc/taos-jdbcdriver)
- [TDengine 官方文档](https://docs.taosdata.com/)
- [taosgen 使用说明](https://docs.taosdata.com/reference/tools/taosgen/)
