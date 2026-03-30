# tsgen 写入功能测试 - TS

## 1. 测试目标

tsgen 写入功能的测试，与 taosBenchmark 性能对比 

## 2. 参考文档

[taosBenchmark 重构 FS](https://taosdata.feishu.cn/wiki/KNDKwZJTIiJk7fkCeCwc0rmMnic)

## 3. 变更历史

| 日期 | 版本 | 作者 | 备忘 |
| --- | --- | --- | --- |
| 2025/07/06 | 0.1 | @裴亚明 |  |
|  |  |  |  |

## 4. 测试结论

- tsgen 实现了创建数据库/超级表/子表，STMT v2 绑定写入的功能；
- tsgen 采用即时生成数据，写入队列，由队列数据消费线程读取并写入到 TDengine，taosBenchmark 则采用预先生成一定规模的数据，然后重复写入，不同的架构设计和处理流程，前者写入性能与后者仍有一定差距，主要是受 CPU cache miss 影响导致，写入同一份数据时，CPU 缓存有速度加成；
- 尝试在 tsgen 中仿照 taosBenchmark 所有子表使用一份数据进行性能测试，性能提升明显，交错模式与 taosBenchmark 解决，批模式比 taosBenchmark 提升 20% 以上；

## 5. 测试环境

|  |
|  |
| 服务器 | 配置 | 说明 |
| CPU | Intel(R) Core(TM) i7-10700 CPU @ 2.90GHz 16核 |
| 内存 | 62G |
| 硬盘 | ST1000DM010-2EP1 500G |
| 网络 | 千兆 |

## 6. 功能测试

### 6.1 基础功能

#### 6.1.1 测试要点

使用单元测试方式测试各子模块的基础功能，包括：连接器、格式化器、数据生成器、读取器、配置解析、作业调度等。

#### 6.1.2 用例列表

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| 1 | 测试数据库连接配置解析功能 | 运行测试用例程序：TestConnectionInfo | 通过 |
| 2 | 测试列模板配置解析功能 | 运行测试用例程序：TestColumnConfig | 通过 |
| 3 | 测试根据列模板配置生成列实例功能 | 运行测试用例程序：TestColumnConfigInstance | 通过 |
| 4 | 测试行动的性能指标采样功能呢 | 运行测试用例程序：TestActionMetrics | 通过 |
| 5 | 测试异步垃圾回收的功能 | 运行测试用例程序：TestGarbageCollector | 通过 |
| 6 | 测试原生连接器执行SQL功能 | 运行测试用例程序：TestNativeConnector | 通过 |
| 7 | 测试创建数据库的SQL格式化相关功能 | 运行测试用例程序：TestSqlDatabaseFormatter | 通过 |
| 8 | 测试创建超级表的SQL格式化相关功能 | 运行测试用例程序：TestSqlSuperTableFormatter | 通过 |
| 9 | 测试创建子表的SQL格式化相关功能 | 运行测试用例程序：TestSqlChildTableFormatter | 通过 |
| 10 | 测试插入数据的SQL格式化相关功能 | 运行测试用例程序：TestSqlInsertDataFormatter | 通过 |
| 11 | 测试插入数据的STMT格式化相关功能 | 运行测试用例程序：TestStmtInsertDataFormatter | 通过 |
| 12 | 测试格式化结果构建和封装功能 | 运行测试用例程序：TestFormatResult | 通过 |
| 13 | 测试生成时间戳的功能 | 运行测试用例程序：TestTimestampGenerator | 通过 |
| 14 | 测试生成表名的功能 | 运行测试用例程序：TestTableNameGenerator | 通过 |
| 15 | 测试随机方式生成列数据的功能 | 运行测试用例程序：TestRandomColumnGenerator | 通过 |
| 16 | 测试列生成器工厂的功能 | 运行测试用例程序：TestColumnGeneratorFactory | 通过 |
| 17 | 测试生成数据行的功能 | 运行测试用例程序：TestRowGenerator | 通过 |
| 18 | 测试读取CSV文件的功能 | 运行测试用例程序：TestCSVReader | 通过 |
| 19 | 测试读取从CSV文件中读取表名的功能 | 运行测试用例程序：TestTableNameCSV | 通过 |
| 20 | 测试读取从CSV文件中读取标签列的功能 | 运行测试用例程序：TestTagsCSV | 通过 |
| 21 | 测试读取从CSV文件中读取数据列的功能 | 运行测试用例程序：TestColumnsCSV | 通过 |
| 22 | 测试创建数据库Action的功能 | 运行测试用例程序：TestCreateDatabaseAction | 通过 |
| 23 | 测试创建超级表Action的功能 | 运行测试用例程序：TestCreateSuperTableAction | 通过 |
| 24 | 测试创建子表Action的功能 | 运行测试用例程序：TestCreateChildTableAction | 通过 |
| 25 | 测试插入数据下生成行数据的功能 | 运行测试用例程序：TestRowDataGenerator | 通过 |
| 26 | 测试插入数据下表数据管理的功能 | 运行测试用例程序：TestTableDataManager | 通过 |
| 27 | 测试插入数据下表名称管理的功能 | 运行测试用例程序：TestTableNameManager | 通过 |
| 28 | 测试数据队列的基础功能 | 运行测试用例程序：TestDataQueue | 通过 |
| 29 | 测试数据管道的基础功能 | 运行测试用例程序：TestDataPipeline | 通过 |
| 30 | 测试时间间隔策略的功能 | 运行测试用例程序：TestTimeIntervalStrategy | 通过 |
| 31 | 测试TDengine写入器的功能 | 运行测试用例程序：TestTDengineWriter | 通过 |
| 32 | 测试插入数据Action的功能 | 运行测试用例程序：TestInsertDataAction | 通过 |
| 33 | 测试解析YAML格式配置的功能 | 运行测试用例程序：TestParseYAML | 通过 |
| 34 | 测试配置解析、未知项、缺失项测试功能 | 运行测试用例程序：TestConfigParser | 通过 |
| 35 | 测试解析参数上下文的功能 | 运行测试用例程序：TestParameterContext | 通过 |
| 36 | 测试作业调度的功能 | 运行测试用例程序：TestJobScheduler | 通过 |


### 6.2 实时写入功能

#### 6.2.1 测试要点

- 数据生成模块：即时生成稍晚于当前系统时间的数据；
- 数据写入模块：时间间隔策略设置 "literal" 模式，根据数据的真实时间以"播放"方式写入数据；

#### 6.2.2 测试用例 {folded="true"}

- YAML 格式基础配置文件 literal-config.yaml
```yaml
global:
  confirm_prompt: false
  log_dir: log/
  cfg_dir: /etc/taos/

  # Common structure definition
  connection_info: &db_conn
    host: 127.0.0.1
    port: 6030
    user: root
    password: taosdata

  data_format: &data_format
    format_type: sql

  data_channel: &data_channel
    channel_type: native

  database_info: &db_info
    name: benchdebug
    drop_if_exists: true
    properties: precision 'ms' vgroups 4

  super_table_info: &stb_info
    name: meters
    columns: &columns_info
      - name: current
        type: float
        min: 0
        max: 100
      - name: voltage
        type: int
        min: 200
        max: 240
      - name: phase
        type: float
        min: 0
        max: 360
    tags: &tags_info
      - name: groupid
        type: int
        min: 1
        max: 10
      - name: location
        type: binary(24)

  tbname_generator: &tbname_generator
    prefix: d
    count: 10000
    from: 0

concurrency: 4

jobs:
  # Create database job
  create-database:
    name: Create Database
    needs: []
    steps:
      - name: Create Database
        uses: actions/create-database
        with:
          connection_info: *db_conn
          database_info: *db_info

  # Create super table job
  create-super-table:
    name: Create Super Table
    needs: [create-database]
    steps:
      - name: Create Super Table
        uses: actions/create-super-table
        with:
          connection_info: *db_conn
          database_info: *db_info
          super_table_info: *stb_info

  # Create child table job
  create-second-child-table:
    name: Create Second Child Table
    needs: [create-super-table]
    steps:
      - name: Create Second Child Table
        uses: actions/create-child-table
        with:
          connection_info: *db_conn
          database_info: *db_info
          super_table_info: *stb_info
          child_table_info:
            table_name:
              source_type: generator
              generator: *tbname_generator
            tags: 
              source_type: generator
              generator:
                schema: *tags_info
          batch:
            size: 1000
            concurrency: 10

  # Insert data job
  insert-second-data:
    name: Insert Second-Level Data
    needs: [create-second-child-table]
    steps:
      - name: Insert Second-Level Data
        uses: actions/insert-data
        with:
          # source
          source:
            table_name:
              source_type: generator
              generator: *tbname_generator
            columns:
              source_type: generator
              generator:
                schema: *columns_info

                timestamp_strategy:
                  generator:
                    start_timestamp: now() + 10s
                    timestamp_precision : s
                    timestamp_step: 3

          # target
          target:
            target_type: tdengine
            tdengine:
              connection_info: *db_conn
              database_info: *db_info
              super_table_info: *stb_info

          # control
          control:
            data_format:
              format_type: stmt
              stmt:
                version: v2
            data_channel:
              channel_type: native
            data_generation:
              interlace_mode:
                enabled: tru\e
                rows: 1
              generate_threads: 2
              per_table_rows: 100
            insert_control:
              per_request_rows: 10000
              auto_create_table: false
              insert_threads: 1
              thread_allocation: index_range
            time_interval:
              enabled: true
              interval_strategy: literal

```


重点：
- 1 万子表，实时产生数据，步长 3秒；
- 交错模式：interlace 1；
- 时间间隔策略：根据数据的真实时间（literal）写入；

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| 1 | 测试时间间隔策略的实时写入功能 | 1. 制作实时写入测试的配置文件，时间间隔策略设置成：literal； 1. 运行新版 benchmark 程序：./taosbench -c literal-config.yaml； 1. 命令行 taos shell 中查询当前最新写入数据时间，观察新数据产生时间是否极接近当前系统时间； | 通过 |

例如：
```yaml
[2025-07-09 15:33:13.950] Welcome to the TDengine Command Line Interface, Native Client Version:3.3.6.12 
[2025-07-09 15:33:13.950] Copyright (c) 2025 by TDengine, all rights reserved.
[2025-07-09 15:33:13.950] 
[2025-07-09 15:33:13.950] taos> select tbname, last_row(*) from benchdebug.meters where tbname in ('d0', 'd1', 'd2', 'd3', 'd4') partition by tbname order by tbname;
[2025-07-09 15:33:13.950]              tbname             |      last_row(ts)       |  last_row(current)   | last_row(voltage) |   last_row(phase)    |
[2025-07-09 15:33:13.950] =============================================================================================================================
[2025-07-09 15:33:13.950]  d0                             | 2025-07-09 15:33:11.000 |               22.083 |               215 |              123.224 |
[2025-07-09 15:33:13.950]  d1                             | 2025-07-09 15:33:11.000 |              58.5089 |               226 |              265.351 |
[2025-07-09 15:33:13.950]  d2                             | 2025-07-09 15:33:11.000 |              2.14037 |               211 |              33.0652 |
[2025-07-09 15:33:13.950]  d3                             | 2025-07-09 15:33:11.000 |              49.2528 |               229 |              194.668 |
[2025-07-09 15:33:13.950]  d4                             | 2025-07-09 15:33:11.000 |              96.6784 |               203 |              192.408 |
[2025-07-09 15:33:13.950] Query OK, 5 row(s) in set (0.008613s)
[2025-07-09 15:33:13.950] 
[2025-07-09 15:33:14.087] Welcome to the TDengine Command Line Interface, Native Client Version:3.3.6.12 
[2025-07-09 15:33:14.087] Copyright (c) 2025 by TDengine, all rights reserved.
[2025-07-09 15:33:14.087] 
[2025-07-09 15:33:14.087] taos> select tbname, last_row(*) from benchdebug.meters where tbname in ('d0', 'd1', 'd2', 'd3', 'd4') partition by tbname order by tbname;
[2025-07-09 15:33:14.087]              tbname             |      last_row(ts)       |  last_row(current)   | last_row(voltage) |   last_row(phase)    |
[2025-07-09 15:33:14.087] =============================================================================================================================
[2025-07-09 15:33:14.087]  d0                             | 2025-07-09 15:33:14.000 |              96.1584 |               208 |              106.608 |
[2025-07-09 15:33:14.087]  d1                             | 2025-07-09 15:33:14.000 |              67.4768 |               203 |               52.647 |
[2025-07-09 15:33:14.087]  d2                             | 2025-07-09 15:33:14.000 |              58.7238 |               233 |              193.044 |
[2025-07-09 15:33:14.087]  d3                             | 2025-07-09 15:33:14.000 |               7.1322 |               216 |              46.5257 |
[2025-07-09 15:33:14.087]  d4                             | 2025-07-09 15:33:14.000 |              22.7451 |               205 |              19.3887 |
[2025-07-09 15:33:14.087] Query OK, 5 row(s) in set (0.010410s)
..................................................................
[2025-07-09 15:33:16.911] Welcome to the TDengine Command Line Interface, Native Client Version:3.3.6.12 
[2025-07-09 15:33:16.911] Copyright (c) 2025 by TDengine, all rights reserved.
[2025-07-09 15:33:16.911] 
[2025-07-09 15:33:16.911] taos> select tbname, last_row(*) from benchdebug.meters where tbname in ('d0', 'd1', 'd2', 'd3', 'd4') partition by tbname order by tbname;
[2025-07-09 15:33:16.911]              tbname             |      last_row(ts)       |  last_row(current)   | last_row(voltage) |   last_row(phase)    |
[2025-07-09 15:33:16.911] =============================================================================================================================
[2025-07-09 15:33:16.911]  d0                             | 2025-07-09 15:33:14.000 |              96.1584 |               208 |              106.608 |
[2025-07-09 15:33:16.911]  d1                             | 2025-07-09 15:33:14.000 |              67.4768 |               203 |               52.647 |
[2025-07-09 15:33:16.911]  d2                             | 2025-07-09 15:33:14.000 |              58.7238 |               233 |              193.044 |
[2025-07-09 15:33:16.911]  d3                             | 2025-07-09 15:33:14.000 |               7.1322 |               216 |              46.5257 |
[2025-07-09 15:33:16.911]  d4                             | 2025-07-09 15:33:14.000 |              22.7451 |               205 |              19.3887 |
[2025-07-09 15:33:16.911] Query OK, 5 row(s) in set (0.011648s)
[2025-07-09 15:33:16.911] 
[2025-07-09 15:33:17.049] Welcome to the TDengine Command Line Interface, Native Client Version:3.3.6.12 
[2025-07-09 15:33:17.049] Copyright (c) 2025 by TDengine, all rights reserved.
[2025-07-09 15:33:17.049] 
[2025-07-09 15:33:17.049] taos> select tbname, last_row(*) from benchdebug.meters where tbname in ('d0', 'd1', 'd2', 'd3', 'd4') partition by tbname order by tbname;
[2025-07-09 15:33:17.049]              tbname             |      last_row(ts)       |  last_row(current)   | last_row(voltage) |   last_row(phase)    |
[2025-07-09 15:33:17.049] =============================================================================================================================
[2025-07-09 15:33:17.049]  d0                             | 2025-07-09 15:33:17.000 |              41.8495 |               239 |              326.095 |
[2025-07-09 15:33:17.049]  d1                             | 2025-07-09 15:33:17.000 |              6.18761 |               236 |              350.659 |
[2025-07-09 15:33:17.049]  d2                             | 2025-07-09 15:33:17.000 |              42.8901 |               205 |              174.363 |
[2025-07-09 15:33:17.049]  d3                             | 2025-07-09 15:33:17.000 |              90.0425 |               202 |              234.615 |
[2025-07-09 15:33:17.049]  d4                             | 2025-07-09 15:33:17.000 |              13.5866 |               205 |              87.3356 |
[2025-07-09 15:33:17.049] Query OK, 5 row(s) in set (0.014485s)

```


## 7. 性能测试

### 7.1 写入测试场景

- meters 表结构：(current float, voltage int, phase float)
- 子表个数：10000
- 单子表行数：10000
- 批请求行数量：10000
- 数据格式：STMT v2
- 数据通道：原生 Native

- 建超级表语句
```sql
CREATE STABLE `meters` (`ts` TIMESTAMP ENCODE 'delta-i' COMPRESS 'lz4' LEVEL 'medium', `current` FLOAT ENCODE 'delta-d' COMPRESS 'lz4' LEVEL 'medium', `voltage` INT ENCODE 'simple8b' COMPRESS 'lz4' LEVEL 'medium', `phase` FLOAT ENCODE 'delta-d' COMPRESS 'lz4' LEVEL 'medium') TAGS (`groupid` INT, `location` VARCHAR(24))
```


- YAML 格式基础配置文件
```yaml
global:
  confirm_prompt: false
  log_dir: log/
  cfg_dir: /etc/taos/

  # Common structure definition
  connection_info: &db_conn
    host: 127.0.0.1
    port: 6030
    user: root
    password: taosdata

  data_format: &data_format
    format_type: sql

  data_channel: &data_channel
    channel_type: native

  database_info: &db_info
    name: benchdebug
    drop_if_exists: true
    properties: precision 'ms' vgroups 32

  super_table_info: &stb_info
    name: meters
    columns: &columns_info
      - name: current
        type: float
        min: 0
        max: 100
      - name: voltage
        type: int
        min: 200
        max: 240
      - name: phase
        type: float
        min: 0
        max: 360
    tags: &tags_info
      - name: groupid
        type: int
        min: 1
        max: 10
      - name: location
        type: binary(24)

  tbname_generator: &tbname_generator
    prefix: d
    count: 1000000
    from: 0

concurrency: 4

jobs:
  # Create database job
  create-database:
    name: Create Database
    needs: []
    steps:
      - name: Create Database
        uses: actions/create-database
        with:
          connection_info: *db_conn
          database_info: *db_info


  # Create super table job
  create-super-table:
    name: Create Super Table
    needs: [create-database]
    steps:
      - name: Create Super Table
        uses: actions/create-super-table
        with:
          connection_info: *db_conn
          database_info: *db_info
          super_table_info: *stb_info


  # Create child table job
  create-second-child-table:
    name: Create Second Child Table
    needs: [create-super-table]
    steps:
      - name: Create Second Child Table
        uses: actions/create-child-table
        with:
          connection_info: *db_conn
          database_info: *db_info
          super_table_info: *stb_info
          child_table_info:
            table_name:
              source_type: generator
              generator: *tbname_generator
            tags: 
              source_type: generator
              generator:
                schema: *tags_info
          batch:
            size: 1000
            concurrency: 10


  # Insert data job
  insert-second-data:
    name: Insert Second-Level Data
    needs: [create-second-child-table]
    steps:
      - name: Insert Second-Level Data
        uses: actions/insert-data
        with:
          # source
          source:
            table_name:
              source_type: generator
              generator: *tbname_generator
            columns:
              source_type: generator
              generator:
                schema: *columns_info

                timestamp_strategy:
                  generator:
                    start_timestamp: 1700000000000
                    timestamp_precision : ms
                    timestamp_step: 1

          # target
          target:
            target_type: tdengine
            tdengine:
              connection_info: *db_conn
              database_info: *db_info
              super_table_info: *stb_info

          # control
          control:
            data_format:
              format_type: stmt
              stmt:
                version: v2
            data_channel:
              channel_type: native
            data_generation:
              interlace_mode:
                enabled: true
                rows: 1
              generate_threads: 8
              per_table_rows: 80
              queue_capacity: 50
              queue_warmup_ratio: 0.5
            insert_control:
              per_request_rows: 10000
              auto_create_table: false
              insert_threads: 8
              thread_allocation: index_range
```


- TDengine 版本
```yaml
root@vm98:~$ taos -V
TDengine Enterprise Edition
taos version: 3.3.6.13 compatible_version: 3.0.0.0
git: 1a3a182ba16ee188c0677fb6cd637b34c52f6bd7
gitOfInternal: b4217edcb5bddbac1f9be965367927ad9b81bc97
build: Linux-x64 2025-06-28 20:16:59 +0800
```


### 7.2 批模式写入

#### 7.2.1 4线程

- taosBenchmark
  - taosBenchmark -h 127.0.0.1 -n 40000 -t 10000 -T 4  --vgroups 32 -I stmt2 -y -d benchdebug
- tsgen
  - interlace_mode.enabled = false
  - tbname_generator.count = 10000
  - per_table_rows = 40000
  - insert_threads = 4

| 版本 | 数据生成方式 | 生成线程数 | 写入线程数 | 平均写入速度（万行/秒） | 运行时长（秒） | 写入性能比 | 框架开销 |
| --- | --- | --- | --- | --- | --- | --- | --- |
| taosBenchmark | 预先生成（多子表共用） | \ | 4 | 178.42 | 224.19 | 基准 | 未知 |
| 148.92 | 268.61 | -16.54% | 0.01% |
| 148.74 | 268.92 | -16.63% | 0.01% |
| 220.55 | 181.37 | +23.61% | 0.02% |
| 221.16 | 180.87 | +23.95% | 0.02% |


#### 7.2.2 8线程

- taosBenchmark
  - taosBenchmark -h 127.0.0.1 -n 10000 -t 50000 -T 8  --vgroups 32 -I stmt2 -y -d benchdebug
- tsgen
  - interlace_mode.enabled = false
  - tbname_generator.count = 10000
  - per_table_rows = 50000
  - insert_threads = 8

| 版本 | 数据生成方式 | 生成线程数 | 写入线程数 | 平均写入速度（万行/秒） | 运行时长（秒） | 写入性能比 | 框架开销 |
| --- | --- | --- | --- | --- | --- | --- | --- |
| taosBenchmark | 预先生成（多子表共用） | \ | 8 | 175.62 | 284.70 | 基准 | 未知 |
| 153.91 | 324.86 | -12.36% | 0.01% |
| 155.85 | 320.82 | -11.26% | 0.01% |
| 229.92 | 217.46 | +30.92% | 0.01% |
| 230.23 | 217.17 | +31.09% | 0.01% |


### 7.3 交错模式写入

#### 7.3.1 4线程

- taosBenchmark
  - taosBenchmark -h 127.0.0.1 -n 200 -t 1000000 -T 4  --vgroups 32 -I stmt2 -y -d benchdebug --interlace-rows 1
- tsgen
  - interlace_mode.enabled = true
  - interlace_mode.rows = 1
  - tbname_generator.count = 1000000
  - per_table_rows = 200
  - insert_threads = 4

| 版本 | 数据生成方式 | 生成线程数 | 写入线程数 | 平均写入速度（万行/秒） | 运行时长（秒） | 写入性能比 | 框架开销 |
| --- | --- | --- | --- | --- | --- | --- | --- |
| taosBenchmark | 预先生成（多子表共用） | \ | 4 | 76.46 | 261.58 | 基准 | 未知 |
| 65.46 | 305.51 | -14.38% | 0.01% |
| 64.64 | 309.42 | -15.46% | 0.01% |
| 74.78 | 267.44 | -2.19% | 0.01% |
| 77.50 | 258.08 | +1.35% | 0.01% |


#### 7.3.2 8线程

- taosBenchmark
  - taosBenchmark -h 127.0.0.1 -n 300 -t 1000000 -T 8  --vgroups 32 -I stmt2 -y -d benchdebug --interlace-rows 1
- tsgen
  - interlace_mode.enabled = true
  - interlace_mode.rows = 1
  - tbname_generator.count = 1000000
  - per_table_rows = 300
  - insert_threads = 8

| 版本 | 数据生成方式 | 生成线程数 | 写入线程数 | 平均写入速度（万行/秒） | 运行时长（秒） | 写入性能比 | 框架开销 |
| --- | --- | --- | --- | --- | --- | --- | --- |
| taosBenchmark | 预先生成（多子表共用） | \ | 8 | 77.82 | 385.52 | 基准 | 未知 |
| 64.27 | 466.81 | -17.41% | 0.01% |
| 64.99 | 461.63 | -16.49% | 0.02% |
| 78.18 | 383.71 | +0.47% | 0.02% |
| 77.91 | 385.01 | +0.12% | 0.01% |
