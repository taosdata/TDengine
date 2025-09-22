---
title: taosgen 参考手册
sidebar_label: taosgen
toc_max_heading_level: 4
---

taosgen 是时序数据领域产品的性能基准测试工具，支持数据生成、写入性能测试等功能。taosgen 以“作业”为基础单元，作业是由用户定义，用于完成特定任务的一组操作集合。每个作业包含一个或多个步骤，并可通过依赖关系与其他作业连接，形成有向无环图（DAG）式的执行流程，实现灵活高效的任务编排。

taosgen 目前支持 Linux 和 macOS 系统。

## taosBenchmark 与 taosgen 功能对比

taosgen 相比 taosBenchmark，具有以下优势和改进：

- 提供作业编排能力，作业支持 DAG 依赖关系，能模拟真实业务流程。
- 支持多种目标/协议（TDengine、MQTT），可用于数据库写入、消息发布等多种场景。
- 更丰富的数据生成方式。支持 lua 表达式生成数据，便于模拟真实业务数据。
- 支持即时数据生成，无需预先生成大批量数据文件，节省准备时间和模拟真实场景。
- 支持使用多种时间间隔策略控制数据写入操作，如根据数据产生的真实时间“播放”数据。
- 未知或错误配置项自动检测，能够及时发现并提示配置文件中的拼写错误或无效参数，提升配置安全性和易用性。
- 支持 TDengine 数据库连接池，能够高效管理和复用数据库连接资源。

taosgen 解决了 taosBenchmark 难以灵活配置、数据生成方式单一、扩展性不足等问题，更适合现代物联网、工业互联网的大数据测试需求。

## 工具获取

根据需要选择下载 [taosgen](https://github.com/taosdata/taosgen/releases) 工具。
下载二进制发布包到本地，解压缩，为了便捷访问，可以创建符号链接存放到系统执行目录中，如 Linux 系统下执行命令：
```shell
tar zxvf tsgen-v0.3.0-linux-amd64.tar.gz
cd tsgen
ln -sf `pwd`/taosgen /usr/bin/taosgen
```

## 运行

taosgen 支持通过命令行、配置文件指定参数配置，相同的参数配置，命令行优先级要高于配置文件。

:::tip
在运行 taosgen 之前，要确保所有待写入的目标 TDengine TSDB 集群已经在正常运行。
:::

启动示例：

```shell
taosgen -h 127.0.0.1 -c config.yaml
```

## 命令行参数

| 命令行参数             | 功能说明                                         |
| ----------------------| ----------------------------------------------- |
| -h/--host             | 指定要连接的服务器的主机名称或 IP 地址，默认值为 localhost |
| -P/--port             | 指定要连接的服务器的端口号，默认值为 6030 |
| -u/--user             | 指定用于连接服务器的用户名，默认为 root |
| -p/--password         | 指定用于连接服务器的密码，默认值为 taosdata |
| -c/--yaml-config-file | 指定 yaml 格式配置文件的路径 |
| -?/--help             | 显示帮助信息并退出|
| -V/--version          | 显示版本信息并退出。不能与其它参数混用 |

提示：当没有指定参数运行 taosgen 时，默认会创建 TDengine 数据库 tsbench、超级表 meters、1 万张子表，并为每张子表批量写入 1 万条数据。

## 配置文件参数

### 整体结构
配置文件分为："tdengine"、"mqtt"、"schema"、"concurrency"、"jobs" 五部分。
- tdengine：描述 TDengine 数据库的相关配置参数。
- mqtt：描述 MQTT Broker 的相关配置参数。
- schema：描述数据定义和生成的相关配置参数。
- concurrency：描述作业执行的并发度。
- jobs：列表结构，描述所有作业的具体相关参数。

#### 作业的格式
作业（Job）是由用户定义并包含一组有序的步骤（steps）。每个作业具有唯一的作业标识符（即键名），并可指定依赖关系（needs），以控制与其他作业之间的执行顺序。作业的组成包括以下属性：
- 作业标识符（Job Key）：字符串类型，表示该作业在 jobs 列表中的唯一键名，用于内部引用和依赖管理。
- name：字符串类型，表示作业的显示名称，用于日志输出或 UI 展示。
- needs：列表类型，表示当前作业所依赖的其他作业的标识符列表。若不依赖任何作业，则为空列表。
- steps：列表类型，由一个或多个步骤（Step）组成，按顺序依次执行，定义了该作业的具体操作流程。
作业默认继承全局配置（如 tdengine、schema 等）。

#### 步骤的格式
步骤（Step）是作业中基础的操作单位，代表某一种具体操作类型的执行过程。每个步骤按顺序运行，并可以引用预定义的 Action 来完成特定功能。步骤的组成包括以下属性：
- name：字符串类型，表示该步骤的显示名称，用于日志输出和界面展示。
- uses：字符串类型，指向要使用的 Action 路径或标识符，指示系统调用哪一个操作模块来执行此步骤。
- with：映射（字典）类型，包含传递给该 Action 的参数集合。参数内容因 Action 类型而异，支持灵活配置。
通过组合多个步骤，作业能够实现复杂的逻辑流程，例如 TDengine 创建超级表 & 子表、TDengine 写入数据等。

### 全局配置参数

#### TDengine 参数
- tdengine：描述 TDengine 数据库的相关配置参数，它包括以下属性：
  - dsn（字符串）：表示要连接的 TDengine 数据库的 DSN 地址，默认值为：taos+ws://root:taosdata@localhost:6041/tsbench。
  - drop_if_exists（布尔）：表示数据库已存在时是否删除该数据库，默认为 true。
  - props（字符串）：表示数据库支持的创建数据库的属性信息。
  例如，precision ms vgroups 20 replica 3 keep 3650 分别设置了虚拟组数量、副本数及数据保留期限。
    - precision：
      指定数据库的时间精度，可选值为："ms"、"us"、"ns"。
    - vgroups：指定数据库的虚拟组的个数。
    - replica：指定数据库的副本格式。
  - pool：连接池配置，包含如下属性：
    - enabled（布尔）：表示是否启用连接池功能，默认值为 true，；
    - max_size（整型）：表示连接池的最大容量，默认值为 100；
    - min_size（整型）：表示连接池的最小容量，默认值为 2；
    - timeout（整型）：表示获取连接超时时间，单位毫秒，默认值为 1000；

#### MQTT 参数
- mqtt：描述 MQTT Broker 的相关配置参数，它包括以下属性：
  - uri（字符串）：MQTT Broker 的 uri 地址，默认值为 tcp://localhost:1883。
  - username（字符串）：登录 Broker 的用户名。
  - password（字符串）：登录 Broker 的密码。
  - topic（字符串）：要发布消息的 MQTT Topic，默认值为 tsbench/`{table}`。支持通过占位符语法发布到动态主题，占位符语法如下：
    - `{table}`：表示表名数据
    - `{column}`：表示列数据，column 是列字段名称
  - client_id（字符串）：客户端唯一标识符前缀，默认值为 taosgen。
  - qos（整数）：QoS 等级，取值范围为 0、1、2，默认为 0。
  - keep_alive（整数）：超时没有消息发送后会发送心跳，单位为秒，默认值为 5。
  - clean_session（布尔）：是否清除就会话状态，默认值为 true。
  - retain（布尔）：MQTT Broker 是否保留最后一条消息，默认值为 false。
  - max_buffered_messages（整数）：客户端的最大缓冲消息数，默认值为 10000。

#### schema 参数
- schema：描述数据定义和生成模式的相关配置参数。
  - name（字符串）：schema 的名称。
  - from_csv：描述 CSV 文件作为数据源时的相关配置参数。
    - tags：描述 tags 的配置参数。
      - file_path（字符串）：标签数据 CSV 文件路径。
      - has_header（布尔）：是否包含表头行，默认为 true。
      - tbname_index（整数）：指定表名称所在的列索引（从 0 开始），默认为 -1，表示未生效。
      - exclude_indices（字符串）：如果仅想使用部分标签列时，此参数用于指定剔除的无用标签列的索引（从 0 开始），列索引之间使用英文逗号,分隔，默认值为空，表示不剔除。
    - columns：描述 tags 的配置参数。
      - file_path（字符串）：时序数据 CSV 文件路径。
      - has_header（布尔）：是否包含表头行，默认为 true。
      - repeat_read（布尔）：是否重复读取数据，默认为 false。
      - tbname_index（整数）：指定子表名称所在的列索引（从 0 开始），默认为 -1，表示未生效。
      - timestamp_index（整数）：指定时间戳列的索引（从 0 开始），默认值为 0。
      - timestamp_precision（字符串）：表示时间戳列的时间精度，可选值为 "s"、"ms"、"us"、"ns"。
      - timestamp_offset：描述时间戳数值的偏移配置参数。
        - offset_type（字符串）：表示时间戳偏移类型，可选值为："relative"、"absolute"。
        - value（字符串或整型）：表示时间戳的偏移量（relative）或起始时间戳（absolute）：
          - 时间戳偏移类型为 "relative" 时：字符串类型，格式为 ±[数值][单位] 组合（示例："+1d3h" 表示加1天3小时），支持以下时间单位：
            - y：年偏移量
            - m：月偏移量
            - d：天偏移量
            - s：秒偏移量
          - 时间戳偏移类型为 "absolute" 时：整型或字符串类型，格式如下：
            - 时间戳数值（精度由 timestamp_precision 参数决定）
            - ISO 8601 格式字符串（"YYYY-MM-DD HH:mm:ss"）
  - tbname：描述生成表名称的相关配置参数：
    - prefix（字符串）：表名前缀，默认为 "d"。
    - count（整数）：要创建的表数量，默认为 10000。
    - from（整数）：表名称的起始下标（包含），默认为 0。

  - tags（列表）：描述表标签列结构的模式定义。默认配置为：groupid INT, location VARCHAR(24)。
  - columns（列表）：描述表普通列结构的模式定义。默认配置为：ts TIMESTAMP, current FLOAT, voltage INT, phase FLOAT。
  - generation：描述数据生成行为相关的配置参数。
    - interlace（整数）：控制交错方式生成表数据的行数，默认值为 0，表示不启用交错模式。
    - concurrency（整数）：表示生成数据的线程数量，默认值为写入线程数量。
    - per_table_rows（整数），每个数据表写入的行数，默认值为 10000，-1 表示无限数据。
    - per_batch_rows（整数），默认值为 10000，表示每次批量请求写入的最大行数。

##### 列配置包含属性
每列包含以下属性：
- name（字符串）：表示列的名称，当 count 属性大于1时，name 表示的是列名称前缀，比如：name：current，count：3，则 3 个列的名字分别为 current1、current2、current3。
- type（字符串）：表示数据类型，支持以下类型（不区分大小写，与 TDengine 的数据类型兼容）：
  - 整型：timestamp、bool、tinyint、tinyint unsigned、smallint、smallint unsigned、int、int unsigned、bigint、bigint unsigned。
  - 浮点型：float、double、decimal。
  - 字符型：nchar、varchar（binary）。
- count（整数）：表示指定该类型的列连续出现的数量，例如 count：4096 即可生成 4096 个指定类型的列。
- properties（字符串）：表示 TDengine 数据库的列支持的属性信息，可以包含以下属性：
  - encode：指定此列两级压缩中的第一级编码算法。
  - compress：指定此列两级压缩中的第二级加密算法。
  - level：指定此列两级压缩中的第二级加密算法的压缩率高低。
- gen_type（字符串）：指定此列生成数据的方式，默认值为 random，支持的类型有：
  - random：随机方式生成。
  - order：按自然数顺序增长，仅适用整数类型。
  - expression：根据表达式生成。适用整数类型、浮点数类型 float、double 和字符类型。

##### 数据生成方式详解
- random：随机方式生成
  - distribution（字符串）：表示随机数的分别模型，目前仅支持均匀分布，后续按需扩充，默认值为 "uniform"。
  - min（浮点数）：表示列的最小值，仅适用整数类型和浮点数类型，生成的值将大于或等于最小值。
  - max（浮点数）：表示列的最大值，仅适用整数类型和浮点数类型，生成的值将小于最大值。
  - values（列表）：指定随机数据的取值范围，生成的数据将从中随机选取。

- order：按自然数顺序增长，仅适用整数类型，达到最大值后会自动翻转到最小值
  - min（整数）：表示列的最小值，生成的值将大于或等于最小值。
  - max（整数）：表示列的最大值，生成的值将小于最大值。

- expression：根据表达式生成。适用于整数类型、浮点类型和字符类型。如果未显式指定 gen_type，但检测到包含 expr 属性，则会自动设置 gen_type 为 expression。
  - expr（字符串）：表示生成数据的表达式内容，表达式语法采用 lua 语言，内置变量：
    -  `_i` 表示调用索引，从 `0` 开始，如："2 + math.sin（_i/10）"；
    -  `_table` 表示该表达式为哪张表构建数据；
    -  `_last` 表示该表达式上次返回的数值，仅对数值类型生效，初始值为 0.0；

  为了说明表达式方式的数据描述能力，下面举一个更复杂的表达式样例：

  ```lua
  （math.sin（_i / 7） * math.cos（_i / 13） + 0.5 *（math.random（80, 120） / 100）） *（（_i % 50 < 25） and（1 + 0.3 * math.sin（_i / 3）） or 0.7） + 10 *（math.floor（_i / 100） % 2）
  ```

    它结合了多种数学函数、条件逻辑、周期性行为和随机扰动，模拟一个非线性、带噪声、分段变化的动态数据生成过程，组成部分（A + B）× C + D。功能分解说明：

    | 部分 | 内容                                                               | 类别           | 作用                                                              |
    |------|--------------------------------------------------------------------|----------------|-------------------------------------------------------------------|
    | A    | `math.sin（_i / 7） * math.cos（_i / 13）`                             | 基础信号       | 双频调制，生成复杂波形（拍频效应）                                |
    | B    | `0.5 *（math.random（80, 120） / 100）`                               | 噪声           | 添加 80%~120% 的随机扰动（模拟噪声）                              |
    | C    | `（（_i % 50 < 25） and（1 + 0.3 * math.sin（_i / 3）） or 0.7）`         | 动态增益调制   | 每 50 次调用切换一次增益（前 25 次高增益，后 25 次低增益）        |
    | D    | `10 *（math.floor（_i / 100） % 2）`                                  | 基线阶跃变化   | 每 100 次调用切换一次基线（0 或 10），模拟阶跃变化，表示高峰/低谷 |

### 行动的种类
行动（Action） 是封装好的可复用操作单元，用于完成特定功能。每个行动代表一类独立的操作逻辑，可以在不同的步骤（Step）中被调用和执行。通过将常用操作抽象为标准化的行动模块，系统实现了良好的扩展性与配置灵活性。
同一类型的行动可以在多个步骤中并行或重复使用，从而支持多样化的任务流程编排。例如：创建数据库、定义超级表、生成子表、写入数据等核心操作，均可通过对应的行动进行统一调度。
目前系统支持以下内置行动：
- `tdengine/create-database`：用于创建 TDengine 数据库
- `tdengine/create-super-table`：用于创建 TDengine 超级表
- `tdengine/create-child-table`：用于创建 TDengine 超级表的子表
- `tdengine/insert-data`：用于向指定的 TDengine 数据库中写入数据
- `mqtt/publish-data`：用于向指定的 MQTT Broker 发布数据
每个行动在调用时可通过 with 字段传入参数，具体参数内容因行动类型而异。

### 创建 TDengine 数据库行动的格式
`tdengine/create-database` 行动用于在指定的 TDengine 数据库服务器上创建一个新的数据库。通过配置的连接信息和数据库参数，用户可以轻松地定义新数据库的各种属性，如数据库名称、是否在存在时删除旧数据库、时间精度等。

- checkpoint：描述写入数据中断/恢复功能相关配置参数：
  - enabled：是否开启写入数据中断/恢复功能。
  - interval_sec：数据写入进度进行存储间隔,单位为秒。

### 创建 TDengine 超级表行动的格式
`tdengine/create-super-table` 行动用于在指定数据库中创建一个新的超级表。通过传递必要的连接信息和超级表配置参数，用户能够定义超级表的各种属性，如表名、普通列和标签列等。

- schema：默认使用全局的 schema 配置信息，当需要差异化时可在此行动下单独定义。

### 创建 TDengine 子表行动的格式
`tdengine/create-child-table` 行动用于基于指定的超级表，在目标数据库中批量创建多张子表。每张子表可以拥有不同的名称和标签列数据，从而实现对时间序列数据的有效分类与管理。该行动支持从生成器（Generator）或 CSV 文件两种来源定义子表名称及标签列信息，具备高度灵活性和可配置性。

- schema：默认使用全局的 schema 配置信息，当需要差异化时可在此行动下单独定义。
- batch：控制批量创建子表时的行为:
  - size（整数）：
    每批创建的子表数量，默认值为 1000。
  - concurrency（整数）：并发执行的批次数量，提升创建效率，默认值为 10。

### 写入 TDengine 数据行动的格式
`tdengine/insert-data` 行动用于将数据写入到指定的子表中。它支持从生成器或 CSV 文件两种来源获取子表名称、普通列数据，并允许用户通过多种时间戳策略控制数据的时间属性。此外，还提供了丰富的写入控制策略以优化数据写入过程，具备高度灵活性和可配置性。

- tdengine：默认使用全局的 tdengine 配置信息，当需要差异化时可在此行动下单独定义。
- schema：默认使用全局的 schema 配置信息，当需要差异化时可在此行动下单独定义。
- format（字符串）：描述数据写入时使用的格式，可选值为：sql、stmt，默认使用 stmt。
- concurrency（整数）：并发写入数据的线程数量，默认值为 8。
- failure_handling：表示失败处理策略：
  - max_retries（整数）：最大重试次数，默认值为 0。
  - retry_interval_ms（整数）：重试间隔，单位为毫秒，默认值为 1000，仅在 max_retries > 0 时有效。
  - on_failure（字符串）：默认值为 exit，表示失败后的行为，可选值为：
    - exit：失败后自动退出程序
    - continue：失败后警告用户并继续执行
- time_interval：控制写入过程中时间间隔分布策略。
  - enabled（布尔）：表示是否启用时间间隔控制，默认值为 false。
  - interval_strategy（字符串）：表示时间间隔策略类型，默认值为 fixed。可选值为：
    - fixed：固定的时间间隔。
    - first_to_first：本次发送数据的首行的时间列 - 上次发送数据的首行的时间列。
    - last_to_first：本次发送数据的首行的时间列 - 上次发送数据的末行的时间列。
    - literal：根据本次发送数据的首行的时间列的值的时间点来发送，模拟实时产生数据的场景。
  - fixed_interval：仅在 interval_strategy = fixed 时生效：
    - base_interval（整数）：表示固定间隔数值，单位毫秒。
  - dynamic_interval：仅在 interval_strategy = first_to_first / last_to_first 时生效：
    - min_interval（整数）：默认值为 -1，表示最小时间间隔阈值。
    - max_interval（整数）：默认值为 -1，表示最大时间间隔阈值。
  - wait_strategy（字符串）：表示在开启时间间隔控制时，发送写入请求之间的等待策略，默认值为：sleep，可选值为：
    - sleep：睡眠，归还当前线程的执行权给操作系统。
    - busy_wait：忙等待，保持当前线程的执行权。
- checkpoint：描述写入数据中断/恢复功能相关配置参数（目前仅支持 stmt 格式、生成器方式数据源）：
  - enabled：是否开启写入数据中断/恢复功能。
  - interval_sec：数据写入进度进行存储间隔,单位为秒。

### 发布 MQTT 数据行动的格式
`mqtt/publish-data` 行动用于将数据发布到指定的 topic 中。它支持从生成器或 CSV 文件两种来源获取数据，并允许用户通过多种时间戳策略控制数据的时间属性。此外，还提供了丰富的写入控制策略以优化数据发布过程，具备高度灵活性和可配置性。

- mqtt：默认使用全局的 mqtt 配置信息，当需要差异化时可在此行动下单独定义。
- schema：默认使用全局的 schema 配置信息，当需要差异化时可在此行动下单独定义。
- format（字符串）：描述数据发布时使用的格式，目前仅支持 json，默认值为 json。
- concurrency（整数）：并发发布数据的线程数量，默认值为 8。
- failure_handling（可选）：描述同“写入 TDengine 数据行动的格式”中的同名参数。
- time_interval：描述同“写入 TDengine 数据行动的格式”中的同名参数。

## 配置文件示例

### 生成器方式生成数据 STMT 方式写入 TDengine 示例

该示例展示了如何使用 taosgen 工具模拟一万台智能电表，每台智能电表采集电流、电压、相位三个物理量，它们每隔 5 分钟产生一条记录，电流的数据用随机数，电压用正弦波模拟，产生的这些数据采用 WebSocket 的方式写入 TDengine TSDB 的 tsbench 数据库的超级表 meters。

配置详解：
- TDengine 配置参数
  - 连接信息: 通过 DSN 定义数据库连接相关参数。
  - 数据库的属性：定义是否重新创建数据库，设置时间精度为毫秒，4个 vgroup。
- schema 配置参数
  - 名称：指定超极表的名称。
  - 子表名称：定义生成一万张子表名称的规则，格式为 d0 到 d9999。
  - 超级表字段结构信息: 定义超级表结构，包含3个普通列（电流、电压、相位）和2个标签列（组ID、位置）。
    - 时间戳: 配置了时间戳生成策略，从指定时间戳 1700000000000 (2023-11-14 22:13:20 UTC) 开始，以 5 分钟的步长递增。
    - 时序数据: current 和 phase 使用指定范围的随机数，voltage 使用正弦波模拟。
    - 标签数据: groupid 和 location 使用指定范围的随机数。
  - 数据生成行为：使用交错模式写入，每张子表写入 1 万条记录，每批写入请求最大行数为 1 万行。
- 创建子表：指定 10 线程并发创建子表，每批发送 1000 张子表创建请求。
- 数据写入：使用默认的 stmt (参数化写入) 格式、8 线程并发写入数据，极大提升了批量数据写入的性能。

场景说明：

此配置专为 TDengine 数据库的性能基准测试 而设计。它适用于模拟大规模物联网设备（如电表、传感器）持续产生高频数据的场景，用于：
- 测试和评估 TDengine 集群在海量时间序列数据写入压力下的吞吐量、延迟和稳定性。
- 验证数据库 schema 设计、资源规划以及不同硬件配置下的性能表现。
- 为工业物联网等领域的系统容量规划提供数据支撑。

```yaml
{{#include docs/doxgen/taosgen_config.md:tdengine_gen_stmt_insert_config}}
```

其中，tdengine、schema::name、sschema::tbname、schema::tags、tdengine/create-child-table::batch、tdengine/insert-data::concurrency 可以使用默认值，进一步简化配置。

```yaml
{{#include docs/doxgen/taosgen_config.md:tdengine_gen_stmt_insert_simple}}
```

### CSV文件方式生成数据 STMT 方式写入 TDengine 实例

该示例展示了如何使用 taosgen 工具模拟一万台智能电表，每台智能电表采集电流、电压、相位三个物理量， 它们每隔 5 分钟产生一条记录，测点数据读取自 CSV 文件，采用 WebSocket 的方式写入 TDengine TSDB 的 tsbench 数据库的超级表 meters。

配置详解：
- TDengine 配置参数
  - 连接信息: 通过 DSN 定义数据库连接相关参数。
  - 数据库的属性：定义是否重新创建数据库，设置时间精度为毫秒，4个 vgroup。
- schema 配置参数
  - 名称：指定超极表的名称。
  - from_csv 配置定义了子表名称、标签列和时序数据列的来源。
    - 子表名称：使用文件 ctb-tags.csv 中索引为 2 的列。
    - 标签数据：使用文件 ctb-tags.csv 中排除子表名称列之外的所有列。
    - 时间戳：使用文件 ctb-data.csv 中索引为 1 的列，且在原始数据基础上增加 10 秒。
    - 时序数据：使用文件 ctb-data.csv 中的数据，该文件第 0 列为子表名，用于关联数据与子表。。
  - 超级表字段结构信息: 定义超级表结构，包含3个普通列（电流、电压、相位）和2个标签列（组ID、位置）。
  - 数据生成行为：使用交错模式写入，每张子表写入 1 万条记录，每批写入请求最大行数为 1 万行。
- 创建子表：指定 10 线程并发创建子表，每批发送 1000 张子表创建请求。
- 数据写入：使用默认的 stmt (参数化写入) 格式、8 线程并发写入数据，极大提升了批量数据写入的性能。

场景说明：

此配置专为从现有CSV文件导入设备元数据和历史数据到TDengine数据库而设计。它适用于以下场景：
- 数据迁移: 将已收集存储于CSV文件中的设备元数据（标签）和历史监测数据迁移至TDengine数据库。
- 系统初始化: 为新的监控系统初始化一批设备及其历史数据，用于系统测试、演示或回溯分析。
- 数据回放: 通过重新注入历史数据，模拟实时数据流，用于测试系统处理能力或重现特定历史场景。

```yaml
{{#include docs/doxgen/taosgen_config.md:tdengine_csv_stmt_insert_config}}
```

其中：
- `ctb-tags.csv` 文件内容格式为：

```csv
groupid,location,tbname
1,California.Campbell,d1
2,Texas.Austin,d2
3,NewYork.NewYorkCity,d3
```

- `ctb-data.csv` 文件内容格式为：

```csv
tbname,current,voltage,phase
tbname,ts,current,voltage,phase
d1,1700000010000,5.23,221.5,146.2
d3,1700000030000,8.76,219.8,148.7
d2,1700000020000,12.45,223.1,147.3
d3,1700000030001,9.12,220.3,149.1
d2,1700000020001,11.87,222.7,145.8
d1,1700000010001,4.98,220.9,147.9
```

### 生成器方式生成数据并发布数据到 MQTT Broker 示例

该示例展示了如何使用 taosgen 工具模拟一万台智能电表，每台智能电表采集电流、电压、相位、位置四个物理量，它们每隔 5 分钟产生一条记录，电流的数据用随机数，电压用正弦波模拟，产生的这些数据通过 MQTT 协议进行发布。

配置详解：
- MQTT 配置参数
  - 连接信息: 使用 URI 描述连接 MQTT Broker 的信息。
  - 主题配置 (topic): 使用动态主题 factory/`{table}`/`{location}`，其中：
    - `{table}` 占位符将被实际生成的子表名称替换。
    - `{location}` 占位符将被生成的 location 列值替换，实现按设备位置发布到不同主题。
  - qos: 服务质量等级设置为 1（至少交付一次）。
- schema 配置参数
  - 名称：指定 schema 的名称。
  - 表名称：定义生成一万张表名称的规则，格式为 d0 到 d9999。虽然不直接创建数据库表，此处表作为逻辑概念用来组织数据。
  - 表字段结构信息: 定义数据表结构，包含4个普通列（电流、电压、相位、设备位置）。
    - 时间戳: 配置了时间戳生成策略，从指定时间戳 1700000000000 (2023-11-14 22:13:20 UTC) 开始，以 5 分钟的步长递增。
    - 时序数据: current、phase 和 location 使用指定范围的随机数，voltage 使用正弦波模拟。
  - 数据生成行为：使用交错模式写入，每张表写入 1 万条记录，每批写入请求最大行数为 1000 行。
- 数据发布：使用 8 线程并发向 MQTT Broker 发布数据，提高吞吐量。

场景说明：

此配置专为向 MQTT 消息代理发布模拟设备数据而设计。它适用于以下场景：
- MQTT 消费者测试: 模拟大量设备向 MQTT 代理发布数据，用于测试 MQTT 消费者端的处理能力、负载均衡和稳定性。
- 物联网平台演示: 快速构建一个模拟的物联网环境，展示设备数据如何通过 MQTT 协议接入平台。
- 规则引擎测试: 结合 MQTT 主题的动态特性（如按设备位置路由），测试基于 MQTT 的主题订阅和消息路由规则。
- 实时数据流模拟: 模拟实时产生的设备数据流，用于测试流处理框架的数据消费和处理能力。

```yaml
{{#include docs/doxgen/taosgen_config.md:mqtt_publish_config}}
```