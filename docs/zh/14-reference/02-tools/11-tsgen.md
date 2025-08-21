---
title: tsgen 参考手册
sidebar_label: tsgen
toc_max_heading_level: 4
---

tsgen 是时序数据领域产品的性能基准测试工具，支持数据生成、写入性能测试等功能。tsgen 以“作业”为基础单元，作业是由用户定义，用于完成特定任务的一组操作集合。每个作业包含一个或多个步骤，并可通过依赖关系与其他作业连接，形成有向无环图（DAG）式的执行流程，实现灵活高效的任务编排。

tsgen 目前仅支持 Linux 系统。

## 构建安装

```shell
git clone git@github.com:taosdata/tsgen.git
mkdir build && cd build
conan install .. --build=missing --output-folder=./conan --settings=build_type=Release
cmake .. -DCMAKE_BUILD_TYPE=Release
cmake --build .

sudo cp src/tsgen /usr/local/taos/bin/
sudo ln -s /usr/local/taos/bin/tsgen /usr/bin/tsgen
```

详情请参考 [tsgen](https://github.com/taosdata/tsgen/blob/main/README-CN.md) 仓库。

## 运行

tsgen 支持通过命令行、配置文件指定参数配置，相同的参数配置，命令行优先级要高于配置文件。

:::tip
在运行 tsgen 之前要确保 TDengine TSDB 集群已经在正常运行。
:::

启动示例：

```shell
tsgen -h 127.0.0.1 -c config.yaml
```

## 命令行参数

| 命令行参数                      | 功能说明                                         |
| ----------------------------- | ----------------------------------------------- |
| -h/--host \<host>             | 指定要连接的服务器的主机名称或 IP 地址，默认值为 localhost |
| -P/--port \<port>             | 指定要连接的服务器的端口号，默认值为 6030 |
| -u/--user \<user>             | 指定用于连接服务器的用户名，默认为 root |
| -p/--password \<passwd>       | 指定用于连接服务器的密码，默认值为 taosdata |
| -c/--config-file \<yaml file> | 指定 yaml 格式配置文件的路径 |
| -?/--help                     | 显示帮助信息并退出|

## 配置文件参数

**整体结构**

配置文件分为："global"、"concurrency"、"jobs" 三部分。
- global：描述全局生效的配置参数。
- concurrency：描述作业执行的并发度。
- jobs：列表结构，描述所有作业的具体相关参数。

**作业的格式**

作业（Job）是由用户定义并包含一组有序的步骤（steps）。每个作业具有唯一的作业标识符（即键名），并可指定依赖关系（needs），以控制与其他作业之间的执行顺序。作业的组成包括以下属性：
- 作业标识符（Job Key）：字符串类型，表示该作业在 jobs 列表中的唯一键名，用于内部引用和依赖管理。
- name：字符串类型，表示作业的显示名称，用于日志输出或 UI 展示。
- needs：列表类型，表示当前作业所依赖的其他作业的标识符列表。若不依赖任何作业，则为空列表。
- steps：列表类型，由一个或多个步骤（Step）组成，按顺序依次执行，定义了该作业的具体操作流程。作业支持复用全局配置（如数据库连接信息等），并通过 YAML 锚点与别名机制减少重复定义，提高配置文件的可读性和可维护性。

**步骤的格式**

步骤（Step）是作业中基础的操作单位，代表某一种具体操作类型的执行过程。每个步骤按顺序运行，并可以引用预定义的 Action 来完成特定功能。步骤的组成包括以下属性：
- name：字符串类型，表示该步骤的显示名称，用于日志输出和界面展示。
- uses：字符串类型，指向要使用的 Action 路径或标识符，指示系统调用哪一个操作模块来执行此步骤。
- with：映射（字典）类型，包含传递给该 Action 的参数集合。参数内容因 Action 类型而异，支持灵活配置。通过组合多个步骤，作业能够实现复杂的逻辑流程，例如创建数据库、写入数据等。

示例配置如下：

```yaml
global:
  connection_info:
    host: 127.0.0.1
    port: 6030
    user: root
    password: taosdata

concurrency: 3

jobs:
  # 创建数据库作业
  create-database:
    name: Create Database
    needs: []
    steps:
      - name: Create Database
        uses: actions/create-database
        with:
          ......

  # 创建超级表作业
  create-super-table:
    name: Create Super Table
    needs: [create-database]
    steps:
      ......

  # 创建秒级子表作业
  create-second-child-table:
    name: Create Second Child Table
    needs: [create-super-table]
    steps:
      ......

  # 创建分钟级子表作业
  create-minute-child-table:
    name: Create Minute Child Table
    needs: [create-super-table]
    steps:
      ......

  # 写入秒级数据作业
  insert-second-data:
    name: Insert Second-Level Data
    needs: [create-second-child-table]
    steps:
      ......

  # 写入分钟级数据作业
  insert-minute-data:
    name: Insert Minute-Level Data
    needs: [create-minute-child-table]
    steps:
      ......
```

要点说明：
- 创建数据库、超级表和子表由单独作业完成，且它们之间有依赖关系，通过属性 `needs` 指定。
- 插入秒级数据作业 insert-second-data 需要等待作业 create-database 、create-super-table、create-second-child-table 依次完成后，然后才开始并发写入任务；插入分钟级别作业 insert-minute-data 同理。

### 全局配置参数

#### 连接信息参数
- connection_info：定义服务器连接信息，它包括以下属性：
  - host (字符串，可选)：表示要连接服务器的主机名或 IP 地址，默认值为 localhost。
  - port (整型，可选)：表示要连接服务器的端口号，默认值为 6030。
  - user (字符串，可选)：表示用于连接服务器的用户名，默认值为 root。
  - password (字符串，可选)：表示用于连接服务器的密码，默认值为 taosdata。
  - pool：连接池配置，包含如下属性：
    - enabled (布尔，可选)：表示是否启用连接池功能，默认值为 true，；
    - max_pool_size（整型，可选）：表示连接池的最大容量，默认值为 100；
    - min_pool_size（整型，可选）：表示连接池的最小容量，默认值为 2；
    - connection_timeout（整型，可选）：表示获取连接超时时间，单位毫秒，默认值为 1000；

#### 数据格式化参数
- data_format：定义输出数据的格式类型及其相关配置，描述数据以何种格式输出到数据存储介质中，它包括以下属性：
  - format_type: (字符串类型，可选)：表示数据格式化的类型，默认值为 sql，可选值包括：
    - sql：以 SQL 语句形式格式化数据。
    - stmt：使用 STMT 接口格式化数据。

  - 相应格式类型的描述信息：包含属性根据 format_type 不同而不同：
    - 当 format_type: sql 时，暂无额外配置项。
    - 当 format_type: stmt 时：
      - version (字符串，可选)：表示 STMT 接口版本，目前仅支持 v2。

#### 数据通道参数
- data_channel：定义数据传输所使用的通信通道或目标路径。
  - channel_type (字符串，可选)：表示数据通道类型，可选值包括：
    - native：使用原生接口与数据库交互。
    - websocket：通过 WebSocket 协议与数据库交互。

    注意：暂时不支持 native、websocket 混合使用！

#### 数据库信息参数
- database_info：定义 TDengine 数据库的实例信息，它包括以下属性：
  - name (字符串，必需)：表示数据库名称。
  - drop_if_exists (布尔，可选)：表示数据库已存在时是否删除该数据库，默认为 true。
  - properties (字符串，可选)：表示数据库支持的创建数据库的属性信息。
  例如，precision ms vgroups 20 replica 3 keep 3650 分别设置了虚拟组数量、副本数及数据保留期限。
    - precision：
      指定数据库的时间精度，可选值为："ms"、"us"、"ns"，默认值为 "ms"。
    - vgroups：
      指定数据库的虚拟组的个数，默认不指定。
    - replica：
      指定数据库的副本格式，默认不指定。

#### 超级表信息参数
- super_table_info：定义 TDengine 数据库的超级表信息的映射结构，它包括以下属性：
  - name (字符串)：表示超级表的名称。
  - columns (列表)：表示超级表的普通列的模式定义。
  - tags (列表)：表示超级表的标签列的模式定义。

**列配置包含属性**

每列包含以下属性：
- name (字符串，必需)：表示列的名称，当 count 属性大于1时，name 表示的是列名称前缀，比如：name：current，count：3，则 3 个列的名字分别为 current1、current2、current3。
- type（字符串，必需）：表示数据类型，支持以下类型（不区分大小写，与 TDengine 的数据类型兼容）：
  - 整型：timestamp、bool、tinyint、tinyint unsigned、smallint、smallint unsigned、int、int unsigned、bigint、bigint unsigned。
  - 浮点型：float、double、decimal。
  - 字符型：nchar、varchar（binary）。
- count (整数，可选)：表示指定该类型的列连续出现的数量，例如 count：4096 即可生成 4096 个指定类型的列。
- properties (字符串，可选)：表示 TDengine 数据库的列支持的属性信息，可以包含以下属性：
  - encode：指定此列两级压缩中的第一级编码算法。
  - compress：指定此列两级压缩中的第二级加密算法。
  - level：指定此列两级压缩中的第二级加密算法的压缩率高低。
- gen_type (字符串，可选)：指定此列生成数据的方式，默认值为 random，支持的类型有：
  - random：随机方式生成。
  - order：按自然数顺序增长，仅适用整数类型。
  - expression：根据表达式生成。适用整数类型、浮点数类型 float、double 和字符类型。

**数据生成方式详解**

- random：随机方式生成
  - distribution (字符串，可选)：表示随机数的分别模型，目前仅支持均匀分布，后续按需扩充，默认值为 "uniform"。
  - min (浮点数，可选)：表示列的最小值，仅适用整数类型和浮点数类型，生成的值将大于或等于最小值。
  - max (浮点数，可选)：表示列的最大值，仅适用整数类型和浮点数类型，生成的值将小于最大值。
  - values（列表，可选）：指定随机数据的取值范围，生成的数据将从中随机选取。

- order：按自然数顺序增长，仅适用整数类型，达到最大值后会自动翻转到最小值
  - min (整数，可选)：表示列的最小值，生成的值将大于或等于最小值。
  - max (整数，可选)：表示列的最大值，生成的值将小于最大值。

- expression：根据表达式生成。适用整数类型、浮点数类型 float、double 和字符类型。
  - formula (字符串，必需)：表示生成数据的表达式内容，表达式语法采用 lua 语言，内置变量 `_i` 表示调用索引，从 `0` 开始，如："2 + math.sin(_i/10)"。

    为了说明表达式方式的数据描述能力，下面举一个更复杂的表达式样例：

    ```lua
    (math.sin(_i / 7) * math.cos(_i / 13) + 0.5 * (math.random(80, 120) / 100)) * ((_i % 50 < 25) and (1 + 0.3 * math.sin(_i / 3)) or 0.7) + 10 * (math.floor(_i / 100) % 2)
    ```
    它结合了多种数学函数、条件逻辑、周期性行为和随机扰动，模拟一个非线性、带噪声、分段变化的动态数据生成过程，组成部分（A + B）× C + D。功能分解说明：

    | 部分 | 内容                                                               | 类别           | 作用                                                              |
    |------|--------------------------------------------------------------------|----------------|-------------------------------------------------------------------|
    | A    | `math.sin(_i / 7) * math.cos(_i / 13)`                             | 基础信号       | 双频调制，生成复杂波形（拍频效应）                                |
    | B    | `0.5 * (math.random(80, 120) / 100)`                               | 噪声           | 添加 80%~120% 的随机扰动（模拟噪声）                              |
    | C    | `((_i % 50 < 25) and (1 + 0.3 * math.sin(_i / 3)) or 0.7)`         | 动态增益调制   | 每 50 次调用切换一次增益（前 25 次高增益，后 25 次低增益）        |
    | D    | `10 * (math.floor(_i / 100) % 2)`                                  | 基线阶跃变化   | 每 100 次调用切换一次基线（0 或 10），模拟阶跃变化，表示高峰/低谷 |


## Action 的种类

Action 是封装好的可复用操作单元，用于完成特定功能。每个 Action 代表一类独立的操作逻辑，可以在不同的步骤（Step）中被调用和执行。通过将常用操作抽象为标准化的Action 模块，系统实现了良好的扩展性与配置灵活性。
同一类型的 Action 可以在多个步骤中并行或重复使用，从而支持多样化的任务流程编排。例如：创建数据库、定义超级表、生成子表、插入数据等核心操作，均可通过对应的 Action 进行统一调度。
目前系统支持以下内置 Action：
- `actions/create-database`：用于创建数据库
- `actions/create-super-table`：用于创建超级表
- `actions/create-child-table`：用于基于超级表生成子表
- `actions/insert-data`：用于向指定的数据表中插入数据
每个 Action 在调用时可通过 with 字段传入参数，具体参数内容因 Action 类型而异。


### 创建数据库的 Action 配置

`actions/create-database` 用于在指定的 TDengine 数据库服务器上创建一个新的数据库。通过传递必要的连接信息和数据库配置参数，用户可以轻松地定义新数据库的各种属性，如数据库名称、是否在存在时删除旧数据库、时间精度等。

#### connection_info (可选)
同《全局配置参数》章节中同名参数的描述，如果未指定，则默认使用全局配置中的参数信息。

#### data_format (可选)
同《全局配置参数》章节中同名参数的描述，如果未指定，则默认使用全局配置中的参数信息。

#### data_channel (可选)
同《全局配置参数》章节中同名参数的描述，如果未指定，则默认使用全局配置中的参数信息。

#### database_info (可选)
同《全局配置参数》章节中同名参数的描述，包含数据库创建所需的所有细节。如果未指定，则默认使用全局配置中的参数信息。

### 创建超级表的 Action 配置
`actions/create-super-table` 用于在指定数据库中创建一个新的超级表（Super Table）。通过传递必要的连接信息和超级表配置参数，用户能够定义超级表的各种属性，如表名、普通列和标签列等。

#### connection_info (可选)
同《全局配置参数》章节中同名参数的描述，如果未指定，则默认使用全局配置中的参数信息。

#### data_format (可选)
同《全局配置参数》章节中同名参数的描述，如果未指定，则默认使用全局配置中的参数信息。

#### data_channel (可选)
同《全局配置参数》章节中同名参数的描述，如果未指定，则默认使用全局配置中的参数信息。

#### database_info (可选)
同《全局配置参数》章节中同名参数的描述，指定要在哪个数据库中创建超级表。如果未指定，则默认使用全局配置中的参数信息。

#### super_table_info (可选)
同《全局配置参数》章节中同名参数的描述，包含超级表创建所需的所有细节。如果未指定，则默认使用全局配置中的参数信息。

### 创建子表的 Action 配置
`actions/create-child-table` 用于基于指定的超级表，在目标数据库中批量创建多个子表（Child Tables）。每个子表可以拥有不同的名称和标签列数据，从而实现对时间序列数据的有效分类与管理。该 Action 支持从生成器（Generator）或 CSV 文件两种来源定义子表名称及标签列信息，具备高度灵活性和可配置性。

#### connection_info (可选)
同《全局配置参数》章节中同名参数的描述，如果未指定，则默认使用全局配置中的参数信息。

#### data_format (可选)
同《全局配置参数》章节中同名参数的描述，如果未指定，则默认使用全局配置中的参数信息。

#### data_channel (可选)
同《全局配置参数》章节中同名参数的描述，如果未指定，则默认使用全局配置中的参数信息。

#### database_info (可选)
同《全局配置参数》章节中同名参数的描述，指定要在哪个数据库中创建子表。如果未指定，则默认使用全局配置中的参数信息。

#### super_table_info (可选)
同《全局配置参数》章节中同名参数的描述，指定基于哪个超级表创建子表。如果未指定，则默认使用全局配置中的参数信息。

#### child_table_info (必需)
包含创建子表所需的核心信息，包括子表名称和标签列数据的来源及具体配置。

**table_name（子表名称）**

- source_type (字符串，必需)：
  子表名称的数据来源支持以下两种方式：generator、csv。
- generator：仅在 source_type="generator" 时生效，包含如下属性：
  使用生成器动态生成子表名称列表，需提供以下属性：
  - prefix (字符串)：
    子表名前缀，默认为 "d"。
  - count (整数)：
    要创建的子表数量，默认为 10000。
  - from (整数)：
    子表名称的起始下标（包含），默认为 0。
- csv：仅在 source_type="csv" 时生效，包含如下属性：
  从 CSV 文件读取子表名称列表，需提供以下属性：
  - file_path (字符串)：
    CSV 文件路径。
  - has_header (布尔)：
    是否包含表头行，默认为 true。
  - tbname_index (整数)：
    指定子表名称所在的列索引（从 0 开始），默认为 0。

**tags（标签列）**

- source_type (字符串，必需)：
  标签列的数据来源支持以下两种方式：generator、csv。
- generator：仅在 source_type="generator" 时生效，包含如下属性：
  使用生成器动态生成标签列数据，需提供以下属性：
  - schema (列表类型，可选)：
    标签列的 Schema 定义，每个元素表示一个标签列，包含字段名（name）、类型（type）以及生成规则（如随机等）。若未指定，则使用全局作用域中预定义的标签列的 Schema。
- csv：仅在 source_type="csv" 时生效，包含如下属性：
  从 CSV 文件读取标签列数据，需提供以下属性：
  - schema (列表类型，可选)：标签列的 Schema 定义，每个元素表示一个标签列，包含字段名（name）、类型（type）等信息。
  - file_path (字符串)：
    CSV 文件路径。
  - has_header (布尔)：
    是否包含表头行，默认为 true。
  - exclude_indices (字符串)：
    若文件中同时包含子表名称列和标签列，或者仅想使用部分标签列时，此参数用于指定剔除的子表名称列/无用标签列等的索引（从 0 开始），列索引之间使用英文逗号,分隔，默认值为空，表示不剔除。

#### batch (可选)
控制批量创建子表时的行为：
- size (整数)：
  每批创建的子表数量，默认值为 1000。
- concurrency (整数)：
  并发执行的批次数量，提升创建效率，默认值为 10。

### 插入数据的 Action 配置
`actions/insert-data` Action 用于将数据插入到指定的子表中。它支持从生成器或 CSV 文件两种来源获取子表名称、普通列数据，并允许用户通过多种时间戳策略控制数据的时间属性。此外，还提供了丰富的写入控制策略以优化数据插入过程，具备高度灵活性和可配置性。

#### source (必需)
包含了需要插入的数据的所有相关信息：

**table_name（子表名称）**

描述同：《创建子表的 Action 的配置》中的同名配置项的描述。

**columns（普通列）**

- source_type (字符串，必需)：
  普通列的数据来源支持以下两种方式：generator、csv。
- generator：
  仅在 source_type="generator" 时生效，使用生成器动态生成普通列数据，需提供以下属性：
  - schema (列表类型，必须)：
    普通列的 Schema 定义，每个元素表示一个普通列，包含字段名（name）、类型（type）以及生成规则（如随机等）。
  - timestamp_strategy (时间戳列策略，可选)：
    generator 类型数据源下的时间戳列策略仅有一种类型，即是生成方式，包含如下属性：
    - start_timestamp (整数或关键字 "now"，可选)：表示子表的时间戳列的起始值，默认值为 "now"。
    - timestamp_precision (字符串，可选)：
      表示时间戳列的时间精度，可选值为："ms"、"us"、"ns"，默认与数据目标中的时间戳列的精度一致。
    - timestamp_step (整数，可选)：表示子表中插入数据的时间戳步长，单位与时间精度一致，默认值是 1。
- csv
  仅在 source_type="csv" 时生效，从 CSV 文件读取普通列数据，需提供以下属性：
  - schema (列表类型，可选)：
    普通列的 Schema 定义，每个元素表示一个普通列，包含字段名（name）、类型（type）。
  - file_path (字符串，必需)：
    CSV 文件路径，支持单个文件或目录路径。
  - has_header (布尔，可选)：
    是否包含表头行，默认为 true。
  - tbname_index （整数，可选）：
    指定子表名称所在的列索引（从 0 开始）。
  - timestamp_strategy (时间戳列策略，必需)：用于控制时间戳的生成逻辑。
    - strategy_type (字符串，必需)：时间戳生成策略类型，默认为 original，可选值包括：
      - original：使用原始文件中的时间列作为时间戳。
      - generator：根据用户规则 start_timestamp 和 timestamp_step 生成时间戳。
    1. original (对象，可选)：仅在 strategy_type="original" 时生效，包含以下属性：
      - timestamp_index (整数，可选)：指定原始时间列的索引（从 0 开始），默认值为 0。
      - timestamp_precision (字符串，可选)：表示原始时间列的时间精度，默认与数据目标中的时间戳列的精度一致，可选值为 "s"、"ms"、"us"、"ns"。
      - offset_config (可选)：
        - offset_type (字符串)：表示时间戳偏移类型，可选值为："relative"、"absolute"。
        - value（字符串或整型）：表示时间戳的偏移量（relative）或起始时间戳（absolute）：
          - 时间戳偏移类型为 "relative" 时：字符串类型，格式为 ±[数值][单位] 组合（示例："+1d3h" 表示加1天3小时），支持以下时间单位：
            - y：年偏移量
            - m：月偏移量
            - d：天偏移量
            - s：秒偏移量
          - 时间戳偏移类型为 "absolute" 时：整型或字符串类型，格式如下：
            - 时间戳数值（精度由 timestamp_precision 参数决定）
            - ISO 8601 格式字符串（"YYYY-MM-DD HH:mm:ss"）
    2. generator (对象，可选)：仅在 strategy_type="generator" 时生效，包含以下属性：
      - start_timestamp (整数或字符串，可选)：表示子表的时间戳列的起始值，默认值为 "now"。
      - timestamp_precision (字符串，可选)：
        表示时间戳列的时间精度，可选值为："ms"、"us"、"ns"，默认与数据目标中的时间戳列的精度一致。
      - timestamp_step (整数，可选)：表示子表中插入数据的时间戳步长，单位与时间精度一致，默认值是 1。

#### target (必需)
描述数据写入的目标数据库或其他存储介质信息：

**timestamp_precision （时间戳精度，可选）**

字符串类型：表示时间戳列的精度，可选值为："ms"、"us"、"ns"，当数据目标是 tdengine 时，默认为数据库的精度，否则默认为 "ms"。

**target_type（目标类型，必需）**

字符串类型，目标数据类型支持以下几种方式：
- tdengine：TDengine 数据库。
- mqtt：轻量级的物联网通信协议。

**tdengine**

仅在 target_type="tdengine" 时生效，包含如下属性：
- connection_info (必需)：数据库连接信息。
- database_info (对象类型，必需)：包含目标数据库的相关信息：
  - name (字符串，必需)：数据库名称。
  - precision (字符串，可选)：数据库的时间精度，与上边 timestamp_precision 的值保持一致。
- super_table_info (必需)：包含超级表的信息：
  - name (字符串，必需)：超级表名称。
  - columns (可选)：引用预定义的普通列 Schema。
  - tags (可选)：引用预定义的标签列 Schema。

**mqtt**

仅在 target_type="mqtt" 时生效，包含如下属性：
- host (字符串，可选)： MQTT Broker 主机地址，默认值为 localhost。
- port (整数，可选)： MQTT Broker 端口，默认值为 1883。
- username (字符串，可选)： 登录 Broker 的用户名。
- password (字符串，可选)： 登录 Broker 的密码。
- client_id (字符串，可选)： 客户端唯一标识符，若未指定则自动生成；
- topic (字符串，必需)： 要发布消息的 MQTT Topic，支持通过占位符语法发布到动态主题，占位符语法如下：
  - `{table}`：表示表名数据
  - `{column}`：表示列数据，column 是列字段名称
- timestamp_precision (字符串，可选)： 表示消息时间戳的精度，可选值为："ms"、"us"、"ns"，默认为 "ms"。
- qos (整数，可选)： QoS 等级，取值范围为 0、1、2，默认为 0。
- keep_alive (整数，可选)： 超时没有消息发送后会发送心跳，单位为秒，默认值为 5。
- clean_session（布尔，可选）：是否清除旧会话状态，默认值为 true。
- retain （布尔，可选）：MQTT Broker 是否保留最后一条消息，默认值为 false。

#### control (必需)
定义数据写入过程中的行为策略，包括数据格式化（data_format）、数据通道（data_channel）、数据生成策略（data_generation）、写入控制策略（insert_control）、时间间隔策略（time_interval）等部分。

**data_format（数据格式化，可选）**

同《全局配置参数》章节中同名参数的描述，如果未指定，则默认使用全局配置中的参数信息。

**data_channel （数据通道，可选）**

同《全局配置参数》章节中同名参数的描述，如果未指定，则默认使用全局配置中的参数信息。

**data_generation（数据生成策略，可选）**

定义数据生成的行为相关设置：
- interlace_mode（可选）：控制交错生成子表数据的方式。
  - enabled (布尔，可选)：表示是否启用交错模式，默认值为 false。
  - rows (整数，可选)：表示每个子表单次生成的行数，默认值为 1。
- generate_threads (整数，可选)，表示生成数据的线程数量，默认值为 1。
- per_table_rows (整数，可选)，每个子表插入的行数，默认值为 10000。
- queue_capacity (整数，可选)，表示存放生成数据的队列的容量，默认值为 100。
- queue_warmup_ratio（浮点，可选），表示队列中数据预热生成的比例，默认值为 0.5，表示提前生成队列容量 50%的数据。

**insert_control（写入控制策略，可选）**

写入控制策略：控制实际数据的写入目标数据库或文件的行为细节。
- per_request_rows (整数，可选)，默认值为 10000，表示每次请求写入的最大行数。
- insert_threads (整数，可选)，默认值为 8，表示并发写入线程数量。

**time_interval（时间间隔策略，可选）**

  控制写入过程中时间间隔分布策略。
- enabled (布尔，可选)：默认值为 false，表示是否启用时间间隔控制。
- interval_strategy (字符串，可选)：表示时间间隔策略类型，默认值为 fixed。可选值为：
  - fixed：固定的时间间隔。
  - first_to_first：本次发送数据的首行的时间列 - 上次发送数据的首行的时间列。
  - last_to_first：本次发送数据的首行的时间列 - 上次发送数据的末行的时间列。
  - literal：根据本次发送数据的首行的时间列的值的时间点来发送，模拟实时产生数据的场景。
- fixed_interval：仅在 interval_strategy = fixed 时生效：
  - base_interval (整数，必需)：表示固定间隔数值，单位毫秒。
- dynamic_interval：仅在 interval_strategy = first_to_first / last_to_first 时生效：
  - min_interval (整数，可选)：默认值为 -1，表示最小时间间隔阈值。
  - max_interval (整数，可选)：默认值为 -1，表示最大时间间隔阈值。
- wait_strategy (字符串，可选)：表示在开启时间间隔控制时，发送写入请求之间的等待策略，默认值为：sleep，可选值为：
  - sleep：睡眠，归还当前线程的执行权给操作系统。
  - busy_wait：忙等待，保持当前线程的执行权。

## 配置文件示例

### 生成器方式生成数据 stmt v2 写入 TDengine 示例

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
    pool:
      enabled: true
      max_size: 10
      min_size: 2
      connection_timeout: 1000

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
    count: 100000
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
              generate_threads: 1
              per_table_rows: 100
              queue_capacity: 100
              queue_warmup_ratio: 0.5
            insert_control:
              per_request_rows: 10000
              auto_create_table: false
              insert_threads: 1
```

### CSV文件方式生成数据 stmt v2 写入 TDengine 实例

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
    pool:
      enabled: true
      max_size: 10
      min_size: 2
      connection_timeout: 1000

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
    count: 100000
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
              source_type: csv
              csv:
                file_path: ../src/parameter/conf/ctb-tags.csv
                tbname_index: 2
            tags:
              source_type: csv
              csv:
                schema: *tags_info
                file_path: ../src/parameter/conf/ctb-tags.csv
                exclude_indices: 2
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
              source_type: csv
              csv:
                file_path: ../src/parameter/conf/ctb-tags.csv
                tbname_index: 2
            columns:
              source_type: csv
              csv:
                schema: *columns_info
                file_path: ../src/parameter/conf/ctb-data.csv
                tbname_index : 0

                timestamp_strategy:
                  strategy_type: generator
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
              generate_threads: 1
              per_table_rows: 100
              queue_capacity: 100
              queue_warmup_ratio: 0.0
            insert_control:
              per_request_rows: 10000
              insert_threads: 1
```

其中：
- `ctb-tags.csv` 文件内容为：

```csv
groupid,location,tbname
1,loc1,d1
2,loc2,d2
3,loc3,d3
```

- `ctb-data.csv` 文件内容为：

```csv
tbname,current,voltage,phase
d1,11,200,1001
d3,13,201,3001
d2,12,202,2001
d3,23,203,3002
d2,22,204,2002
d1,21,205,1002
```

### 生成器方式生成数据并写入 MQTT 示例

```yaml
global:
  confirm_prompt: false

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
      - name: state
        type: varchar(20)
        values:
          - "normal"
          - "warning"
          - "critical"

  tbname_generator: &tbname_generator
    prefix: d
    count: 10000
    from: 0

concurrency: 1

jobs:
  # Insert data job
  insert-into-mqtt:
    name: Insert Data Into MQTT
    needs: []
    steps:
      - name: Insert Data Into MQTT
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
            target_type: mqtt
            mqtt:
              host: localhost
              port: 1883
              user: testuser
              password: testpassword
              client_id: mqtt_client
              keep_alive: 60
              clean_session: true
              qos: 1
              topic: factory/{table}/{state}/data

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
              generate_threads: 1
              per_table_rows: 1000
              queue_capacity: 10
              queue_warmup_ratio: 0.00
            insert_control:
              per_request_rows: 10
              insert_threads: 8
```



