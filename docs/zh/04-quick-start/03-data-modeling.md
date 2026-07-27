---
sidebar_label: 数据建模
title: 数据建模
description: 使用 SQL 快速创建数据库、超级表、子表、普通表和虚拟表
toc_max_heading_level: 4
---

本章基于 [基本概念](./02-basic-concepts.md) 中的智能电表模型，介绍如何在 TDengine 中使用 SQL 创建数据库、超级表、子表、普通表和虚拟表。

## 创建数据库

创建一个数据库以存储电表数据的 SQL 如下：

```sql
CREATE DATABASE power PRECISION 'ms' KEEP 3650 DURATION 10 BUFFER 16;
```

该 SQL 将创建一个名为 `power` 的数据库，各参数说明如下：

- `PRECISION 'ms'`：这个数据库的时序数据使用毫秒（ms）精度的时间戳
- `KEEP 3650`：这个库的数据将保留 3650 天，超过 3650 天的数据将被自动删除
- `DURATION 10`：每 10 天的数据放在一个数据文件中
- `BUFFER 16`：写入使用大小为 16 MB 的内存池

创建 `power` 数据库后，可以执行 `USE` 语句切换数据库。

```sql
USE power;
```

该 SQL 将当前数据库切换为 `power`，表示之后的插入、查询等操作都在 `power` 数据库中进行。

## 创建超级表

创建一张名为 `meters` 的超级表的 SQL 如下：

```sql
CREATE STABLE meters (
    ts timestamp,
    current float,
    voltage int,
    phase float
) TAGS (
    location varchar(64),
    group_id int
);
```

在 TDengine 中，创建超级表的 SQL 语句与关系型数据库类似。例如，上面的 SQL 中，`CREATE STABLE` 为关键字，表示创建超级表；接着，`meters` 是超级表的名称；在表名后面的括号中，定义超级表的列（列名、数据类型等），规则如下：

1. 第 1 列必须为时间戳列。例如：`ts timestamp` 表示，时间戳列名是 `ts`，数据类型为 `timestamp`；
2. 第 2 列开始是采集量列。采集量的数据类型可以为整型、浮点型、字符串等。例如：`current float` 表示，采集量电流 `current`，数据类型为 `float`。

最后，`TAGS` 是关键字，表示标签。在 `TAGS` 后面的括号中，可以定义超级表的标签（标签名、数据类型等）。

1. 标签的数据类型可以为整型、浮点型、字符串等。例如：`location varchar(64)` 表示，地区标签 `location` 的数据类型为 `varchar(64)`；
2. 标签的名称不能与采集量列的名称相同。

## 创建子表

通过超级表创建子表 `d1001` 的 SQL 如下：

```sql
CREATE TABLE d1001
USING meters (
    location,
    group_id
) TAGS (
    "California.SanFrancisco",
    2
);
```

上面的 SQL 中，`CREATE TABLE` 为关键字，表示创建子表；`d1001` 是子表的名称；`USING` 是关键字，表示使用超级表作为模板；`meters` 是超级表的名称；超级表名后的括号中，`location`、`group_id` 是超级表的标签列名列表；`TAGS` 是关键字，后面的括号中指定子表的标签列值。`"California.SanFrancisco"` 和 `2` 表示子表 `d1001` 的位置为 `California.SanFrancisco`，分组 ID 为 `2`。

当对超级表进行写入或查询操作时，用户可以使用伪列 `tbname` 指定或输出对应操作的子表名。

## 自动建表

在 TDengine 中，为了简化操作并确保数据顺利写入，即使子表尚不存在，用户也可以使用带有 `USING` 关键字的自动建表 SQL 进行数据写入。这种机制允许系统在遇到不存在的子表时，先自动创建该子表，再执行数据写入操作。如果子表已经存在，系统会直接写入数据，不需要额外步骤。

在写入数据的同时自动建表的 SQL 如下：

```sql
INSERT INTO d1002
USING meters
TAGS (
    "California.SanFrancisco",
    2
) VALUES (
    NOW,
    10.2,
    219,
    0.32
);
```

上面的 SQL 中，`INSERT INTO d1002` 表示向子表 `d1002` 中写入数据；`USING meters` 表示使用超级表 `meters` 作为模板；`TAGS ("California.SanFrancisco", 2)` 表示子表 `d1002` 的标签值分别为 `California.SanFrancisco` 和 `2`；`VALUES (NOW, 10.2, 219, 0.32)` 表示向子表 `d1002` 插入一行记录，值分别为 `NOW`（当前时间戳）、`10.2`（电流）、`219`（电压）和 `0.32`（相位）。在 TDengine 执行这条 SQL 时，如果子表 `d1002` 已经存在，则直接写入数据；如果子表 `d1002` 不存在，则先自动创建子表，再写入数据。

## 创建普通表

在 TDengine 中，除了具有标签的子表以外，还存在一种不带任何标签的普通表。这类表与普通关系型数据库中的表相似，用户可以使用 SQL 创建它们。

普通表与子表的区别在于：

1. 标签扩展性：子表在普通表的基础上增加了静态标签，这使得子表能够携带更多的元数据信息。此外，子表的标签是可变的，用户可以根据需要增加、删除或修改标签。
2. 表归属：子表总是隶属于某张超级表，它们是超级表的一部分。而普通表则独立存在，不属于任何超级表。
3. 转换限制：在 TDengine 中，普通表无法直接转换为子表，同样，子表也无法转换为普通表。这两种表类型在创建时就确定了它们的结构和属性，后期无法更改。

总结来说，普通表提供了类似于传统关系型数据库的表功能，而子表则通过引入标签机制，为时序数据提供了更丰富的描述能力和更灵活的管理方式。用户可以根据实际需求选择创建普通表还是子表。

创建不带任何标签的普通表的 SQL 如下：

```sql
CREATE TABLE d1003(
    ts timestamp,
    current float,
    voltage int,
    phase float,
    location varchar(64),
    group_id int
);
```

上面的 SQL 表示创建普通表 `d1003`。表结构包括 `ts`、`current`、`voltage`、`phase`、`location`、`group_id`，共 6 列。这样的数据模型与关系型数据库一致。

采用普通表作为数据模型意味着静态标签数据（如 `location` 和 `group_id`）会重复存储在表的每一行中。这种做法不仅增加了存储空间消耗，而且在查询时无法直接利用标签数据进行过滤，查询性能会低于使用超级表的数据模型。

## 多列模型与单列模型

TDengine 支持灵活的数据模型设计，包括多列模型和单列模型。多列模型允许将多个由同一数据采集点同时采集且时间戳一致的物理量作为不同列存储在同一张超级表中。然而，在某些极端情况下，可能会采用单列模型，即每个采集的物理量都单独建立一张表。例如，对于电流、电压和相位这 3 种物理量，可能会分别建立 3 张超级表。

尽管 TDengine 推荐使用多列模型，因为这种模型在写入效率和存储效率方面通常更优，但在某些特定场景下，单列模型可能更为适用。例如，当一个数据采集点的采集量种类经常发生变化时，如果采用多列模型，就需要频繁修改超级表的结构定义，这会增加应用程序的复杂性。在这种情况下，采用单列模型可以简化应用程序的设计和管理，因为它允许独立地管理和扩展每个物理量的超级表。

总之，TDengine 提供了灵活的数据模型选项，用户可以根据实际需求和场景选择最适合的模型，以优化性能和管理复杂性。

## 创建虚拟表

无论选择单列模型还是多列模型，TDengine 都可以通过虚拟表进行跨表运算。以智能电表为例，这里介绍虚拟表的两种使用场景：

1. 单源多维度时序聚合
2. 跨源采集量对比分析

### 单源多维度时序聚合

在单源多维度时序聚合场景中，“单源”并非指单一物理表，而是指来自**同一数据采集点**下的多个单列时序数据表。这些数据因业务需求或其他限制被拆分为多个单列存储的表，但通过设备标签和时间基准保持逻辑一致性。虚拟表在此场景中的作用是将一个采集点中“纵向”拆分的数据，还原为完整的“横向”状态。
例如，在建模时采用了单列模型，对于电流、电压和相位这 3 种物理量，分别建立 3 张超级表。在这种场景下，用户可以通过虚拟表将这 3 种不同的采集量聚合到一张表中，以便进行统一的查询和分析。

创建单列模型的超级表的 SQL 如下：

```sql

CREATE STABLE current_stb (
    ts timestamp,
    current float
) TAGS (
    device_id varchar(64),
    location varchar(64),
    group_id int
);

CREATE STABLE voltage_stb (
    ts timestamp,
    voltage int
) TAGS (
    device_id varchar(64),
    location varchar(64),
    group_id int
);

CREATE STABLE phase_stb (
    ts timestamp,
    phase float
) TAGS (
    device_id varchar(64),
    location varchar(64),
    group_id int
);
```

假设有 d1001、d1002、d1003、d1004 四个设备，为四个设备的电流、电压、相位采集量分别创建子表，SQL 如下：

```sql
create table current_d1001 using current_stb(device_id, location, group_id) tags("d1001", "California.SanFrancisco", 2);
create table current_d1002 using current_stb(device_id, location, group_id) tags("d1002", "California.SanFrancisco", 3);
create table current_d1003 using current_stb(device_id, location, group_id) tags("d1003", "California.LosAngeles", 3);
create table current_d1004 using current_stb(device_id, location, group_id) tags("d1004", "California.LosAngeles", 2);

create table voltage_d1001 using voltage_stb(device_id, location, group_id) tags("d1001", "California.SanFrancisco", 2);
create table voltage_d1002 using voltage_stb(device_id, location, group_id) tags("d1002", "California.SanFrancisco", 3);
create table voltage_d1003 using voltage_stb(device_id, location, group_id) tags("d1003", "California.LosAngeles", 3);
create table voltage_d1004 using voltage_stb(device_id, location, group_id) tags("d1004", "California.LosAngeles", 2);

create table phase_d1001 using phase_stb(device_id, location, group_id) tags("d1001", "California.SanFrancisco", 2);
create table phase_d1002 using phase_stb(device_id, location, group_id) tags("d1002", "California.SanFrancisco", 3);
create table phase_d1003 using phase_stb(device_id, location, group_id) tags("d1003", "California.LosAngeles", 3);
create table phase_d1004 using phase_stb(device_id, location, group_id) tags("d1004", "California.LosAngeles", 2);
```

可以通过一张虚拟超级表将这 3 种采集量聚合到一张表中。创建虚拟超级表的 SQL 如下：

```sql
CREATE STABLE meters_v (
    ts timestamp,
    current float,
    voltage int,
    phase float
) TAGS (
    location varchar(64),
    group_id int
) VIRTUAL 1;
```

然后为 `d1001`、`d1002`、`d1003`、`d1004` 这 4 个设备分别创建虚拟子表，SQL 如下：

```sql
CREATE VTABLE d1001_v (
    current from current_d1001.current,
    voltage from voltage_d1001.voltage,
    phase from phase_d1001.phase
)
USING meters_v
TAGS (
    "California.SanFrancisco",
    2
);

CREATE VTABLE d1002_v (
    current from current_d1002.current,
    voltage from voltage_d1002.voltage,
    phase from phase_d1002.phase
)
USING meters_v
TAGS (
    "California.SanFrancisco",
    3
);

CREATE VTABLE d1003_v (
    current from current_d1003.current,
    voltage from voltage_d1003.voltage,
    phase from phase_d1003.phase
)
USING meters_v
TAGS (
    "California.LosAngeles",
    3
);

CREATE VTABLE d1004_v (
    current from current_d1004.current,
    voltage from voltage_d1004.voltage,
    phase from phase_d1004.phase
)
USING meters_v
TAGS (
    "California.LosAngeles",
    2
);
```

以设备 d1001 为例，假设 d1001 设备的电流、电压、相位数据如下：

<table>
    <tr>
        <th colspan="2" align="center">current_d1001</th>
        <th rowspan="7" align="center"></th>
        <th colspan="2" align="center">voltage_d1001</th>
        <th rowspan="7" align="center"></th>
        <th colspan="2" align="center">phase_d1001</th>
    </tr>
    <tr>
        <td align="center">Timestamp</td>
        <td align="center">Current</td>
        <td align="center">Timestamp</td>
        <td align="center">Voltage</td>
        <td align="center">Timestamp</td>
        <td align="center">Phase</td>
    </tr>
    <tr>
        <td align="center">1538548685000</td>
        <td align="center">10.3</td>
        <td align="center">1538548685000</td>
        <td align="center">219</td>
        <td align="center">1538548685000</td>
        <td align="center">0.31</td>
    </tr>
    <tr>
        <td align="center">1538548695000</td>
        <td align="center">12.6</td>
        <td align="center">1538548695000</td>
        <td align="center">218</td>
        <td align="center">1538548695000</td>
        <td align="center">0.33</td>
    </tr>
    <tr>
        <td align="center">1538548696800</td>
        <td align="center">12.3</td>
        <td align="center">1538548696800</td>
        <td align="center">221</td>
        <td align="center">1538548696800</td>
        <td align="center">0.31</td>
    </tr>
    <tr>
        <td align="center">1538548697100</td>
        <td align="center">12.1</td>
        <td align="center">1538548697100</td>
        <td align="center">220</td>
        <td align="center">1538548697200</td>
        <td align="center">0.32</td>
    </tr>
    <tr>
        <td align="center">1538548697700</td>
        <td align="center">11.8</td>
        <td align="center">1538548697800</td>
        <td align="center">222</td>
        <td align="center">1538548697800</td>
        <td align="center">0.33</td>
    </tr>
</table>

虚拟表 `d1001_v` 中的数据如下：

|   Timestamp   | Current | Voltage | Phase |
| :-----------: | :-----: | :-----: | :---: |
| 1538548685000 |  10.3   |   219   | 0.31  |
| 1538548695000 |  12.6   |   218   | 0.33  |
| 1538548696800 |  12.3   |   221   | 0.31  |
| 1538548697100 |  12.1   |   220   | NULL  |
| 1538548697200 |  NULL   |  NULL   | 0.32  |
| 1538548697700 |  11.8   |  NULL   | NULL  |
| 1538548697800 |  NULL   |   222   | 0.33  |

### 跨源采集量对比分析

在跨源采集量对比分析中，“跨源”指数据来自**不同数据采集点**。从不同数据采集点中提取具有可比语义的采集量后，可以通过虚拟表将这些采集量按照时间戳进行对齐和合并，并进行对比分析。
例如，用户可以将来自不同设备的电流数据聚合到一张虚拟表中，以便进行电流数据的对比分析。

以分析 d1001、d1002、d1003、d1004 四个设备的电流数据为例，创建虚拟表的 SQL 如下：

```sql
CREATE VTABLE current_v (
    ts timestamp,
    d1001_current float from current_d1001.current,
    d1002_current float from current_d1002.current,
    d1003_current float from current_d1003.current,
    d1004_current float from current_d1004.current
);
```

假设 `d1001`、`d1002`、`d1003`、`d1004` 这 4 个设备的电流数据如下：

<table>
    <tr>
        <th colspan="2" align="center">d1001</th>
        <th rowspan="7" align="center"></th>
        <th colspan="2" align="center">d1002</th>
        <th rowspan="7" align="center"></th>
        <th colspan="2" align="center">d1003</th>
        <th rowspan="7" align="center"></th>
        <th colspan="2" align="center">d1004</th>
    </tr>
    <tr>
        <td align="center">Timestamp</td>
        <td align="center">Current</td>
        <td align="center">Timestamp</td>
        <td align="center">Current</td>
        <td align="center">Timestamp</td>
        <td align="center">Current</td>
        <td align="center">Timestamp</td>
        <td align="center">Current</td>
    </tr>
    <tr>
        <td align="center">1538548685000</td>
        <td align="center">10.3</td>
        <td align="center">1538548685000</td>
        <td align="center">11.7</td>
        <td align="center">1538548685000</td>
        <td align="center">11.2</td>
        <td align="center">1538548685000</td>
        <td align="center">12.4</td>
    </tr>
    <tr>
        <td align="center">1538548695000</td>
        <td align="center">12.6</td>
        <td align="center">1538548695000</td>
        <td align="center">11.9</td>
        <td align="center">1538548695000</td>
        <td align="center">10.8</td>
        <td align="center">1538548695000</td>
        <td align="center">11.3</td>
    </tr>
    <tr>
        <td align="center">1538548696800</td>
        <td align="center">12.3</td>
        <td align="center">1538548696800</td>
        <td align="center">12.4</td>
        <td align="center">1538548696800</td>
        <td align="center">12.3</td>
        <td align="center">1538548696800</td>
        <td align="center">10.1</td>
    </tr>
    <tr>
        <td align="center">1538548697100</td>
        <td align="center">12.1</td>
        <td align="center">1538548697200</td>
        <td align="center">12.2</td>
        <td align="center">1538548697100</td>
        <td align="center">11.1</td>
        <td align="center">1538548697200</td>
        <td align="center">11.7</td>
    </tr>
    <tr>
        <td align="center">1538548697700</td>
        <td align="center">11.8</td>
        <td align="center">1538548697700</td>
        <td align="center">11.4</td>
        <td align="center">1538548697800</td>
        <td align="center">12.1</td>
        <td align="center">1538548697800</td>
        <td align="center">12.6</td>
    </tr>
</table>

虚拟表 `current_v` 中的数据如下：

|   Timestamp   | d1001_current | d1002_current | d1003_current | d1004_current |
| :-----------: | :-----------: | :-----------: | :-----------: | :-----------: |
| 1538548685000 |     10.3      |     11.7      |     11.2      |     12.4      |
| 1538548695000 |     12.6      |     11.9      |     10.8      |     11.3      |
| 1538548696800 |     12.3      |     12.4      |     12.3      |     10.1      |
| 1538548697100 |     12.1      |     NULL      |     11.1      |     NULL      |
| 1538548697200 |     NULL      |     12.2      |     NULL      |     11.7      |
| 1538548697700 |     11.8      |     11.4      |     NULL      |     NULL      |
| 1538548697800 |     NULL      |     NULL      |     12.1      |     12.6      |
