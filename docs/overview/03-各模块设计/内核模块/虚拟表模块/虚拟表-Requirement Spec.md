# 虚拟表-Requirement Spec

## 1. 修订记录

| 编写日期 | 发布日期 | 版本 | 修订人 | 主要修改内容 |
| --- | --- | --- | --- | --- |
| 2025-01-22 | 2025-01-22 | 1.0 | 司马靖 | 第一次安可送测 |
| 2025-12-26 | 2025-12-26 | 1.1 | 廖浩均 | 重构文档 |

## 2. 引言

### 2.1 术语与缩写名词

无

### 2.2 相关文档资料

| 文档 | 链接 |
| --- | --- |
| 需求报告 | [opc存储不同超级表之间的数据连接查询需求](https://taosdata.feishu.cn/wiki/MCRCwIihpiK3dukRWtYc0Swcn6f) |
| 用户需求 | [TD 支持大宽表](https://taosdata.feishu.cn/wiki/JlCuwxDhhiPPa4kjdnicxzyFn8d) |
| 用户需求 | [用户需求：用单列模型实现宽表](https://taosdata.feishu.cn/wiki/QT9SwfV6fiLl1dkWXnCcZjA0nBb) |

### 2.3 优先级要求

用于实现单列模型模拟大宽表的逻辑呈现，具有较高的优先级。

### 2.4 版本要求

企业版和社区版均支持。

## 3. 需求目标

### 3.1 背景

在工业自动化、SCADA 及能源监控系统中，测点数据通常通过 OPC、IEC 104 等工控协议，或经由实时数据库同步通道，最终写入 TDengine 时序数据库。为适配高并发、低延迟的采集特性，数据模型普遍采用单列超级表结构，每个测点独立成列，实现高效存储与扩展。
某钢铁项目：机械动力室部署 Kepware 采集模拟量与开关量，通过 taosX 组件对接 OPC 服务，数据写入 TDengine 时构建三类单列超级表：bool_tags（布尔型）、float_tags（浮点型）、int_tags（整型）。每类表对应一种数据类型，实现类型安全与查询隔离。
某水电院项目：用户自主开发 IEC 104 协议解析模块，从集控系统及其他厂站平台抽取时序数据，同样采用三类超级表结构进行分类存储，确保协议解析与数据建模解耦。
在汽车制造、风力发电等高密度测点场景，单台设备可达数千测点。依据采集源与采样频率，这些测点被划分为数十个逻辑组，每组映射为一个独立超级表，实现：1）数据隔离，避免跨组干扰；2）存储策略差异化（如保留策略、压缩算法）；3）查询性能优化。
数据接入后，典型分析需求包括：1）断面查询，获取同一设备在某一时刻的所有测点快照；2）时间窗口对齐：在连续时间区间内，同步比对多个测点的变化趋势。
当前 TDengine 的 `interp()` 与窗口函数仅支持单表内多列插值与聚合。若需跨表（即跨超级表）对齐数据，必须使用 JOIN 操作。然而：`JOIN` 语法需显式指定表名、时间条件与关联字段，多表关联易引发性能瓶颈与语法错误，且编写复杂度高，维护成本大。上述用户的业务场景需求有悖于 “简单、高效、易用”的宗旨，亟需原生支持跨表时间对齐查询能力，以降低用户使用门槛，提升时序分析效率。

### 3.2 目标

通过增强视图，汇总单个设备分布在多个子表的测点，用简单的 SQL 即可查询。用于“数据接入前不了解数据模型，或者设备有多个不同频率测点组”的场景，在使用上等同于大宽表。
设备视图构建：通过原生设备视图功能，可无缝聚合多张超级表，构建逻辑上的宽表结构，实现“一视图览全貌”，无需手动拼接或维护冗余物理表，灵活适配设备级分析场景。
标签存储革新：标签数据由原先的多份冗余存储，升级为单一权威源管理，彻底消除数据不一致风险，支持原子级更新，确保配置变更一次生效、全局同步。
高效时序连接：内置时序感知的多表关联引擎，无需显式 JOIN，即可在时间轴上自动对齐跨表数据，查询性能较传统 JOIN 提升数倍，尤其在千万级测点场景下优势显著。
查询语法简化：断面查询与时间窗口对齐操作，仅需一行语义清晰的语法（如 `SELECT * FROM device_view WHERE _c0 = '2025-12-25 10:00:00'`），告别复杂 JOIN 条件与子查询，降低开发门槛，提升分析效率。

## 4. 功能需求

### 4.1 **语法示例**

创建虚拟表的基础语法如下所示：
```sql

-- 创建虚拟普通表
CREATE VTABLE [IF NOT EXISTS] [db_name].vtb_name 
    ts_col_name timestamp, 
    (create_definition[ ,create_definition] ...) 
     
  create_definition:
    vtb_col_name column_definition
    
  column_definition:
    type_name [FROM [db_name.]table_name.col_name]
    
    
-- 创建虚拟子表
CREATE VTABLE [IF NOT EXISTS] [db_name].vtb_name 
    (create_definition[ ,create_definition] ...) 
    USING [db_name.]stb_name 
    [(tag_name [, tag_name] ...)] 
    TAGS (tag_value [, tag_value] ...)
     
  create_definition:
    [stb_col_name FROM] [db_name.]table_name.col_name
  tag_value:
     const_value

```

### 4.2 **虚拟表查询生成规则**

1. 虚拟表以时间戳为基准，对多个原始表的数据进行对齐。
2. 如果多个原始表在相同时间戳下有数据，则这些列的值组合成同一行；否则，对于缺失的列，填充 `NULL`。
3. 虚拟表的时间戳的值是查询中包含的所有列所在的原始表的时间戳的并集，因此当不同查询选择列不同时可能出现结果集行数不一样的情况。
4. 用户可以从多个表中选择任意列进行组合，未选择的列不会出现在虚拟表中。

### 4.3 投影查询

`select * from virtual_table_name`，默认以查询列表第一个出现的列为基准，其他列对齐。

| ts1 | c1 | c2 | ts2 | c3 | c4 |
| --- | --- | --- | --- | --- | --- |
| 2023-08-28 12:00:00.123 | 1 | 6 | 2023-08-28 12:00:00.123 | 11 | 32 |
| 2023-08-28 12:00:10.224 | 2 | 7 | NULL | NULL | NULL |
| 2023-08-28 12:00:20.241 | 3 | 8 | NULL | NULL | NULL |
| NULL | NULL | NULL | 2023-08-28 12:00:30.388 | 234 | 67 |
| 2023-08-28 12:00:40.326 | 5 | 10 | 2023-08-28 12:00:40.326 | NULL | NULL |
| 2023-08-28 12:00:50.822 | 6 | 23 | NULL | NULL | NULL |
| NULL | NULL | NULL | 2023-08-28 12:01:00.727 | 22 | 32 |


| ts | c1 | c2 | c3 | c4 |
| --- | --- | --- | --- | --- |
| 2023-08-28 12:00:00.123 | 1 | 6 | 11 | 32 |
| 2023-08-28 12:00:10.224 | 2 | 7 | NULL | NULL |
| 2023-08-28 12:00:20.241 | 3 | 8 | NULL | NULL |
| 2023-08-28 12:00:30.388 | NULL | NULL | 234 | 67 |


| ts | c1 | c2 | c3 | c4 |
| --- | --- | --- | --- | --- |
| 2023-08-28 12:00:00.123 | 1 | 6 | 11 | 32 |
| 2023-08-28 12:00:10.224 | 2 | 7 | 11 | 32 |
| 2023-08-28 12:00:20.241 | 3 | 8 | 11 | 32 |

`select ts, c1, c2, c3, c4 from view_name where ts = <const_value> ``~~using c2~~`` join(left/full）`，默认是 left。按照列`c2` 的 `ts` 对齐数据，插值函数与 interp 函数相似，有默认值后续再设计。

| ts1 | c1 | c2 | ~~ts2~~ | c3 | c4 |
| --- | --- | --- | --- | --- | --- |
| 2023-08-28 12:00:00.123 | 1 | 6 | ~~2023-08-28 12:00:00.123~~ | 11 | 32 |
| 2023-08-28 12:00:10.224 | 2 | 7 | ~~2023-08-28 12:00:00.123~~ | 11 | 32 |
| 2023-08-28 12:00:20.241 | 3 | 8 | ~~2023-08-28 12:00:00.123~~ | 11 | 32 |
| 2023-08-28 12:00:40.326 | 5 | 10 | ~~2023-08-28 12:00:30.388~~ | 234 | 67 |
| 2023-08-28 12:00:50.822 | 6 | 23 | ~~2023-08-28 12:00:30.388~~ | 234 | 67 |

### 4.4 窗口查询

扩展窗口划分的语法，其他列按划分后的时间范围进行聚合。

#### 4.4.1 时间窗口

无论窗口内部是否有数据，都会输出时间窗口。对比之前，窗口内部无数据且无 `FILL` 语法时，不输出窗口。事件窗口、状态窗口、断面查询、分组查询、聚合查询不需要做改造。

#### 4.4.2 会话窗口

`SESSION(c2, tol_val)`，以前指定的是 ts 列 。

#### 4.4.3 计数窗口

`COUNT_WINDOW(c2, count_val[, sliding_val])`，需要指定一个数据列。

## 5. 性能需求

针对虚拟表的查询，与普通表的查询，根据数据规模的不同，

## 6. 安全需求

1. 虚拟表读写权限控制，用户需要具有创建虚拟表的原始表具有相应的访问权限才能够具有虚拟表的访问权限。
2. 对于数据表的访问权限，本模块不针对其进行具体的控制，其控制逻辑与《查询 - Requirement Spec》文档中对于表访问的安全控制需求一致。

## 7. 第三方依赖需求

无，不依赖第三方的库或开源软件包

## 8. 其他需求

无

## 9. 附录 —— 使用虚拟表样例

### 9.1 创建超级表

```sql
create table devices (
  ts             timestamp,
  switch         bool,
  voltage        double,
  current        double,
  temperature    double,
  rotation_speed int,
  humidity       double,
  amplitude      double,
  pressure       double,
  density        double,
  wind_speed     double,
) 
tags (
  tags(device varchar(20)
);
```

### 9.2 创建虚拟表

```sql
-- 创建 d1/d2/d3 三个虚拟表
create vtable d1 as (
  select p10.ts, 
         p10.val,
         p11.val,
         p12.val,
         p13.val,
         p14.val,
         p15.val,
         p16.val,
         p17.val,
         p18.val,
         p19.val
  from p10, p11, p12, p13, p14, p15, p16, p17, p18, p19
) using devices tags('d1');

create vtable d2 as (
  select p20.ts, 
         p20.val,
         p21.val,
         p22.val,
         p23.val,
         p24.val,
         p25.val,
         p26.val,
         p27.val,
         p28.val,
         p29.val
  from p20, p21, p22, p23, p24, p25, p26, p27, p28, p29
) using devices tags('d2');

create vtable d3 as (
  select p30.ts, 
         p30.val,
         p31.val,
         p32.val,
         p33.val,
         p34.val,
         p35.val,
         p36.val,
         p37.val,
         p38.val,
         p39.val
  from p30, p31, p32, p33, p34, p35, p36, p37, p38, p39
) using devices tags('d3');
```

### 9.3 列表聚合查询

```sql
select _wstart hour, 
       last(switch), 
       avg(voltage),
       avg(current),
       avg(temperature),
       avg(humidity),
       avg(rotation_speed),
       avg(amplitude),
       avg(pressure),
       avg(density),
       avg(wind_speed)
from d1
where ts between '2024-03-01 00:00:00.000' and '2024-03-01 23:59:59.999' 
interval(1h) 
fill(value, 0)
;
```

### 9.4 断面查询

```sql
select ts, 
       interp(switch), 
       interp(voltage),
       interp(current),
       interp(temperature),
       interp(humidity),
       interp(rotation_speed),
       interp(amplitude),
       interp(pressure),
       interp(density),
       interp(wind_speed)
from d1
range('2024-03-01 12:34:56.000') 
fill(linear)
;
```

### 9.5 断面列表查询

```sql
select _wstart hour, 
       last(switch), 
       avg(voltage),
       avg(current),
       avg(temperature),
       avg(humidity),
       avg(rotation_speed),
       avg(amplitude),
       avg(pressure),
       avg(density),
       avg(wind_speed)
  from devices
  where ts between '2024-03-01 00:00:00.000' and '2024-03-01 23:59:59.999' 
  partition by device
  interval(1h) 
  fill(value, 0)
;
```

### 9.6 状态窗口查询

查询发电机在不同的开关状态下，其他物理量的聚合值
```sql
select _wstart start, _wduration duration,
       switch, 
       avg(voltage),
       avg(current),
       avg(temperature),
       avg(humidity),
       avg(rotation_speed),
       avg(amplitude),
       avg(pressure),
       avg(density),
       avg(wind_speed)
  from devices
  where ts between '2024-03-01 00:00:00.000' and '2024-03-01 23:59:59.999' 
  partition by device
  state_window(switch) 
  fill(value, 0)
;
```

### 9.7 某设备缺少时序数据的状态

发电机（d4）采集的测点与其他发电机（d1/d2/d3）相比少了风速测点，也就是缺少风速子表。仍希望能为 d4 创建虚拟子表，当风速子表被创建后，可被加入到查询中。
```sql {wrap}
-- 创建 d4 
create view d4 as (
  select p40.ts, 
         p40.val,
         p41.val,
         p42.val,
         p43.val,
         p44.val,
         p45.val,
         p46.val,
         p47.val,
         p48.val,
         NULL
  from p40, p41, p42, p44, p45, p46, p47, p48, p49
) using devices tags('d1');

```
