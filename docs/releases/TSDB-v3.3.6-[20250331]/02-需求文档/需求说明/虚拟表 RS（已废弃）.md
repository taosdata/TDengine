# 虚拟表 RS（已废弃）

## 1. 引言

### 1.1 需求背景

在工厂、车间、SCADA 等场景，测点数据通过工控协议进入 TDengine，或者通过实时数据库同步进入 TDengine。各个测点是独立采集、上传的，一般采用单列模式的超级表。
1. 广西钢铁项目中，机械动力室用 kepware 采集模拟值和开关量，为上层帆软及其他监控平台提供数据。售前阶段提供的技术方案中，使用 taosX 通过 OPC 协议采集数据，在写入到 TDengine 时创建三个超级表，分别为布尔类型超级表、浮点类型超级表、整数类型超级表，每个超级表都是单列。
2. 大唐水电院项目中，用户自己编写 104 协议解析程序，从南瑞集控软件和其他系统中接入时序数据。在写入到 TDengine 时，也同样创建三个超级表。
在汽车、风机等场景，每台设备有上千测点，这些测点按照数据来源或者采集频率可以分为几十个组，对应几十个超级表。
数据成功接入到 TDengine 后，产生了共性的查询需求。进行数据分析时，需把同一设备的多个测点按时间对齐，也就是常说的断面查询。除断面查询外，用户还需要在一个时间轴上，按时间窗口对齐多个测点的数据。TDengine 的 interp 函数、窗口函数，都只处理同一表中的不同数据列；处理不同表的数据列时，必须采用 JOIN 语法。JOIN 语法编写较为复杂，使用麻烦且容易出错，不符合 TDengine 的易用性目标。

### 1.2 需求分析

#### 1.2.1 数据模型

以大唐水电院项目为例，大唐水电院需要对接 6 个不同的水力发电站。其中，攀枝花水电站需接入包括南瑞集控系统在内的 10 个实时数据监控系统。南瑞集控系统的实时数据通过 104 协议接入到 TDengine，水电院对南瑞监控系统的数据模型并不清楚，故而在数据接入时采用单列模型建模。共创建三个超级表，标签列包括设备 ID、采集点类型、点号。
```sql {wrap}
create table bool_stb  (ts timestamp, val bool)   tags(device varchar(20), type varchar(20), point int);
create table int_stb   (ts timestamp, val int)    tags(device varchar(20), type varchar(20), point int);
create table double_stb(ts timestamp, val double) tags(device varchar(20), type varchar(20), point int);
```

水电院需监控水力发电机组的状态，以该类型设备的 10 个采集点为例，包括开关、电压、电流、温度、湿度、转速、振幅、压力、密度、风速，在 TDengine 中创建了 10 个子表。除点号之外，其他标签值只能设置为 NULL（104 协议只能提供点号）。
```sql {wrap}
create table p10 using bool_stb tags   (NULL, NULL, 10);
create table p11 using double_stb tags (NULL, NULL, 11);
create table p12 using double_stb tags (NULL, NULL, 12);
create table p13 using double_stb tags (NULL, NULL, 13);
create table p14 using double_stb tags (NULL, NULL, 14);
create table p15 using int_stb tags    (NULL, NULL, 15);
create table p16 using double_stb tags (NULL, NULL, 16);
create table p17 using double_stb tags (NULL, NULL, 17);
create table p18 using double_stb tags (NULL, NULL, 18);
create table p19 using double_stb tags (NULL, NULL, 19);
```

数据接入到 TDengine 后，用户与南瑞集控系统的运维人员沟通，找到并更新了这 10 个采集点的设备名称、采集点类型。下面以 create table 语句描述了这 10 个采集点对应子表的当前标签值。
```sql {wrap}
create table p10 using bool_stb tags   ('d1', 'switch',         10);
create table p11 using double_stb tags ('d1', 'voltage',        11);
create table p12 using double_stb tags ('d1', 'current',        12);
create table p13 using double_stb tags ('d1', 'temperature',    13);
create table p14 using double_stb tags ('d1', 'humidity',       14);
create table p15 using int_stb tags    ('d1', 'rotation_speed', 15);
create table p16 using double_stb tags ('d1', 'amplitude',      16);
create table p17 using double_stb tags ('d1', 'pressure',       17);
create table p18 using double_stb tags ('d1', 'density',        19);
create table p19 using double_stb tags ('d1', 'wind_speed',     19);
```

#### 1.2.2 列表需求

用户需要通过 BI 软件（或者 Grafana） 绘制一张表格。需要 TDengine 提供一条 SQL 即可生成如下的表格数据（报表系统原理大多如此）。

| 时间 | 开关 | 电压 | 电流 | 温度 | 湿度 | 转速 | 振幅 | 压力 | 密度 | 风速 |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 2024-03-01 00:00:00 |  |  |  |  |  |  |  |  |  |  |
| 2024-03-01 01:00:00 |  |  |  |  |  |  |  |  |  |  |
| 2024-03-01 02:00:00 |  |  |  |  |  |  |  |  |  |  |
| 2024-03-01 03:00:00 |  |  |  |  |  |  |  |  |  |  |
| 2024-03-01 04:00:00 |  |  |  |  |  |  |  |  |  |  |
| 2024-03-01 05:00:00 |  |  |  |  |  |  |  |  |  |  |
| …… |  |  |  |  |  |  |  |  |  |  |
| 2024-03-01 23:00:00 |  |  |  |  |  |  |  |  |  |  |

在未来，TDengine 通过一条 SQL 语句可以支持（本期开发的 JOIN 功能仅能关联一次）。
```sql
select a.ts hour, a.x switch, b.x voltage, c.x current, d.x temperature, e.x humidity, f.x rotation_speed, g.x amplitude, h.x pressure, i.x density, j.x wind_speed from 
(
  (     select _wstart ts, last(val) x from p10 where ts between '2024-03-01 00:00:00.000' and '2024-03-01 23:59:59.999' interval(1h) fill(value, 0)) a 
  join (select _wstart ts, avg(val)  x from p11 where ts between '2024-03-01 00:00:00.000' and '2024-03-01 23:59:59.999' interval(1h) fill(value, 0)) b on a.ts = b.ts 
  join (select _wstart ts, avg(val)  x from p12 where ts between '2024-03-01 00:00:00.000' and '2024-03-01 23:59:59.999' interval(1h) fill(value, 0)) c on a.ts = c.ts 
  join (select _wstart ts, avg(val)  x from p13 where ts between '2024-03-01 00:00:00.000' and '2024-03-01 23:59:59.999' interval(1h) fill(value, 0)) d on a.ts = d.ts
  join (select _wstart ts, avg(val)  x from p14 where ts between '2024-03-01 00:00:00.000' and '2024-03-01 23:59:59.999' interval(1h) fill(value, 0)) e on a.ts = e.ts
  join (select _wstart ts, avg(val)  x from p15 where ts between '2024-03-01 00:00:00.000' and '2024-03-01 23:59:59.999' interval(1h) fill(value, 0)) f on a.ts = f.ts
  join (select _wstart ts, avg(val)  x from p16 where ts between '2024-03-01 00:00:00.000' and '2024-03-01 23:59:59.999' interval(1h) fill(value, 0)) g on a.ts = g.ts
  join (select _wstart ts, avg(val)  x from p17 where ts between '2024-03-01 00:00:00.000' and '2024-03-01 23:59:59.999' interval(1h) fill(value, 0)) h on a.ts = h.ts
  join (select _wstart ts, avg(val)  x from p18 where ts between '2024-03-01 00:00:00.000' and '2024-03-01 23:59:59.999' interval(1h) fill(value, 0)) i on a.ts = i.ts
  join (select _wstart ts, avg(val)  x from p19 where ts between '2024-03-01 00:00:00.000' and '2024-03-01 23:59:59.999' interval(1h) fill(value, 0)) j on a.ts = j.ts
);
```

由于这条语句调用频繁且比较长，很自然的想创建一个视图。删除 ts 的筛选条件 `where ts between '2024-03-01 00:00:00.000' and '2024-03-01 23:59:59.999' `，并在使用视图查询时重新传入。
```sql
create view d1_view as
  select a.ts hour, a.x switch, b.x voltage, cb.x current, d.x temperature, e.x humidity, f.x rotation_speed, g.x amplitude, h.x pressure, i.x density, j.x wind_speed from 
  (
    (     select _wstart ts, last(val) x from p10 interval(1h) fill(value, 0)) a 
    join (select _wstart ts, avg(val)  x from p11 interval(1h) fill(value, 0)) b on a.ts = b.ts 
    join (select _wstart ts, avg(val)  x from p12 interval(1h) fill(value, 0)) c on a.ts = c.ts 
    join (select _wstart ts, avg(val)  x from p13 interval(1h) fill(value, 0)) d on a.ts = d.ts
    join (select _wstart ts, avg(val)  x from p14 interval(1h) fill(value, 0)) e on a.ts = e.ts
    join (select _wstart ts, avg(val)  x from p15 interval(1h) fill(value, 0)) f on a.ts = f.ts
    join (select _wstart ts, avg(val)  x from p16 interval(1h) fill(value, 0)) g on a.ts = g.ts
    join (select _wstart ts, avg(val)  x from p17 interval(1h) fill(value, 0)) h on a.ts = h.ts
    join (select _wstart ts, avg(val)  x from p18 interval(1h) fill(value, 0)) i on a.ts = i.ts
    join (select _wstart ts, avg(val)  x from p20 interval(1h) fill(value, 0)) j on a.ts = j.ts
  )
;
```

对该视图进行查询，指定时间筛选条件 `where hour between '2024-03-01 00:00:00.000' and '2024-03-01 23:59:59.999' `后，视图的内层查询会遍历子表所有时间范围的数据，然后再使用 hour 筛选条件过滤，性能显然非常糟糕。
```sql
select hour, switch, voltage, current, temperature, humidity, rotation_speed, amplitude, pressure, density, wind_speed 
  from d1_view
  where hour between '2024-03-01 00:00:00.000' and '2024-03-01 23:59:59.999'
;
```

如果想要修改时间窗口大小（interval）为 2 小时，或者改变填充策略，还必须创建新的视图。

#### 1.2.3 断面需求

用户需要制作一个面板，选择具体时刻、查看所选发电机当时的各项参数值，需要如下格式的数据。

| 时间 | 开关 | 电压 | 电流 | 温度 | 湿度 | 转速 | 振幅 | 压力 | 密度 | 风速 |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 2024-03-01 12:34:56 |  |  |  |  |  |  |  |  |  |  |

在未来，TDengine 通过一条 SQL 语句仍然可以支持（本期开发的 JOIN 功能仅能关联一次）。
```sql {wrap}
select a.ts hour, a.x switch, b.x voltage, cb.x current, d.x temperature, e.x humidity, f.x rotation_speed, g.x amplitude, h.x pressure, i.x density, j.x wind_speed from 
(
  (     select _wstart ts, interp(val) x from p10 range('2024-03-01 12:34:56.000') fill(linear)) a 
  join (select _wstart ts, interp(val) x from p11 range('2024-03-01 12:34:56.000') fill(linear)) b on a.ts = b.ts 
  join (select _wstart ts, interp(val) x from p12 range('2024-03-01 12:34:56.000') fill(linear)) c on a.ts = c.ts 
  join (select _wstart ts, interp(val) x from p13 range('2024-03-01 12:34:56.000') fill(linear)) d on a.ts = d.ts
  join (select _wstart ts, interp(val) x from p14 range('2024-03-01 12:34:56.000') fill(linear)) e on a.ts = e.ts
  join (select _wstart ts, interp(val) x from p15 range('2024-03-01 12:34:56.000') fill(linear)) f on a.ts = f.ts
  join (select _wstart ts, interp(val) x from p16 range('2024-03-01 12:34:56.000') fill(linear)) g on a.ts = g.ts
  join (select _wstart ts, interp(val) x from p17 range('2024-03-01 12:34:56.000') fill(linear)) h on a.ts = h.ts
  join (select _wstart ts, interp(val) x from p19 range('2024-03-01 12:34:56.000') fill(linear)) i on a.ts = i.ts
  join (select _wstart ts, interp(val) x from p19 range('2024-03-01 12:34:56.000') fill(linear)) j on a.ts = j.ts
);
```

与 2.2 相同，不能采用视图来简化这个查询。

#### 1.2.4 断面列表需求

用户需要制作一个断面列表，选择具体时刻、选择发电机集合，获取当时各发电机的各项参数值。

| 时间 | 设备 | 开关 | 电压 | 电流 | 温度 | 湿度 | 转速 | 振幅 | 压力 | 密度 | 风速 |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 2024-03-01 12:34:56 | d1 |  |  |  |  |  |  |  |  |  |  |
| 2024-03-01 12:34:56 | d2 |  |  |  |  |  |  |  |  |  |  |
| 2024-03-01 12:34:56 | d3 |  |  |  |  |  |  |  |  |  |  |
| 2024-03-01 12:34:56 | d4 |  |  |  |  |  |  |  |  |  |  |
| 2024-03-01 12:34:56 | d5 |  |  |  |  |  |  |  |  |  |  |
| 2024-03-01 12:34:56 | d6 |  |  |  |  |  |  |  |  |  |  |
| …… |  |  |  |  |  |  |  |  |  |  |  |

在未来，TDengine 通过一条 SQL 语句可以支持（本期提供的 JOIN 功能仅能关联一次），但 SQL 语句已经非常复杂。
```sql {wrap}
select a.ts hour, a.x switch, b.x voltage, cb.x current, d.x temperature, e.x humidity, f.x rotation_speed, g.x amplitude, h.x pressure, i.x density, j.x wind_speed from 
(
  (     select _wstart ts, interp(val) x from p10 range('2024-03-01 12:34:56.000') fill(linear)) a 
  join (select _wstart ts, interp(val) x from p11 range('2024-03-01 12:34:56.000') fill(linear)) b on a.ts = b.ts 
  join (select _wstart ts, interp(val) x from p12 range('2024-03-01 12:34:56.000') fill(linear)) c on a.ts = c.ts 
  join (select _wstart ts, interp(val) x from p13 range('2024-03-01 12:34:56.000') fill(linear)) d on a.ts = d.ts
  join (select _wstart ts, interp(val) x from p14 range('2024-03-01 12:34:56.000') fill(linear)) e on a.ts = e.ts
  join (select _wstart ts, interp(val) x from p15 range('2024-03-01 12:34:56.000') fill(linear)) f on a.ts = f.ts
  join (select _wstart ts, interp(val) x from p16 range('2024-03-01 12:34:56.000') fill(linear)) g on a.ts = g.ts
  join (select _wstart ts, interp(val) x from p17 range('2024-03-01 12:34:56.000') fill(linear)) h on a.ts = h.ts
  join (select _wstart ts, interp(val) x from p18 range('2024-03-01 12:34:56.000') fill(linear)) i on a.ts = i.ts
  join (select _wstart ts, interp(val) x from p19 range('2024-03-01 12:34:56.000') fill(linear)) j on a.ts = j.ts
)

union

select a.ts hour, a.x switch, b.x voltage, cb.x current, d.x temperature, e.x humidity, f.x rotation_speed, g.x amplitude, h.x pressure, i.x density, j.x wind_speed from 
(
  (     select _wstart ts, interp(val) x from p20 range('2024-03-01 12:34:56.000') fill(linear)) a 
  join (select _wstart ts, interp(val) x from p21 range('2024-03-01 12:34:56.000') fill(linear)) b on a.ts = b.ts 
  join (select _wstart ts, interp(val) x from p22 range('2024-03-01 12:34:56.000') fill(linear)) c on a.ts = c.ts 
  join (select _wstart ts, interp(val) x from p23 range('2024-03-01 12:34:56.000') fill(linear)) d on a.ts = d.ts
  join (select _wstart ts, interp(val) x from p24 range('2024-03-01 12:34:56.000') fill(linear)) e on a.ts = e.ts
  join (select _wstart ts, interp(val) x from p25 range('2024-03-01 12:34:56.000') fill(linear)) f on a.ts = f.ts
  join (select _wstart ts, interp(val) x from p26 range('2024-03-01 12:34:56.000') fill(linear)) g on a.ts = g.ts
  join (select _wstart ts, interp(val) x from p27 range('2024-03-01 12:34:56.000') fill(linear)) h on a.ts = h.ts
  join (select _wstart ts, interp(val) x from p29 range('2024-03-01 12:34:56.000') fill(linear)) i on a.ts = i.ts
  join (select _wstart ts, interp(val) x from p29 range('2024-03-01 12:34:56.000') fill(linear)) j on a.ts = j.ts
)

union

select a.ts hour, a.x switch, b.x voltage, cb.x current, d.x temperature, e.x humidity, f.x rotation_speed, g.x amplitude, h.x pressure, i.x density, j.x wind_speed from 
(
  (     select _wstart ts, interp(val) x from p30 range('2024-03-01 12:34:56.000') fill(linear)) a 
  join (select _wstart ts, interp(val) x from p31 range('2024-03-01 12:34:56.000') fill(linear)) b on a.ts = b.ts 
  join (select _wstart ts, interp(val) x from p32 range('2024-03-01 12:34:56.000') fill(linear)) c on a.ts = c.ts 
  join (select _wstart ts, interp(val) x from p33 range('2024-03-01 12:34:56.000') fill(linear)) d on a.ts = d.ts
  join (select _wstart ts, interp(val) x from p34 range('2024-03-01 12:34:56.000') fill(linear)) e on a.ts = e.ts
  join (select _wstart ts, interp(val) x from p35 range('2024-03-01 12:34:56.000') fill(linear)) f on a.ts = f.ts
  join (select _wstart ts, interp(val) x from p36 range('2024-03-01 12:34:56.000') fill(linear)) g on a.ts = g.ts
  join (select _wstart ts, interp(val) x from p37 range('2024-03-01 12:34:56.000') fill(linear)) h on a.ts = h.ts
  join (select _wstart ts, interp(val) x from p38 range('2024-03-01 12:34:56.000') fill(linear)) i on a.ts = i.ts
  join (select _wstart ts, interp(val) x from p39 range('2024-03-01 12:34:56.000') fill(linear)) j on a.ts = j.ts
)

union

……

```

与 2.2 相同，不能采用视图来简化这个查询。

#### 1.2.5 小结

用户虽然可以在子表上打标签值，但这些标签值难以在 SQL 查询中被使用。在多维度分析时，需要记住每个设备、每个测点的表名，拼接 SQL 语句也容易出错。

### 1.3 相关文档资料

| 文档 | 链接 |
| --- | --- |
| 需求报告 | [opc存储不同超级表之间的数据连接查询需求](https://taosdata.feishu.cn/wiki/MCRCwIihpiK3dukRWtYc0Swcn6f) |

### 1.4 优先级要求

待与 Jeff 讨论后确定

### 1.5 版本要求

企业版支持，社区版不支持

## 2. 需求目标

提出虚拟表的概念，将单个设备分布在多个子表的测点进行汇总，用简单的 SQL 实现查询。虚拟表用在“数据接入前不了解数据模型，或者一个设备有多个不同采集频率测点组”的场景。它和大宽表有相似之处，但大宽表会在物理上将所有测点（表）的数据放在一个 vnode 中。
```sql
create virtual table vtb_name (  
  table_subquery1 [, table_subquery2] ...
）
using stb_name [(tag_name [, tag_name] ...)]
tags (tag_value [, tag_value] ...)
```

**使用说明**
- 虚拟表：virtual table，用于合并多个表子查询
  - 必须是对普通表、子表、超级表的简单投影查询，不包含聚合、分组、关联等复杂查询
  - 对各表的查询可以带筛选条件
- 虚拟普通表
  - 未设置 stb_name 时，虚拟表成为虚拟普通表
- 虚拟子表
  - 已设置 stb_name 时，虚拟表成为虚拟子表
  - 虚拟子表的数据列（数目、类型）需要与超级表相同，创建时对查询结果集类型进行检查
  - 虚拟子表可以设置标签值，同样存储在 tdb 中
  - 超级表不应同时包含虚拟子表和普通子表（实现时视开发难度决定）
- 虚拟表的查询
  - 窗口查询：划分时间窗口，各表的字段按窗口时间进行聚合
    - 时间窗口：以第一个表的时间戳为基准
    - 事件窗口：以采用的数据列对应的表为基准
    - 状态窗口：以采用的数据列对应的表为基准
    - 计数窗口：以第一个表的记录数目为基准
  - 断面查询
    - Interp：以第一个表的时间戳为基准，各表的字段按窗口时间进行聚合
  - 投影查询
    - 以第一个表的时间戳为基准，各表的字段按 interp 方式进行聚合，采用 linear 方式插值
  - 分组查询：支持
  - 聚合查询：支持
- 筛选条件
  - 时间字段筛选：对任意表的时间字段筛选，都将被优化到虚拟表中的其他表
  - 其他字段筛选：无限制
**主要特点**
- 轻松构建设备视图，实现宽表方式查询
- 简单的多表时序连接，比 JOIN 更快速
- 时间对齐、断面查询的语法更加简单

## 3. 需求收益

以第 1.2 节的需求为例，看一下简化后的 SQL 语句。

### 3.1 创建虚拟普通表

```sql
create virtual table d1_vt (
  select ts, val switch         from p10,
  select     val voltage        from p11,
  select     val current        from p12,
  select     val temperature    from p13,
  select     val rotation_speed from p14,
  select     val humidity       from p15,
  select     val amplitude      from p16,
  select     val pressure       from p17,
  select     val density        from p18,
  select     val wind_speed     from p19,
);
```

### 3.2 轻松实现列表需求

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
  from d1_vt
  where ts between '2024-03-01 00:00:00.000' and '2024-03-01 23:59:59.999' 
  interval(1h) 
  fill(value, 0)
;
```

### 3.3 轻松实现断面需求

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
  from d1_state
  range('2024-03-01 12:34:56.000') 
  fill(linear)
;
```

### 3.4 创建虚拟子表

```sql

## 4. 先创建超级表

create table vt (
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
tags
(
  tags(device varchar(20)
);

## 5. 创建 d1/d2/d3 三个子表

create virtual table d1_vt (
  select ts, val switch         from p10
  select     val voltage        from p11,
  select     val current        from p12,
  select     val temperature    from p13,
  select     val rotation_speed from p14,
  select     val humidity       from p15,
  select     val amplitude      from p16,
  select     val pressure       from p17,
  select     val density        from p18,
  select     val wind_speed     from p19,
) using vt tags('d1');

create virtual table d2_vt (
  select ts, val switch         from p20,
  select     val voltage        from p21,
  select     val current        from p22,
  select     val temperature    from p23,
  select     val rotation_speed from p24,
  select     val humidity       from p25,
  select     val amplitude      from p26,
  select     val pressure       from p27,
  select     val density        from p28,
  select     val wind_speed     from p29,
) using vt tags('d2');

create virtual table d3_vt (
  select ts, val switch         from p30,
  select     val voltage        from p31,
  select     val current        from p32,
  select     val temperature    from p33,
  select     val rotation_speed from p34,
  select     val humidity       from p35,
  select     val amplitude      from p36,
  select     val pressure       from p37,
  select     val density        from p38,
  select     val wind_speed     from p39,
) using vt tags('d3');

```

### 5.1 轻松实现断面列表需求

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
  from d1_vt
  where ts between '2024-03-01 00:00:00.000' and '2024-03-01 23:59:59.999' 
  partition by device
  interval(1h) 
  fill(value, 0)
;
```

### 5.2 轻松实现状态窗口查询需求

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
  from d1_vt
  where ts between '2024-03-01 00:00:00.000' and '2024-03-01 23:59:59.999' 
  partition by device
  state_window(switch) 
  fill(value, 0)
;
```

### 5.3 某设备缺少测点时

发电机（d4）采集的测点与其他发电机（d1/d2/d3）相比少了风速测点，也就是缺少风速子表。仍希望能为 d4 创建虚拟子表，当风速子表被创建后，自动加入到查询中。
```sql {wrap}

## 6. 参照 4.4 的超级表结构

## 7. 创建 d4 

create virtual table d4_vt (
  select ts, val switch         from p40,
  select     val voltage        from p41,
  select     val current        from p42,
  select     val temperature    from p43,
  select     val rotation_speed from p44,
  select     val humidity       from p45,
  select     val amplitude      from p46,
  select     val pressure       from p47,
  select     val density        from p48,
  select     val wind_speed     from double_stb where device='d4' and type='wind_speed'
) using vt tags('d3');

```

当最后一个查询不存在时，认为 wind_speed 的取值为空。

### 7.1 通过标签值构建虚拟子表

使用单列模型导入数据，在超级表中为设备打好标签后，可以直接参与计算
```sql {wrap}

## 8. 参照 4.4 的超级表结构

create virtual table d4_vt (
  select ts, val switch         from bool_stb   where device='d4' and type='switch'
  select     val voltage        from double_stb where device='d4' and type='voltage'
  select     val current        from double_stb where device='d4' and type='current'
  select     val temperature    from double_stb where device='d4' and type='temperature'
  select     val rotation_speed from int_stb    where device='d4' and type='rotation_speed'
  select     val humidity       from double_stb where device='d4' and type='humidity'
  select     val amplitude      from double_stb where device='d4' and type='amplitude'
  select     val pressure       from double_stb where device='d4' and type='pressure'
  select     val density        from double_stb where device='d4' and type='density'
  select     val wind_speed     from double_stb where device='d4' and type='wind_speed'
) using vt tags('d3');

```
