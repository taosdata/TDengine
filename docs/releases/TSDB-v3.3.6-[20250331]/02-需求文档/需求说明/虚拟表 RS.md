# 虚拟表 RS

## 1. 引言

### 1.1 需求背景

在工厂、车间、SCADA 等场景，测点数据通过工控协议进入 TDengine，或者通过实时数据库同步进入 TDengine。各个测点是独立采集、上传的，一般采用单列模式的超级表。
1. 广西钢铁项目中，机械动力室用 kepware 采集模拟值和开关量，为上层帆软及其他监控平台提供数据。售前阶段提供的技术方案中，使用 taosX 通过 OPC 协议采集数据，在写入到 TDengine 时创建三个超级表，分别为布尔类型超级表、浮点类型超级表、整数类型超级表，每个超级表都是单列。
2. 大唐水电院项目中，用户自己编写 104 协议解析程序，从南瑞集控软件和其他系统中接入时序数据。在写入到 TDengine 时，也同样创建三个超级表。
在汽车、风机等场景，每台设备有上千测点，这些测点按照数据来源或者采集频率可以分为几十个组，对应几十个超级表。
数据成功接入到 TDengine 后，产生了共性的查询需求。进行数据分析时，需把同一设备的多个测点按时间对齐，也就是常说的断面查询。除断面查询外，用户还需要在一个时间轴上，按时间窗口对齐多个测点的数据。TDengine 的 interp 函数、窗口函数，都只处理同一表中的不同数据列；处理不同表的数据列时，必须采用 JOIN 语法。JOIN 语法编写较为复杂，使用麻烦且容易出错，不符合 TDengine 的易用性目标。

### 1.2 相关文档资料

| 文档 | 链接 |
| --- | --- |
| 需求报告 | [opc存储不同超级表之间的数据连接查询需求](https://taosdata.feishu.cn/wiki/MCRCwIihpiK3dukRWtYc0Swcn6f) |
| 用户需求 | [TD 支持大宽表](https://taosdata.feishu.cn/wiki/JlCuwxDhhiPPa4kjdnicxzyFn8d) |
| 用户需求 | [用户需求：用单列模型实现宽表](https://taosdata.feishu.cn/wiki/QT9SwfV6fiLl1dkWXnCcZjA0nBb) |

### 1.3 优先级要求

预期 8 月底发布。

### 1.4 版本要求

企业版支持，社区版不支持。

## 2. 需求目标

通过增强视图，汇总单个设备分布在多个子表的测点，用简单的 SQL 即可查询。用于“数据接入前不了解数据模型，或者设备有多个不同频率测点组”的场景。它和大宽表有相似之处，但大宽表会在物理上将所有测点（子表）的数据放在一个 vnode 中。

### 2.1 **语法示例**

```sql
CREATE VIEW v AS (
  SELECT t1.ts, t2.c2, t3.c3, t4.c2, NULL, t4.c5 
  FROM t1, t2, t3, t4
) 
USING device_stb tags(t1.location, t2.model); 

```

### 2.2 **使用说明**

1. 视图及其标签存储在 vnode 中
2. 视图中的表可以是普通表、子表，但不能是超级表
3. 视图中各表的数据列需要与超级表的列定义相同
4. 示例中的 t1、t2、t3、t4 之间的 join 条件为各表的时间戳列，但在语法中不明确指定

### 2.3 投影查询（语法需要增加更多选项）

`select * from view_name`，默认以查询列表第一个出现的列为基准，其他列对齐，~~输出所有数据~~~~，很大可能是很多NUL~~L。

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

### 2.4 窗口查询

扩展窗口划分的语法，其他列按划分后的时间范围进行聚合。

#### 2.4.1 时间窗口

无论窗口内部是否有数据，都会输出时间窗口。对比之前，窗口内部无数据且无 `FILL` 语法时，不输出窗口。

#### 2.4.2 事件窗口

语法不需要做改造。

#### 2.4.3 状态窗口

语法不需要做改造。

#### 2.4.4 会话窗口

`SESSION(c2, tol_val)`，以前指定的是 ts 列 。

#### 2.4.5 计数窗口

`COUNT_WINDOW(c2``, ``count_val[, sliding_val])`，需要指定一个数据列。

### 2.5 断面查询

语法不需要做改造。

### 2.6 分组查询

语法不需要做改造。

### 2.7 聚合查询

语法不需要做改造。

## 3. 需求收益

1. 轻松构建设备视图，实现宽表方式查询，设备视图灵活多表
2. 以前需存储多份标签，且无法原子性修改，现在只需存储一份标签
3. 简单的多表时序连接，比 JOIN 更快速
4. 时间对齐、断面查询的语法更加简单

## 4. 附录：改造前的语法

### 4.1 数据模型

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

### 4.2 列表需求

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

### 4.3 断面需求

用户需要制作一个面板，选择具体时刻、查看所选发电机当时的各项参数值，需要如下格式的数据。

| 时间 | 开关 | 电压 | 电流 | 温度 | 湿度 | 转速 | 振幅 | 压力 | 密度 | 风速 |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 2024-03-01 12:34:56 |  |  |  |  |  |  |  |  |  |  |

在未来，TDengine 通过一条 SQL 语句仍然可以支持（本期开发的 JOIN 功能仅能关联一次）。
```sql
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

### 4.4 断面列表需求

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

与 4.2 相同，不能采用视图来简化这个查询。

### 4.5 小结

用户虽然可以在子表上打标签值，但这些标签值难以在 SQL 查询中被使用。在多维度分析时，需要记住每个设备、每个测点的表名，拼接 SQL 语句也容易出错。

## 5. 附录：改造后的语法

### 5.1 创建超级表

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

### 5.2 创建视图（虚拟表)

```sql
-- 创建 d1/d2/d3 三个视图
create view d1 as (
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

create view d2 as (
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

create view d3 as (
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

### 5.3 轻松实现列表需求

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

### 5.4 轻松实现断面需求

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

### 5.5 轻松实现断面列表需求

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

### 5.6 轻松实现状态窗口查询需求

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

### 5.7 某设备缺少测点时

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
