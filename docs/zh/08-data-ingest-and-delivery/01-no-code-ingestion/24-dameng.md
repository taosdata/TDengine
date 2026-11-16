---
title: 达梦
sidebar_label: 达梦
---

本节讲述如何通过 taosExplorer 界面创建数据写入任务，从达梦数据库将数据接入当前 TDengine 集群。

## 功能概述

达梦数据库（DM）是国产关系型数据库，广泛用于电力、能源、政务等行业，常用于保存 SCADA 断面数据、设备台账和业务数据。TDengine 可以高效地从达梦读取数据并写入 TDengine，支持历史数据全量迁移、实时增量同步和定时同步。

同步模式、字段映射、类型转换、事务一致性和数据校验等通用说明见 [关系型数据库集成](./23-rdb-integration.md)。

## 前置条件

| 项目 | 要求 |
| --- | --- |
| 达梦版本 | DM8 |
| 运行环境 | taosX 或 taosX-Agent 运行在 Linux（X64、ARM64、海光 C86）或 Windows 上 |
| 客户端库 | 运行 taosX 或 taosX-Agent 的主机上需安装达梦 DPI 客户端库（`libdmdpi.so` 或 `dmdpi.dll`，随达梦客户端安装包提供），并将其所在目录加入动态库搜索路径（Linux 为 `LD_LIBRARY_PATH`，Windows 为 `PATH`），然后重启 taosX 或 taosX-Agent |
| 网络 | taosX 或 taosX-Agent 能访问达梦服务端口（默认 `5236`）。使用代理时的网络要求见 [数据接入前置条件](./00-prerequisites.md) |
| 账号权限 | 用于同步的达梦账号对源表具有 `SELECT` 权限；使用 **检查连通性** 和 **从服务器检索** 时还需要能查询数据字典视图（如 `ALL_TABLES`、`ALL_TAB_COLUMNS`） |

建议在达梦中创建专用的只读账号，例如：

```sql
CREATE USER TDSYNC IDENTIFIED BY "Sync_Passw0rd";
GRANT SELECT ON SCADA.METER_READINGS TO TDSYNC;
```

## 创建任务

### 新增数据源

在数据写入页面中点击 **+新增数据源** 按钮，进入新增数据源页面。

### 配置基本信息

在 **名称** 字段中输入任务名称，例如 *`test_dameng_01`*。

选择 **类型** 下拉框中的 *`达梦`*，页面中的字段会随之变化。

**代理** 是非必填项。taosX 无法直接访问达梦数据库时，可以在能访问达梦的主机上部署 taosX-Agent，并在下拉框中选择该代理，也可以点击右侧的 **+创建新的代理** 按钮创建一个新的代理。代理与 taosX 之间的网络要求（端口放通与检查方法）见 [数据接入前置条件](./00-prerequisites.md)。

**目标数据库** 是必填项，可以先点击右侧的 **+创建数据库** 按钮创建一个新的数据库。目标数据库的时间精度应不低于源数据的时间精度，见 [时间与时区](./23-rdb-integration.md#时间与时区)。

### 配置连接信息

在 **连接配置** 区域填写源达梦数据库的连接信息：

| 字段 | 是否必填 | 说明 |
| --- | --- | --- |
| **主机** | 是 | 达梦服务器的 IP 地址或主机名 |
| **端口** | 是 | 达梦服务端口，默认 `5236` |
| **模式** | 否 | 默认模式（Schema）。填写后，SQL 模板中的表名可以省略模式前缀；不填写时使用登录用户的同名模式 |
| **字符集** | 否 | 连接字符集，可选 `UTF-8`、`GB18030`，默认 `UTF-8`。应与达梦实例的字符集一致，否则中文可能出现乱码 |
| **SSL 模式** | 否 | 可选 `DISABLED`（不加密）、`REQUIRED`（使用 SSL 加密连接）。达梦服务端开启 SSL 后选择 `REQUIRED`，并上传达梦服务端的 CA 证书 |

### 配置认证信息

**用户** 输入源达梦数据库的用户，该用户必须对要同步的表具有读取权限。

**密码** 输入该用户的登录密码。

然后点击 **检查连通性** 按钮，检查上方填写的信息能否正常连接达梦数据库并读取数据。连接失败时，页面会显示达梦返回的错误码和错误信息，常见原因见 [常见问题](#常见问题)。

### 配置 SQL 查询

**子表字段** 用于拆分子表的字段，它是一条 `select distinct` 的 SQL 语句，查询指定字段组合的非重复项，通常与 transform 中的 tag 相对应：

> 此项配置主要为了解决数据迁移乱序问题，需要结合 **SQL 模板** 共同使用，否则不能达到预期效果，使用示例如下：
>
> 1. 子表字段填写语句 `select distinct col_name1, col_name2 from table`，它表示使用源表中的字段 col_name1 与 col_name2 拆分目标超级表的子表
> 2. 在 **SQL 模板** 中添加子表字段占位符，例如 `select * from table where ts >= ${start} and ts < ${end} and ${col_name1} and ${col_name2}`。运行时 `${col_name1}`、`${col_name2}` 会展开为等值条件（如 `col_name1='deviceA'`、`col_name2=1`），而不是裸列名
> 3. 在 **transform** 中配置 `col_name1` 与 `col_name2` 两个 tag 映射

**SQL 模板** 用于查询的 SQL 语句模板，SQL 语句中必须包含时间范围条件，且开始时间和结束时间必须成对出现。SQL 语句模板中定义的时间范围由源数据库中的某个代表时间的列和下面定义的占位符组成。

> SQL 使用不同的占位符表示不同的时间格式要求，具体有以下占位符格式：
>
> 1. `${start}`、`${end}`：按任务的时间配置生成达梦 `TO_TIMESTAMP('2024-03-14 08:00:00.000', 'YYYY-MM-DD HH24:MI:SS.FF3')` 表达式，适用于 `TIMESTAMP`、`DATETIME` 类型的列
> 2. `${start_no_tz}`、`${end_no_tz}`：生成不带时区信息的时间字符串，如 `'2024-03-14 08:00:00'`
> 3. `${start_date}`、`${end_date}`：表示仅日期，如 `DATE '2024-03-14'`，适用于 `DATE` 类型的列。建议仅在查询间隔为 1 天时使用
>
> 为了解决迁移数据乱序的问题，应在查询语句中添加排序条件，例如 `ORDER BY ts ASC`。

例如，从电表读数表中按入库时间增量读取数据：

```sql
SELECT METER_ID, LOCATION, READING_TS, INGEST_TS, VOLTAGE, CURRENT_A
FROM SCADA.METER_READINGS
WHERE INGEST_TS >= ${start} AND INGEST_TS < ${end}
ORDER BY INGEST_TS, ID
```

### 配置采集信息

**起始时间** 迁移数据的起始时间，此项为必填字段。

**结束时间** 迁移数据的结束时间，可留空。设置结束时间时，任务读取到结束时间后自动完成；留空则持续同步新数据，任务不会自动停止。

**查询间隔** 分段查询数据的时间间隔，默认 1 天。为了避免单次查询数据量过大，任务按查询间隔把时间范围划分为多个窗口逐个查询。

**延迟时长** 避免因源库事务延迟提交而漏读数据。持续同步模式下，每个窗口在窗口结束时间加上延迟时长后才查询；定时同步模式下，读取的安全水位为当前时间减去延迟时长。延迟时长应大于达梦中最长写入事务的持续时间。

**定时计划** 可选择 **持续同步** 或 **定时窗口同步**。选择定时窗口同步时，可设置首次计划时刻和固定调度周期（分钟），或设置五字段分钟级 Cron 表达式，并设置 **单轮最多窗口数**。调度使用 taosExplorer 页面当前时区。

**时区** 解释达梦中不带时区的时间类型（`DATETIME`、`TIMESTAMP`）时使用的时区，默认使用 taosX 所在主机的时区。

上述参数的组合决定任务的同步模式：

| 同步模式 | 起始时间 | 结束时间 | 定时计划 |
| --- | --- | --- | --- |
| 全量迁移 | 历史数据的开始时间 | 迁移截止时间 | 持续同步 |
| 增量同步 | 开始同步的时间 | 留空 | 持续同步 |
| 定时同步 | 开始同步的时间 | 可选 | 定时窗口同步 |

各模式的详细行为见 [关系型数据库集成 · 同步模式](./23-rdb-integration.md#同步模式)。

### 配置数据映射

在 **数据映射** 区域填写数据映射相关的配置参数。

点击 **从服务器检索** 按钮，从达梦服务器获取示例数据。

在 **从列中提取或拆分** 中填写从消息体中提取或拆分的字段，例如：将 vValue 字段拆分成 `vValue_0` 和 `vValue_1` 这 2 个字段，选择 split 提取器，separator 填写分割符 `,`，number 填写 2。

在 **过滤** 中，填写过滤条件，例如：填写 `Value > 0`，则只有 Value 大于 0 的数据才会被写入 TDengine。

在 **映射** 中，选择要映射到 TDengine 的超级表，以及映射到超级表的列。超级表不存在时，可以根据示例数据自动生成超级表结构。

点击 **预览**，可以查看映射的结果。

达梦数据类型与 TDengine 数据类型的对应关系如下，完整规则见 [数据类型转换](./23-rdb-integration.md#数据类型转换)：

| 达梦类型 | TDengine 类型 | 说明 |
| --- | --- | --- |
| `BIT`、`BOOLEAN` | `BOOL` | |
| `TINYINT`、`BYTE` | `TINYINT` | |
| `SMALLINT` | `SMALLINT` | |
| `INT`、`INTEGER` | `INT` | |
| `BIGINT` | `BIGINT` | |
| `NUMBER(p,s)`、`DECIMAL(p,s)`、`NUMERIC(p,s)` | `DECIMAL(p,s)` | `p` 超过 38 时转换为 `DOUBLE` |
| `NUMBER`（未指定精度） | `DOUBLE` | |
| `REAL` | `FLOAT` | |
| `FLOAT`、`DOUBLE`、`DOUBLE PRECISION` | `DOUBLE` | |
| `CHAR`、`VARCHAR`、`VARCHAR2` | `NCHAR(n)` | 只含 ASCII 字符时可改为 `VARCHAR` |
| `TEXT`、`CLOB`、`LONGVARCHAR` | `NCHAR` / `VARCHAR` | 超过 65,517 字节时按 **列长度溢出** 策略处理 |
| `BINARY`、`VARBINARY` | `VARBINARY` | |
| `BLOB`、`IMAGE`、`LONGVARBINARY` | `BLOB` | 单值最大 16 MB |
| `DATE`、`DATETIME`、`TIMESTAMP` | `TIMESTAMP` | 按任务时区解释 |
| `TIMESTAMP WITH TIME ZONE`、`DATETIME WITH TIME ZONE` | `TIMESTAMP` | 按值中的时区换算 |
| `TIME` | `VARCHAR(18)` | 以 `HH:MI:SS[.ffffff]` 格式写入 |

### 配置高级选项

import AdvancedOptions from './resources/_02-advanced_options.mdx'

<AdvancedOptions/>

除上述通用选项外，达梦数据源还提供以下选项：

**数据校验** 逐窗口核对源数据与写入 TDengine 的数据，可选 **关闭**、**行数校验**、**行数和摘要校验**，默认关闭。需要保证同步数据准确率 100% 时，选择 **行数和摘要校验**。

**校验失败策略** 校验不一致时的处理方式，可选 **重做窗口**（默认）或 **报错停止**。

**校验重试次数** 校验失败策略为 **重做窗口** 时，单个窗口的最大重试次数，默认 3。

校验规则和结果查看方式见 [关系型数据库集成 · 数据校验](./23-rdb-integration.md#数据校验)。

### 异常处理策略

import ExceptionHandling from './resources/_03-exception-handling-strategy.mdx'

<ExceptionHandling />

### 创建完成

点击 **提交** 按钮，完成创建达梦到 TDengine 的数据同步任务，回到 **数据源列表** 页面可查看任务执行情况。开启数据校验时，可以在任务详情页的 **数据校验** 页签中查看每个窗口的校验结果。

## 示例：迁移 SCADA 断面数据

电力调度系统常在达梦中按 1 分钟或 5 分钟保存断面数据，表结构如下：

```sql
CREATE TABLE SCADA.SECTION_DATA (
    POINT_ID    VARCHAR(32),
    POINT_NAME  VARCHAR(128),
    STATION     VARCHAR(64),
    SAMPLE_TIME DATETIME(3),
    VAL         NUMBER(12, 4),
    QUALITY     INT,
    PRIMARY KEY (POINT_ID, SAMPLE_TIME)
);
```

目标是把 2024 年的历史数据迁移到 TDengine，迁移完成后持续同步新数据。

1. 在 TDengine 中创建毫秒精度的数据库和超级表：

    ```sql
    CREATE DATABASE scada PRECISION 'ms';
    CREATE STABLE scada.section_data (
        ts      TIMESTAMP,
        val     DECIMAL(12, 4),
        quality INT
    ) TAGS (
        point_id   VARCHAR(32),
        point_name NCHAR(128),
        station    NCHAR(64)
    );
    ```

2. 创建达梦数据源任务，SQL 模板如下：

    ```sql
    SELECT POINT_ID, POINT_NAME, STATION, SAMPLE_TIME, VAL, QUALITY
    FROM SCADA.SECTION_DATA
    WHERE SAMPLE_TIME >= ${start} AND SAMPLE_TIME < ${end}
    ORDER BY SAMPLE_TIME, POINT_ID
    ```

3. **起始时间** 设为 `2024-01-01 00:00:00`，**结束时间** 留空，**查询间隔** 设为 `1h`，**延迟时长** 设为 `60s`，**定时计划** 选择 **持续同步**。任务先按小时窗口迁移历史数据，追上当前时间后转为增量同步。
4. 在 **数据映射** 中，子表名称设为 `p_{POINT_ID}`，`POINT_ID`、`POINT_NAME`、`STATION` 映射为标签，`SAMPLE_TIME` 映射为 `ts`，`VAL`、`QUALITY` 映射为同名列。
5. 在 **高级选项** 中把 **数据校验** 设为 **行数和摘要校验**。
6. 提交任务。迁移完成后，在 TDengine 中查询断面数据：

    ```sql
    SELECT ts, val, quality FROM scada.section_data
    WHERE point_id = 'P0001' AND ts >= '2024-06-01' AND ts < '2024-06-02';
    ```

源表中 `(POINT_ID, SAMPLE_TIME)` 唯一，映射后每个测点一张子表，`ts` 在子表内唯一，满足 [幂等写入](./23-rdb-integration.md#幂等写入) 的要求。

## 常见问题

**连接时报错找不到 `libdmdpi.so` 或 `dmdpi.dll`**

运行 taosX 或 taosX-Agent 的主机上没有安装达梦 DPI 客户端库，或库所在目录不在动态库搜索路径中。安装达梦客户端后，把库所在目录加入 `LD_LIBRARY_PATH`（Linux）或 `PATH`（Windows），然后重启 taosX 或 taosX-Agent。

**连接超时或网络不可达**

检查达梦服务是否启动、端口是否为实际监听端口（默认 `5236`），以及防火墙是否放通。端口检查方法见 [数据接入前置条件](./00-prerequisites.md)。

**中文显示为乱码**

连接配置中的 **字符集** 与达梦实例的字符集不一致。可以在达梦中执行 `SELECT SF_GET_UNICODE_FLAG();` 查看实例字符集：返回 `1` 表示 UTF-8，返回 `0` 表示 GB18030，然后修改任务的字符集并重启任务。

**同步后行数少于源表**

常见原因有三类：目标数据库时间精度低于源数据，导致不同时间被截断为同一时间戳；同一测点在同一时间有多行数据，但没有配置复合主键；延迟时长小于源库写入事务的持续时间，导致漏读。开启 **行数和摘要校验** 可以定位到具体的窗口，处理方法见 [关系型数据库集成 · 事务一致性](./23-rdb-integration.md#事务一致性)。
