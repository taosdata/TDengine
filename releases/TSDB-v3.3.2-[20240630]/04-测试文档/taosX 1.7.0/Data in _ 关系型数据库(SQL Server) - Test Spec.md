# Data in : 关系型数据库(SQL Server) - Test Spec

## 1. 测试目标

- 验证 SQL Server 数据库数据迁移、数据同步至TDengine

## 2. 变更历史

| Date | Version | Owner | Memo |
| --- | --- | --- | --- |
| 2024-05-27 | 0.1 | @秦冲 |  |
| 2024-06-15 | 1.0 | @贾晨阳 |  |

## 3. 测试范围

本需求的覆盖范围：
- 关系型数据库支持通过指定 sql 语句模板查询将数据写入TDengine
- 关系型数据库指定时间区间的历史数据迁移
- 关系型数据库通过指定起始时间进行实时数据同步
- 关系型数据库按时间进行分表场景

## 4. 测试结论

- 通过指定 SQL 语句模板查询将 Sql Server 历史数据，实时数据同步到 TDengine, 验证通过。支持的数据类型见 13.1 节
- 按日期分表，将 SQL Server 数据同步到 TDengine，验证通过。
- 遗留问题：
  - binary二进制流的数据类型，导入到TDengine后以字符串存储，目前TDengine已经支持通过varbinary存储原始二进制数据，期望是通过varbinary存储二进制流，避免因类型转换导致数据不一致。
    TD-30589

  - 加密策略只验证了not supported，其他加密方法由于需要修改服务端配置，目前未测试。

## 5. 开发质量报告

结论：本特性的开发质量是优

| 统计指标 | 数量 |
| --- | --- |
| 提测被拒次数 | 0 |
| 基础测试用例不通过 | 0 |
| Bug 总数 | 4 |
| 严重 Bug 总数 | 0 |

## 6. 已知问题和限制

- SQL server版本只验证2022版

## 7. 测试环境

- OS:  Linux
- Browser: Chrome

## 8. 测试数据

在 SQL Server 中创建表，表中字段类型涵盖 FS 中所有类型。
SQL Server 中建表结构：
```sql
CREATE TABLE TestTable (
    tTinyInt tinyint,
    sSmallInt smallint,
    iInt int,
    bBigInt bigint,
    rReal real,
    fFloat float,
    dDecimal decimal(18, 2),
    nNumeric numeric(18, 2),
    cChar char(10),
    nNChar nchar(10),
    vVarChar varchar(50),
    vVarCharMax varchar(MAX),
    nNVarchar nvarchar(50),
    nNVarcharMax nvarchar(MAX),
    tText text,
    nNText ntext,
    bBit bit,
    bBinary binary(50),
    vVarBinary varbinary(50),
    vVarBinaryMax varbinary(MAX),
    dDate date,
    dDateTime datetime,
    dDateTime2 datetime2,
    dDateTimeOffset datetimeoffset,
    sSmallDateTime smalldatetime,
    tTime time,
    tTimestamp timestamp,
    iImage image,
    mMoney money,
    sSmallMoney smallmoney,
    uUniqueIdentifier uniqueidentifier,
    xXML xml
);

```



## 9. 测试用例

### 9.1 功能

在提测时，开发应保证 basic 类型的用例全部通过。
以下测试用例中，如无特殊描述，数据源中的表结构均为包含 13.1 章节所有类型的表结构，同时表结构中存在一个 timestamp 字段，**字段名为 ts**。
| basic case | Description | Expected Results | result for developer | Result | Jira | Automated | Memo |
| --- | --- | --- | --- | --- | --- | --- | --- |
| basic | 配置有效的服务地址和端口、数据库名、用户名、密码，不使用其他配置 | 连通性校验通过 |  | Pass |  |  |  |
| basic | 1. 在数据源中创建包含 int32、float、double、varchar(32)字段的表并写入数据
1. 在TDengine中提前创建 schema 一致的超级表
2. 任务配置中设置匹配的 transformer 规则为 mapping，创建任务 | 任务创建成功，对应数据源表中的数据正确写入TDengine中 |  | Pass |  |  |  |
|  | 连接选项指定加密方式为 On，进行连通性校验 | 连通性校验通过 |  | Pass |  |  |  |
|  | 连接选项指定加密方式为 NotSupported 进行连通性校验 | 连通性校验通过 |  | Pass |  |  |  |
|  | 连接选项指定加密方式为 Required 进行连通性校验 | 连通性校验通过 |  | Pass |  |  |  |
|  | 连接选项指定加密方式为 off 进行连通性校验 | 连通性校验通过 |  | Pass |  |  |  |
|  | 连接选项启用信任证书 | 连通性校验通过 |  | Pass |  |  |  |
|  | 连接选项不启用信任证书同时上传信任证书 CA | 连通性校验通过 |  |  |  |  |  |
|  | 连接选项不启用信任证书同时上传错误的信任证书 CA | 连通性校验不通过 |  | Pass |  |  |  |
|  | 用户名、密码包含特殊字符（%$#@!等）
用户名：AbE$322_test
密码：Taos@data_2024!@#$ | 连通性校验通过 |  | Pass |  |  |  |
|  | 使用错误的用户名、密码、db名、访问地址信息 | 连通性校验不通过 |  | Pass |  |  |  |
| 历史数据迁移 | 使用UI中的起始时间和终止时间作为时间区间：
1. 在数据源中创建包含13.1章节中所有字段的表并写入数据
2. 在TDengine中提前创建schema一致的超级表
3. 任务配置中配置sql：select * from tbname where time >= &start and time < &end，配置起始时间、终止时间有效，设置匹配的transformer规则分别配置为mapping、value、sum、format、expr规则 | TDengine中对应表写入数据且数据一致 |  | Pass |  |  |  |
|  | 在sql中使用数据源的不同时间字段过滤：
1. 数据源中对应字段使用带时区 RFC3339 格式时间戳，如2024-03-14T08:00:00+0800
2. 数据源中对应字段使用不带时区RFC3339 格式时间戳，如2024-03-14T08:00:00
3. 数据源中对应字段只包含日期，如2024-03-14
4. 数据源中对应字段只包含时间，如08:00:00 | 写入TDengine中的数据均满足该过滤条件 |  | Pass |  |  |  |
|  | 1. 配置起始时间和终止时间，在数据源中对应时间区间没有任何数据 | 获取的示例数据为空并提示错误 |  | Pass |  |  |  |
|  | 启动任务后编辑任务，修改endtime：
1. endtime < checkpoint;
2. endtime >= checkpoint |  |  | Pass |  |  |  |
| 实时数据同步 | 使用UI中的起始时间作为时间起点：
1. 在数据源中创建包含13.1章节中所有字段的表并写入数据
2. 在TDengine中提前创建schema一致的超级表
3. 任务配置中配置sql：select * from tbname where time >=&start and time<&end，配置起始时间有效，设置匹配的transformer规则分别配置为mapping、value、sum、format、expr规则
4.通过测试程序持续写入实时数据 | TDengine中写入表的数据最早时间满足配置的起始时间，且实时有新数据写入，任务不会自动停止 |  | Pass |  |  |  |
|  | 在sql中使用数据源的不同时间字段过滤：
1. 数据源中对应字段使用带时区 RFC3339 格式时间戳，如2024-03-14T08:00:00+0800（MySQL除外）
2. 数据源中对应字段使用不带时区RFC3339 格式时间戳，如2024-03-14T08:00:00
3. 数据源中对应字段只包含日期，如2024-03-14
4. 数据源中对应字段只包含时间，如08:00:00 | 写入TDengine中的数据均满足该过滤条件 |  | Pass |  |  |  |
| 输入校验 | sql语句时间起止占位符校验：
1.配置 ${start} 、${end}
2.配置 ${start_no_tz} 、${end_no_tz}
3.配置 ${start_date} 、${end_date}
4.配置${start_time} 、${end_time} | tranfsormer示例数据和TDengine写入数据满足对应占位符的过滤条件 |  | Pass |  |  |  |
|  | sql语句时间起止占位符异常校验：
1.配置起止占位符格式不匹配，比如 ${start}、${end_no_tz}
2.配置起止占位符格式与源端的数据类型不一致 | 1.提示错误
1. 如果数据源精度为date（年月日），占位符精度为time（时分秒），即占位符精度更高时,提示错误
2. 如果数据源精度为time（时分秒），占位符精度为date或year，即数据源精度更高时，不提示错误 |  | Pass |  |  |  |
|  | sql语句异常：
1. where条件中缺少 $start 或 $end
2. sql语法错误 | 提示错误 |  | Pass |  |  |  |
| 按日期分表 | 1. 在数据源中构建表名中带日期的表结构，如：
create table tb_20240708(
  ts time,
  current float,
  voltage int,
  phase float
);
并写入只包含时分秒时间（如“10:00:00”）的时间字符串。
1. 在sql配置中配置sql语句：
select * from (select id, concat('${F}', 'T', ts, '+08:00') as ts, current, voltage, phase from meters_${Ymd}) t where ts >= ${start_time} and ts < ${end_time};
1. transformer规则使用mapping映射到超级表中 | transformer示例数据中的ts列为拼接后的时间字符串，并能够正确写入TDengine中 |  | Pass |  |  |  |
| 宽表列转行 | 1.在数据源中构建字段表示时间序列的表结构，如：create table mytable(
  id int,
  dt date,
  hour smallint,
  m00 int,
  m01 int,
  m02 int,
  m03 int,
  m04 int,
  m05 int
); 每列（m00，m01等）分别代表一个时间序列的数据
1. 在sql模版中使用union all语法，如：
SELECT concat(dt, 'T', LPAD(hour, 2, '0'), ':00:00+0800') AS m, m00 AS value FROM mytable
UNION ALL
1. transformer规则中使用mapping映射到超级表中 | transformer示例数据中ts列为时间列转行后拼接的时间字符串，如“2024-04-04T01:00:00+0800”，并能够正确写入TDengine中 |  |  |  |  |  |
| 高级选项 | 配置最大读并发数为最大值、最小值 | 下发任务中该参数值与设置一致 |  | Pass |  |  |  |
|  | 配置最大读并发数为边界外值/非法值 | 前端限制无法设置 |  | Pass |  |  |  |
|  | 配置批次大小为最大值、最小值 | 下发任务中该参数值与设置一致 |  | Pass |  |  |  |
|  | 配置批次大小为边界外值/非法值 | 前端限制无法设置 |  | Pass |  |  |  |
|  | 任务修改配置，修改了start_time和end_time, 使得新的时间区间不在原来的时间范围内 | 从上次结束时的时间开始重新拉取数据 |  | Pass |  |  |  |
| 异常测试 | 任务编辑状态，尝试编辑sql语句 | 前端限制无法编辑 |  | Pass |  |  |  |
|  | 启动任务后尝试编辑sql语句 | 前端限制无法编辑 |  | Pass |  |  |  |

### 9.2 可用性

UI 需和 FS 中描述一致。

### 9.3 可靠性

无

### 9.4 性能

无

### 9.5 安全性

无

### 9.6 兼容性

只直接支持 SQL Server 2022 版本，其他版本没有进行测试。

### 9.7 本地化

无

## 10. 待讨论事项

## 11. Jira

此feature相关的所有Jira, 标题中应包含统一的标签: taosx、mssql、explorer
<!-- Unsupported block type: 999 -->

## 12. 测试计划 

## 13. 测试备忘

### 13.1 数据类型映射关系对照表

| SQL Server 字段类型 | Sample Data | Arrow 类型 | TDengine 类型 |
| --- | --- | --- | --- |
| tinyint | i8 | Int8 | TINYINT |
| smallint | i16 | Int16 | SMALLINT |
| int | i32 | Int32 | INT |
| bigint | i64 | Int64 | BIGINT |
| real | f32 | Float32 | FLOAT |
| float | f64 | Float64 | DOUBLE |
| decimal | String | Utf8 | NCHAR |
| numeric | String | Utf8 | NCHAR |
| char | String | Utf8 | NCHAR |
| nchar | String | Utf8 | NCHAR |
| varchar | String | Utf8 | NCHAR |
| varchar(MAX) | String | Utf8 | NCHAR |
| nvarchar | String | Utf8 | NCHAR |
| nvarchar(MAX) | String | Utf8 | NCHAR |
| text | String | Utf8 | NCHAR |
| ntext | String | Utf8 | NCHAR |
| bit | String | Utf8 | NCHAR |
| binary | String | Utf8 | NCHAR |
| varbinary | String | Utf8 | NCHAR |
| varbinary(MAX) | String | Utf8 | NCHAR |
| date | String | Utf8 | NCHAR |
| datetime | String | Utf8 | NCHAR |
| datetime2 | String | Utf8 | NCHAR |
| datetimeoffset | DateTime | Timestamp(TimeUnit::Nanosecond, None) | TIMESTAMP |
| smalldatetime | String | Utf8 | NCHAR |
| time | String | Utf8 | NCHAR |
| timestamp | String | Utf8 | NCHAR |
| geography |
| geometry |
| hierarchyid |
| image | String | Utf8 | NCHAR |
| money | String | Utf8 | NCHAR |
| smallmoney | String | Utf8 | NCHAR |
| sql_variant |
| uniqueidentifier | String | Utf8 | NCHAR |
| xml | String | Utf8 | NCHAR |


## 14. 参考文档

- [Data In: SQL Server](https://taosdata.feishu.cn/wiki/YYGuw9LXmiePQ7kQS1Uc9oIsnPh)
