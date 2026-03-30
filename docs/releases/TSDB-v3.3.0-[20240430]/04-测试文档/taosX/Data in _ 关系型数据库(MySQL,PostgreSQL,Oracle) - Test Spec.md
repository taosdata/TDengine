# Data in : 关系型数据库(MySQL,PostgreSQL,Oracle) - Test Spec

## 1. 测试目标

- 验证MySQL数据库数据迁移、数据同步至TDengine
- 验证PostgreSQL 数据库数据迁移、数据同步至TDengine
- 验证Oracle 数据库数据迁移、数据同步至TDengine

## 2. 变更历史

| Date | Version | Owner | Memo |
| --- | --- | --- | --- |
| 2024.4.10 | 0.1 | @贾晨阳 |  |
| 2024.4.15 | 0.2 | @贾晨阳 | 依据组内review和研发reivew进行测试用例修改 |
| 2024.4.18 | 1.0 | @贾晨阳 | 依据最新FS修改测试用例，增加当前测试范围限制 |
| 2024.5.11 | 1.1 | @贾晨阳 | 修改oracle datain 测试用例 |

## 3. 测试范围

本需求的覆盖范围：
- 关系型数据库支持通过指定sql语句模板查询将数据写入TDengine
- 关系型数据库指定时间区间的历史数据迁移
- 关系型数据库通过指定起始时间进行实时数据同步
- 关系型数据库按时间进行分表场景

## 4. 测试结论

- 通过指定 SQL 语句模板查询将 MySQL/PostgreSQL 历史数据，实时数据同步到 TDengine, 验证通过。支持的数据类型见 13.1 和 13.5 节
- 按日期分表，将 MySQL/PostgreSQL 数据同步到 TDengine，验证通过。
- 遗留问题见第 6 节

## 5. 开发质量报告

结论：本特性的开发质量是良

| 统计指标 | 数量 |
| --- | --- |
| 提测被拒次数 | 0 |
| 基础测试用例不通过 | 0 |
| Bug 总数 | 15 |
| 严重 Bug 总数 | 0 |

## 6. 已知问题和限制

- MySQL、PostgreSQL ： ssl_mode = VERIFY_CA、VERIFY_IDENTITY两个访问模式该版本不支持，相关测试不开展
- 本次测试不进行binary数据流类型的验证
- 由于数据源库中无法写入不含值的列（即写入None），导致transformer mapping default value功能无法生效，经讨论，该问题未来可能采用和CSV相同的处理方式，本次测试不验证相关模块。
  TD-29790

- PostgreSQL 数据源， ssl_mode 为perfer， allow不符合预期。
  TD-29722

- Oracle 连接信息中 **client_info、client_id、schema、call_time **参数本次未实现，不开展测试。
- Oracle 版本本次使用 Oracle19C 进行测试，其他版本暂不测试
- taosx/agent在引用oracle的ODPI-C库时，需要确保依赖库版本与服务端版本一致。
- 分库分表的场景中，需要注意起始时间的配置。起始时间的表需要存在。

## 7. 测试环境

- OS: Windows, Linux
- Browser: Chrome

## 8. 测试数据

在 MySQL/PostgreSQL 中创建表，表中字段类型涵盖13.1章节中所有类型。
MySQL中建表结构：
```sql
mysql> describe tb_test;
+-----------+--------------------+------+-----+---------+-------+
| Field     | Type               | Null | Key | Default | Extra |
+-----------+--------------------+------+-----+---------+-------+
| tint      | tinyint            | YES  |     | NULL    |       |
| utint     | tinyint unsigned   | YES  |     | NULL    |       |
| sint      | smallint           | YES  |     | NULL    |       |
| usint     | smallint unsigned  | YES  |     | NULL    |       |
| mint      | mediumint          | YES  |     | NULL    |       |
| umint     | mediumint unsigned | YES  |     | NULL    |       |
| iint      | int                | YES  |     | NULL    |       |
| uint      | int unsigned       | YES  |     | NULL    |       |
| bint      | bigint             | YES  |     | NULL    |       |
| ubint     | bigint unsigned    | YES  |     | NULL    |       |
| ffloat    | float              | YES  |     | NULL    |       |
| ddouble   | double             | YES  |     | NULL    |       |
| ddecimal  | decimal(10,0)      | YES  |     | NULL    |       |
| cchar     | char(1)            | YES  |     | NULL    |       |
| vvarchar  | varchar(255)       | YES  |     | NULL    |       |
| ttext     | tinytext           | YES  |     | NULL    |       |
| test_sam  | text               | YES  |     | NULL    |       |
| mtext     | mediumtext         | YES  |     | NULL    |       |
| ltext     | longtext           | YES  |     | NULL    |       |
| ddate     | date               | YES  |     | NULL    |       |
| ttime     | time               | YES  |     | NULL    |       |
| ddatetime | datetime           | YES  |     | NULL    |       |
| ts        | timestamp          | YES  |     | NULL    |       |
| yyear     | year               | YES  |     | NULL    |       |
| bbit      | bit(1)             | YES  |     | NULL    |       |
+-----------+--------------------+------+-----+---------+-------+

```

MySQL/PostgreSQL 数据源均已经创建了需通过 ssl 访问的用户
在 MySQL 中创建 SSL 登录用户的方式见13.2章节。
MySQL：
SSL-only 测试用户为 **test_ssl_only/taosdata**
DISABLE-only 测试用户为 **test_DISABLE_only/taosdata**

PostgreSQL：@聂敏慧 需要增加ssl-only用户和disable-only用户
SSL-only测试用户： **test_ssl_only/taosdata**
DISABLE-only 测试用户: **test_disable_only/taosdata**
用户在pg_hba.conf中的配置如下：
```plaintext {wrap}
hostssl all             test_ssl_only   192.168.1.1/16          md5
host all             postgres        192.168.1.1/16          scram-sha-256
hostnossl all             test_disable_only        192.168.1.1/16          scram-sha-256
```

注：
local ：使用unix-domain的socket连接
host ：使用TCP/IP socket连接，允许客户端以SSL连接、也允许客户以非SSL连接
hostssl ：使用SSL加密的TCP/IP socket连接，仅允许客户端以SSL连接
hostnossl ：使用非SSL的TCP/IP socket连接， 仅允许客户端以非SSL连接
客户端验证方法：
```plaintext {wrap}
psql "host=192.168.1.40 port=5432 dbname=test_ssl_only user=test_ssl_only password=taosdata sslmode=allow"
```


Oracle中创建表结构：
```sql {wrap}
SQL> desc taosx_test;
 Name                                           Null?    Type
 ----------------------------------------- -------- ----------------------------
 VVARCHAR                                            VARCHAR2(128)
 NNVARCHAR                                            NVARCHAR2(128)
 NNUMBER                                            NUMBER(6,2)
 FFLOAT                                             FLOAT(126)
 LLONG                                                    LONG
 DDATE                                                    DATE
 B_FLOAT                                            BINARY_FLOAT
 B_DOUBLE                                            BINARY_DOUBLE
 T_TIME                                             TIMESTAMP(6)
 T_TIME_Z                                            TIMESTAMP(6) WITH TIME ZONE
 T_TIME_LZ                                            TIMESTAMP(6) WITH LOCAL TIM

```

## 9. 测试用例

### 9.1 功能

在提测时，开发应保证 basic 类型的用例全部通过。
以下测试用例中，如无特殊描述，数据源中的表结构均为包含 13.1 章节所有类型的表结构，同时表结构中存在一个 timestamp 字段，**字段名为 ts**。
| basic case | DBs | Description | Expected Results | result for developer | Result | Jira | Automated | Memo |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| basic | MySQL | 配置有效的服务地址和端口、数据库名、用户名、密码，不使用其他配置 | 连通性校验通过 | Pass | Pass |  |  |  |
|  | PostgreSQL |  |  | Pass | Pass |  |  |  |
|  | Oracle |  |  | Pass | Pass |  |  |  |
| basic | MySQL | 1. 在数据源中创建包含int32、float、double、varchar(32)字段的表并写入数据
1. 在TDengine中提前创建schema一致的超级表
2. 任务配置中设置匹配的transformer规则为mapping，创建任务 | 任务创建成功，对应数据源表中的数据正确写入TDengine中 | Pass | Pass |  |  |  |
|  | PostgreSQL |  |  |  | Pass |  |  |  |
|  | Oracle |  |  |  | Pass | [TD-29998](https://jira.taosdata.com:18080/browse/TD-29998) |  | 问题原因是client和server版本不匹配，导致sql执行报错；切换client的库版本后问题就没有了；但为何连通性可以通过仍无法定位 |
| 连通性检测 | MySQL | SSL模式 = disabled
输入正确的用户名、密码、数据库名 | 连通性校验通过 |  | Pass |  |  |  |
|  |  | SSL模式 = disabled
输入 SSL-only 用户名、密码、数据库名 | 连通性校验不通过 |  | Pass | [TD-29686](https://jira.taosdata.com:18080/browse/TD-29686) |  |  |
|  |  | SSL模式 = disabled
输入错误的用户名/密码,或是该用户不可访问的数据库名 | 连通性校验不通过 |  | Pass |  |  |  |
|  |  | SSL模式 = REQUIRED
1. 使用ssl访问的用户名、密码、数据库名 | 连通性校验通过 |  | Pass |  |  |  |
|  |  | SSL模式 = REQUIRED
1. 使用非 ssl 访问的用户名、密码、数据库名 | 连通性校验通过 |  | Pass |  |  |  |
|  |  | 用户名、密码包含特殊字符（%$#@!等）
用户名：AbE$322_test
密码：Taos@data_2024% | 连通性校验通过 |  | Pass |  |  |  |
|  |  | SSL模式 = PREFERERED
配置正确的用户名密码 | 连通性校验通过 |  | Pass |  |  |  |
|  |  | SSL模式 = PREFERERED
配置错误的用户名密码,或是该用户不可访问的数据库名 | 连通性校验不通过 |  | Pass |  |  |  |
|  | PostgreSQL | SSL模式 = disabled
输入正确的用户名、密码、数据库名 | 连通性校验通过 |  | Pass | [TD-29722](https://jira.taosdata.com:18080/browse/TD-29722) |  |  |
|  |  | SSL模式 = disabled
输入 SSL 用户名、密码、数据库名 | 连通性校验不通过 |  | Pass | [TD-29722](https://jira.taosdata.com:18080/browse/TD-29722) |  |  |
|  |  | SSL模式 = disabled
输入错误的用户名/密码,或是该用户不可访问的数据库名 | 连通性校验不通过 |  | Pass | [TD-29722](https://jira.taosdata.com:18080/browse/TD-29722) |  |  |
|  |  | SSL模式 = REQUIRED
1. 使用ssl访问的用户名、密码、数据库名 | 连通性校验通过 |  | Pass |  |  |  |
|  |  | SSL模式 = REQUIRED
1. 使用非 ssl 访问的用户名、密码、数据库名 | 连通性校验不通过 |  | Pass |  |  |  |
|  |  | 用户名、密码包含特殊字符 | 连通性校验通过 |  |  |  |  |  |
|  |  | SSL模式 = PREFER
配置正确的用户名密码 | 连通性校验通过 |  | Pass | [TD-29722](https://jira.taosdata.com:18080/browse/TD-29722) |  |  |
|  |  | SSL模式 = PREFER
配置错误的用户名密码,或是该用户不可访问的数据库名 | 连通性校验不通过 |  | Fail | [TD-29722](https://jira.taosdata.com:18080/browse/TD-29722) |  |  |
|  |  | SSL模式 = ALLOW
配置正确的用户名密码 | 连通性校验通过 |  | Pass | [TD-29722](https://jira.taosdata.com:18080/browse/TD-29722) |  |  |
|  |  | SSL模式 = ALLOW
配置错误的用户名密码,或是该用户不可访问的数据库名 | 连通性校验不通过 |  | Fail | [TD-29722](https://jira.taosdata.com:18080/browse/TD-29722) |  |  |
|  | Oracle | client_info (客户端信息)
设置 正确的DBMS_APPLICATION_INFO.SET_CLIENT_INFO | 连通性校验通过 |  |  |  |  |  |
|  |  | client_id(客户端 ID)
设置 正确的DBMS_SESSION.SET_IDENTIFIER ，进行客户端识别 | 连通性校验通过 |  |  |  |  |  |
|  |  | schema(设置当前模式)
设置为与用户名不同的模式以直接使用对象名称（表名等） | 连通性校验通过 |  |  |  |  |  |
|  |  | call_time(调用超时)
设置此超时时间以避免查询长时间等待 | 当查询时间超过call_time时，会直接退出查询 |  |  |  |  |  |
|  |  | 安装脚本添加 Oracle 运行时库检查 | 1. 当没有安装Oracle 运行时库时，应该提示
1. 当已安装Oracle 运行时库时，不再提示 |  |  |  |  |  |
|  |  | 使用错误的用户名、密码、db名、访问地址信息 | 连通性校验不通过 |  | Pass |  |  |  |
| 历史数据迁移 | MySQL | 使用UI中的起始时间和终止时间作为时间区间：
1. 在数据源中创建包含13.1章节中所有字段的表并写入数据
2. 在TDengine中提前创建schema一致的超级表
3. 任务配置中配置sql：select * from tbname where time >= &start and time < &end，配置起始时间、终止时间有效，设置匹配的transformer规则分别配置为mapping、value、sum、format、expr规则 | TDengine中对应表写入数据且数据一致 |  | Pass |  |  |  |
|  | PostgreSQL |  |  |  | Pass | [TD-29699](https://jira.taosdata.com:18080/browse/TD-29699) |  |  |
|  | Oracle |  |  |  | Fail | [TD-30074](https://jira.taosdata.com:18080/browse/TD-30074) |  |  |
|  | MySQL | 在sql中使用数据源的不同时间字段过滤：
1. 数据源中对应字段使用带时区 RFC3339 格式时间戳，如2024-03-14T08:00:00+0800（MySQL除外）
2. 数据源中对应字段使用不带时区RFC3339 格式时间戳，如2024-03-14T08:00:00
3. 数据源中对应字段只包含日期，如2024-03-14（oracle除外）
4. 数据源中对应字段只包含时间，如08:00:00（oracle除外） | 写入TDengine中的数据均满足该过滤条件 |  | Pass | [TD-29777](https://jira.taosdata.com:18080/browse/TD-29777) |  | 只包含时间时，过滤出的结果会自动转换为utc时区 |
|  | PostgreSQL |  |  |  | Pass | [TD-29777](https://jira.taosdata.com:18080/browse/TD-29777) |  | 数据源中对应字段只包含时间 |
|  | Oracle |  |  |  | Fail | [TD-30283](https://jira.taosdata.com:18080/browse/TD-30283) |  |  |
|  | MySQL | 1. 在数据源中创建表，没有表示时间的字段
1. 在TDengine中创建超级表
2. 任务配置中配置sql：select * from tbname where time >= $start and time < $end
3. 配置有效的起始时间和终止时间
5.transformer规则中，ts列选择generator，其他列transformer规则分别配置为mapping、value、sum、format、expr规则 | TDengine中写入表的数据时间为实时生成的时间戳，其他数据写入均满足transformer规则 |  |  |  |  | 如果ts列指定为generator，写到TDengine中的数据就会因为时间戳重复而被覆盖，导致丢数据。 |
|  | PostgreSQL |  |  |  |  |  |  |  |
|  | Oracle |  |  |  |  |  |  |  |
|  | MySQL | 1. 配置起始时间和终止时间，在数据源中对应时间区间没有任何数据 | 获取的示例数据为空并提示错误 |  | Pass | [TD-29693](https://jira.taosdata.com:18080/browse/TD-29693) |  |  |
|  | PostgreSQL |  |  |  | Pass | [TD-29693](https://jira.taosdata.com:18080/browse/TD-29693) |  |  |
|  | Oracle |  |  |  | Pass |  |  |  |
|  | MySQL | 启动任务后编辑任务，修改endtime：
1. endtime < checkpoint;
2. endtime >= checkpoint | 1. 任务直接结束，无新数据迁移
3. 任务运行至endtime的数据时结束 |  | Pass |  |  |  |
|  | PostgreSQL |  |  |  | Pass |  |  |  |
|  | Oracle |  |  |  | Pass |  |  |  |
| 实时数据同步 | MySQL | 使用UI中的起始时间作为时间起点：
1. 在数据源中创建包含13.1章节中所有字段的表并写入数据
2. 在TDengine中提前创建schema一致的超级表
3. 任务配置中配置sql：select * from tbname where time >=&start and time<&end，配置起始时间有效，设置匹配的transformer规则分别配置为mapping、value、sum、format、expr规则
4.通过测试程序持续写入实时数据 | TDengine中写入表的数据最早时间满足配置的起始时间，且实时有新数据写入，任务不会自动停止 |  | Pass |  |  |  |
|  | PostgreSQL |  |  |  | Pass | [TD-29795](https://jira.taosdata.com:18080/browse/TD-29795) |  |  |
|  | Oracle |  |  |  | Pass |  |  |  |
|  | MySQL | 在sql中使用数据源的不同时间字段过滤：
1. 数据源中对应字段使用带时区 RFC3339 格式时间戳，如2024-03-14T08:00:00+0800（MySQL除外）
2. 数据源中对应字段使用不带时区RFC3339 格式时间戳，如2024-03-14T08:00:00
3. 数据源中对应字段只包含日期，如2024-03-14
4. 数据源中对应字段只包含时间，如08:00:00 | 写入TDengine中的数据均满足该过滤条件 |  | Pass |  |  |  |
|  | PostgreSQL |  |  |  | Pass |  |  |  |
|  | Oracle |  |  |  |  | [TD-30283](https://jira.taosdata.com:18080/browse/TD-30283) |  |  |
| 输入校验 | MySQL | sql语句时间起止占位符校验：
1.配置 ${start} 、${end}
2.配置 ${start_no_tz} 、${end_no_tz}
3.配置 ${start_date} 、${end_date}
4.配置${start_time} 、${end_time} | tranfsormer示例数据和TDengine写入数据中对应字段的格式与占位符规定格式一致 |  | Pass |  |  |  |
|  | PostgreSQL |  |  |  | Pass |  |  |  |
|  | Oracle |  |  |  | Pass |  |  |  |
|  | MySQL | sql语句时间起止占位符异常校验：
1.配置起止占位符格式不匹配，比如 ${start}、${end_no_tz}
2.配置起止占位符格式与源端的数据类型不一致 | 1.提示错误
1. 如果数据源精度为date（年月日），占位符精度为time（时分秒），即占位符精度更高时,提示错误
2. 如果数据源精度为time（时分秒），占位符精度为date或year，即数据源精度更高时，不提示错误 |  | Pass |  |  |  |
|  | PostgreSQL |  |  |  | Pass |  |  |  |
|  | Oracle |  |  |  | Pass |  |  |  |
|  | MySQL | sql语句异常：
1. where条件中缺少 $start 或 $end
2. sql语法错误 | 提示错误 |  | Pass |  |  |  |
|  | PostgreSQL |  |  |  | Pass |  |  |  |
|  | Oracle |  |  |  | Pass |  |  |  |
| 按日期分表 | MySQL | 1. 在数据源中构建表名中带日期的表结构，如：
create table meters_part_20240708(
  id int,
  ts time,
  current float,
  voltage int,
  phase float
);
并写入只包含时分秒时间（如“10:00:00”）的时间字符串。
1. 在sql配置中配置sql语句：
select * from (select id, concat('${F}', 'T', ts, '+08:00') as ts, current, voltage, phase from meters_${Ymd}) t where ts >= ${start_time} and ts < ${end_time};

select sint ,concat('${F}', 'T', ttnozone, '+08:00') as ts from public.pg_test_${Ymd} where ttnozone >=${start_time} and ttnozone <${end_time}

1. transformer规则使用mapping映射到超级表中 | transformer示例数据中的ts列为拼接后的时间字符串，并能够正确写入TDengine中 |  | Fail | [TD-29804](https://jira.taosdata.com:18080/browse/TD-29804)
[TD-29802](https://jira.taosdata.com:18080/browse/TD-29802) |  | 1. 截止日期如果在mysql中没有对应表，会一直panic
1. 使用agent会无法停止任务 |
|  | PostgreSQL |  |  |  | Pass | [TD-29799](https://jira.taosdata.com:18080/browse/TD-29799) |  |  |
|  | Oracle |  |  |  | Pass |  |  |  |
| 宽表列转行 | MySQL | 1.在数据源中构建字段表示时间序列的表结构，如：create table mytable(
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
|  | PostgreSQL |  |  |  |  |  |  |  |
|  | Oracle |  |  |  |  |  |  |  |
| 高级选项 | MySQL | 配置最大读并发数为最大值、最小值 | 下发任务中该参数值与设置一致 |  | Pass |  |  |  |
|  | PostgreSQL |  |  |  | Pass |  |  |  |
|  | Oracle |  |  |  | Pass |  |  |  |
|  | MySQL | 配置最大读并发数为边界外值/非法值 | 前端限制无法设置 |  | Pass |  |  |  |
|  | PostgreSQL |  |  |  | Pass |  |  |  |
|  | Oracle |  |  |  | Pass |  |  |  |
|  | MySQL | 配置批次大小为最大值、最小值 | 下发任务中该参数值与设置一致 |  | Pass |  |  |  |
|  | PostgreSQL |  |  |  | Pass |  |  |  |
|  | Oracle |  |  |  | Pass |  |  |  |
|  | MySQL | 配置批次大小为边界外值/非法值 | 前端限制无法设置 |  | Pass |  |  |  |
|  | PostgreSQL |  |  |  | Pass |  |  |  |
|  | Oracle |  |  |  | Pass |  |  |  |
|  | MySQL | 任务修改配置，修改了start_time和end_time, 使得新的时间区间不在原来的时间范围内 | 从上次结束时的时间开始重新拉取数据 |  |  |  |  |  |
|  | PostgreSQL |  |  |  |  |  |  |  |
|  | Oracle |  |  |  |  |  |  |  |
| transformer | MySQL | 为每个mapping的数据类型配置默认值，并在数据源中写入对应时间区间的null值数据 | 写入TDengine的对应列的数据应为transformer中配置的默认值 |  | Fail | [TD-29747](https://jira.taosdata.com:18080/browse/TD-29747) |  | 配置decimal类型且第一批数值为null值时，taosx会panic |
|  | PostgreSQL |  |  |  |  |  |  |  |
|  | Oracle |  |  |  |  |  |  |  |
| 异常测试 | MySQL | 任务编辑状态，尝试编辑sql语句 | 前端限制无法编辑 |  | Pass |  |  |  |
|  | PostgreSQL |  |  |  | Pass | [TD-29702](https://jira.taosdata.com:18080/browse/TD-29702) |  |  |
|  | Oracle |  |  |  | Pass |  |  |  |
|  | MySQL | 启动任务后尝试编辑sql语句 | 前端限制无法编辑 |  | Pass |  |  |  |
|  | PostgreSQL |  |  |  | Pass |  |  |  |
|  | Oracle |  |  |  | Pass |  |  |  |

### 9.2 可用性

UI 需和 FS 中描述一致。

### 9.3 可靠性

无

### 9.4 性能

无

### 9.5 安全性

无

### 9.6 兼容性

对 Mysql 的各个版本（以5,8为例）进行兼容性。

### 9.7 本地化

无

## 10. 待讨论事项

## 11. Jira

此feature相关的所有Jira, 标题中应包含统一的标签: taosx、mysql/postgresql/oracle

## 12. 测试计划 

## 13. 测试备忘

### 13.1 MySQL 数据类型映射关系对照表

| MySQL 字段类型 | Sample Data | TDengine 类型 |
| --- | --- | --- |
| TINYINT | i8 | Int8 |
| TINYINT UNSIGNED | u8 | UInt8 |
| SMALLINT | i16 | Int16 |
| SMALLINT UNSIGNED | u16 | UInt16 |
| MEDIUMINT | i32 | Int32 |
| MEDIUMINT UNSIGNED | u32 | UInt32 |
| INT | i32 | Int32 |
| INT UNSIGNED | u32 | UInt32 |
| BIGINT | i64 | Int64 |
| BIGINT UNSIGNED | u64 | UInt64 |
| FLOAT | f32 | Float32 |
| DOUBLE | f64 | Float64 |
| DECIMAL | String | NChar(50) |
| CHAR | String | NChar(50) |
| VARCHAR | String | NChar(50) |
| TINYTEXT | String | NChar(50) |
| TEXT | String | NChar(50) |
| MEDUIMTEXT | String | NChar(50) |
| LONGTEXT | String | NChar(50) |
| ~~BINARY~~ | ~~[u8]~~ | ~~NChar(50)~~ |
| ~~VARBINARY~~ | ~~[u8]~~ | ~~NChar(50)~~ |
| ~~TINYBLOB~~ | ~~[u8]~~ | ~~NChar(50)~~ |
| ~~BLOB~~ | ~~[u8]~~ | ~~NChar(50)~~ |
| ~~MEDIUMBLOB~~ | ~~[u8]~~ | ~~NChar(50)~~ |
| ~~LONGBLOB~~ | ~~[u8]~~ | ~~NChar(50)~~ |
| DATE | NaiveDate | NChar(50) |
| TIME | NaiveTime | NChar(50) |
| DATETIME | DateTime | Timestamp(TimeUnit::Nanosecond) |
| TIMESTAMP | DateTime | Timestamp(TimeUnit::Nanosecond) |
| YEAR | u16 | Int16 |
| BIT | u8 | UInt8 |

### 13.2 MySQL 中创建仅支持 ssl 登录的用户

1. 通过 root 用户访问 192.168.1.40 上的 MySQL 服务：
```shell
mysql -u root -p
```

1. 创建一个新用户并设置其登录方式仅为ssl，其中 '%' 代表可以从任何主机通过该用户进行访问：
```shell
CREATE USER 'newuser'@'%' IDENTIFIED BY 'password' REQUIRE SSL;
```

1. 配置好后，使用该用户登录时需要添加ssl-mode选项才能成功登录：
```shell
##直接使用用户名密码无法访问
root@u1-40 ~ $ mysql -u test_ssl_only -p taosdata
Enter password: 
ERROR 1045 (28000): Access denied for user 'test_ssl_only'@'localhost' (using password: YES)

##添加ssl-mode=required时，才能正常访问
root@u1-40 ~ $ mysql -u test_ssl_only -p --ssl-mode=required
Enter password: 
Welcome to the MySQL monitor.  Commands end with ; or \g.
Your MySQL connection id is 1264
Server version: 8.0.36-0ubuntu0.22.04.1 (Ubuntu)

Copyright (c) 2000, 2024, Oracle and/or its affiliates.

Oracle is a registered trademark of Oracle Corporation and/or its
affiliates. Other names may be trademarks of their respective
owners.

Type 'help;' or '\h' for help. Type '\c' to clear the current input statement.

You are enforcing ssl connection via unix socket. Please consider
switching ssl off as it does not make connection via unix socket
any more secure.
mysql> quit;
Bye

```

### 13.3 MySQL 服务配置 ssl 访问

在MySQL的配置文件（my.cnf或my.ini）中更新以下设置：
```shell
   [mysqld]
   ssl=on
   ssl-ca=/path/to/ca.pem
   ssl-cert=/path/to/server-cert.pem
   ssl-key=/path/to/server-key.pem
```

重启MySQL服务：
```shell
   service mysql restart
```


### 13.4 Oracle数据类型映射关系对照表

| Oracle字段类型 | TDengine 类型 |
| --- | --- |
| NUMBER(p,s) | varchar |
| LONG | varchar |
| FLOAT(b) | varchar |
| BINARY_FLOAT | Float32 |
| BINARY_DOUBLE | Float64 |
| NCHAR(50) | varchar |
| VARCHAR2 | varchar |
| NVARCHAR2 | varchar |
| BLOB | varchar |
| NCLOB | varchar |
| CLOB | varchar |
| DATE | varchar |
| TIMESTAMP [(fractional_seconds_precision)] | varchar |
| TIMESTAMP [(fractional_seconds_precision)] WITH TIME ZONE | Timestamp(TimeUnit::Nanosecond) |
| TIMESTAMP [(fractional_seconds_precision)] WITH LOCAL TIME ZONE | Timestamp(TimeUnit::Nanosecond) |



| Oracle字段类型 | Sample Data | TDengine 类型 |
| --- | --- | --- |
| ~~NUMBER(8)~~ | ~~i8~~ | ~~Int8~~ |
| ~~NUMBER(8)~~ | ~~u8~~ | ~~UInt8~~ |
| ~~NUMBER(16)~~ | ~~i16~~ | ~~Int16~~ |
| ~~NUMBER(16)~~ | ~~u16~~ | ~~UInt16~~ |
| ~~NUMBER(32)~~ | ~~i32~~ | ~~Int32~~ |
| ~~NUMBER(32)~~ | ~~u32~~ | ~~UInt32~~ |
| ~~NUMBER(32)~~ | ~~i32~~ | ~~Int32~~ |
| ~~NUMBER(32)~~ | ~~u32~~ | ~~UInt32~~ |
| ~~NUMBER(64)~~ | ~~i64~~ | ~~Int64~~ |
| ~~NUMBER(64)~~ | ~~u64~~ | ~~UInt64~~ |
| ~~LONG~~ | ~~i64~~ | ~~Int64~~ |
| ~~LONG~~ | ~~u64~~ | ~~Int64~~ |
| ~~FLOAT(32)~~ | ~~f32~~ | ~~Float32~~ |
| ~~FLOAT(64)~~ | ~~f64~~ | ~~Float64~~ |
| ~~BINARY_FLOAT~~ | ~~f32~~ | ~~Float32~~ |
| ~~BINARY_DOUBLE~~ | ~~f64~~ | ~~Float64~~ |
| ~~NCHAR(50)~~ | ~~String~~ | ~~NChar(50)~~ |
| ~~VARCHAR2~~ | ~~String~~ | ~~NChar(50)~~ |
| ~~NVARCHAR2~~ | ~~String~~ | ~~NChar(50)~~ |
| `~~RAW~~`~~(~~`~~*size*~~`~~)~~ | ~~[u8]~~ | ~~NChar(50)~~ |
| ~~BLOB~~ | ~~[u8]~~ | ~~NChar(50)~~ |
| ~~NCLOB~~ | ~~[u8]~~ | ~~NChar(50)~~ |
| ~~CLOB~~ | ~~[u8]~~ | ~~NChar(50)~~ |
| ~~LONG RAW~~ | ~~[u8]~~ | ~~NChar(50)~~ |
| ~~DATE~~ | ~~DateTime~~ | ~~Timestamp(TimeUnit::Nanosecond)~~ |
| ~~TIMESTAMP [(fractional_seconds_precision)]~~ | ~~TimeStamp~~ | ~~Timestamp(TimeUnit::Nanosecond)~~ |
| ~~TIMESTAMP [(fractional_seconds_precision)] WITH TIME ZONE~~ | ~~TimeStamp~~ | ~~Timestamp(TimeUnit::Nanosecond)~~ |
| ~~TIMESTAMP [(fractional_seconds_precision)] WITH LOCAL TIME ZONE~~ | ~~TimeStamp~~ | ~~Timestamp(TimeUnit::Nanosecond)~~ |

### 13.5 PostgreSQL 数据类型映射关系对照表

| PostgreSQL 字段类型 | TDengine 类型 |
| --- | --- |
| smallint | Int16 |
| interger(int) | Int32 |
| bigint | Int64 |
| decimal | VARCHAR |
| numeric(precision, scale) | VARCHAR |
| real | float |
| double precision | double |
| smallserial/serial/bigserial | \ |
| character varying(n), varchar(n) 变长，有长度限制 | VARCHAR |
| character(n), char(n) 定长，不足补空白 | VARCHAR |
| text | VARCHAR |
| timestamp [ (p) ] [ without time zone ] 日期和时间(无时区) | VARCHAR |
| timestamp [ (p) ] with time zone 日期和时间，有时区 | Timestamp |
| date 只用于日期 | VARCHAR |
| time [ (p) ] [ without time zone ] 只用于一日内时间 | VARCHAR |
| time [ (p) ] with time zone 只用于一日内时间，带时区 | VARCHAR |
| interval [ fields ] [ (p) ] 时间间隔 | \ |
| boolean | VARCHAR |
| bit(n) | VARCHAR |
| Bit varying(n) | VARCHAR |
| JSON类型 | \ |
| 二进制类型 | \ |
| 枚举类型 | \ |
| 货币类型 | \ |
| 几何类型 | \ |
| 网络地址类型 | \ |
| 文本搜索类型 | \ |
| UUID类型 | \ |
| XML类型 | \ |
| 数组 | \ |
| 复合类型 | \ |
| 范围类型 | \ |
| 对象标识符类型 | \ |
| 伪类型 | \ |

### 13.6 PostgreSQL 中创建SSL-only的用户

1. 使用postgres登录到PostgreSQL, 并创建新用户 test_ssl_only
```sql {wrap}
postgres=# create user test_ssl_only with password 'taosdata';
CREATE ROLE
postgres=# 
```

1. 编辑pg_hba.conf, 在文件中添加如下行
```go {wrap}
```conf
hostssl your_database newuser address/length md5
```

这行配置的意思是，只允许 "newuser" 通过使用md5权限验证的SSL连接来访问 "your_database" 数据库。"address/length" 应被替换为允许访问的客户端IP地址或者网段。


添加如下：
vi /etc/postgresql/14/main/pg_hba.conf
hostssl all             test_ssl_only   192.168.1.1/16          md5

```

1. 重新加载pg_hba.conf文件（不能使用root用户）
```shell {wrap}
root@u1-40 /usr/share/postgresql-common $ su - postgres
postgres@u1-40:~$ 
postgres@u1-40:~$ /usr/lib/postgresql/14/bin/pg_ctl reload -D /etc/postgresql/14/main
server signaled
postgres@u1-40:~$ 

```

1. 重新启动PostgreSQL服务
```shell {wrap}
root@u1-40 ~ $ systemctl restart postgresql
```

### 13.7 Oracle 安装 ODPI-C 库

1. 下载 ODPI-C 库
```shell {wrap}
wget https://download.oracle.com/otn_software/linux/instantclient/2340000/instantclient-basic-linux.x64-23.4.0.24.05.zip
unzip instantclient-basic-linux.x64-23.4.0.24.05.zip

```

1. 在 /etc/default/taosx 文件中增加 oracle client 路径；如果使用了agent，则需要再/etc/default/taosx-agent文件中增加对应路径。
```shell {wrap}
vim /etc/default/taosx

TAOSX_LOGS_HOME=/data/taosx/logs/
LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/data/cyjia/instantclient_23_4
~                                                                                          
~                                                                                           
~                                                                                           
~  
vim /etc/default/taosx-agent
LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/data/cyjia/instantclient_23_4                                     
```

需要特别注意的是，由于不同版本之间oracle server 支持的数据类型不一致，通过最新oracle client访问旧版本oracle server时，某些数据类型的数据可能会获取失败。因此，在部署ODPI-C库时，应保证client库和server的版本一致。

Note: 可参考 https://www.oracle.com/database/technologies/instant-client/linux-x86-64-downloads.html#ic_x64_inst 下载 ODPI-C 库

## 14. 参考文档

- [Data In：关系型数据库](https://taosdata.feishu.cn/wiki/CZYbwE0O2iPZ1mk3XndcvJDhnQe)
- https://docs.oracle.com/en/database/oracle/oracle-database/19/sqlqr/Data-Types.html#GUID-219C338B-FE60-422A-B196-2F0A01CAD9A4
