# BLOB RS

## 1. 引言

### 1.1 术语与缩写名词

| 名词 | 描述 |
| --- | --- |
| blob | Binary Large Object 用于存储大量二进制数据的数据类型，包括文档、图像、音频、视频等文件 |

### 1.2 相关文档资料

| 文档 | 连接 |
| --- | --- |
| Function Spec | [BLOB 数据类型 FS（废弃）](https://taosdata.feishu.cn/wiki/BPCJwmWDoi5aZBknjzrcR1N9ndi) [BLOB FS](https://taosdata.feishu.cn/wiki/U2F7wkwjxizN85k73AQcne8PnQb) |
| JIRA | [TS-4902](https://jira.taosdata.com:18080/browse/TS-4902) |

### 1.3 优先级要求

车联网有强烈的原始数据存储需求，但 varbinary 无法存储超过 64 KB 的记录。支持 blob 类型且提供足够的压缩比后，TDengine 可存储更长时间的原始数据，提高用户粘性，产生更大的业务价值。在 2024-01-04 的版本规划会议上，提升为高优先级。
预期在四月底的 3.2.4.0 版本发布

### 1.4 版本要求

在开源版支持。

## 2. 需求目标

用户在创建普通表或者超级表时，可以创建 blob 类型的普通数据列。blob 字段支持插入、读取、更新和删除，最大长度越大越好。 建议 SQL 语法如下。
```sql
CREATE TABLE t (
    ts TIMESTAMP, 
    data BLOB COMPRESS LZ4,
    ……
);
```

用户主要场景是向 blob 中写入超长字符串，因此用 cast 函数将其转化为字符串、~~用 to_json 函数将其转化为 ~~~~json~~~~ 类型~~是关键需求，此外最需要关注的是压缩比。

## 3. 功能需求

### 3.1 结构定义

| 需求编号 | **需求描述** | 研发确认 |
| --- | --- | --- |
| R101 | 普通表支持 blob，超级表的普通列支持 blob，标签列不支持 blob |  |
| R102 | blob 列定义时不需要指定长度，但内部支持的最大长度越大越好，至少为 1MB |  |
| R103 | blob 允许增加、删除、修改列名 |  |
| R104 | 支持多种压缩算法，本期仅需要支持 LZ4、Disabled 两种 |  |
| R105 | 在 describe table 和 show create table 语法中显示 blob 类型 |  |

### 3.2 数据写入

| 需求编号 | **需求描述** | 研发确认 |
| --- | --- | --- |
| R201 | blob 字段支持 NONE、NULL 两种取值 |  |
| R202 | 支持 STMT 绑定方式写入 |  |
| R203 | 支持 SQL 语句方式写入 - 以 "\x 开头的字符串，为十六进制表示的数据，如 VALUES (now, "\x393866343665") - 不以 "\x 开头的字符串，存储相应编码的二进制内容，如 VALUES (now, "98f46e") - 其他类型写入报错 |  |
| R204 | 支持无模式写入协议，由于 blob 和 varbinary 不易区分，所以不自动创建 blob 类型字段，但可以写入已存在的 blob 字段 |  |
| R205 | 支持来自文件的写入，例如 insert into t values(now, load_file("/path/to/your/file")); |  |
| R206 | 支持 insert into select 方式写入，当查询获取的字段元数据类型为 varchar、varbinary、blob 时才可以写入目标 blob 字段中 |  |
| R207 | 支持数据更新与删除，须有测试用例覆盖 |  |

### 3.3 数据查询

| 需求编号 | **需求描述** | 研发确认 |
| --- | --- | --- |
| R301 | 投影查询结果，blob 列以 \x 开头的十六进制形式显示到 shell 和 explorer 中 ```sql taos> insert into t values(now, "98f463"); Insert OK, 1 row(s) affected (0.002910s) taos> select * from t; ts | v | ============================================= 2024-01-10 10:18:25.630 | \x393866343633 | Query OK, 1 row(s) in set (0.003333s) taos> insert into t values(now, "\x393866343633"); Insert OK, 1 row(s) affected (0.001338s) taos> select * from t; ts | v | ============================================= 2024-01-10 10:18:25.630 | \x393866343633 | 2024-01-10 10:19:04.236 | \x393866343633 | Query OK, 2 row(s) in set (0.005155s) ``` |  |
| R302 | 运算符支持情况如下（支持的用红色标识、不支持的用删除线标识） - ~~**算术运算符**~~ - ~~**位运算符**~~ - ~~**JSON**~~~~** 运算符**~~ - **集合运算符**：UNION ALL、UNION - **比较运算符**：~~=、<>、!=、>、<、>=、<=~~、IS [NOT] NULL、[~~NOT] BETWEEN AND、IN、LIKE、MATCH, NMATCH、CONTAINS~~ - ~~**逻辑运算符**~~ |  |
| R303 | 函数支持情况如下（支持的用红色标识、不支持的用删除线标识） - **数据函数**：~~ABS、ACOS、ASIN、ATAN、CEIL、COS、FLOOR、LOG、POW、ROUND、SIN、SQRT、TAN~~ - **字符串函数**：~~CHAR_LENGTH、~~~~CONCAT~~~~、CONCAT_WS~~、LENGTH、~~LOWER、LTRIM、RTRIM、~~SUBSTR~~、UPPER~~ - **转换函数**：CAST、~~TO_ISO8601、~~~~TO_JSON~~~~、TO_UNIXTIMESTAMP、TO_CHAR、TO_TIMESTAMP~~ - **时间和日期函数**：~~TIMEDIFF、TIMETRUNCATE~~ - **聚合函数**：~~APERCENTILE、AVG~~、COUNT、~~ELAPSED、LEASTSQUARES、SPREAD、STDDEV、SUM、HYPERLOGLOG、~~~~HISTOGRAM、PERCENTILE~~ - **选择函数**：~~BOTTOM~~、FIRST、~~INTERP~~、LAST、LAST_ROW、~~MAX、MIN~~、~~MODE、SAMPLE、TAIL、TOP、UNIQUE~~ - **时序数据特有函数**：~~CSUM、DERIVATIVE、DIFF、IRATE、MAVG、STATECOUNT、STATEDURATION、TWA~~ - 不必支持 distinct 关键字 count 函数如果在设计过程中发现工作量较大，可以协商暂不开发 |  |
| R304 | 改造 cast 函数，支持数据类型间的转换 - blob -> varchar - varbinary -> varchar |  |
| R305 | 支持 concat、cast、substr、length 之间级联调用，以下为几个简单示例 ```sql create table t (ts timestmap, v varchar(12), b blob); where cast(substr(b, 0, 10) as varchar) like "type:1" select concat(v, cast(substr(b, 0, 10) as varchar)) select char_length(cast(substr(b, 0, 10) as varchar)) ``` |  |

### 3.4 其他功能

| 需求编号 | **需求描述** | 研发确认 |
| --- | --- | --- |
| R401 | 数据订阅支持 blob 字段 |  |
| R402 | taosX 进行集群间同步的时候需要考虑到 blob 字段的同步复制 |  |
| R403 | taosBenchmark 需支持 blob 列的定义，和其他类型一样可以使用样例数据 |  |
| R404 | ~~流计算可不支持 blob，但需要说明禁止的方式~~ | 否决 |
| R405 | UDF 中需要支持 blob |  |

## 4. 性能需求

| 需求编号 | **需求描述** | 研发确认 |
| --- | --- | --- |
| P101 | **创建两个超级表** ```sql create table v_stb(ts timestmap, val varchar(16384)) ……; create table b_stb (ts timestamp, val blob) ……; ``` **写入数据及其规模**：1000 个子表，每个子表写入 1 万条记录，interlace 方式写入，由于 v_stb 和 b 的结构基本相同，保证这两个超级表的每个子表中存在的真实数据一致 **查询 SQL 语句** ```sql select * from v_stb / b_stb select count(*) from v_stb / b_stb select ts, length(val) from v_stb / b_stb select substr(val, 200, 1000) from v_stb / b_stb select cast(val as varchar) from b_stb 和 select val from v_stb select last(*) from b_stb / v_stb group by tag select last(*) from b_stb / v_stb where ts <= …… ``` 在这样的场景对比下 - 写入性能下降 10% 以内 - 查询性能下降 10% 以内 - 压缩比下降 10% 以内 |  |

## 5. 其他需求

### 5.1 兼容性需求

| 需求编号 | **需求描述** | 研发确认 |
| --- | --- | --- |
| S101 | 不必支持滚动升级、向下降级，须支持向上升级。 |  |

### 5.2 接口需求

| 需求编号 | **需求描述** | 研发确认 |
| --- | --- | --- |
| S201 | 涉及各语言连接器修改，但优先级可降低，优先支持 JAVA |  |

### 5.3 测试需求

| 需求编号 | **需求描述** | 研发确认 |
| --- | --- | --- |
| S301 | 测试场景中必须设计一个包含写入、查询、订阅、UDF 的场景，可参考台网中心项目 |  |
