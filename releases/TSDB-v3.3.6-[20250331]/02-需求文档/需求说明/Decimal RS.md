# Decimal RS

## 1. 引言

### 1.1 术语与缩写名词

| 名词 | 描述 |
| --- | --- |
| DECIMAL | 1. 精度：Decimal 不是浮点数据类型，是一种用于精确表示小数的数据类型，通常用于需要高精度存储、计算的场景。 1. 性能：Decimal 是所有数值类型中最慢的，在选择数据类型之前，应权衡精度与性能的重要性。 |

### 1.2 相关文档资料

| 文档 | 链接 |
| --- | --- |
| 需求报告 | [需求报告：Decimal 数据类型](https://taosdata.feishu.cn/wiki/VjvOwzqk4iq6QOkT9K4cMkHrnZg) |

### 1.3 优先级要求

待定

### 1.4 版本要求

开源版

## 2. 需求目标

DECIMAL 类型在数据库中用于存储精确的数值，常用于经纬度、货币、表计等数据。用户在创建普通表或者超级表时，可以创建 DECIMAL 类型的普通数据列，DECIMAL 类型支持写入、读取、更新和删除，DECIMAL 类型的结构定义不支持修改。建议 SQL 语法如下。
```sql
CREATE TABLE t (
    ts TIMESTAMP, 
    data DECIMAL(P, S), 
    ……
);
```

除对 DECIMAL 类型的数据进行存储、读取之外，还需要改造运算符、函数、客户端显示、连接器等功能。

## 3. 功能需求

### 3.1 结构定义

| 需求编号 | **需求描述** | 研发确认 |
| --- | --- | --- |
| R101 | 1. 普通表：支持 DECIMAL 1. 超级表：普通列支持 DECIMAL，标签列是否支持 DECIMAL 视开发难度而定 |  |
| R102 | DECIMAL 类型定义参照 DECIMAL(P, S)，不需支持 UNSIGNED，其中 1. P 表示表示有效数字的精度，范围为 [1, 38] 1. S 表示小数点后的位数，范围是 [0, P] |  |
| R103 | DECIMAL 允许增加列、删除列、修改列名，不允许修改列的 P、S 定义 |  |
| R104 | DECIMAL 需要提供默认的压缩算法 |  |
| R105 | 在 describe table 和 show create table 语法中显示 DECIMAL 类型及 P、S 的取值 |  |

### 3.2 数据写入

| 需求编号 | **需求描述** | 研发确认 |
| --- | --- | --- |
| R201 | DECIMAL 支持 NONE、NULL 两种取值 |  |
| R202 | 支持 SQL 语句方式写入 1. 与字符串类型相似，需以单、双引号包围数值 1. 如果数值未被引号包围，精度可能丢失（视语法解析模块的具体实现而定） |  |
| R203 | 支持其他写入方式，包括 1. 支持自动建表 1. 支持 STMT 绑定方式写入 1. 支持无模式写入，不自动创建 DECIMAL 字段，但可写入已存在的 DECIMAL 字段 1. 支持来自文件的写入 1. 支持 insert into select 方式写入，当查询字段为数值类型时才可写入 DECIMAL 字段 |  |
| R204 | 支持数据更新与删除 |  |

### 3.3 溢出检查

| 需求编号 | **需求描述** | 研发确认 |
| --- | --- | --- |
| R301 | 数据写入：写入不满足 P、S 定义的数值时，需要明确报错，例如 ```sql create table t (ts timestamp, data decimal(5, 2)); insert into t values(now, '1123.45');// 报错 insert into t values(now, '1234.5'); // 报错（MySQL 提示警告） insert into t values(now, '123.456');// 报错（MySQL 提示警告） insert into t values(now, 123.456); // 可不报错，存储 123.46 ``` |  |
| R302 | 数据读取：即使创建数值时未超出有效数值范围，但随着数值在计算过程中小数位数的增多或者数值的增大，也有可能超出有效数值范围，导致溢出。 对于查询计算过程的溢出，只检测不处理，但在用户遍历结果集时，可通过如下函数随时获取是否存在溢出错误 `DLL_EXPORT int taos_errno(TAOS_RES *res);` |  |

### 3.4 数据查询

| 需求编号 | **需求描述** | 研发确认 |
| --- | --- | --- |
| R301 | DECIMAL 属于数值类型，支持数值类型的运算符也应支持 DECIMAL - **算术运算符：**+ - * / % 数值类型通过算术运算符计算后，计算结果的精度将被提升，提升方式需要在 Function Spec 中详细描述，例如： - 加法：S = max(S1, S2) - 减法：S = max(S1, S2) - 乘法：S = S1 + S2 (S1 >= S2) - 除法：S = S1 (S1 为被除数精度) - ~~**位运算符**~~ - ~~**JSON 运算符**~~ - **集合运算符**：UNION ALL、UNION - **比较运算符**：=、<>、!=、>、<、>=、<=、IS [NOT] NULL、[NOT] BETWEEN AND、IN、~~LIKE、MATCH, NMATCH、CONTAINS~~ 数值类型进行比较时，左右两侧的类型不同时，精度需要提升，例如数据列 data 定义为 decimal(5, 2)，那么对于 data 为 9.80 的数据行，data == 9.80 的比较结果为真 - ~~**逻辑运算符**~~ |  |
| R302 | DECIMAL 属于数值类型，支持数值类型的函数也应支持 DECIMAL，需明确函数的返回类型 - **数据函数**： - ABS：返回 DECIMAL，P、S 不变 - ACOS：返回 DOUBLE - ASIN：返回 DOUBLE - ATAN：返回 DOUBLE - CEIL：返回 DECIMAL，P、S 不变 - COS：返回 DOUBLE - FLOOR：返回 DECIMAL，P、S 不变 - LOG：返回 DOUBLE - POW：返回 DOUBLE - ROUND：返回 DECIMAL，P、S 不变 - SIN：返回 DOUBLE - SQRT：返回 DOUBLE - TAN：返回 DOUBLE - **字符串函数**： - ~~CHAR_LENGTH、CONCAT、CONCAT_WS、LENGTH、LOWER、LTRIM、RTRIM、SUBSTR、UPPER~~ - **转换函数**： - CAST：参数 expr 和 type_name 均可为 DECIMAL - ~~TO_ISO8601、TO_JSON、TO_UNIXTIMESTAMP、TO_CHAR、TO_TIMESTAMP~~ - **时间和日期函数**： - ~~NOW、TIMEDIFF、TIMETRUNCATE、TIMEZONE、TODAY~~ - **聚合函数**： - APERCENTILE：返回 DECIMAL，P、S 不变 - AVG：返回 DOUBLE - COUNT：返回 BIGINT - ~~ELAPSED~~ - LEASTSQUARES：返回类型和原函数定义相同 - SPREAD：返回类型为 DECIMAL，P、S 不变 - STDDEV：返回 DOUBLE - SUM：返回 DECIMAL，P 为 38，S 不变 - HYPERLOGLOG：返回 BIGINT - HISTOGRAM：返回类型和原函数定义相同 - PERCENTILE：返回类型为 DECIMAL，P、S 不变 - **选择函数**：返回 DECIMAL，P、S 不变 - BOTTOM、FIRST、INTERP、LAST、LAST_ROW、MAX、MIN、MODE、SAMPLE、TAIL、TOP、UNIQUE - **时序数据特有函数**： - CSUM：返回 DECIMAL，P 为 38，S 不变 - DERIVATIVE：返回 DOUBLE - DIFF：返回 DECIMAL，P、S 不变 - IRATE：返回 DOUBLE - MAVG：返回 DOUBLE - STATECOUNT：返回 BIGINT - STATEDURATION：返回 BIGINT - TWA：返回 DOUBLE - **支持 distinct 关键字** |  |

### 3.5 其他功能

| 需求编号 | **需求描述** | 研发确认 |
| --- | --- | --- |
| R201 | 数据订阅支持 DECIMAL（理论上透明，需测试验证） |  |
| R202 | UDF 支持 DECIMAL（理论上透明，需测试验证） |  |
| R203 | 流计算支持 DECIMAL（理论上透明，需测试验证） |  |
| R204 | taosBenchmark 支持 DECIMAL 类型定义，且和其他数据类型一样可使用样例数据 |  |

## 4. 性能需求

| 需求编号 | **需求描述** | 研发确认 |
| --- | --- | --- |
| P101 | 选取日常进行的基准性能测试场景，将其中数据类型为数值类型的数据列，修改为 DECIMAL 数据类型，例如： - bigint -> decimal(20, 0） - double -> decimal(20, 5) 数据写入、查询性能，性能衰减应在 50% 以内 |  |

## 5. 其他需求

### 5.1 接口需求

| 需求编号 | **需求描述** | 研发确认 |
| --- | --- | --- |
| S101 | 必须支持 C 连接器，查询返回 char * 类型 |  |
| S102 | 必须支持 JDBC 连接器，查询返回 BigDecimal 类型 |  |
| S103 | 优先支持 Python 连接器，查询返回 Decimal 类型 |  |
| S104 | 逐步支持其他语言的连接器，例如 1. C#（DECIMAL） 1. NODEJS（decimal.js） 1. ODBC（char *） 1. …… |  |

### 5.2 测试需求

| 需求编号 | **需求描述** | 研发确认 |
| --- | --- | --- |
| S201 | 测试用例须覆盖所有函数，重点验证各函数的返回类型 |  |
| S201 | 测试用例须覆盖精度提升场景，重点验证精度丢失情况 |  |

### 5.3 其他需求

| 需求编号 | **需求描述** | 研发确认 |
| --- | --- | --- |
| S301 | TAOS SHELL 对 DECIMAL 的显示要进行特殊处理 1. 小数点后的位数不足 S 的部分以 0 补齐 1. 屏幕显示宽度随 P 的定义而自动调整 1. 任何情况下不应以科学计数方式展示 ```sql taos> create table t(ts timestamp, data decimal(5, 2)); taos> insert into t values(now, '112.4'); taos> select * from t; ts | data | =================================== 2024-01-10 10:18:25.630 | 112.40 | Query OK, 1 row(s) in set (0.003333s) ``` |  |
| S302 | TAOS Explorer 对 DECIMAL 的显示要进行特殊处理 1. 小数点后的位数不足 S 的部分以 0 补齐 1. 任何情况下不应以科学计数方式展示 |  |
| S301 | 修订用户手册的“函数”章节，返回值说明中存在不少疏漏，需一并更新 |  |
