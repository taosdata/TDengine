---
sidebar_label: 参数限制与保留关键字 
---
# TDengine 参数限制与保留关键字

## 名称命名规则

1. 合法字符：英文字符、数字和下划线
2. 允许英文字符或下划线开头，不允许以数字开头
3. 不区分大小写
4. 转义后表（列）名规则：
   为了兼容支持更多形式的表（列）名，TDengine 引入新的转义符 "`"。可用让表名与关键词不冲突，同时不受限于上述表名称合法性约束检查。
   转义后的表（列）名同样受到长度限制要求，且长度计算的时候不计算转义符。使用转义字符以后，不再对转义字符中的内容进行大小写统一。

   例如：\`aBc\` 和 \`abc\` 是不同的表（列）名，但是 abc 和 aBc 是相同的表（列）名。
   需要注意的是转义字符中的内容必须是可打印字符。
   支持转义符的功能从 2.3.0.1 版本开始。

## 密码合法字符集

`[a-zA-Z0-9!?$%^&*()_–+={[}]:;@~#|<,>.?/]`

去掉了 `` ‘“`\ `` (单双引号、撇号、反斜杠、空格)

- 数据库名：不能包含“.”以及特殊字符，不能超过 32 个字符
- 表名：不能包含“.”以及特殊字符，与所属数据库名一起，不能超过 192 个字符，每行数据最大长度 16k 个字符
- 表的列名：不能包含特殊字符，不能超过 64 个字符
- 数据库名、表名、列名，都不能以数字开头，合法的可用字符集是“英文字符、数字和下划线”
- 表的列数：不能超过 1024 列，最少需要 2 列，第一列必须是时间戳（从 2.1.7.0 版本开始，改为最多支持 4096 列）
- 记录的最大长度：包括时间戳 8 byte，不能超过 16KB（每个 BINARY/NCHAR 类型的列还会额外占用 2 个 byte 的存储位置）
- 单条 SQL 语句默认最大字符串长度：1048576 byte，但可通过系统配置参数 maxSQLLength 修改，取值范围 65480 ~ 1048576 byte
- 数据库副本数：不能超过 3
- 用户名：不能超过 23 个 byte
- 用户密码：不能超过 15 个 byte
- 标签(Tags)数量：不能超过 128 个，可以 0 个
- 标签的总长度：不能超过 16K byte
- 记录条数：仅受存储空间限制
- 表的个数：仅受节点个数限制
- 库的个数：仅受节点个数限制
- 单个库上虚拟节点个数：不能超过 64 个
- 库的数目，超级表的数目、表的数目，系统不做限制，仅受系统资源限制
- SELECT 语句的查询结果，最多允许返回 1024 列（语句中的函数调用可能也会占用一些列空间），超限时需要显式指定较少的返回数据列，以避免语句执行报错。（从 2.1.7.0 版本开始，改为最多允许 4096 列）

## 保留关键字

目前 TDengine 有将近 200 个内部保留关键字，这些关键字无论大小写均不可以用作库名、表名、STable 名、数据列名及标签列名等。这些关键字列表如下：

| 关键字列表  |            |           |            |              |
| ----------- | ---------- | --------- | ---------- | ------------ |
| ABORT       | CREATE     | IGNORE    | NULL       | STAR         |
| ACCOUNT     | CTIME      | IMMEDIATE | OF         | STATE        |
| ACCOUNTS    | DATABASE   | IMPORT    | OFFSET     | STATEMENT    |
| ADD         | DATABASES  | IN        | OR         | STATE_WINDOW |
| AFTER       | DAYS       | INITIALLY | ORDER      | STORAGE      |
| ALL         | DBS        | INSERT    | PARTITIONS | STREAM       |
| ALTER       | DEFERRED   | INSTEAD   | PASS       | STREAMS      |
| AND         | DELIMITERS | INT       | PLUS       | STRING       |
| AS          | DESC       | INTEGER   | PPS        | SYNCDB       |
| ASC         | DESCRIBE   | INTERVAL  | PRECISION  | TABLE        |
| ATTACH      | DETACH     | INTO      | PREV       | TABLES       |
| BEFORE      | DISTINCT   | IS        | PRIVILEGE  | TAG          |
| BEGIN       | DIVIDE     | ISNULL    | QTIME      | TAGS         |
| BETWEEN     | DNODE      | JOIN      | QUERIES    | TBNAME       |
| BIGINT      | DNODES     | KEEP      | QUERY      | TIMES        |
| BINARY      | DOT        | KEY       | QUORUM     | TIMESTAMP    |
| BITAND      | DOUBLE     | KILL      | RAISE      | TINYINT      |
| BITNOT      | DROP       | LE        | REM        | TOPIC        |
| BITOR       | EACH       | LIKE      | REPLACE    | TOPICS       |
| BLOCKS      | END        | LIMIT     | REPLICA    | TRIGGER      |
| BOOL        | EQ         | LINEAR    | RESET      | TSERIES      |
| BY          | EXISTS     | LOCAL     | RESTRICT   | UMINUS       |
| CACHE       | EXPLAIN    | LP        | ROW        | UNION        |
| CACHELAST   | FAIL       | LSHIFT    | RP         | UNSIGNED     |
| CASCADE     | FILE       | LT        | RSHIFT     | UPDATE       |
| CHANGE      | FILL       | MATCH     | SCORES     | UPLUS        |
| CLUSTER     | FLOAT      | MAXROWS   | SELECT     | USE          |
| COLON       | FOR        | MINROWS   | SEMI       | USER         |
| COLUMN      | FROM       | MINUS     | SESSION    | USERS        |
| COMMA       | FSYNC      | MNODES    | SET        | USING        |
| COMP        | GE         | MODIFY    | SHOW       | VALUES       |
| COMPACT     | GLOB       | MODULES   | SLASH      | VARIABLE     |
| CONCAT      | GRANTS     | NCHAR     | SLIDING    | VARIABLES    |
| CONFLICT    | GROUP      | NE        | SLIMIT     | VGROUPS      |
| CONNECTION  | GT         | NONE      | SMALLINT   | VIEW         |
| CONNECTIONS | HAVING     | NOT       | SOFFSET    | VNODES       |
| CONNS       | ID         | NOTNULL   | STABLE     | WAL          |
| COPY        | IF         | NOW       | STABLES    | WHERE        |
