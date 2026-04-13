# TDengine 支持联邦查询 TS

# 修订记录

| 编写日期 | 发布日期 | 版本 | 修订人 | 主要修改内容 |
| --- | --- | --- | --- | --- |
| 2026-04-13 | 2026-04-13 | 0.1 | wpan | 基于 FS/DS 生成首版 Test Spec，覆盖功能与单元测试设计 |


# 测试目标

- 覆盖 FS 与 DS 定义的联邦查询全部能力：外部源管理、路径解析、概念映射、类型映射、运算符映射、函数映射、窗口、子查询、视图、下推优化、虚拟表、错误恢复。
- 验证“可下推/转换下推/本地计算/不支持”四类行为与预期一致，保证正确性优先于性能。
- 覆盖 Parser/Catalog/Planner/Executor/External Connector/Virtual Table 的全部关键逻辑与边界分支（含异常与兼容分支）。
- 对所有不支持范围进行显式校验：流计算、订阅、社区版限制、写入/DDL、跨源强一致事务等。
- 为后续自动化回归提供稳定、可扩展的全覆盖基线。

# 参考文档

- TDengine支持联邦查询FS.md
- TDengine支持联邦查询DS.md


# 测试结论

- 当前文档为测试规格与执行计划，测试结果列先标记为“待执行”。
- 功能测试设计用例：331 条。
- 单元测试设计用例：182 条。
- 合计设计用例：513 条。
- 覆盖目标：
  - 功能覆盖：FS 3.x/4/5/6/7/8/9/10/11/12 章节全覆盖。
  - 设计覆盖：DS 5.2/5.3/5.5/6.1/6.2 全量设计点覆盖。
  - 不支持范围覆盖：流计算、订阅、社区版、写入/DDL、外部对象操作、跨源强一致事务全覆盖。


# 测试环境

- OS: Linux x86_64（Ubuntu 22.04+）、Windows 10 x86_64
- TDengine: 企业版（联邦查询特性开启）
- TDengine: 社区版（用于功能限制与错误码校验）
- 外部源:
  - MySQL 8.0 / 5.7
  - PostgreSQL 14 / 12
  - InfluxDB v3.x（Flight SQL）
- 网络: 同网段低时延 + 人工注入高时延/丢包场景
- 关键配置:
  - federatedQueryEnable
  - federatedQueryConnectTimeoutMs
  - federatedQueryMetaCacheTtlSeconds
  - federatedQueryCapabilityCacheTtlSeconds
  - SOURCE OPTIONS: connect_timeout_ms/read_timeout_ms/tls_enabled/tls_ca_cert

# 功能测试

## 1 外部数据源管理

### 测试要点

- CREATE/SHOW/DESCRIBE/ALTER/DROP/REFRESH 全流程正确性。
- 密码与敏感 OPTIONS 脱敏、权限可见性控制。
- 配置动态生效与对象生命周期管理。

### 用例列表

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| FQ-EXT-001 | 创建 MySQL 外部源 | 完整参数创建，预期成功并可 SHOW 出现。 | 待执行 |
| FQ-EXT-002 | 创建 PG 外部源 | 含 DATABASE+SCHEMA，预期成功。 | 待执行 |
| FQ-EXT-003 | 创建 InfluxDB 外部源 | 使用 api_token + protocol=flight_sql。 | 待执行 |
| FQ-EXT-004 | 幂等创建 | IF NOT EXISTS 重复创建返回成功且不重复。 | 待执行 |
| FQ-EXT-005 | 重名创建失败 | 无 IF NOT EXISTS 时重复创建报错。 | 待执行 |
| FQ-EXT-006 | 与本地库重名 | source_name 与 DB 同名被拒绝。 | 待执行 |
| FQ-EXT-007 | SHOW 列表 | 返回字段完整、记录数量正确。 | 待执行 |
| FQ-EXT-008 | SHOW 脱敏 | password/options 敏感值脱敏。 | 待执行 |
| FQ-EXT-009 | DESCRIBE 定义 | 单源定义字段与创建参数一致。 | 待执行 |
| FQ-EXT-010 | ALTER 主机端口 | 修改 HOST/PORT 后连接指向新地址。 | 待执行 |
| FQ-EXT-011 | ALTER 账号口令 | 修改 USER/PASSWORD 后新连接生效。 | 待执行 |
| FQ-EXT-012 | ALTER OPTIONS 整体替换 | OPTIONS 替换后旧值失效。 | 待执行 |
| FQ-EXT-013 | ALTER TYPE 禁止 | 修改 TYPE 被拒绝。 | 待执行 |
| FQ-EXT-014 | DROP IF EXISTS | 存在时删除，不存在时不报错。 | 待执行 |
| FQ-EXT-015 | DROP 不存在 | 无 IF EXISTS 时返回对象不存在错误。 | 待执行 |
| FQ-EXT-016 | DROP 被引用对象 | 正在查询或被虚拟表引用时行为符合设计（失败或导致查询失败）。 | 待执行 |
| FQ-EXT-017 | OPTIONS 未识别 key 忽略与警告 | 创建外部源时 OPTIONS 中携带未知 key，预期创建成功，服务端日志可见 WARNING 记录，未知 key 不进入存储。 | 待执行 |
| FQ-EXT-018 | MySQL tls_enabled+ssl_mode=disabled 冲突 | tls_enabled=true 且 ssl_mode=disabled 时创建外部源返回参数冲突错误，不允许创建。 | 待执行 |
| FQ-EXT-019 | PG tls_enabled+sslmode=disable 冲突 | tls_enabled=true 且 sslmode=disable 时创建外部源返回参数冲突错误，不允许创建。 | 待执行 |
| FQ-EXT-020 | MySQL 专属选项 charset/ssl_mode 落盘与读取 | charset/ssl_mode 选项按正确类型存入 options 列，SHOW 时可见且未脱敏（非敏感项）。 | 待执行 |
| FQ-EXT-021 | PG 专属选项 sslmode/application_name/search_path 落盘 | sslmode/application_name/search_path 落盘正确，SHOW/DESCRIBE 可见。 | 待执行 |
| FQ-EXT-022 | InfluxDB 专属选项 api_token 脱敏 | api_token 在 SHOW/DESCRIBE 的 options 字段中脱敏显示，存储侧不落明文。 | 待执行 |
| FQ-EXT-023 | InfluxDB protocol 选项 flight_sql/http 切换 | protocol=flight_sql 与 protocol=http 均可创建，查询均能成功返回结果。 | 待执行 |
| FQ-EXT-024 | ALTER 后不重验证已有虚拟表 | ALTER EXTERNAL SOURCE 修改 HOST 指向不可达地址后，已有虚拟表 DDL 不报错；查询时才返回连接失败。 | 待执行 |
| FQ-EXT-025 | ALTER OPTIONS 整体替换旧选项完全清除 | OPTIONS 整体替换后，旧选项（含已移除 key）在 SHOW 中不再出现，新选项生效。 | 待执行 |
| FQ-EXT-026 | REFRESH 元数据 | 外部表结构变更后刷新可见。 | 待执行 |
| FQ-EXT-027 | REFRESH 异常源 | 外部源不可用时返回对应错误码。 | 待执行 |
| FQ-EXT-028 | 普通用户查看系统表 | user/password 列返回 NULL。 | 待执行 |
| FQ-EXT-029 | 管理员查看系统表 | password 始终显示 ******。 | 待执行 |
| FQ-EXT-030 | ALTER DATABASE 修改默认数据库 | 修改 DATABASE 后短路径查询使用新数据库，SHOW 输出一致。 | 待执行 |
| FQ-EXT-031 | ALTER SCHEMA 修改默认 schema | 修改 SCHEMA 后 PG 短路径查询使用新 schema，SHOW 输出一致。 | 待执行 |
| FQ-EXT-032 | FS 文档建源示例可运行性 | 逐条执行 FS §3.4.1 中 MySQL/PG/InfluxDB 建源示例 SQL，预期均执行成功且 SHOW 输出与示例一致。 | 待执行 |

## 2 路径解析与命名规则

### 测试要点

- 查询 FROM 路径与虚拟表列引用路径分场景解析。
- 三段式消歧规则、大小写规则、默认 database/schema 规则。

### 用例列表

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| FQ-PATH-001 | MySQL 二段式表路径 | source.table 使用默认 database。 | 待执行 |
| FQ-PATH-002 | MySQL 三段式表路径 | source.database.table 显式路径正确。 | 待执行 |
| FQ-PATH-003 | PG 二段式表路径 | source.table 使用默认 schema。 | 待执行 |
| FQ-PATH-004 | PG 三段式表路径 | source.schema.table 显式路径正确。 | 待执行 |
| FQ-PATH-005 | Influx 二段式表路径 | source.table 使用默认 database。 | 待执行 |
| FQ-PATH-006 | 缺省命名空间错误 | 未配置 default db/schema 时短路径报错。 | 待执行 |
| FQ-PATH-007 | 虚拟表内部二段列引用 | table.column 解析正确。 | 待执行 |
| FQ-PATH-008 | 虚拟表内部三段列引用 | db.table.column 解析正确。 | 待执行 |
| FQ-PATH-009 | 虚拟表外部三段列引用 | source.table.column 使用默认命名空间。 | 待执行 |
| FQ-PATH-010 | 虚拟表外部四段列引用 | source.db_or_schema.table.column 解析正确。 | 待执行 |
| FQ-PATH-011 | 三段式消歧-外部 | 首段命中 source_name，按外部路径解析。 | 待执行 |
| FQ-PATH-012 | 三段式消歧-内部 | 首段命中本地 db，按内部路径解析。 | 待执行 |
| FQ-PATH-013 | 名称冲突防止 | source 名与本地 db 名冲突创建即拦截。 | 待执行 |
| FQ-PATH-014 | MySQL 大小写规则 | 默认不区分大小写验证。 | 待执行 |
| FQ-PATH-015 | PG 大小写规则 | 未加引号折叠小写；加引号保留大小写。 | 待执行 |
| FQ-PATH-016 | 路径层级错误 | 非法段数路径返回解析错误。 | 待执行 |

## 3 概念映射与类型映射

### 测试要点

- MySQL/PG/Influx 语义映射一致。
- 精确映射、降级映射、不可映射三类覆盖。
- 时间戳主键约束与视图豁免逻辑。

### 用例列表

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| FQ-TYPE-001 | MySQL 对象映射 | database/table/view 映射符合定义。 | 待执行 |
| FQ-TYPE-002 | PG 对象映射 | database+schema 到命名空间映射正确。 | 待执行 |
| FQ-TYPE-003 | Influx 对象映射 | measurement/tag/field/tag set 映射正确。 | 待执行 |
| FQ-TYPE-004 | 视图时间戳豁免 | 无 ts 视图支持非时间线查询。 | 待执行 |
| FQ-TYPE-005 | MySQL 时间戳主键 | 存在 DATETIME/TIMESTAMP 主键时通过。 | 待执行 |
| FQ-TYPE-006 | PG 时间戳主键 | TIMESTAMP/TIMESTAMPTZ 主键通过。 | 待执行 |
| FQ-TYPE-007 | 多时间戳列选择 | 使用主键列作为 ts 对齐列。 | 待执行 |
| FQ-TYPE-008 | 无时间戳主键拦截 | 返回约束错误码。 | 待执行 |
| FQ-TYPE-009 | 精确类型映射 | INT/DOUBLE/BOOLEAN/VARCHAR 精确映射。 | 待执行 |
| FQ-TYPE-010 | DATE 降级映射 | DATE -> TIMESTAMP（零点补齐）。 | 待执行 |
| FQ-TYPE-011 | TIME 降级映射 | TIME -> BIGINT（毫秒语义）。 | 待执行 |
| FQ-TYPE-012 | JSON 普通列映射 | JSON 数据列序列化为字符串。 | 待执行 |
| FQ-TYPE-013 | JSON Tag 映射 | JSON 作为 tag 的映射行为正确。 | 待执行 |
| FQ-TYPE-014 | DECIMAL 精度截断 | precision>38 时截断并记录日志。 | 待执行 |
| FQ-TYPE-015 | UUID 映射 | uuid -> VARCHAR(36)。 | 待执行 |
| FQ-TYPE-016 | 复合类型降级 | 数组/范围/复合类型序列化为 JSON 字符串。 | 待执行 |
| FQ-TYPE-017 | 不可映射类型拒绝 | 返回 TSDB_CODE_EXT_TYPE_MISMATCH。 | 待执行 |
| FQ-TYPE-018 | 时区处理 | timestamptz 转 UTC 丢弃时区。 | 待执行 |
| FQ-TYPE-019 | NULL 处理一致性 | 三方源 NULL 到 TDengine 语义一致。 | 待执行 |
| FQ-TYPE-020 | 字符编码 | utf8mb4/UTF8 场景字符不乱码。 | 待执行 |
| FQ-TYPE-021 | 大字段边界 | 大长度字符串边界值处理正确。 | 待执行 |
| FQ-TYPE-022 | 二进制字段 | bytea/binary 映射与读取正确。 | 待执行 |
| FQ-TYPE-023 | MySQL BIT(n≤64) → BIGINT 位掩码语义丢失 | BIT(32) 列读取为 BIGINT 数值正确；位掩码约束语义丢失，日志无降级记录（精确对应）。 | 待执行 |
| FQ-TYPE-024 | MySQL BIT(n>64) → VARBINARY 位语义丢失 | BIT(128) 列读取为 VARBINARY，位语义丢失，记录降级日志。 | 待执行 |
| FQ-TYPE-025 | MySQL YEAR → SMALLINT 值域 1901~2155 | YEAR 列映射为 SMALLINT，1901 与 2155 边界值读取正确。 | 待执行 |
| FQ-TYPE-026 | MySQL LONGBLOB 超 TDengine BLOB 4MB 上限报错 | LONGBLOB 行超过 4MB 时返回错误，不截断静默写入。 | 待执行 |
| FQ-TYPE-027 | MySQL MEDIUMBLOB 超 VARBINARY 上限记录日志 | MEDIUMBLOB 超过 TDengine VARBINARY 上限（16MB）时记录日志，行为符合设计。 | 待执行 |
| FQ-TYPE-028 | PG serial/smallserial/bigserial 自增语义丢失 | serial 列读取为 INT/SMALLINT/BIGINT，数值正确；TDengine 侧无自增约束，不报错。 | 待执行 |
| FQ-TYPE-029 | PG money → DECIMAL(18,2) 货币精度 | money 列映射为 DECIMAL(18,2)，货币精度语义保留，货币符号丢失。 | 待执行 |
| FQ-TYPE-030 | PG interval → BIGINT 微秒数与降级日志 | interval 列映射为 BIGINT（午夜起微秒数），区间语义丢失，记录降级日志。 | 待执行 |
| FQ-TYPE-031 | PG hstore → VARCHAR key-value 文本形式 | hstore 列映射为 VARCHAR，值为 `"key"=>"value"` 文本，结构语义丢失。 | 待执行 |
| FQ-TYPE-032 | PG tsvector/tsquery → VARCHAR 全文索引语义丢失 | tsvector/tsquery 列映射为 VARCHAR，文本表示正确，全文索引语义丢失，记录日志。 | 待执行 |
| FQ-TYPE-033 | InfluxDB Decimal128 超 38 位 precision 截断与日志 | Decimal128 precision>38 时截断为 DECIMAL(38,s) 并记录日志，数值精度差异可接受。 | 待执行 |
| FQ-TYPE-034 | InfluxDB Duration/Interval → BIGINT 纳秒数与日志 | Duration 列映射为 BIGINT（纳秒总量），区间语义丢失，记录降级日志。 | 待执行 |
| FQ-TYPE-035 | MySQL/PG GEOMETRY/POINT 精确映射 | MySQL GEOMETRY/POINT/LINESTRING/POLYGON → TDengine GEOMETRY 精确对应，WKT 数据往返一致。 | 待执行 |
| FQ-TYPE-036 | PG PostGIS GEOMETRY → TDengine GEOMETRY（需安装 PostGIS） | PG 安装 PostGIS 扩展后地理列可读取；未安装时能力探测失败，地理函数降级本地计算。 | 待执行 |
| FQ-TYPE-037 | MySQL 整数族全量映射 | TINYINT/SMALLINT/MEDIUMINT/INT/BIGINT（含 UNSIGNED）逐项验证。 | 待执行 |
| FQ-TYPE-038 | MySQL 浮点与定点全量映射 | FLOAT/DOUBLE/DECIMAL(含精度边界)逐项验证。 | 待执行 |
| FQ-TYPE-039 | MySQL 字符串族全量映射 | CHAR/VARCHAR/TEXT 族映射与长度边界验证。 | 待执行 |
| FQ-TYPE-040 | MySQL 二进制族全量映射 | BINARY/VARBINARY/BLOB 族映射验证。 | 待执行 |
| FQ-TYPE-041 | MySQL 时间日期族全量映射 | DATE/TIME/DATETIME/TIMESTAMP/YEAR 行为验证。 | 待执行 |
| FQ-TYPE-042 | MySQL ENUM/SET/JSON 映射 | ENUM/SET/JSON 的转换或降级行为验证。 | 待执行 |
| FQ-TYPE-043 | PostgreSQL 数值族全量映射 | SMALLINT/INTEGER/BIGINT/REAL/DOUBLE/NUMERIC 全量验证。 | 待执行 |
| FQ-TYPE-044 | PostgreSQL NUMERIC 精度边界 | precision/scale 边界与截断行为验证。 | 待执行 |
| FQ-TYPE-045 | PostgreSQL 字符与文本族 | CHAR/VARCHAR/TEXT 映射一致性验证。 | 待执行 |
| FQ-TYPE-046 | PostgreSQL 时间日期族 | DATE/TIME/TIMESTAMP/TIMESTAMPTZ 全量验证。 | 待执行 |
| FQ-TYPE-047 | PostgreSQL UUID/BYTEA/BOOLEAN | 特殊类型映射与读取正确性验证。 | 待执行 |
| FQ-TYPE-048 | PostgreSQL 结构化类型降级 | ARRAY/RANGE/COMPOSITE 降级序列化验证。 | 待执行 |
| FQ-TYPE-049 | InfluxDB 标量类型全量映射 | Int/UInt/Float/Boolean/String/Timestamp 全量验证。 | 待执行 |
| FQ-TYPE-050 | InfluxDB 复杂类型降级 | List/Decimal 等复杂类型降级行为验证。 | 待执行 |
| FQ-TYPE-051 | 三源不可映射类型拒绝矩阵 | 各外部源不可映射类型统一报错验证。 | 待执行 |
| FQ-TYPE-052 | 视图列类型边界 | 视图场景类型映射与非时间线查询行为验证。 | 待执行 |
| FQ-TYPE-053 | PG xml → NCHAR 结构语义丢失 | xml 列映射为 NCHAR，XML 结构语义丢失，文本内容读取正确。 | 待执行 |
| FQ-TYPE-054 | PG inet/cidr/macaddr/macaddr8 → VARCHAR | 地址类型列映射为 VARCHAR，字面字符串读取正确，地址语义丢失。 | 待执行 |
| FQ-TYPE-055 | PG bit(n)/bit varying(n) → VARBINARY | PG 位类型列映射为 VARBINARY，位语义丢失，二进制内容读取正确。 | 待执行 |
| FQ-TYPE-056 | PG 用户自定义 ENUM → VARCHAR/NCHAR | PG 自定义枚举类型映射为 VARCHAR/NCHAR，枚举约束语义丢失。 | 待执行 |
| FQ-TYPE-057 | InfluxDB Dictionary → VARCHAR/NCHAR | Dictionary 类型映射为 VARCHAR/NCHAR，枚举约束语义丢失。 | 待执行 |
| FQ-TYPE-058 | InfluxDB Struct/Map → JSON 序列化 | Struct/Map 列映射为 NCHAR/VARCHAR，JSON 序列化存储，结构语义丢失。 | 待执行 |
| FQ-TYPE-059 | InfluxDB Date32/Date64 → TIMESTAMP 补零点 | Date 列映射为 TIMESTAMP 补零点 00:00:00，精度信息丢失，记录日志。 | 待执行 |
| FQ-TYPE-060 | InfluxDB Time32/Time64 → BIGINT | Time 列映射为 BIGINT（午夜起毫秒/微秒数），时间语义丢失，记录日志。 | 待执行 |

## 4 SQL 功能支持（算子/函数/子查询/窗口）

### 测试要点

- 覆盖 DS 5.3.3~5.3.8 各类规则。
- 区分“直接下推/转换下推/本地计算/报错”。

### 用例列表

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| FQ-SQL-001 | 基础查询 | SELECT+WHERE+ORDER+LIMIT 在外部表执行正确。 | 待执行 |
| FQ-SQL-002 | GROUP BY/HAVING | 分组与过滤结果正确。 | 待执行 |
| FQ-SQL-003 | DISTINCT | 去重语义一致。 | 待执行 |
| FQ-SQL-004 | UNION ALL 同源 | 同一外部源整体下推。 | 待执行 |
| FQ-SQL-005 | UNION 跨源 | 多源本地合并去重。 | 待执行 |
| FQ-SQL-006 | CASE 表达式 | 标准 CASE 下推并返回正确。 | 待执行 |
| FQ-SQL-007 | 算术/比较/逻辑运算符 | + - * / %、比较、AND/OR/NOT 覆盖。 | 待执行 |
| FQ-SQL-008 | REGEXP 转换(MySQL) | MATCH/NMATCH 转 MySQL REGEXP/NOT REGEXP。 | 待执行 |
| FQ-SQL-009 | REGEXP 转换(PG) | MATCH/NMATCH 转 ~ / !~。 | 待执行 |
| FQ-SQL-010 | JSON 运算转换(MySQL) | -> 转 JSON_EXTRACT 等价表达。 | 待执行 |
| FQ-SQL-011 | JSON 运算转换(PG) | -> 转 ->> 或等价表达。 | 待执行 |
| FQ-SQL-012 | CONTAINS 行为 | PG 转换下推，其它源本地计算。 | 待执行 |
| FQ-SQL-013 | 数学函数集 | ABS/ROUND/CEIL/SIN/COS 等映射。 | 待执行 |
| FQ-SQL-014 | LOG 参数顺序转换 | LOG(value, base) 与目标库参数顺序一致。 | 待执行 |
| FQ-SQL-015 | TRUNCATE/TRUNC 转换 | 各数据库函数名兼容转换。 | 待执行 |
| FQ-SQL-016 | RAND 语义 | seed/no-seed 差异处理符合预期。 | 待执行 |
| FQ-SQL-017 | 字符串函数集 | CONCAT/TRIM/REPLACE 等映射。 | 待执行 |
| FQ-SQL-018 | LENGTH 字节语义 | PG/DataFusion 使用 OCTET_LENGTH。 | 待执行 |
| FQ-SQL-019 | SUBSTRING_INDEX 处理 | PG/Influx 无等价时本地计算。 | 待执行 |
| FQ-SQL-020 | 编码函数 | TO_BASE64/FROM_BASE64 映射行为正确。 | 待执行 |
| FQ-SQL-021 | 哈希函数 | MD5/SHA2 映射与本地回退正确。 | 待执行 |
| FQ-SQL-022 | 类型转换函数 | CAST/TO_CHAR/TO_TIMESTAMP 映射正确。 | 待执行 |
| FQ-SQL-023 | 时间函数映射 | DAYOFWEEK/WEEK/TIMEDIFF 等差异处理。 | 待执行 |
| FQ-SQL-024 | 基础聚合函数 | COUNT/SUM/AVG/MIN/MAX/STDDEV/VAR。 | 待执行 |
| FQ-SQL-025 | 分位数函数 | PERCENTILE/APERCENTILE 在不同源处理正确。 | 待执行 |
| FQ-SQL-026 | 选择函数 | FIRST/LAST/TOP/BOTTOM 等本地计算正确。 | 待执行 |
| FQ-SQL-027 | LAG/LEAD | 使用 OVER(ORDER BY ts) 语义正确。 | 待执行 |
| FQ-SQL-028 | TAGS on InfluxDB | 转 DISTINCT tag 组合返回。 | 待执行 |
| FQ-SQL-029 | TAGS on MySQL/PG | 返回不支持错误。 | 待执行 |
| FQ-SQL-030 | TBNAME on MySQL/PG | 返回不支持错误。 | 待执行 |
| FQ-SQL-031 | PARTITION BY TBNAME Influx | 转为按 Tag 分组。 | 待执行 |
| FQ-SQL-032 | PARTITION BY TBNAME MySQL/PG | 报错处理正确。 | 待执行 |
| FQ-SQL-033 | INTERVAL 翻滚窗口 | 可转换下推。 | 待执行 |
| FQ-SQL-034 | 算术运算符全量 | + - * / % 及溢出/除零边界验证。 | 待执行 |
| FQ-SQL-035 | 比较运算符全量 | = != <> > < >= <= BETWEEN IN LIKE 全量验证。 | 待执行 |
| FQ-SQL-036 | 逻辑运算符全量 | AND/OR/NOT 与空值逻辑全量验证。 | 待执行 |
| FQ-SQL-037 | 位运算符全量 | & 和 \| 在 MySQL/PG 下推及 Influx 本地执行验证。 | 待执行 |
| FQ-SQL-038 | JSON 运算符全量 | -> 与 CONTAINS 在三源行为矩阵验证。 | 待执行 |
| FQ-SQL-039 | REGEXP 运算全量 | MATCH/NMATCH 到目标方言转换全量验证。 | 待执行 |
| FQ-SQL-040 | NULL 判定表达式全量 | IS NULL/IS NOT NULL/ISNULL/ISNOTNULL 转换验证。 | 待执行 |
| FQ-SQL-041 | UNION 族全量 | UNION/UNION ALL 单源下推、跨源回退全量验证。 | 待执行 |
| FQ-SQL-042 | ORDER BY NULLS 语义全量 | MySQL 转换表达式与 PG/Influx 原生语义一致性验证。 | 待执行 |
| FQ-SQL-043 | LIMIT/OFFSET 全量边界 | 大 OFFSET、PARTITION 场景、本地下推组合全量验证。 | 待执行 |
| FQ-SQL-044 | 数学函数白名单全量 | DS 数学函数清单逐项参数化验证。 | 待执行 |
| FQ-SQL-045 | 数学函数特殊映射全量 | LOG/TRUNC/RAND/MOD/GREATEST/LEAST/CORR 全量验证。 | 待执行 |
| FQ-SQL-046 | 字符串函数白名单全量 | DS 字符串函数清单逐项参数化验证。 | 待执行 |
| FQ-SQL-047 | 字符串函数特殊映射全量 | LENGTH/SUBSTRING/POSITION/FIND_IN_SET 等全量验证。 | 待执行 |
| FQ-SQL-048 | 编码函数全量 | TO_BASE64/FROM_BASE64 三源行为验证。 | 待执行 |
| FQ-SQL-049 | 哈希函数全量 | MD5/SHA1/SHA2 在三源下推或回退全量验证。 | 待执行 |
| FQ-SQL-050 | 位运算函数全量 | CRC32 等位函数支持矩阵验证。 | 待执行 |
| FQ-SQL-051 | 脱敏函数全量 | MASK_FULL/MASK_PARTIAL/MASK_NONE 本地执行验证。 | 待执行 |
| FQ-SQL-052 | 加密函数全量 | AES/SM4 函数本地执行与行为边界验证。 | 待执行 |
| FQ-SQL-053 | 类型转换函数全量 | CAST/TO_CHAR/TO_TIMESTAMP/TO_UNIXTIMESTAMP 全量验证。 | 待执行 |
| FQ-SQL-054 | 时间日期函数全量 | NOW/TODAY/DATE/DAYOFWEEK/WEEK/WEEKDAY/TIMEDIFF/TIMETRUNCATE 全量验证。 | 待执行 |
| FQ-SQL-055 | 基础聚合函数全量 | COUNT/SUM/AVG/MIN/MAX/STD/VAR 全量验证。 | 待执行 |
| FQ-SQL-056 | 分位数与近似统计全量 | PERCENTILE/APERCENTILE 等映射或回退全量验证。 | 待执行 |
| FQ-SQL-057 | 特殊聚合函数全量 | ELAPSED/HISTOGRAM/HYPERLOGLOG 本地执行验证。 | 待执行 |
| FQ-SQL-058 | 选择函数全量 | FIRST/LAST/LAST_ROW/TOP/BOTTOM/TAIL/LAG/LEAD/MODE/UNIQUE 全量验证。 | 待执行 |
| FQ-SQL-059 | 比较函数与条件函数全量 | IFNULL/COALESCE/GREATEST/LEAST 等函数矩阵验证。 | 待执行 |
| FQ-SQL-060 | 时序函数全量 | CSUM/DERIVATIVE/DIFF/IRATE/TWA 等本地执行验证。 | 待执行 |
| FQ-SQL-061 | 系统元信息函数全量 | 系统/元信息函数下推或本地策略全量验证。 | 待执行 |
| FQ-SQL-062 | 地理函数全量 | ST_* 系列函数在三源映射/回退全量验证。 | 待执行 |
| FQ-SQL-063 | UDF 全量场景 | 标量/聚合 UDF 在联邦查询中的本地执行验证。 | 待执行 |
| FQ-SQL-064 | SESSION_WINDOW 全量 | 会话窗口本地计算与边界条件验证。 | 待执行 |
| FQ-SQL-065 | EVENT_WINDOW 全量 | 事件窗口本地计算与边界条件验证。 | 待执行 |
| FQ-SQL-066 | COUNT_WINDOW 全量 | 计数窗口本地计算与边界条件验证。 | 待执行 |
| FQ-SQL-067 | 窗口伪列全量 | _wstart/_wend 生成及语义验证。 | 待执行 |
| FQ-SQL-068 | 窗口与 FILL 组合全量 | INTERVAL + FILL 多模式组合验证。 | 待执行 |
| FQ-SQL-069 | 窗口与 PARTITION 组合全量 | PARTITION+窗口语义与回退策略验证。 | 待执行 |
| FQ-SQL-070 | FROM 嵌套子查询全量 | 内层可下推/不可下推分支全量验证。 | 待执行 |
| FQ-SQL-071 | 非相关标量子查询全量 | 标量子查询在三源行为全量验证。 | 待执行 |
| FQ-SQL-072 | IN/NOT IN 子查询全量 | MySQL/PG 下推与 Influx 回退验证。 | 待执行 |
| FQ-SQL-073 | EXISTS/NOT EXISTS 子查询全量 | MySQL/PG 下推与 Influx 回退验证。 | 待执行 |
| FQ-SQL-074 | ALL/ANY/SOME 子查询全量 | MySQL/PG 下推与 Influx 回退验证。 | 待执行 |
| FQ-SQL-075 | Influx 子查询不支持矩阵 | Influx 子查询限制与错误路径全量验证。 | 待执行 |
| FQ-SQL-076 | 跨源子查询全量 | 跨源子查询本地组合执行正确性验证。 | 待执行 |
| FQ-SQL-077 | 子查询含专有函数全量 | 专有函数触发本地计算路径验证。 | 待执行 |
| FQ-SQL-078 | 视图非时间线查询全量 | 无时间线依赖语句在视图上正确执行。 | 待执行 |
| FQ-SQL-079 | 视图时间线依赖边界 | 含时间线依赖语义场景按设计处理。 | 待执行 |
| FQ-SQL-080 | 视图参与 JOIN/GROUP/ORDER | 视图参与复合查询场景验证。 | 待执行 |
| FQ-SQL-081 | 视图结构变更与 REFRESH | 视图 schema 变化后刷新与查询行为验证。 | 待执行 |
| FQ-SQL-082 | TO_JSON 转换下推 | MySQL 转 CAST(str AS JSON)，PG 转 str::json，InfluxDB 本地计算，结果正确。 | 待执行 |
| FQ-SQL-083 | 比较函数 IF/NVL2/IFNULL/NULLIF 三源转换下推 | IF 转 CASE WHEN（PG/Influx）、NVL2 转 CASE WHEN、IFNULL 转 COALESCE，结果一致。 | 待执行 |
| FQ-SQL-084 | 除以零行为差异 MySQL NULL vs PG 报错 | MySQL 除以零返回 NULL，PG 报错，下推后行为与目标库一致。 | 待执行 |
| FQ-SQL-085 | InfluxDB PARTITION BY tag_col → GROUP BY tag_col | 单个 Tag 列分组直接转换为 GROUP BY tag_col 下推，结果正确。 | 待执行 |
| FQ-SQL-086 | FS/DS 查询示例可运行性 | 逐条执行 FS §3.4、DS §5.3 中涉及 SELECT/JOIN/GROUP BY/窗口的示例 SQL，预期均返回正确结果，无语法错误。 | 待执行 |

## 5 不支持项与本地计算项

### 测试要点

- 明确“不可下推但可执行”与“完全不支持”边界。

### 用例列表

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| FQ-LOCAL-001 | STATE_WINDOW | 本地计算路径正确。 | 待执行 |
| FQ-LOCAL-002 | INTERVAL 滑动窗口 | 本地计算路径正确。 | 待执行 |
| FQ-LOCAL-003 | FILL 子句 | 本地填充语义正确。 | 待执行 |
| FQ-LOCAL-004 | INTERP 子句 | 本地插值语义正确。 | 待执行 |
| FQ-LOCAL-005 | SLIMIT/SOFFSET | 本地分片级截断语义正确。 | 待执行 |
| FQ-LOCAL-006 | UDF | 不下推，TDengine 本地执行。 | 待执行 |
| FQ-LOCAL-007 | Semi/Anti Join(MySQL/PG) | 子查询转换后执行正确。 | 待执行 |
| FQ-LOCAL-008 | Semi/Anti Join(Influx) | 不支持转换时本地执行。 | 待执行 |
| FQ-LOCAL-009 | EXISTS/IN 子查询 | 各源按能力下推或本地回退。 | 待执行 |
| FQ-LOCAL-010 | ALL/ANY/SOME on Influx | 本地计算路径正确。 | 待执行 |
| FQ-LOCAL-011 | CASE 表达式含不可映射子表达式整体本地计算 | CASE WHEN 中某分支引用不可映射函数时，整个 CASE 表达式不下推，走本地计算路径，结果正确。 | 待执行 |
| FQ-LOCAL-012 | SPREAD 函数三源 MAX-MIN 表达式替代验证 | SPREAD(col) 在 MySQL/PG 转换为 MAX(col)-MIN(col) 下推；在 InfluxDB 同样替代，结果与本地计算一致。 | 待执行 |
| FQ-LOCAL-013 | GROUP_CONCAT(MySQL)/STRING_AGG(PG/InfluxDB) 转换 | MySQL 下推 GROUP_CONCAT；PG/InfluxDB 转换为 STRING_AGG，分隔符参数正确映射，结果一致。 | 待执行 |
| FQ-LOCAL-014 | LEASTSQUARES 本地计算路径验证 | LEASTSQUARES(col, start, step) 在三源均走本地计算路径，拉取原始数据后本地求解，结果正确。 | 待执行 |
| FQ-LOCAL-015 | LIKE_IN_SET/REGEXP_IN_SET 本地计算 | TDengine 专有函数 LIKE_IN_SET/REGEXP_IN_SET 在三源均不下推，本地执行结果正确。 | 待执行 |
| FQ-LOCAL-016 | FILL SURROUND 子句不影响下推行为 | FILL(PREV) + SURROUND 子句时，下推部分（WHERE 过滤/列裁剪）不受 SURROUND 影响，填充语义在本地正确执行。 | 待执行 |
| FQ-LOCAL-017 | INTERP 查询时间范围 WHERE 条件下推 | INTERP + RANGE 时，时间范围部分转换为 WHERE ts BETWEEN 下推，减少拉取数据量；本地插值结果正确。 | 待执行 |
| FQ-LOCAL-018 | JOIN ON 条件含 TBNAME 时 Parser 报错 | 外部表 JOIN 的 ON 子句中引用 TBNAME 伪列，Parser 阶段返回 TSDB_CODE_EXT_SYNTAX_UNSUPPORTED。 | 待执行 |
| FQ-LOCAL-019 | MySQL 同源跨库 JOIN 可下推 | mysql_src.db1.t1 JOIN mysql_src.db2.t2（同一外部源，不同 database）可整体下推，结果正确。 | 待执行 |
| FQ-LOCAL-020 | PG/InfluxDB 跨库 JOIN 不可下推本地执行 | pg_src.db1.t1 JOIN pg_src.db2.t2（同外部源，不同 database）不可下推，分别拉取后本地 JOIN，结果正确。 | 待执行 |
| FQ-LOCAL-021 | InfluxDB IN(subquery) 改写为常量列表 | InfluxDB 不支持 IN(subquery) 时，系统先执行子查询获取结果集，改写为 IN(v1,v2,...) 后下推，结果集过大时走本地计算。 | 待执行 |
| FQ-LOCAL-022 | 流计算中联邦查询拒绝 | 流计算语境中联邦语句报错与错误码稳定。 | 待执行 |
| FQ-LOCAL-023 | 订阅中联邦查询拒绝 | 订阅语境中联邦语句报错与错误码稳定。 | 待执行 |
| FQ-LOCAL-024 | 外部写入 INSERT 拒绝 | 外部表 INSERT 被拒绝并返回预期错误码。 | 待执行 |
| FQ-LOCAL-025 | 外部写入 UPDATE 拒绝 | 外部表 UPDATE 被拒绝并返回预期错误码。 | 待执行 |
| FQ-LOCAL-026 | 外部写入 DELETE 拒绝 | 外部表 DELETE 被拒绝并返回预期错误码。 | 待执行 |
| FQ-LOCAL-027 | 外部对象操作拒绝 | 索引/触发器/存储过程相关操作拒绝验证。 | 待执行 |
| FQ-LOCAL-028 | 跨源强一致事务限制 | 跨源事务语义不支持边界验证。 | 待执行 |
| FQ-LOCAL-029 | 社区版联邦查询限制 | 社区版执行联邦查询语句受限行为验证。 | 待执行 |
| FQ-LOCAL-030 | 社区版外部源 DDL 限制 | 社区版 CREATE/ALTER/DROP EXTERNAL SOURCE 限制验证。 | 待执行 |
| FQ-LOCAL-031 | 版本能力提示一致性 | 社区版/企业版差异提示、错误码与文案验证。 | 待执行 |
| FQ-LOCAL-032 | tdengine 外部源预留行为 | TYPE='tdengine' 预留项不交付边界验证。 | 待执行 |
| FQ-LOCAL-033 | 版本支持矩阵限制 | 外部数据库版本不在支持矩阵时行为验证。 | 待执行 |
| FQ-LOCAL-034 | 不支持语句错误码稳定 | 流计算/订阅/写入等不支持语句错误码稳定性验证。 | 待执行 |
| FQ-LOCAL-035 | Hints 不下推全量 | Hints 在远端剥离、本地生效验证。 | 待执行 |
| FQ-LOCAL-036 | 伪列限制全量 | TBNAME/TAGS 及其它伪列边界验证。 | 待执行 |
| FQ-LOCAL-037 | TAGS 语义差异验证 | Influx 无数据 tag set 不返回的差异验证。 | 待执行 |
| FQ-LOCAL-038 | MySQL FULL OUTER JOIN 路径 | 改写或本地回退路径结果一致性验证。 | 待执行 |
| FQ-LOCAL-039 | ASOF/WINDOW JOIN 路径 | 专有 JOIN 本地执行一致性验证。 | 待执行 |
| FQ-LOCAL-040 | 伪列 _ROWTS/_c0 联邦查询中本地映射 | 外部表查询中引用 _ROWTS/_c0 伪列时，本地映射到时间戳列，结果正确。 | 待执行 |
| FQ-LOCAL-041 | 伪列 _QSTART/_QEND 本地计算 | _QSTART/_QEND 伪列不下推，由 Planner 从 WHERE 条件解析生成，值正确。 | 待执行 |
| FQ-LOCAL-042 | 伪列 _IROWTS/_IROWTS_ORIGIN 本地计算 | INTERP 配套伪列不下推，本地插值时生成，值正确。 | 待执行 |
| FQ-LOCAL-043 | TO_ISO8601/TIMEZONE() 本地计算 | TDengine 专有函数 TO_ISO8601 和 TIMEZONE() 在三源均本地计算，结果正确。 | 待执行 |
| FQ-LOCAL-044 | COLS()/UNIQUE()/SAMPLE() 本地计算 | TDengine 专有选择函数在三源均本地计算，语义正确。 | 待执行 |
| FQ-LOCAL-045 | FILL_FORWARD/MAVG/STATECOUNT/STATEDURATION 本地计算 | 时序函数在三源均本地计算，拉取原始数据后本地执行，结果正确。 | 待执行 |

## 6 下推优化与兜底恢复

### 测试要点

- 覆盖 DS 5.3.10 的 8 条联邦规则与执行顺序。
- 验证 pRemotePlan 逐步构建、pushdown_flags、失败恢复。

### 用例列表

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| FQ-PUSH-001 | 全能力关闭 | 能力位全 false 走零下推路径。 | 待执行 |
| FQ-PUSH-002 | 条件全可映射 | FederatedCondPushdown 全量下推。 | 待执行 |
| FQ-PUSH-003 | 条件部分可映射 | 可下推条件下推，不可下推本地保留。 | 待执行 |
| FQ-PUSH-004 | 条件不可映射 | 全部本地过滤。 | 待执行 |
| FQ-PUSH-005 | 聚合可下推 | Agg+Group Key 全可映射时下推。 | 待执行 |
| FQ-PUSH-006 | 聚合不可下推 | 任一函数不可映射则聚合整体本地。 | 待执行 |
| FQ-PUSH-007 | 排序可下推 | ORDER BY 可映射，MySQL NULLS 规则改写正确。 | 待执行 |
| FQ-PUSH-008 | 排序不可下推 | 排序表达式不可映射时本地排序。 | 待执行 |
| FQ-PUSH-009 | LIMIT 可下推 | 无 partition 且依赖前置满足。 | 待执行 |
| FQ-PUSH-010 | LIMIT 不可下推 | PARTITION 或本地 Agg/Sort 时本地 LIMIT。 | 待执行 |
| FQ-PUSH-011 | Partition 转换 | PARTITION BY 列转换到 GROUP BY。 | 待执行 |
| FQ-PUSH-012 | Window 转换 | 翻滚窗口转等效 GROUP BY 表达式。 | 待执行 |
| FQ-PUSH-013 | 同源 JOIN 下推 | 同 source（及库约束）可下推。 | 待执行 |
| FQ-PUSH-014 | 跨源 JOIN 回退 | 保留本地 JOIN。 | 待执行 |
| FQ-PUSH-015 | 子查询递归下推 | 内外层可映射场景合并下推。 | 待执行 |
| FQ-PUSH-016 | 子查询部分下推 | 仅内层下推，外层本地执行。 | 待执行 |
| FQ-PUSH-017 | pRemotePlan 构建顺序 | Filter->Agg->Sort->Limit 节点顺序正确。 | 待执行 |
| FQ-PUSH-018 | pushdown_flags 编码 | 位掩码与实际下推内容一致。 | 待执行 |
| FQ-PUSH-019 | 下推失败语法类 | 产生 TSDB_CODE_EXT_PUSHDOWN_FAILED。 | 待执行 |
| FQ-PUSH-020 | 客户端禁用下推重规划 | 重规划后零下推结果正确。 | 待执行 |
| FQ-PUSH-021 | 连接错误重试 | Scheduler 按可重试语义重试。 | 待执行 |
| FQ-PUSH-022 | 认证错误不重试 | 置 unavailable 并快速失败。 | 待执行 |
| FQ-PUSH-023 | 资源限制退避 | degraded + backoff 行为正确。 | 待执行 |
| FQ-PUSH-024 | 可用性状态流转 | available/degraded/unavailable 切换正确。 | 待执行 |
| FQ-PUSH-025 | 诊断日志完整性 | 原 SQL/远端 SQL/远端错误/pushdown_flags 记录完整。 | 待执行 |
| FQ-PUSH-026 | 三路径正确性一致 | 全下推/部分下推/零下推结果一致。 | 待执行 |
| FQ-PUSH-027 | PG FDW 外部表映射为普通表查询 | PG 中通过 FDW 定义的外部表通过联邦查询可正常读取，映射语义与普通表一致。 | 待执行 |
| FQ-PUSH-028 | PG 继承表映射为独立普通表 | PG 继承子表通过联邦查询独立读取，继承关系不影响映射结果，按独立普通表处理。 | 待执行 |
| FQ-PUSH-029 | InfluxDB 标识符大小写区分 | InfluxDB measurement/tag/field 名称区分大小写，大小写不同的名称被视为不同标识符，查询结果正确。 | 待执行 |
| FQ-PUSH-030 | 多节点环境外部连接器版本检查 | 集群启动时校验各节点连接器版本一致性；版本不一致时启动校验报错或记录告警。 | 待执行 |
| FQ-PUSH-031 | 下推执行失败诊断日志完整性 | 下推失败时服务端日志包含：原始 SQL、远端 SQL、远端错误信息（remote_code/message）、pushdown_flags，字段均不缺失。 | 待执行 |
| FQ-PUSH-032 | 客户端重规划禁用下推结果一致性 | 收到 TSDB_CODE_EXT_PUSHDOWN_FAILED 后客户端发起零下推重规划，最终查询结果与部分下推路径一致。 | 待执行 |
| FQ-PUSH-033 | Full Outer JOIN PG/InfluxDB 直接下推 | PG/InfluxDB 单源场景 FULL OUTER JOIN 直接下推，结果与本地执行一致。 | 待执行 |
| FQ-PUSH-034 | 联邦规则列表独立性验证 | 含外部扫描节点时使用联邦规则列表，纯本地查询仍使用原 31 条规则，互不影响。 | 待执行 |
| FQ-PUSH-035 | 通用结构优化规则在联邦计划中生效 | MergeProjects/EliminateProject/EliminateSetOperator 等通用规则在联邦计划中正常执行，本地算子链结构优化正确。 | 待执行 |

## 7 虚拟表外部列引用

### 测试要点

- 覆盖 DS 5.5 的结构、DDL 校验、执行路径、计划拆分与动态参数注入。

### 用例列表

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| FQ-VTBL-001 | 创建虚拟普通表(混合列) | 内部列+外部列 DDL 成功。 | 待执行 |
| FQ-VTBL-002 | 创建虚拟子表(混合列) | USING 稳定表 + 外部列引用成功。 | 待执行 |
| FQ-VTBL-003 | 虚拟超级表多子表多源 | 子表可引用不同 external source。 | 待执行 |
| FQ-VTBL-004 | 必须归属内部库 | 未 USE/CREATE 本地库时创建失败。 | 待执行 |
| FQ-VTBL-005 | 全外部列虚拟表 | 全部列外部引用可创建。 | 待执行 |
| FQ-VTBL-006 | 外部源不存在 | DDL 报外部源不存在错误。 | 待执行 |
| FQ-VTBL-007 | 外部表不存在 | DDL 报表不存在错误。 | 待执行 |
| FQ-VTBL-008 | 外部列不存在 | DDL 报列不存在错误。 | 待执行 |
| FQ-VTBL-009 | 外部类型不兼容 | DDL 报类型不匹配错误。 | 待执行 |
| FQ-VTBL-010 | 无时间戳主键 | DDL 报约束错误。 | 待执行 |
| FQ-VTBL-011 | 视图豁免 | 视图无 ts key 允许创建（按约束边界）。 | 待执行 |
| FQ-VTBL-012 | 虚拟表基础查询 | 投影与过滤正确。 | 待执行 |
| FQ-VTBL-013 | 虚拟表聚合查询 | GROUP BY owner 等聚合正确。 | 待执行 |
| FQ-VTBL-014 | 虚拟表窗口查询 | INTERVAL 查询结果正确。 | 待执行 |
| FQ-VTBL-015 | 虚拟表 JOIN 本地表 | 结果正确且计划合理。 | 待执行 |
| FQ-VTBL-016 | 虚拟表 JOIN 外部维表 | 结果正确。 | 待执行 |
| FQ-VTBL-017 | 外部列缓存命中 | TTL 内命中缓存。 | 待执行 |
| FQ-VTBL-018 | 外部列缓存失效 | TTL 到期后重拉 schema。 | 待执行 |
| FQ-VTBL-019 | REFRESH 触发缓存失效 | 手动刷新后重新加载。 | 待执行 |
| FQ-VTBL-020 | 子表切换重建连接 | source 变化时 Connector 重新初始化。 | 待执行 |
| FQ-VTBL-021 | 虚拟超级表串行处理 | 多子表逐个处理结果正确。 | 待执行 |
| FQ-VTBL-022 | 多源 ts 归并排序 | SORT_MULTISOURCE_TS_MERGE 对齐正确。 | 待执行 |
| FQ-VTBL-023 | Plan Splitter 行为 | 外部扫描不拆分，内部扫描经 Exchange。 | 待执行 |
| FQ-VTBL-024 | 删除被引用源后查询 | 行为符合约束（失败/中断）。 | 待执行 |
| FQ-VTBL-025 | CREATE STABLE ... VIRTUAL 1 语法正确性 | 使用 VIRTUAL 1 标志创建虚拟超级表成功，可在其上创建引用外部列的子表。 | 待执行 |
| FQ-VTBL-026 | 虚拟表 DDL 外部源不存在返回 TSDB_CODE_FOREIGN_SERVER_NOT_EXIST | 列引用指向未注册的 source_name 时，DDL 返回 TSDB_CODE_FOREIGN_SERVER_NOT_EXIST，含源名信息。 | 待执行 |
| FQ-VTBL-027 | 虚拟表 DDL 外部 database 不存在返回 TSDB_CODE_FOREIGN_DB_NOT_EXIST | 四段式路径中 database 不存在时返回 TSDB_CODE_FOREIGN_DB_NOT_EXIST。 | 待执行 |
| FQ-VTBL-028 | 虚拟表 DDL 外部表不存在返回 TSDB_CODE_FOREIGN_TABLE_NOT_EXIST | 外部表名拼错时返回 TSDB_CODE_FOREIGN_TABLE_NOT_EXIST。 | 待执行 |
| FQ-VTBL-029 | 虚拟表 DDL 外部列不存在返回 TSDB_CODE_FOREIGN_COLUMN_NOT_EXIST | 外部列名拼错时返回 TSDB_CODE_FOREIGN_COLUMN_NOT_EXIST，含列名信息。 | 待执行 |
| FQ-VTBL-030 | 虚拟表 DDL 类型不兼容返回 TSDB_CODE_FOREIGN_TYPE_MISMATCH | 虚拟表声明类型与外部列映射结果不兼容时返回 TSDB_CODE_FOREIGN_TYPE_MISMATCH，错误信息含源类型与目标类型。 | 待执行 |
| FQ-VTBL-031 | 虚拟表 DDL 无时间戳主键返回 TSDB_CODE_FOREIGN_NO_TS_KEY | 外部表无可映射为 TIMESTAMP 的主键列时返回 TSDB_CODE_FOREIGN_NO_TS_KEY。 | 待执行 |

## 8 系统表、配置、可观测性

### 测试要点

- 覆盖系统表、SHOW/DESCRIBE 改写、动态配置生效与观测指标。

### 用例列表

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| FQ-SYS-001 | SHOW 改写 | SHOW EXTERNAL SOURCES 改写到 ins_ext_sources。 | 待执行 |
| FQ-SYS-002 | DESCRIBE 改写 | DESCRIBE EXTERNAL SOURCE 改写 WHERE source_name。 | 待执行 |
| FQ-SYS-003 | 系统表列定义 | ins_ext_sources 列类型/长度/顺序正确。 | 待执行 |
| FQ-SYS-004 | 表级权限 | 普通用户可查询基础列。 | 待执行 |
| FQ-SYS-005 | sysInfo 列保护 | 非管理员 user/password 为 NULL。 | 待执行 |
| FQ-SYS-006 | ConnectTimeout 动态生效 | 修改后新查询按新超时执行。 | 待执行 |
| FQ-SYS-007 | MetaCacheTTL 生效 | 缓存命中/过期行为与 TTL 一致。 | 待执行 |
| FQ-SYS-008 | CapabilityCacheTTL 生效 | 能力缓存过期后重算。 | 待执行 |
| FQ-SYS-009 | OPTIONS 覆盖全局参数 | 每源 connect/read timeout 覆盖全局。 | 待执行 |
| FQ-SYS-010 | TLS 参数落盘与脱敏 | tls 证书参数可用且展示脱敏。 | 待执行 |
| FQ-SYS-011 | 外部请求指标 | 请求次数/失败率/超时率可观测。 | 待执行 |
| FQ-SYS-012 | 下推命中指标 | 下推命中率/回退率可观测。 | 待执行 |
| FQ-SYS-013 | 缓存指标 | 元数据/能力缓存命中率可观测。 | 待执行 |
| FQ-SYS-014 | 链路日志串联 | 解析-规划-执行-连接器日志可串联。 | 待执行 |
| FQ-SYS-015 | 健康状态展示 | 最近错误与 source 健康状态可见。 | 待执行 |
| FQ-SYS-016 | 默认关闭兼容 | feature 关闭时本地行为无回归。 | 待执行 |
| FQ-SYS-017 | SHOW 输出 options 字段 JSON 格式与敏感脱敏 | SHOW EXTERNAL SOURCES 的 options 列以 JSON 格式展示，tls_client_key/api_token 等敏感值脱敏。 | 待执行 |
| FQ-SYS-018 | SHOW 输出 create_time 字段正确 | create_time 字段类型为 TIMESTAMP，值与创建时刻一致，精度到毫秒。 | 待执行 |
| FQ-SYS-019 | DESCRIBE 与 SHOW 输出字段一致性 | DESCRIBE EXTERNAL SOURCE name 的所有字段与 SHOW EXTERNAL SOURCES 中对应行完全一致。 | 待执行 |
| FQ-SYS-020 | ins_ext_sources 系统表 options 列 JSON 格式 | 直接查询 information_schema.ins_ext_sources 的 options 列，返回有效 JSON 字符串，敏感值已脱敏。 | 待执行 |
| FQ-SYS-021 | federatedQueryConnectTimeoutMs 最小值 100ms 生效 | 设置为 100 时新查询按 100ms 超时，超时场景正常触发错误码。 | 待执行 |
| FQ-SYS-022 | federatedQueryConnectTimeoutMs 低于最小值 99 时被拒绝 | 动态修改为 99 返回参数越界错误，配置保持原值不变。 | 待执行 |
| FQ-SYS-023 | federatedQueryMetaCacheTtlSeconds 最大值 86400 生效 | 设置为 86400 时缓存超时逻辑正确，超限值 86401 被拒绝。 | 待执行 |
| FQ-SYS-024 | federatedQueryEnable 两端参数：仅服务端开启时客户端拒绝 | 服务端开启但客户端未开启时，联邦查询语句返回功能未启用提示。 | 待执行 |
| FQ-SYS-025 | federatedQueryConnectTimeoutMs 仅服务端参数 | 在客户端配置文件修改该参数对服务端行为无影响，服务端值仍按自身配置执行。 | 待执行 |
| FQ-SYS-026 | 升级降级零数据限制 | 无新数据时降级可用性验证。 | 待执行 |
| FQ-SYS-027 | 升级降级有联邦数据限制 | 已配置外部源与相关对象时升级降级边界验证。 | 待执行 |
| FQ-SYS-028 | read_timeout_ms/connect_timeout_ms 每源 OPTIONS 覆盖全局 | 每源 OPTIONS 设置的 read_timeout_ms/connect_timeout_ms 覆盖全局配置，超时行为符合每源值。 | 待执行 |

## 9 单元测试用例（核心模块）

### 测试要点

- 覆盖模块级全部关键逻辑、边界分支、异常分支与错误码分支。
- 覆盖类型/运算符/函数/窗口/子查询/视图/不支持范围的判定与执行链路。
- 不依赖真实外部数据库的场景优先 mock 化，保证稳定与速度；需真实源验证的场景保留集成用例。

### 用例列表

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| UT-001 | Parser-外部源 DDL AST 解析 | CREATE/ALTER/DROP/SHOW/DESCRIBE/REFRESH 解析节点正确。 | 待执行 |
| UT-002 | Parser-查询路径解析 | source.table/source.db.table 路径解析正确。 | 待执行 |
| UT-003 | Parser-虚拟表列引用解析 | table.col/db.table.col/source.table.col/source.db.table.col。 | 待执行 |
| UT-004 | Parser-三段式消歧 | 首段按 source/db 命中规则分流。 | 待执行 |
| UT-005 | Parser-源不存在拦截 | 返回 TSDB_CODE_EXT_SOURCE_NOT_FOUND。 | 待执行 |
| UT-006 | Parser-源不可用拦截 | 返回 TSDB_CODE_EXT_SOURCE_UNAVAILABLE。 | 待执行 |
| UT-007 | Parser-不可映射类型拦截 | 返回 TSDB_CODE_EXT_TYPE_MISMATCH。 | 待执行 |
| UT-008 | Parser-TBNAME 拦截 | MySQL/PG 外部表 TBNAME 报语法不支持。 | 待执行 |
| UT-009 | Parser-TAGS 拦截 | MySQL/PG TAGS 报语法不支持。 | 待执行 |
| UT-010 | Parser-外部写操作拦截 | 外部表写入/DDL 直接拒绝。 | 待执行 |
| UT-011 | Parser-DDL 外部对象不存在 | foreign db/table/column 缺失错误码正确。 | 待执行 |
| UT-012 | Parser-DDL 类型兼容校验 | foreign type mismatch 分支正确。 | 待执行 |
| UT-013 | Parser-DDL ts key 校验 | 无时间戳主键报错，视图豁免。 | 待执行 |
| UT-014 | Parser-标识符大小写规则 | MySQL/PG/Influx 命名规则处理正确。 | 待执行 |
| UT-015 | Parser-OPTIONS 解析 | 字符串参数转换与保存正确。 | 待执行 |
| UT-016 | Parser-错误码稳定性 | 非法语义统一落到指定错误码族。 | 待执行 |
| UT-017 | Catalog-source 缓存增删改查 | 按 source_name 的 CRUD 正确。 | 待执行 |
| UT-018 | Catalog-层级元数据缓存 | source->db->schema->table 层级索引正确。 | 待执行 |
| UT-019 | Catalog-meta TTL | 命中与过期重载逻辑正确。 | 待执行 |
| UT-020 | Catalog-capability TTL | 能力缓存独立过期逻辑正确。 | 待执行 |
| UT-021 | Catalog-refresh 失效策略 | REFRESH 后缓存强制失效。 | 待执行 |
| UT-022 | Catalog-version 比较更新 | 版本变化触发替换，无变化不替换。 | 待执行 |
| UT-023 | Catalog-可用性状态流转 | available/degraded/unavailable 转移正确。 | 待执行 |
| UT-024 | Catalog-运行时错误不改能力位 | 仅更新 availability。 | 待执行 |
| UT-025 | Catalog-并发读写安全 | 并发查询与更新无竞态破坏。 | 待执行 |
| UT-026 | Catalog-敏感字段保护 | 密码/证书不明文输出。 | 待执行 |
| UT-027 | Catalog-sysInfo 列可见性 | 非管理员列返回 NULL。 | 待执行 |
| UT-028 | Catalog-source 名唯一性 | 冲突创建拦截。 | 待执行 |
| UT-029 | Catalog-删除源清理资源 | 删除后相关缓存清理完整。 | 待执行 |
| UT-030 | Catalog-能力字段完整性 | 五个 ext_can_pushdown_* 字段完整。 | 待执行 |
| UT-031 | Planner-规则列表切换 | 含外部扫描时使用联邦规则列表。 | 待执行 |
| UT-032 | Planner-CondPushdown 全量 | 条件全可映射时下推。 | 待执行 |
| UT-033 | Planner-CondPushdown 部分 | 可下推/本地残留分裂正确。 | 待执行 |
| UT-034 | Planner-CondPushdown 零下推 | 能力关闭或不可映射时保留本地。 | 待执行 |
| UT-035 | Planner-AggPushdown 可下推 | 函数与 group key 全可映射。 | 待执行 |
| UT-036 | Planner-AggPushdown 不可下推 | 任一不可映射时 Agg 保留本地。 | 待执行 |
| UT-037 | Planner-OrderPushdown | 可映射排序下推。 | 待执行 |
| UT-038 | Planner-MySQL NULLS 改写 | NULLS FIRST/LAST 转换表达式正确。 | 待执行 |
| UT-039 | Planner-LimitPushdown 前置条件 | partition/本地 agg/sort 分支覆盖。 | 待执行 |
| UT-040 | Planner-PartitionConvert 普通列 | PARTITION BY 合并到 GROUP BY。 | 待执行 |
| UT-041 | Planner-PartitionConvert TBNAME | Influx 转 tags；MySQL/PG 报错。 | 待执行 |
| UT-042 | Planner-WindowConvert 翻滚 | INTERVAL 转等价 GROUP BY 表达式。 | 待执行 |
| UT-043 | Planner-WindowConvert 非翻滚 | 滑动/状态/会话/事件/计数窗口保留本地。 | 待执行 |
| UT-044 | Planner-JoinPushdown 同源 | 同 source 满足条件时下推。 | 待执行 |
| UT-045 | Planner-JoinPushdown 跨源 | 跨源 JOIN 不下推。 | 待执行 |
| UT-046 | Planner-Join 类型分支 | Inner/Outer/Semi/Anti/ASOF/WINDOW 分支覆盖。 | 待执行 |
| UT-047 | Planner-Subquery 递归下推 | 内层先决策，外层再决策。 | 待执行 |
| UT-048 | Planner-Subquery 部分下推 | 内层下推、外层本地分支正确。 | 待执行 |
| UT-049 | Planner-表达式可映射递归 | 函数/运算符/列/常量节点递归判定正确。 | 待执行 |
| UT-050 | Planner-函数白名单判定 | 可下推函数与本地函数分类正确。 | 待执行 |
| UT-051 | Planner-运算符映射判定 | 直接/转换/本地分类正确。 | 待执行 |
| UT-052 | Planner-pRemotePlan 初始化 | 首条规则生效时创建 remote scan。 | 待执行 |
| UT-053 | Planner-pRemotePlan 逐步追加 | Filter->Agg->Sort->Limit 追加顺序正确。 | 待执行 |
| UT-054 | Planner-pushdown_flags 组合 | 位掩码按规则组合正确。 | 待执行 |
| UT-055 | Planner-零下推计划 | pRemotePlan=NULL 且本地链完整。 | 待执行 |
| UT-056 | Planner-部分下推计划 | 本地节点与远端节点拓扑一致。 | 待执行 |
| UT-057 | Planner-确定性 | 同 SQL+同能力画像决策稳定。 | 待执行 |
| UT-058 | Planner-物理节点封装 | Logical 字段到 SFederatedScanPhysiNode 映射正确。 | 待执行 |
| UT-059 | Executor-FederatedScan 创建 | createFederatedScanOperatorInfo 初始化正确。 | 待执行 |
| UT-060 | Executor-查询句柄生命周期 | exec/fetch/close 生命周期正确。 | 待执行 |
| UT-061 | Executor-EOF 语义 | fetch 返回 NULL 表示数据耗尽。 | 待执行 |
| UT-062 | Executor-SSDataBlock 转换 | 外部结果转换后列类型正确。 | 待执行 |
| UT-063 | Executor-NULL/边界值 | NULL/溢出边界转换正确。 | 待执行 |
| UT-064 | Executor-错误分类 | 连接/认证/语法/类型/资源分类正确。 | 待执行 |
| UT-065 | Executor-PUSHDOWN_FAILED 上抛 | 语法或类型转换问题上抛专用错误。 | 待执行 |
| UT-066 | Executor-连接错误可重试标记 | retryable=true 分支正确。 | 待执行 |
| UT-067 | Executor-认证错误不可重试 | retryable=false 分支正确。 | 待执行 |
| UT-068 | Executor-日志脱敏 | 不输出密码/token。 | 待执行 |
| UT-069 | Executor-资源释放 | 错误路径资源完整释放。 | 待执行 |
| UT-070 | Executor-并发安全 | 多线程执行无句柄污染。 | 待执行 |
| UT-071 | Executor-零下推读取路径 | pRemotePlan NULL 路径正确。 | 待执行 |
| UT-072 | Executor-部分下推读取路径 | 本地算子消费 SSDataBlock 正常。 | 待执行 |
| UT-073 | Executor-虚拟表参数化拉取 | getNextExtFn 接口参数变化处理正确。 | 待执行 |
| UT-074 | Executor-子表切换重建连接 | 参数变化触发 close+reopen。 | 待执行 |
| UT-075 | Connector-模块初始化销毁 | init/destroy 生命周期正确。 | 待执行 |
| UT-076 | Connector-连接池复用 | 同 source 并发复用连接池。 | 待执行 |
| UT-077 | Connector-池上限保护 | 超上限返回 RESOURCE_EXHAUSTED。 | 待执行 |
| UT-078 | Connector-元数据接口 | getTableSchema 返回原始类型结构。 | 待执行 |
| UT-079 | Connector-能力探测流程 | 静态声明∩实例收敛∩探测结果。 | 待执行 |
| UT-080 | Connector-运行时反馈 | 仅更新 availability，不改能力位。 | 待执行 |
| UT-081 | Connector-SQL 生成 Filter | 远端 SQL 过滤表达式生成正确。 | 待执行 |
| UT-082 | Connector-SQL 生成 Agg | 聚合与 group key SQL 生成正确。 | 待执行 |
| UT-083 | Connector-SQL 生成 Sort/Limit | 排序和分页 SQL 生成正确。 | 待执行 |
| UT-084 | Connector-SQL 生成 Join/Subquery | JOIN 和子查询 SQL 生成正确。 | 待执行 |
| UT-085 | Connector-方言转换 | MySQL/PG/Influx 方言改写正确。 | 待执行 |
| UT-086 | Connector-错误码映射 | 远端错误映射到统一错误码族。 | 待执行 |
| UT-087 | Connector-retryable 判定 | extConnectorIsRetryable 语义正确。 | 待执行 |
| UT-088 | Connector-TLS 参数处理 | tls 配置加载与校验正确。 | 待执行 |
| UT-089 | VTable-SColRef 三路径序列化 | tmsg.h/tmsg.c/metaEntry 兼容一致。 | 待执行 |
| UT-090 | VTable-SColRef 向后兼容 | 老数据 refType 默认 0 行为不变。 | 待执行 |
| UT-091 | VTable-SColumnRefNode 四段解析 | source.db.table.col 节点字段正确。 | 待执行 |
| UT-092 | VTable-LogicCreator 同表合并 | 同 source.db.table 多列合并一个扫描节点。 | 待执行 |
| UT-093 | VTable-PlanSplitter 行为 | 外部扫描不拆分，内部扫描拆分 Exchange。 | 待执行 |
| UT-094 | VTable-DynCtrl 分流 | refType=0/1 分流到内部 map/foreign map。 | 待执行 |
| UT-095 | VTable-下游参数注入 | buildFederatedScanOperatorParam 注入正确。 | 待执行 |
| UT-096 | VTable-多源 ts 归并 | 多源数据按 ts 归并结果正确。 | 待执行 |
| UT-097 | Parser-MySQL 类型映射全量表驱动 | MySQL 类型映射清单逐项断言。 | 待执行 |
| UT-098 | Parser-PG 类型映射全量表驱动 | PostgreSQL 类型映射清单逐项断言。 | 待执行 |
| UT-099 | Parser-Influx 类型映射全量表驱动 | InfluxDB 类型映射清单逐项断言。 | 待执行 |
| UT-100 | Parser-降级映射日志断言 | DATE/TIME/DECIMAL/复合类型降级日志断言。 | 待执行 |
| UT-101 | Parser-不可映射类型矩阵 | 各源不可映射类型统一错误码断言。 | 待执行 |
| UT-102 | Parser-视图豁免边界 | 视图与普通表时间戳主键规则差异断言。 | 待执行 |
| UT-103 | Parser-子查询类型识别 | FROM/标量/IN/EXISTS/ALL-ANY-SOME 识别断言。 | 待执行 |
| UT-104 | Parser-窗口类型识别 | INTERVAL/STATE/SESSION/EVENT/COUNT 分类断言。 | 待执行 |
| UT-105 | Planner-算术运算映射全量 | 算术表达式映射判定全量断言。 | 待执行 |
| UT-106 | Planner-比较运算映射全量 | 比较/NULL 判定映射判定全量断言。 | 待执行 |
| UT-107 | Planner-JSON/REGEXP 映射全量 | JSON 与正则运算映射全量断言。 | 待执行 |
| UT-108 | Planner-集合运算映射全量 | UNION/UNION ALL 映射判定全量断言。 | 待执行 |
| UT-109 | Planner-函数白名单全量 | DS 函数清单逐项判定断言。 | 待执行 |
| UT-110 | Planner-函数特殊转换全量 | LOG/LENGTH/TIMEDIFF 等特殊转换断言。 | 待执行 |
| UT-111 | Planner-数学函数全量映射 | 数学函数族映射与回退断言。 | 待执行 |
| UT-112 | Planner-字符串函数全量映射 | 字符串函数族映射与回退断言。 | 待执行 |
| UT-113 | Planner-时间函数全量映射 | 时间函数族映射与回退断言。 | 待执行 |
| UT-114 | Planner-聚合函数全量映射 | 基础聚合/分位数/特殊聚合判定断言。 | 待执行 |
| UT-115 | Planner-选择函数全量映射 | FIRST/LAST/LAG/LEAD/MODE 等判定断言。 | 待执行 |
| UT-116 | Planner-地理函数全量映射 | ST_* 函数判定断言。 | 待执行 |
| UT-117 | Planner-窗口转换细分 | 翻滚/滑动/状态/会话/事件/计数窗口断言。 | 待执行 |
| UT-118 | Planner-子查询转换细分 | 各子查询类型下推/回退判定断言。 | 待执行 |
| UT-119 | Planner-视图查询边界判定 | 视图非时间线与时间线依赖场景判定断言。 | 待执行 |
| UT-120 | Planner-不支持语义拦截矩阵 | 流计算/订阅/写入等不支持语义拦截断言。 | 待执行 |
| UT-121 | Executor-函数回退执行全量 | 不可下推函数本地执行路径断言。 | 待执行 |
| UT-122 | Executor-窗口回退执行全量 | 不可下推窗口本地执行路径断言。 | 待执行 |
| UT-123 | Executor-子查询回退执行全量 | 子查询不支持场景本地执行路径断言。 | 待执行 |
| UT-124 | Executor-视图查询执行边界 | 视图场景执行路径选择断言。 | 待执行 |
| UT-125 | Connector-MySQL SQL 生成全量 | MySQL 方言 SQL 生成清单断言。 | 待执行 |
| UT-126 | Connector-PG SQL 生成全量 | PostgreSQL 方言 SQL 生成清单断言。 | 待执行 |
| UT-127 | Connector-Influx SQL 生成全量 | Influx 方言 SQL 生成清单断言。 | 待执行 |
| UT-128 | Connector-NULLS/JSON/REGEXP 特殊改写 | 特殊语法改写正确性断言。 | 待执行 |
| UT-129 | Connector-聚合与窗口改写全量 | 聚合与窗口转换 SQL 生成断言。 | 待执行 |
| UT-130 | Connector-Join/Subquery 改写全量 | JOIN/子查询 SQL 改写断言。 | 待执行 |
| UT-131 | Parser-社区版门禁拦截 | 社区版联邦语义入口拦截断言。 | 待执行 |
| UT-132 | Planner-社区版计划生成禁止 | 社区版不生成联邦计划节点断言。 | 待执行 |
| UT-133 | Executor-社区版执行禁止 | 社区版执行阶段返回预期错误码断言。 | 待执行 |
| UT-134 | Parser-流计算语义拦截 | 流计算联邦查询语义拦截断言。 | 待执行 |
| UT-135 | Parser-订阅语义拦截 | 订阅联邦查询语义拦截断言。 | 待执行 |
| UT-136 | Parser-写入语义拦截 | INSERT/UPDATE/DELETE 外部写入拦截断言。 | 待执行 |
| UT-137 | Parser-外部对象操作拦截 | 索引/触发器/存储过程相关操作拦截断言。 | 待执行 |
| UT-138 | Parser-跨源事务语义拦截 | 强一致事务语义不支持拦截断言。 | 待执行 |
| UT-139 | Catalog-社区版元数据可见性 | 社区版系统表与错误提示一致性断言。 | 待执行 |
| UT-140 | Connector-错误码映射稳定性 | 不支持类错误码映射稳定断言。 | 待执行 |
| UT-141 | Scheduler-不可重试错误终止 | 不支持类错误不进入重试断言。 | 待执行 |
| UT-142 | Scheduler-可重试错误策略 | 连接类错误退避重试策略断言。 | 待执行 |
| UT-143 | Executor-日志与诊断字段完整 | 不支持语义错误日志字段完整断言。 | 待执行 |
| UT-144 | VTable-视图豁免在 DDL 路径一致 | 视图豁免在普通表/子表 DDL 路径一致性断言。 | 待执行 |
| UT-145 | VTable-外部源删除后执行防护 | 外部源失效时执行防护路径断言。 | 待执行 |
| UT-146 | VTable-子表切换错误恢复 | 子表切换异常时资源回收与重建断言。 | 待执行 |
| UT-147 | 兼容-升级降级门禁断言 | 升降级过程中联邦对象门禁与校验断言。 | 待执行 |
| UT-148 | 兼容-默认关闭行为断言 | federatedQueryEnable=false 全链路无副作用断言。 | 待执行 |
| UT-149 | 安全-敏感字段脱敏断言全链路 | SHOW/日志/错误消息脱敏全链路断言。 | 待执行 |
| UT-150 | 安全-异常输入不崩溃断言 | 异常元数据/异常结果返回时不崩溃断言。 | 待执行 |
| UT-151 | Parser-OPTIONS 未识别 key 忽略处理 | 带未知 key 的 OPTIONS 创建外部源后，Catalog 中存储的 options 不含未知 key，警告日志字段正确。 | 待执行 |
| UT-152 | Parser-TLS 冲突验证(tls_enabled 与 ssl_mode/sslmode 冲突) | MySQL ssl_mode=disabled+tls_enabled=true、PG sslmode=disable+tls_enabled=true 均在 Parser 阶段报错，错误码正确。 | 待执行 |
| UT-153 | Parser-CASE 子表达式不可映射整体不可下推断言 | CASE WHEN 某分支含不可映射函数时，整个 CASE 节点标记为不可下推，不拆分处理。 | 待执行 |
| UT-154 | Parser-JOIN ON 含 TBNAME 拦截断言 | 外部表 JOIN ON 子句引用 TBNAME 伪列时，Parser 拦截返回 TSDB_CODE_EXT_SYNTAX_UNSUPPORTED。 | 待执行 |
| UT-155 | Parser-虚拟表 DDL 外部源错误码矩阵 | 6 个 FOREIGN_* 错误码（SERVER_NOT_EXIST / DB_NOT_EXIST / TABLE_NOT_EXIST / COLUMN_NOT_EXIST / TYPE_MISMATCH / NO_TS_KEY）逐项断言，错误信息含对应路径。 | 待执行 |
| UT-156 | Planner-MySQL 跨库 JOIN 可下推条件断言 | 同一 MySQL 外部源、不同 database 的两张表 JOIN 时，下推条件判断为真，生成跨库下推 SQL。 | 待执行 |
| UT-157 | Planner-PG/InfluxDB 跨库 JOIN 不可下推断言 | 同一 PG/InfluxDB 外部源、不同 database 的两张表 JOIN 时，下推条件判断为假，回退本地 JOIN 路径。 | 待执行 |
| UT-158 | Planner-SPREAD 特殊映射 MAX-MIN 替换断言 | SPREAD(col) 表达式映射判定时替换为 MAX(col)-MIN(col)，生成 SQL 正确。 | 待执行 |
| UT-159 | Planner-GROUP_CONCAT → STRING_AGG 转换断言 | MySQL 下推 GROUP_CONCAT、PG/InfluxDB 转换为 STRING_AGG，分隔符参数映射正确。 | 待执行 |
| UT-160 | Planner-LEASTSQUARES/LIKE_IN_SET/REGEXP_IN_SET 本地计算断言 | 上述函数在映射判定中标记为不可映射，不生成下推 SQL，走本地计算路径。 | 待执行 |
| UT-161 | Connector-BIT 类型细粒度映射全量断言 | BIT(n≤64)→BIGINT、BIT(n>64)→VARBINARY 映射逻辑逐项断言。 | 待执行 |
| UT-162 | Connector-MySQL YEAR/BOOL/TINYINT(1)映射断言 | YEAR→SMALLINT、BOOL/BOOLEAN/TINYINT(1)→BOOL 映射逻辑断言。 | 待执行 |
| UT-163 | Connector-PG serial/money/interval/hstore/tsvector 映射断言 | serial→INT(自增语义丢失)/money→DECIMAL(18,2)/interval→BIGINT(微秒)/hstore→VARCHAR/tsvector→VARCHAR 逐项断言。 | 待执行 |
| UT-164 | Connector-InfluxDB Decimal128/Decimal256/Duration 映射与截断断言 | Decimal128(p>38)截断+日志、Decimal256→DECIMAL(38,s)、Duration→BIGINT(纳秒)逐项断言。 | 待执行 |
| UT-165 | Connector-GEOMETRY 类型精确映射断言 | MySQL/PG GEOMETRY/POINT/LINESTRING/POLYGON → TDengine GEOMETRY 精确对应，无降级。 | 待执行 |
| UT-166 | Connector-虚拟表 DDL 6 个 FOREIGN_* 错误码全量断言 | extConnectorGetTableSchema 各失败场景映射到对应 FOREIGN_* 错误码逐项断言。 | 待执行 |
| UT-167 | Connector-InfluxDB IN 子查询常量列表改写断言 | IN(subquery) 子查询执行后结果集改写为 IN(v1,v2,...) 的 SQL 生成逻辑断言。 | 待执行 |
| UT-168 | Connector-PostGIS 能力探测失败降级断言 | PG 连接器探测 PostGIS 不可用时，地理函数能力位置 false，地理函数走本地计算路径断言。 | 待执行 |
| UT-169 | Catalog-ALTER EXTERNAL SOURCE 后已有虚拟表不重验证断言 | ALTER 操作完成后 Catalog 对已有虚拟表列引用不触发重新校验，运行时查询才返回连接类错误。 | 待执行 |
| UT-170 | Executor-FILL SURROUND 语义本地执行路径断言 | FILL+SURROUND 组合被识别为不可下推，SURROUND 参数仅在本地填充阶段生效，不进入远端 SQL。 | 待执行 |
| UT-171 | Executor-INTERP 查询时间范围 WHERE 下推断言 | INTERP+RANGE 查询时，时间范围条件转为 WHERE ts BETWEEN 加入远端 SQL，其余插值逻辑本地执行。 | 待执行 |
| UT-172 | Connector-MySQL 跨库 JOIN SQL 生成断言 | MySQL 跨库 JOIN 生成的远端 SQL 含完整 db.table 前缀，JOIN 语法正确。 | 待执行 |
| UT-173 | Connector-PG 跨库 JOIN 本地执行路径断言 | PG 跨库场景不生成跨库 JOIN 的远端 SQL，各表独立下推 SELECT，本地 JOIN 执行。 | 待执行 |
| UT-174 | 配置参数边界值校验断言 | federatedQueryConnectTimeoutMs[100,600000]、MetaCacheTtlSeconds[1,86400]、CapabilityCacheTtlSeconds[1,86400] 边界与越界逐项断言。 | 待执行 |
| UT-175 | VTable-虚拟超级表 VIRTUAL 1 标志 DDL 路径断言 | CREATE STABLE ... VIRTUAL 1 在 Parser 和 Mnode 路径中设置正确标志位，virtualStb=1 存储与读取一致。 | 待执行 |
| UT-176 | Parser-PG xml/inet/macaddr/bit 类型映射断言 | PG xml→NCHAR、inet/cidr/macaddr→VARCHAR、bit(n)/bit varying(n)→VARBINARY 逐项映射断言。 | 待执行 |
| UT-177 | Connector-InfluxDB Date32/Date64/Time32/Time64 映射断言 | Date32→TIMESTAMP(补零点)、Date64→TIMESTAMP、Time32→BIGINT(毫秒)、Time64→BIGINT(微秒) 逐项断言。 | 待执行 |
| UT-178 | Planner-Full Outer Join PG/InfluxDB 直接下推断言 | PG/InfluxDB 单源 FULL OUTER JOIN 下推条件判断为真，生成标准 FULL OUTER JOIN SQL；MySQL 走改写路径。 | 待执行 |
| UT-179 | Planner-联邦规则列表切换断言 | 含 SCAN_TYPE_EXTERNAL 时 Optimizer 使用联邦规则列表，纯本地查询使用原 31 条规则列表，切换逻辑正确。 | 待执行 |
| UT-180 | Executor-伪列 _ROWTS/_QSTART/_QEND 本地生成断言 | 联邦查询中 _ROWTS 从时间戳列映射、_QSTART/_QEND 从 WHERE 解析，值正确且不出现在下推 SQL 中。 | 待执行 |
| UT-181 | Planner-InfluxDB PARTITION BY tag_col 转 GROUP BY 断言 | InfluxDB 源 PARTITION BY 单个 tag 列转换为 GROUP BY tag_col，下推判断正确。 | 待执行 |
| UT-182 | Connector-比较函数 IF/NVL2/IFNULL/NULLIF SQL 生成断言 | IF→CASE WHEN(PG/Influx)、NVL2→CASE WHEN、IFNULL→COALESCE(PG/Influx)、NULLIF 直接下推，SQL 生成正确。 | 待执行 |


# 易用性测试（可选）

- SQL 语法一致性：外部查询语句与本地查询书写体验一致。**已覆盖**：§4 FQ-SQL-001 等基础查询用例隐含此验证，无需单独新增。
- 错误提示可读性：错误码 + 原因 + 建议可定位问题。**已覆盖**：§1 FQ-EXT 错误码用例、§7 FQ-VTBL-006~010 DDL 错误码用例均含错误信息完整性断言，无需单独新增。
- SHOW/DESCRIBE 输出可读性：关键字段完整、敏感字段脱敏明确。**已覆盖**：§8 FQ-SYS-001~010 系统表与输出字段完整性用例，无需单独新增。
- 文档示例可运行性：FS/DS 中示例 SQL 可直接复现。**已覆盖**：FQ-EXT-032（建源示例）、FQ-SQL-086（查询示例）。

# 长期稳定性测试（可选）

- 72h 持续查询：单源查询/跨源 JOIN/虚拟表混合查询连续运行。
- 故障注入：外部源短时不可达、慢查询、限流、连接抖动。
- 缓存稳定性：meta/capability 缓存反复过期刷新，内存无泄漏。
- 连接池稳定性：并发高峰与低峰切换，无僵尸连接。

# 性能测试

## 测试准备

### 数据集规格

| 数据集 | 外部源 | 表数量 | 单表行数 | 列数（含主键） | 总数据量（估算） | 备注 |
| --- | --- | --- | --- | --- | --- | --- |
| 小规模基线 | MySQL 8.0 | 1 张宽表 | 100 万行 | 20 列（1 TIMESTAMP PK + 10 INT + 5 DOUBLE + 4 VARCHAR） | ~500 MB | 单源下推基线 |
| 中规模多表 | MySQL 8.0 | 10 张表 | 各 100 万行 | 10 列 | ~500 MB × 10 | 跨库 JOIN、LIMIT/排序场景 |
| 大规模聚合 | PostgreSQL 14 | 1 张宽表 | 1000 万行 | 15 列（1 TIMESTAMPTZ PK + 8 DOUBLE + 6 TEXT） | ~5 GB | 聚合/大窗口场景 |
| 时序基线 | InfluxDB v3 | 1 个 measurement（1000 设备子表） | 每子表 10 万行（共 1 亿行） | 5 tag + 8 field | ~10 GB | 虚拟表多源归并场景 |
| JOIN 组合 | MySQL + PostgreSQL | MySQL 10 张（各 100 万行）+ PG 10 张（各 100 万行） | — | 各 10 列 | ~1 GB | 跨源 JOIN 本地执行场景 |
| TDengine 本地 | TDengine 企业版 | 1000 张子表 | 各 10 万行 | 5 tag + 8 列 | ~2 GB | 虚拟表内部列配对对照 |

### 客户端并发配置

| 场景 | 并发连接数 | 持续时间 | 备注 |
| --- | --- | --- | --- |
| 基线（单并发） | 1 | 5 min 稳定后采样 | PERF-001 / PERF-002 |
| 低并发 | 4 | 5 min | PERF-003~006 |
| 中并发 | 16 | 10 min | PERF-008 |
| 高并发 | 64 | 10 min | PERF-008 峰值测试 |

### 度量指标

- **查询延迟**：P50 / P95 / P99（ms）
- **吞吐量**：QPS（queries per second）
- **拉取数据量**：下推路径 vs 零下推路径每查询传输字节数
- **CPU / 内存**：TDengine 进程峰值占用
- **连接池使用率**：活跃连接数 / 连接池上限

### 退化阈值（回归门槛）

| 指标 | 可接受退化上限 |
| --- | --- |
| P99 延迟（全下推基线） | 较上一版本 ≤ 10% |
| P99 延迟（零下推基线） | 较上一版本 ≤ 5% |
| QPS（基线场景） | 较上一版本 ≤ 5% |
| 拉取数据量（全下推） | 较上一版本 ≤ 5% |

### 工具

- 数据准备：taosBenchmark（TDengine 本地）、自定义 Python 脚本（MySQL/PG/InfluxDB 批量插入）
- 查询压测：taos-bench / wrk / 自定义多线程客户端
- 指标采集：Prometheus + Grafana 或脚本采集 TDengine 系统表

## 用例列表

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| PERF-001 | 单源全下推基线 | 使用小规模基线数据集，Filter+Agg+Sort+Limit 全下推，采集 P50/P95/P99 延迟与 QPS。 | 待执行 |
| PERF-002 | 单源零下推基线 | 同数据集，禁用下推全本地计算，对比 P99 延迟与传输字节数。 | 待执行 |
| PERF-003 | 全下推 vs 零下推 | 在小规模与大规模聚合数据集上对比吞吐、延迟、拉取数据量三项指标。 | 待执行 |
| PERF-004 | 跨源 JOIN | 使用 JOIN 组合数据集，MySQL×PG 各 1~10 张表组合，记录不同数据量下延迟曲线。 | 待执行 |
| PERF-005 | 虚拟表混合查询 | 使用时序基线 + TDengine 本地数据集，内外列融合查询，评估多源归并额外开销。 | 待执行 |
| PERF-006 | 大窗口聚合 | 使用大规模聚合数据集，INTERVAL 1h + FILL(PREV) + INTERP，评估本地计算成本。 | 待执行 |
| PERF-007 | 缓存命中收益 | 同一查询连续执行先命中再失效，对比元数据/能力缓存命中与重拉的延迟差异。 | 待执行 |
| PERF-008 | 连接池并发能力 | 4 / 16 / 64 并发客户端压测，记录各并发度下 P99 延迟与失败率，确认连接池上限表现。 | 待执行 |
| PERF-009 | 超时参数敏感性 | 调整 connect_timeout_ms / read_timeout_ms，注入可控延迟，验证超时触发与错误码正确。 | 待执行 |
| PERF-010 | 退避重试影响 | 模拟外部源资源限制（限流）场景，评估退避重试策略对整体查询延迟的放大倍数。 | 待执行 |
| PERF-011 | 多源归并成本 | 时序基线数据集中 1000 子表归并，评估 SORT_MULTISOURCE_TS_MERGE 随子表数量增长的延迟曲线。 | 待执行 |
| PERF-012 | 回归门槛 | 对 PERF-001/002/008 三项指标与上一版本基线对比，超出退化阈值时标记回归失败。 | 待执行 |

# 安全测试

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| SEC-001 | 密码加密存储 | 元数据侧不落明文密码。 | 待执行 |
| SEC-002 | SHOW/DESCRIBE 脱敏 | password/token/cert 私钥不明文展示。 | 待执行 |
| SEC-003 | 日志脱敏 | 错误日志不含敏感信息。 | 待执行 |
| SEC-004 | 普通用户可见性 | sysInfo 列权限保护正确。 | 待执行 |
| SEC-005 | TLS 单向校验 | tls_enabled + ca_cert 生效。 | 待执行 |
| SEC-006 | TLS 双向校验 | client cert/key 生效。 | 待执行 |
| SEC-007 | 鉴权失败阻断 | auth failed 后 source 状态更新。 | 待执行 |
| SEC-008 | 权限不足阻断 | access denied 错误码与状态处理正确。 | 待执行 |
| SEC-009 | SQL 注入防护 | SOURCE/路径/标识符解析无注入漏洞。 | 待执行 |
| SEC-010 | 异常数据边界校验 | 外部异常返回不导致崩溃。 | 待执行 |
| SEC-011 | 连接重置安全性 | 连接中断后句柄清理完整。 | 待执行 |
| SEC-012 | 敏感配置修改审计 | ALTER SOURCE 变更有审计记录。 | 待执行 |

# 兼容性测试

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| COMP-001 | MySQL 5.7/8.0 兼容 | 核心查询与映射行为一致。 | 待执行 |
| COMP-002 | PostgreSQL 12/14/16 兼容 | 核心查询与映射行为一致。 | 待执行 |
| COMP-003 | InfluxDB v3 兼容 | Flight SQL 路径稳定。 | 待执行 |
| COMP-004 | Linux 发行版兼容 | Ubuntu/CentOS 环境行为一致。 | 待执行 |
| COMP-005 | 默认关闭兼容性 | 关闭联邦时历史行为不变。 | 待执行 |
| COMP-006 | 升级后外部源元数据 | 升级脚本迁移后对象可用。 | 待执行 |
| COMP-007 | 升级后零数据场景 | 未使用联邦时可平滑升级降级。 | 待执行 |
| COMP-008 | 升级后已写入场景 | 已存在外部源配置时行为正确。 | 待执行 |
| COMP-009 | 函数方言兼容 | 关键转换函数跨版本稳定。 | 待执行 |
| COMP-010 | 大小写/引号兼容 | 标识符规则跨源一致。 | 待执行 |
| COMP-011 | 字符集兼容 | 多语言字符集跨源一致。 | 待执行 |
| COMP-012 | 连接器版本矩阵 | 连接器版本不一致时启动校验有效。 | 待执行 |

# 已知问题和限制（可选）

- 不支持外部源写入、外部对象 DDL 操作、跨源强一致事务。
- MySQL/PostgreSQL 外部表不支持 TBNAME 与 TAGS 查询。
- TDengine 专有窗口/插值/部分函数为本地计算路径，超大数据量可能性能退化。
- 跨源 JOIN、跨源子查询主要依赖本地计算，需通过时间范围和过滤条件控制数据量。
- InfluxDB TAGS 转 DISTINCT 的语义与 TDengine 元数据语义存在边界差异（无数据 tag set 不返回）。
- 本文档为测试规格，需在执行后补齐“测试结果/缺陷单/回归结论”。
