---
sidebar_label: 联邦查询
title: 联邦查询
toc_max_heading_level: 3
---

联邦查询（Federated Query）允许 TDengine 在查询执行时读取外部数据源，并与 TDengine 本地数据在同一条 SQL 中完成过滤、聚合、排序、关联和窗口计算。使用联邦查询时无需预先将外部数据迁移到 TDengine。

外部访问通过外部数据源（External Source）对象定义。该对象保存访问外部系统所需的连接信息，并按数据源类型选择相应的连接器完成元数据获取、读取和结果转换。

## 功能范围

联邦查询仅可用于独立的 `SELECT` 查询，支持 MySQL、PostgreSQL 和 InfluxDB v3 作为外部数据源。除没有对应概念或无法映射类型的场景外，外部表可使用 TDengine 的全部查询功能，并可与本地表或其他外部源表组合查询。

### 支持能力

联邦查询支持以下能力：

- 外部数据源的创建、查看、修改、删除和刷新。
- 跨数据源查询，以及本地表与外部表联合查询。
- 虚拟表引用外部数据源的表列。
- 查询执行过程的权限、审计和可观测性。

### 不支持场景

以下场景不能引用外部表：

- 数据订阅，例如 `CREATE TOPIC ... AS SELECT ... FROM 外部表`，或涉及外部表的 `CREATE TOPIC ... AS DATABASE/STABLE`。
- 写入子查询，例如 `INSERT INTO 本地表 SELECT ... FROM 外部表`。
- 外部系统写入、外部对象的 DDL 操作，以及跨数据源强一致性事务。
- 外部源具备但 TDengine 不具备的功能。

### 兼容性

该功能为企业版功能。默认关闭时，现有本地查询行为不变；启用后仅增加外部数据源对象和联邦查询行为。无法支持的语义会明确报错，不会返回不确定结果。当前版本以 SQL 语句提供外部数据源管理和联邦查询能力，不新增独立编程接口。

## 使用前准备

使用前需要在客户端和服务端启用 `federatedQueryEnable`。联邦查询的连接、超时、连接池和缓存配置由组件配置统一管理：

- 服务端参数参见 [taosd 联邦查询配置](../../12-operations-and-tooling/03-components/01-taosd.md#联邦查询相关)。
- 客户端参数参见 [taosc 联邦查询配置](../../12-operations-and-tooling/03-components/02-taosc.md#联邦查询相关)。

**注意事项**：客户端和服务端均支持的联邦查询参数应保持一致；修改参数后是否立即生效以组件配置文档中的“动态修改”说明为准。

### 联邦查询依赖库

使用联邦查询前，必须安装与 TDengine 版本匹配的联邦查询依赖库安装包，并在实际执行联邦查询的服务器上完成安装。该安装包提供外部连接器所需的运行库：MySQL 使用 MariaDB Connector/C，PostgreSQL 使用 `libpq`，InfluxDB 使用 Apache Arrow Flight SQL 运行库。

**注意事项**：未安装对应运行库时，无法创建或查询相应类型的外部数据源。升级 TDengine 时，应同时升级版本匹配的联邦查询依赖库安装包。

### 联邦查询插件包（第三方运行库）

除客户端与服务端配置外，还可以单独安装联邦查询插件包（包含第三方运行库），用于补齐外部连接器依赖。

- 下载名称：`TDengine TSDB Federated Query Plugin`。
- 下载渠道：自 `v3.4.3.0` 起可从下载中心获取。
- 当前支持平台：Linux x64、Linux ARM64；其他平台暂不支持。

插件包解压后通常包含如下结构：

```text
.
├── lib
│   ├── libarrow_flight.so
│   ├── libarrow_flight_sql.so
│   ├── libarrow.so
│   ├── libmariadb.so
│   ├── libparquet.so
│   ├── libpq.so
│   └── libtaos_ext_influx_arrow.so
├── MANIFEST.txt
└── README.txt
```

**说明**

- `libmariadb.so`、`libpq.so`、Arrow/Parquet 相关库用于外部连接器运行时依赖。
- 安装后应确保动态链接库路径可被 `taosd` 与 `taosc` 进程加载。
- 附录：当前内部下载地址（内网）为 `http://192.168.1.131/data/nas/TDengine/smoking/v3.4.2.4.0805/enterprise/tdengine-tsdb-enterprise-fq-runtime-3.4.2.4.0805-linux-x64.tar.gz`。
- 该地址为内网临时地址，`v3.4.3.0` 发布后以下载中心提供的正式链接为准。
- 详细安装与启动步骤将在后续补充。

### 执行节点要求

- 纯联邦查询，即仅读取外部数据源而不读取本地 TSDB 表的查询，不受 `queryPolicy` 影响，必须在 qnode 上执行。使用前应确保集群已部署 qnode；未部署时，查询返回错误。
- 查询涉及本地 TSDB 表时，本地扫描由相关 vnode 处理；例如，含外部列引用的虚拟表查询会访问其关联的 vnode。

### 权限和安全

密码采用 AES-CBC 加密保存，并在展示和日志中脱敏。外部源访问权限由外部数据库自身控制：创建外部数据源时指定的 `USER`、`PASSWORD` 或 `api_token` 代表该外部源的访问凭证，外部数据库据此决定可访问范围。TDengine 仅允许 root 用户管理外部数据源对象。

**注意事项**：外部通信支持加密传输和证书校验。不要在 SQL、日志或应用配置中暴露明文凭证。

## 创建外部数据源

使用 `CREATE EXTERNAL SOURCE` 创建外部数据源。创建和修改操作只保存元数据，不验证网络连通性或账号密码；首次实际查询时才建立外部连接。连接失败或认证失败时，查询会返回相应错误。

### 语法

```sql
CREATE EXTERNAL SOURCE [IF NOT EXISTS] source_name
TYPE = 'mysql' | 'postgresql' | 'influxdb'
HOST = 'hostname'
PORT = port_number
USER = 'username'
PASSWORD = 'password'
[DATABASE = database_name]
[SCHEMA = schema_name]
[OPTIONS (
    'option_key' = 'option_value'
    [, ...]
)];
```

### 支持类型

可用的外部源类型及其命名空间如下：

| 类型 | 默认命名空间 | 说明 |
| ------------ | --- | --- |
| `mysql`      | `DATABASE` | 访问 MySQL 数据库和表。 |
| `postgresql` | `DATABASE` 和可选的 `SCHEMA` | `DATABASE` 必填，`SCHEMA` 可作为默认 schema。 |
| `influxdb`   | `DATABASE` | 访问 InfluxDB 数据库和 Measurement。 |

### 字段说明和约束

| 字段 | 是否必填 | 取值或上限 | 说明 |
| --------------- | --- | --- | --- |
| `IF NOT EXISTS` | 否 | 固定关键字 | 对象已存在时不报错。 |
| `source_name`   | 是 | 最大 64 字节 | 外部数据源名称，全局唯一，且不得与本地数据库同名。 |
| `TYPE`          | 是 | `mysql`、`postgresql`、`influxdb` | 外部源类型，不区分大小写，决定连接器和路径解析规则。 |
| `HOST`          | 是 | 主机名或 IP，最大 256 字节 | 外部数据源地址，支持完整 FQDN。 |
| `PORT`          | 是 | `1` 到 `65535` | 外部数据源端口。 |
| `USER`          | 是 | 最大 128 字节 | 外部数据源访问账号。 |
| `PASSWORD`      | 是 | 最大 128 字节 | 外部数据源访问密码，保存时加密，展示时脱敏。 |
| `DATABASE`      | MySQL、InfluxDB 可选；PostgreSQL 必填 | 最大 64 字节 | 默认数据库。未设置默认数据库时，查询必须显式指定数据库。PostgreSQL 连接建立后不能切换数据库。 |
| `SCHEMA`        | 否 | 最大 64 字节 | 默认 schema。未设置时，必要的查询必须显式指定 schema。 |
| `OPTIONS`       | 否 | key 最大 64 字节，value 最大 4095 字节，整体 JSON 最大 4095 字节 | 连接扩展参数。 |

**注意事项**：所有标识符遵循 TDengine 数据库和表名规则：默认情况下限制字符类型且不区分大小写；使用转义标识符后可放宽字符限制且区分大小写。

### OPTIONS 参数

`OPTIONS` 中的 key 和 value 均为字符串。连接器按参数语义进行类型转换，例如将 `'true'` 转换为布尔值。

#### 通用参数

所有外部源均支持以下通用参数：

| 参数 | 取值 | 默认值 | 说明 |
| -------------------- | --- | --- | --- |
| `tls_enabled`        | `true` 或 `false` | `false` | 是否启用 TLS 加密连接。 |
| `tls_ca_cert`        | PEM 文本 | 空，使用系统默认 CA | 用于验证服务端证书，仅在 `tls_enabled = true` 时生效。 |
| `tls_client_cert`    | PEM 文本 | 空 | 双向 TLS 的客户端证书，仅在 `tls_enabled = true` 时生效，必须与 `tls_client_key` 成对配置。 |
| `tls_client_key`     | PEM 文本 | 空 | 双向 TLS 的客户端私钥，仅在 `tls_enabled = true` 时生效，必须与 `tls_client_cert` 成对配置。 |
| `connect_timeout_ms` | `0` 或 `100` 到 `600000` | 使用全局配置 | 单次连接建立超时，单位为毫秒。`0` 表示无限等待；`1` 到 `99` 为非法值。该值覆盖 `federatedQueryConnectTimeoutMs`。 |
| `read_timeout_ms`    | `0` 或 `100` 到 `600000` | 使用全局配置 | 单次查询读取超时，单位为毫秒。`0` 表示无限等待；`1` 到 `99` 为非法值。该值覆盖 `federatedQueryQueryTimeoutMs`。 |

#### MySQL 参数

MySQL 专属参数如下：

| 参数 | 取值 | 默认值 | 说明 |
| ---------- | --- | --- | --- |
| `charset`  | 字符集名称 | `utf8mb4` | 连接字符集，对应 `SET NAMES`。 |
| `ssl_mode` | `disabled`、`preferred`、`required`、`verify_ca`、`verify_identity` | `preferred` | MySQL SSL 连接模式。设置 `tls_enabled = true` 时不得为 `disabled`。 |

#### PostgreSQL 参数

PostgreSQL 专属参数如下：

| 参数 | 取值 | 默认值 | 说明 |
| --------- | --- | --- | --- |
| `sslmode` | `disable`、`allow`、`prefer`、`require`、`verify-ca`、`verify-full` | `prefer` | libpq SSL 连接模式。设置 `tls_enabled = true` 时不得为 `disable`。 |

#### InfluxDB 参数

InfluxDB 专属参数如下：

| 参数 | 取值 | 默认值 | 说明 |
| ----------- | --- | --- | --- |
| `api_token` | 字符串 | 空 | InfluxDB 必填的认证 Token。设置该参数时，`USER` 和 `PASSWORD` 可以为空。展示时脱敏。 |
| `protocol`  | `flight_sql` 或 `http` | `flight_sql` | 与 InfluxDB 通信的协议。`flight_sql` 使用 Arrow Flight SQL（gRPC），`http` 使用 HTTP API；两种协议均支持通用 TLS 参数。 |

#### 参数行为和注意事项

**说明**：显式设置 `connect_timeout_ms` 或 `read_timeout_ms`，包括设置为 `0`，优先于全局配置；只有未设置该 key 时才回退到全局配置。

**注意事项**：未知 key 会在 DDL 阶段返回 `TSDB_CODE_PAR_SYNTAX_ERROR`，且不会保存。`ALTER EXTERNAL SOURCE ... SET OPTIONS(...)` 按增量合并处理，未写出的 key 保持原值；将 value 设置为空字符串 `''` 可删除任意已有 key。`SHOW` 和 `DESCRIBE` 中，`password`、`api_token`、`tls_client_cert` 和 `tls_client_key` 会脱敏显示。

#### 示例

以下示例创建三个外部数据源：

```sql
CREATE EXTERNAL SOURCE mysql_prod
TYPE = 'mysql'
HOST = 'mysql.example.com'
PORT = 3306
USER = 'reader'
PASSWORD = 'your_password'
DATABASE = power
OPTIONS (
    'connect_timeout_ms' = '5000',
    'read_timeout_ms' = '30000'
);

CREATE EXTERNAL SOURCE pg_prod
TYPE = 'postgresql'
HOST = 'pg.example.com'
PORT = 5432
USER = 'readonly'
PASSWORD = 'your_password'
DATABASE = iot
SCHEMA = public
OPTIONS ('sslmode' = 'require');

CREATE EXTERNAL SOURCE IF NOT EXISTS influx_prod
TYPE = 'influxdb'
HOST = 'influx.example.com'
PORT = 8086
USER = ''
PASSWORD = ''
DATABASE = telegraf
OPTIONS (
    'api_token' = 'your_token',
    'protocol' = 'flight_sql',
    'tls_enabled' = 'true'
);
```

以下示例说明超时、未知参数和删除参数的行为：

```sql
-- 合法：显式设置为无限等待
ALTER EXTERNAL SOURCE pg_prod
SET OPTIONS('read_timeout_ms' = '0');

-- 非法：1 到 99 不在允许范围内
ALTER EXTERNAL SOURCE pg_prod
SET OPTIONS('read_timeout_ms' = '99');

-- 非法：未知参数会返回 TSDB_CODE_PAR_SYNTAX_ERROR
ALTER EXTERNAL SOURCE mysql_prod
SET OPTIONS('unknown_opt' = 'x');

-- 合法：删除已有参数
ALTER EXTERNAL SOURCE mysql_prod
SET OPTIONS('ssl_mode' = '');
```

## 管理外部数据源

### 查看外部数据源列表

使用 `SHOW EXTERNAL SOURCES` 查看已注册外部数据源：

#### 语法

```sql
SHOW EXTERNAL SOURCES;
```

#### 返回字段

返回字段如下：

| 字段 | 说明 |
| ------------- | --- |
| `source_name` | 外部数据源名称。 |
| `TYPE`        | 外部源类型。 |
| `HOST`        | 外部源地址。 |
| `PORT`        | 外部源端口。 |
| `USER`        | 外部源访问账号。 |
| `PASSWORD`    | 外部源访问密码，脱敏显示。 |
| `DATABASE`    | 默认数据库，未配置时为空。 |
| `SCHEMA`      | 默认 schema，未配置时为空。 |
| `OPTIONS`     | 已配置的 key-value 参数，敏感值脱敏。 |
| `create_time` | 外部源创建时间。 |

**注意事项**：`SHOW` 和 `DESCRIBE` 中，`password`、`api_token`、`tls_client_cert` 和 `tls_client_key` 会脱敏显示。

### 查看外部数据源详情

使用 `DESCRIBE EXTERNAL SOURCE source_name` 查看某个外部数据源的定义：

#### 语法

```sql
DESCRIBE EXTERNAL SOURCE mysql_prod;
```

#### 说明

该命令返回与 `SHOW EXTERNAL SOURCES` 相同的定义字段；`PASSWORD` 及敏感 `OPTIONS` 值始终脱敏。

### 查询外部数据源系统表

外部数据源定义也可从 `information_schema.ins_ext_sources` 查询：

#### 示例

```sql
SELECT source_name, type, host, port, database, schema, create_time
FROM information_schema.ins_ext_sources
WHERE type = 'mysql';
```

`information_schema.ins_ext_sources` 的字段如下：

| 列名 | 类型 | 说明 |
| ------------- | --- | --- |
| `source_name` | VARCHAR | 全局唯一的外部数据源名称。 |
| `type`        | VARCHAR | 外部源类型。 |
| `host`        | VARCHAR | 外部数据源地址。 |
| `port`        | INT | 外部数据源端口。 |
| `user`        | VARCHAR | 外部数据源访问账号，所有用户可见。 |
| `password`    | VARCHAR | 外部数据源密码，所有用户只能看到 `******`。 |
| `database`    | VARCHAR | 默认数据库，未配置时为空。 |
| `schema`      | VARCHAR | 默认 schema，未配置时为空。 |
| `options`     | VARCHAR | JSON 格式的可选参数，敏感值脱敏。 |
| `create_time` | TIMESTAMP | 外部源创建时间。 |

**注意事项**：所有用户均可查询该系统表，但无法通过该表获取原始密码。

### 修改外部数据源

使用 `ALTER EXTERNAL SOURCE` 修改连接信息：

#### 语法

```sql
ALTER EXTERNAL SOURCE source_name
SET HOST = 'hostname',
    PORT = port_number;
```

#### 可修改字段

| 字段 | 是否可修改 | 说明 |
| ------------------------------- | --- | --- |
| `source_name`                   | 否 | 仅用于定位外部数据源。 |
| `HOST`、`PORT`、`USER`、`PASSWORD` | 是 | 修改连接地址、端口和访问凭证。 |
| `DATABASE`、`SCHEMA`             | 是 | 修改默认命名空间。 |
| `OPTIONS`                       | 是 | 增量新增或覆盖参数；未写出的 key 保留；空字符串删除 key。 |
| `TYPE`                          | 否 | 不支持修改，需要删除后重新创建。 |

#### 注意事项

- 修改操作同样不检查外部连接，新配置会在后续查询时验证。
- `ALTER EXTERNAL SOURCE ... SET OPTIONS(...)` 按增量合并处理，未写出的 key 保持原值；将 value 设置为空字符串 `''` 可删除任意已有 key。

#### 示例

```sql
-- 切换到只读从库
ALTER EXTERNAL SOURCE mysql_prod
SET HOST = 'mysql-ro.example.com',
    PORT = 3307;

-- 修改 PostgreSQL 账号和密码
ALTER EXTERNAL SOURCE pg_prod
SET USER = 'new_reader',
    PASSWORD = 'new_password';

-- 修改默认数据库
ALTER EXTERNAL SOURCE mysql_prod
SET DATABASE = power_v2;

-- 增量增加读取超时，已有参数仍保留
ALTER EXTERNAL SOURCE mysql_prod
SET OPTIONS('read_timeout_ms' = '3000');
```

### 刷新外部数据源

外部对象结构发生变化后，使用 `REFRESH EXTERNAL SOURCE` 强制刷新外部元数据和缓存：

#### 语法

```sql
REFRESH EXTERNAL SOURCE mysql_prod;
```

**说明**：外部表结构、列定义或其他元数据变化后，应先刷新再重新查询。

### 删除外部数据源

使用 `DROP EXTERNAL SOURCE` 删除外部数据源：

#### 语法

```sql
DROP EXTERNAL SOURCE IF EXISTS mysql_prod;
```

**注意事项**：`IF EXISTS` 使对象不存在时不报错。删除正在被查询或被其他对象引用的外部数据源可能导致当前查询失败。

## 外部对象和类型映射

### 外部对象映射

| 外部源 | 外部对象 | TDengine 查询对象 |
| ---------- | --- | --- |
| MySQL      | Database | 数据库。 |
| MySQL      | Table、View | 普通表。视图可不含时间戳列；无时间线时的规则见 [无时间线的外部表](#无时间线的外部表)。 |
| PostgreSQL | Database 和 Schema | 一个查询命名空间。 |
| PostgreSQL | Table、View、FDW、Inheritance | 普通表。视图不受时间戳主键约束；无时间线时的规则见 [无时间线的外部表](#无时间线的外部表)。 |
| InfluxDB   | Database | 数据库。 |
| InfluxDB   | Measurement | 超级表。 |
| InfluxDB   | Tag | 标签列，保留索引和分组语义。 |
| InfluxDB   | Field | 数据列。 |
| InfluxDB   | Tag Set | 子表，每个唯一 Tag 组合对应一个子表。 |
| InfluxDB   | `time` | 时间戳主键。 |

MySQL 的索引、存储过程和触发器，以及 PostgreSQL 的索引、序列和触发器不参与联邦查询。InfluxDB 的 Bucket 和 Retention Policy 为存储策略，不影响联邦查询。

**说明**：标识符大小写遵循外部数据库规则：MySQL 默认不区分大小写；PostgreSQL 默认折叠为小写，使用引号时保留原始大小写；InfluxDB 区分大小写。

### 时间戳主键要求

参与联邦查询并执行**依赖时间线**的操作时，外部表（Table / Measurement）须具备可映射为 TDengine `TIMESTAMP` 的主键时间列。该列用于时间范围过滤、排序、窗口与时序函数。

- MySQL 的时间戳主键列须为 `DATETIME` 或 `TIMESTAMP` 类型。
- PostgreSQL 的时间戳主键列须为 `TIMESTAMP` 或 `TIMESTAMPTZ` 类型。
- InfluxDB Measurement 的 `time` 列天然满足该要求。

**说明**：时间戳主键可以是复合主键中的一列，复合主键中的其他列以及其他索引均可存在；但同一外部表不能有第二个可映射为 TDengine `TIMESTAMP` 的主键列。**MySQL / PostgreSQL 视图（View）不受时间戳主键约束**；结果集不含时间戳列时的规则见下节。

### 无时间线的外部表

指 MySQL / PostgreSQL 中不含可映射为 `TIMESTAMP` 的主键时间列的普通表，或结果集不含时间戳列的外部视图。典型场景包括设备台账、资产维度表等。InfluxDB Measurement 均含 `time`，不适用本节。

| 场景 | 是否支持 | 说明 |
| --- | --- | --- |
| 单表查询 | **部分支持** | 支持 `COUNT` 等不依赖时间线的查询；外部视图可直接作为查询对象 |
| 放入虚拟表 | **不作为受支持用法** | 外部列引用的虚拟表执行路径按外部表主时间列排序、按时间戳归并；外部普通表必须有可映射为 `TIMESTAMP` 的主键时间列，外部视图不能作为虚拟表列引用 |
| 与带时间线表 `JOIN` | **不支持** | 外部表 `JOIN` 的 `ON` 条件必须包含主时间戳列；无时间线外部表没有可用的主时间戳列，不能只按业务键关联 |

**不依赖时间线的查询**示例：

```sql
-- 统计无时间戳主键的外部表行数
SELECT COUNT(*) FROM pg_prod.public.device_info;

-- 查询不含时间戳列的外部视图
SELECT * FROM mysql_prod.v_device_dim LIMIT 100;
```

无时间线外部表不能用于 `INTERVAL`、状态/会话/事件/计数窗口、`FILL`、`INTERP`、时序差分/积分类函数、ASOF JOIN、Window JOIN 或虚拟表列引用。当前版本中，含非时间戳主键的外部普通表可能通过虚拟表 DDL 校验，但该路径不具备受测试保障的时间归并语义，不应在生产中使用。若需将外部维度数据与 TDengine 时序数据关联，应先在外部系统中提供具有时间戳主键的关联结果，或将维度数据同步到 TDengine 后再关联。

### 类型映射

#### 映射规则

外部列在查询时映射为 TDengine 数据类型，遵循以下规则：

- 可精确对应的类型直接映射，例如 MySQL `INT` 映射为 `INT`、PostgreSQL `double precision` 映射为 `DOUBLE`、InfluxDB `Float64` 映射为 `DOUBLE`。
- 可降级转换的类型允许映射，但可能存在精度或语义损失。例如 `DATE` 在零点转换为 `TIMESTAMP`，`TIME` 转换为从午夜开始的毫秒数并存入 `BIGINT`，`SET` 序列化为逗号分隔字符串，`uuid` 转换为 `VARCHAR(36)`。发生降级转换时会记录日志。
- 数组、范围、复合类型和 `hstore` 等结构化类型序列化为 JSON 或文本字符串并存入 `NCHAR` 或 `VARCHAR`，结构语义不保留，并会记录日志。
- 无法识别或支持的外部类型码报错处理；若 `SELECT *` 展开到不可映射列，整条查询失败。

#### 特殊类型和精度

**注意事项**：外部源中的 `JSON`、`JSONB` 和 string 列统一序列化为字符串存入 `NCHAR`，不会映射为 TDengine 原生 JSON 类型。对此类列使用 JSON 子字段运算符 `->` 或 `CONTAINS` 会返回 错误。

外部已知类型的声明长度、precision、scale 或容器容量超过 TDengine 上限时，不会仅因声明超限而失败。结果列使用 TDengine 支持的最大范围；只有实际值无法无损表示时才在执行期报错，不会静默截断、舍入、替换为 `NULL` 或跳过。

外部时间戳列的时区和精度规则如下：

- PostgreSQL `timestamptz`、MySQL `TIMESTAMP` 等带时区类型按其自带时区解释。
- PostgreSQL `timestamp`、MySQL `DATETIME` 和 `DATE` 等不带时区类型，按连接时区或客户端时区解释。
- 单库查询中，TDengine 数据库使用其 `PRECISION`；MySQL 和 PostgreSQL 使用微秒精度，InfluxDB 使用纳秒精度。
- 多库查询中，结果时间戳采用参与数据库的最高精度，低精度值补零扩展。例如 TDengine 毫秒库与 MySQL 联合查询得到微秒精度；与 InfluxDB 联合查询得到纳秒精度。

## 外部路径和 USE

### 外部表路径

#### 说明

在查询的 `FROM` 子句中，外部表路径解析到表级别：

| 外部源 | 使用默认命名空间 | 显式指定命名空间 |
| ---------- | --- | --- |
| MySQL      | `source_name.table` | `source_name.database.table` |
| PostgreSQL | `source_name.table` | `source_name.schema.table` |
| InfluxDB   | `source_name.table` | `source_name.database.table` |

两段式路径使用创建外部数据源时指定的默认 `DATABASE` 或 `SCHEMA`；未设置默认值时必须使用三段式完整路径。

### 虚拟表外部列路径

#### 说明

虚拟表 DDL 中的外部列引用在表路径后追加列名：

| 外部源 | 使用默认命名空间 | 显式指定命名空间 |
| ---------- | --- | --- |
| MySQL      | `source_name.table.column` | `source_name.database.table.column` |
| PostgreSQL | `source_name.table.column` | `source_name.schema.table.column` |
| InfluxDB   | `source_name.table.column` | `source_name.database.table.column` |

**注意事项**：内部列路径仍为 `table.column` 或 `db.table.column`。外部路径的 `source_name`、`database`、`schema` 和 `column` 最大为 64 字节，`table` 最大为 192 字节；超限对象会直接报错，不会截断、改名或按前缀匹配。

**说明**：三段式路径 `A.B.C` 在虚拟表 DDL 中按首段消歧：首段匹配已注册外部数据源时，解析为 `source_name.table.column`；首段匹配本地数据库时，解析为 `db.table.column`。外部数据源名称不得与本地数据库同名，因此不会产生冲突。查询 `FROM` 子句中的三段式路径始终解析为 `source_name.{database|schema}.table`。

```sql
-- 查询外部表
SELECT * FROM mysql_prod.meters;
SELECT * FROM mysql_prod.power.meters;
SELECT * FROM pg_prod.devices;
SELECT * FROM pg_prod.public.devices;

-- 虚拟表 DDL 中引用外部列
current FLOAT FROM mysql_prod.meters.current
current FLOAT FROM mysql_prod.power.meters.current
owner VARCHAR(64) FROM pg_prod.public.meter_asset.owner
```

### 使用 USE 切换外部命名空间

#### 语法

`USE` 可将当前会话切换到外部数据源命名空间，使后续查询使用单段表名：

```sql
USE source_name;
USE source_name.database;
USE source_name.schema;
```

#### 说明

`USE source_name` 要求外部源已经设置默认命名空间：MySQL 和 InfluxDB 需要 `DATABASE`，PostgreSQL 需要 `SCHEMA`。未设置时返回 `TSDB_CODE_EXT_DEFAULT_NS_MISSING`。`USE source_name.database` 为 MySQL 或 InfluxDB 显式指定数据库；`USE source_name.schema` 为 PostgreSQL 显式指定 schema，PostgreSQL 的数据库仍以创建外部源时指定的 `DATABASE` 为准。

**注意事项**：解析 `USE` 的名称时，系统先匹配已注册外部数据源，再匹配本地数据库；由于两者禁止同名，不会产生歧义。

#### 示例

```sql
-- 使用 MySQL 默认数据库
USE mysql_prod;
SELECT * FROM meters LIMIT 10;

-- 显式选择 MySQL 数据库
USE mysql_prod.power;
SELECT * FROM meters LIMIT 10;

-- 显式选择 PostgreSQL schema
USE pg_prod.public;
SELECT * FROM devices LIMIT 10;

-- 切回本地数据库并清除外部上下文
USE power;
SELECT * FROM meters LIMIT 10;
```

## 查询外部数据

### 查询示例

```sql
-- 单一外部源查询
SELECT ts, current, voltage
FROM mysql_prod.power_meters
WHERE meter_id = 1001
  AND ts >= '2026-04-01 00:00:00'
  AND ts < '2026-04-02 00:00:00'
ORDER BY ts
LIMIT 1000;

-- 两个均含时间戳主键的外部时序表关联（ON 须包含主时间戳列）
SELECT m.ts, m.current, a.voltage
FROM mysql_prod.meters m
JOIN pg_prod.public.archive_meters a
  ON m.ts = a.ts AND m.meter_id = a.meter_id
WHERE m.ts >= '2026-04-01 00:00:00'
  AND m.ts < '2026-04-02 00:00:00';
```

更多示例如下：

```sql
-- InfluxDB 外部表窗口聚合（按 Tag 组合分组）
SELECT _wstart AS ts,
       location,
       AVG(temperature) AS avg_temp
FROM influx_prod.telegraf.sensor_readings
WHERE ts >= '2026-04-01 00:00:00'
  AND ts < '2026-04-02 00:00:00'
PARTITION BY location
INTERVAL(10m)
ORDER BY ts;

-- 外部时序表分组统计
SELECT location,
       COUNT(*) AS point_cnt,
       AVG(temperature) AS avg_temperature
FROM influx_prod.telegraf.sensor_readings
WHERE ts >= '2026-04-01 00:00:00'
  AND ts < '2026-04-02 00:00:00'
GROUP BY location
ORDER BY point_cnt DESC;

-- 本地库与外部归档库 UNION ALL 统一分析
SELECT ts, meter_id, current
FROM power.meters
WHERE ts >= '2026-03-01 00:00:00'
  AND ts < '2026-03-15 00:00:00'
UNION ALL
SELECT ts, meter_id, current
FROM mysql_prod.power_archive
WHERE ts >= '2026-03-01 00:00:00'
  AND ts < '2026-03-15 00:00:00'
ORDER BY ts;
```

当外部源具备等价语义时，系统优先在源端执行可等价的计算，以减少数据传输；外部源不具备等价语义或源端执行失败时，系统读取必要原始数据并在本地完成计算。执行位置只影响性能，不影响正常完成查询的结果。

### 功能限制

#### TBNAME

外部表不支持 `TBNAME` 伪列，例如 `SELECT TBNAME`、`WHERE TBNAME = ...`、`JOIN ON ... TBNAME` 等。InfluxDB 的 `PARTITION BY TBNAME` 是例外，等价于按全部 Tag 列分组。

#### TAGS

MySQL 和 PostgreSQL 外部表不支持 `SELECT TAGS ...`；InfluxDB 外部表支持 `TAGS`，但仅返回至少有一条数据的 Tag 组合。TDengine 中无数据子表也可返回 Tag 值，二者语义不同。

#### JOIN 约束

查询中一旦引用外部表，**JOIN 的 ON 条件须包含 TIMESTAMP 列**（如 `a.ts = b.ts`）。不能仅用业务键（如 `meter_id`）将本地时序表与无时间线的外部维度表做 JOIN；此类场景应先将维度数据同步到 TDengine，或由外部系统提供带时间戳主键的关联结果。含外部表的两表 JOIN 若 ON 不含 TIMESTAMP 列，将返回 `External source JOIN requires primary timestamp column in ON condition` 错误。

#### 列、Tag 和时间戳约束

外部表或 Measurement 的总列数和总 Tag 数可超过 TDengine 上限。只有当前语句实际引用的列或 Tag 数量超过上限时才报错；`SELECT *`、过滤、分组、排序、关联和函数所引用的列或 Tag 均计入该数量。

外部访问必须通过外部数据源对象完成，路径必须遵循数据源类型约束，TDengine 既有功能限制同样适用于联邦查询。

### 性能退化场景

以下查询可以使用，但在外部源没有等价语义时会读取更多原始数据并在本地计算，大数据量场景可能出现性能退化：

- TDengine 专有时序功能，如部分窗口计算、函数。
- 本地表与外部表、或不同外部源之间的 JOIN。
- ASOF JOIN 和 Window JOIN。
- 标量和聚合 UDF。
- 其他外部源没有直接等价语义的函数、运算符和功能。

**注意事项**：应通过缩小时间范围、增加过滤条件和减少返回列来降低外部读取量；同时检查外部数据库的慢查询和索引状态。

## 虚拟表引用外部列

### 使用说明

虚拟表的列引用可指向外部数据源中的表列。虚拟表本身必须创建在 TDengine 内部数据库中，即使其所有数据列均来自外部源，也必须先创建或 `USE` 一个本地数据库。

查询包含外部引用的虚拟表时，系统读取外部列并与本地列按时间戳归并为统一结果。外部表必须具有可映射为 `TIMESTAMP` 的主键时间列；外部视图不能作为虚拟表列引用。

### 创建虚拟普通表

创建虚拟普通表时，外部列引用语法如下：

```sql
CREATE VTABLE [IF NOT EXISTS] [db_name.]vtb_name
    (create_definition [, create_definition] ...)

create_definition:
    ts_col_name TIMESTAMP
  | vtb_col_name type_name [FROM column_reference]

column_reference:
    [db_name.]table_name.col_name
  | source_name.table_name.col_name
  | source_name.{database|schema}_name.table_name.col_name
```

### 创建虚拟子表

创建虚拟子表时，可在列引用中使用相同的外部路径：

```sql
CREATE VTABLE [IF NOT EXISTS] [db_name.]vtb_name
    (create_definition [, create_definition] ...)
    USING [db_name.]stb_name
    [(tag_name [, tag_name] ...)]
    TAGS (tag_value [, tag_value] ...)

create_definition:
    [stb_col_name FROM] column_reference

tag_value:
    const_value | table_name.tag_name
```

### InfluxDB SERIES 列引用

对于 InfluxDB，可在创建虚拟普通表或虚拟子表时使用 `SERIES` 声明，将一个别名绑定到由 Tag 条件确定的具体 Series：

```sql
SERIES series_alias AS source_name.database_name.measurement_name
    (tag_name = 'tag_value' [, tag_name = 'tag_value'] ...)
```

该语法具有以下约束：

- 仅支持 InfluxDB 外部数据源，不支持 MySQL 或 PostgreSQL。
- `SERIES` 目标必须使用完整的三段式路径 `source_name.database_name.measurement_name`，不能省略外部源名称或数据库名称。
- 必须指定至少一个 Tag 条件。条件仅支持 `tag_name = 'tag_value'`，并且在声明时必须完整且不重复地包含该 Measurement 的全部 Tag；缺少 Tag、包含未知 Tag 或重复指定同一 Tag 均会报错。
- `series_alias.field_name` 表示该 Tag 条件所限定的 Measurement Field。别名在同一虚拟表中必须唯一，且不能通过别名引用 InfluxDB Tag。

以下示例将 `s1` 绑定到 `device = 'd1'` 的 Series；`s1.value` 会展开为带有该 Tag 条件的 `influx_source.metrics.meters.value` 外部列引用：

```sql
CREATE VTABLE v_a1 (
  ts TIMESTAMP,
  value DOUBLE FROM s1.value
)
SERIES s1 AS influx_source.metrics.meters (device = 'd1');
```

可以使用 `ALTER VTABLE` 为已有虚拟表增加 `SERIES`、绑定列、解除绑定并删除 `SERIES`：

```sql
CREATE VTABLE v_a2 (
  ts TIMESTAMP,
  value DOUBLE
);

ALTER VTABLE v_a2
  ADD SERIES s1 AS influx_source.metrics.meters (device = 'd1');

ALTER VTABLE v_a2 ALTER COLUMN value SET s1.value;

ALTER VTABLE v_a2 ALTER COLUMN value SET NULL;
ALTER VTABLE v_a2 REMOVE SERIES s1;
```

`ADD SERIES` 与创建时的 `SERIES` 声明采用相同的目标路径和 Tag 条件约束。删除 `SERIES` 前，必须先通过 `ALTER COLUMN ... SET NULL` 或将列改为其他引用，确保没有虚拟表列继续引用该别名。

### 创建虚拟超级表

虚拟超级表语法保持不变，仅定义 schema，不包含外部列引用：

```sql
CREATE STABLE [IF NOT EXISTS] stb_name
    (create_definition [, create_definition] ...)
    TAGS (create_definition [, create_definition] ...)
    VIRTUAL 1;
```

### 示例

以下示例将本地时序数据和外部 MySQL 时序数据合并到一个虚拟表：

```sql
CREATE EXTERNAL SOURCE meter_mysql
TYPE = 'mysql'
HOST = '10.0.0.1'
PORT = 3306
USER = 'reader'
PASSWORD = 'your_password';

CREATE VTABLE v_d1001 (
  ts TIMESTAMP,
  current FLOAT FROM power.d1001.current,
  voltage INT FROM power.d1001.voltage,
  temperature FLOAT FROM meter_mysql.asset_db.meter_samples.temperature,
  humidity FLOAT FROM meter_mysql.asset_db.meter_samples.humidity
);
```

### 时间戳键要求

虚拟表按时间戳归并内部和外部数据：

- 虚拟表自身须声明 `TIMESTAMP` 列（如 `ts TIMESTAMP`）；该列不能使用 `FROM` 直接引用外部列。
- 引用的 **外部时序表** 须满足[时间戳主键要求](#时间戳主键要求)。
- 外部视图和无时间戳主键的外部维度/台账表不能作为虚拟表列引用。

若虚拟表缺少有效的主时间列，或引用的外部表不满足时间戳主键要求，将无法与 TDengine 时序数据正确对齐。

### 查询行为

含外部引用的虚拟表支持所有标准 TDengine 查询语法，包括普通查询、聚合和窗口聚合：

```sql
SELECT ts, current, voltage, temperature
FROM v_d1001
WHERE ts >= '2026-04-01' AND ts < '2026-04-02'
ORDER BY ts;

SELECT COUNT(*), AVG(current), AVG(temperature)
FROM v_d1001
WHERE ts >= '2026-04-01' AND ts < '2026-04-02'
GROUP BY humidity;

SELECT _wstart, AVG(current), AVG(voltage)
FROM v_d1001
WHERE ts >= '2026-04-01' AND ts < '2026-04-02'
INTERVAL(1h);
```

## EXPLAIN

联邦查询支持使用 `EXPLAIN` 命令进行计划展示与执行过程分析。

```sql
EXPLAIN
SELECT ts, voltage, current
FROM mysql_prod.power.sensor_data
WHERE ts >= '2024-01-01'
ORDER BY ts
LIMIT 1000;
```

输出示例如下：

```text
FederatedScan on mysql_prod.power.sensor_data
  Remote SQL: SELECT ts, voltage, current FROM sensor_data WHERE ts >= '2024-01-01' ORDER BY ts LIMIT 1000
  Output: rows=1000, width=24
```

## 错误码和排查

### 外部源运行时错误

外部源运行时错误如下：

| 错误码 | 说明 |
| ---------------------------------- | --- |
| `TSDB_CODE_EXT_CONNECT_FAILED`     | 外部连接建立失败或连接中断。 |
| `TSDB_CODE_EXT_AUTH_FAILED`        | 外部源认证失败，账号、密码或 Token 无效。 |
| `TSDB_CODE_EXT_ACCESS_DENIED`      | 外部源权限不足。 |
| `TSDB_CODE_EXT_QUERY_TIMEOUT`      | 外部查询或网络调用超时。 |
| `TSDB_CODE_EXT_OBJECT_NOT_FOUND`   | 外部数据库、schema、表或列不存在。 |
| `TSDB_CODE_EXT_SYNTAX_UNSUPPORTED` | 查询语法错误、方言不兼容或使用了外部表不支持的伪列。 |
| `TSDB_CODE_EXT_TYPE_NOT_MAPPABLE`  | 外部列类型无法映射。 |
| `TSDB_CODE_EXT_RESOURCE_EXHAUSTED` | 外部源受到并发、配额、内存或限流等资源限制。 |
| `TSDB_CODE_EXT_TXN_CONFLICT`       | 外部源事务或锁冲突。 |
| `TSDB_CODE_EXT_REMOTE_INTERNAL`    | 外部源内部错误或未分类错误。 |

### 本地检测错误

本地检测错误如下：

| 错误码 | 说明 |
| ----------------------------------- | --- |
| `TSDB_CODE_EXT_SOURCE_NOT_FOUND`    | 引用的外部数据源未注册。 |
| `TSDB_CODE_EXT_CONSTRAINT_VIOLATED` | 外部表不满足 TDengine 约束，例如缺少时间戳主键列。 |
| `TSDB_CODE_EXT_PUSHDOWN_FAILED`     | 外部源执行失败，无法完成相应的源端执行。 |

### 常见问题

常见问题的排查方式如下：

| 问题 | 排查方式 |
| ---------------- | --- |
| 无法连接外部源   | 检查主机、端口、账号、密码或 Token，检查网络、防火墙及目标数据库是否允许当前来源访问。 |
| 路径解析失败     | 检查路径层级、默认 database 或 schema、对象名称拼写和大小写。 |
| 外部对象结构变化 | 执行 `REFRESH EXTERNAL SOURCE source_name` 后重试，并检查类型映射。 |
| 查询超时         | 调整超时和并发配置，缩小时间范围和返回规模，检查外部源慢查询与索引。 |
| 虚拟表创建失败   | 检查外部数据源名称、外部 database、表和列、声明类型兼容性、时间戳主键以及外部连接。 |

## 版本和兼容性

### 外部数据库版本

支持和测试的外部数据库版本如下：

| 数据库 | 支持版本 | 测试版本 | 说明 |
| ---------- | --- | --- | --- |
| MySQL      | 5.7、8.x | 8.0 | 推荐使用 8.0 及以上。MariaDB 10.x 兼容，但不做专项测试。 |
| PostgreSQL | 14 及以上 | 16 | 需要支持标准 SQL 特性集。 |
| InfluxDB   | v3.x | v3.0 | 不支持 v1.x 和 v2.x。 |

### 安装、升级和卸载

**说明**：除 TDengine 主安装包外，还必须安装与其版本匹配的联邦查询依赖库安装包（或下载中心提供的联邦查询插件包）。该包提供外部连接器的运行库，应安装在实际执行联邦查询服务器和 `taosc` 所在的机器上；升级时应与 TDengine 同步升级。

建议按以下顺序检查：

1. 确认 `taosd` 与 `taosc` 均已启用 `federatedQueryEnable`。
2. 确认联邦查询依赖库或插件包版本与 TDengine 版本匹配。
3. 确认目标机器架构受支持（当前为 Linux x64、Linux ARM64）。
4. 执行 `SHOW EXTERNAL SOURCES`、`DESCRIBE EXTERNAL SOURCE ...` 与最小查询样例验证运行时依赖是否可用。

## 使用场景

### 本地和外部时序统一查询

本地库保存近期高频数据，MySQL 保存归档数据时，可以使用 `UNION ALL` 统一分析：

```sql
SELECT ts, meter_id, current, voltage
FROM power.meters
WHERE ts >= '2026-03-10' AND ts < '2026-04-10'

UNION ALL

SELECT ts, meter_id, current, voltage
FROM mysql_prod.power_meters
WHERE ts >= '2026-03-10' AND ts < '2026-03-17'
ORDER BY ts;
```

### 直接分析外部时序数据

无需同步数据，也可对 InfluxDB Measurement 使用 TDengine 的窗口聚合：

```sql
SELECT _wstart AS ts,
       AVG(temperature) AS avg_temp,
       MAX(temperature) AS max_temp,
       MIN(temperature) AS min_temp
FROM influx_prod.telegraf.sensor_readings
WHERE ts >= '2026-04-01' AND ts < '2026-04-08'
INTERVAL(1h)
ORDER BY ts;
```

### 跨系统关联分析

外部参与 `JOIN` 的表须有可映射为 `TIMESTAMP` 的主键时间列，且 `ON` 条件须包含该时间列。例如，可将 TDengine 本地时序与外部 PostgreSQL 时序归档按时间戳和设备标识关联：

```sql
SELECT l.meter_id,
       AVG(l.current * a.voltage) AS avg_power
FROM power.meters l
JOIN pg_prod.public.archive_meters a
  ON l.ts = a.ts AND l.meter_id = a.meter_id
WHERE l.ts >= '2026-04-01' AND l.ts < '2026-04-02'
GROUP BY l.meter_id
ORDER BY avg_power DESC
LIMIT 20;
```
