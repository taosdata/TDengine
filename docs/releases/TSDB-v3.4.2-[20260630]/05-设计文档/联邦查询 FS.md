# TDengine 支持联邦查询 FS

# 修订记录

| 编写日期 | 发布日期 | 版本 | 修订人       | 主要修改内容    |
| --- | --- | --- |-----------|-----------|
| 2026-04-07 | 2026-04-07 | 0.1 | wpan      | 初稿        |
| 2026-04-08 | 2026-04-08 | 0.2 | Joey Sima | 新增虚拟表相关行为 |
| 2026-04-13 | 2026-04-13 | 1.0 | Simon Guan | 评审后发布 |

# 1 背景

在工业与企业场景中，时序数据可能分散在多类系统中，例如 MySQL、PostgreSQL、InfluxDB 与 TDengine 本地集群。当前很多场景需要先做数据同步再统一分析，导致链路变长、成本上升、交付周期拉长。

本特性的目标是让 TDengine 在不搬迁外部数据的前提下，提供统一查询入口，能够支持 TDengine TSDB 的所有查询功能，因此 IDMP 也不需要针对第三方数据库进行额外查询功能适配就可以使用 TSDB 的全部查询功能。

# 2 定义

## 2.1 联邦查询（Federated Query）

在查询执行期访问外部数据源，并与 TDengine 本地数据（如有）在同一查询链路中完成过滤、聚合、排序、关联、窗口等计算。

## 2.2 外部数据源（External Source）

由系统管理的外部访问入口对象，通过创建连接的方式进行定义，一个外部数据源包括静态的外部连接器实现，也包括动态定义的连接相关信息。

## 2.3 外部连接器（External Connector）

按数据库类型实现的适配层，用于完成元数据获取、能力声明、读取执行和结果转换。

## 2.4 外部连接（External Connection）

外部连接定义了访问外部系统的一组配置，包括如何连目标系统、目标库表信息等。

## 2.5 虚拟表外部列引用

虚拟表的列引用（FROM 子句）指向外部数据源上的表列。DDL 中的外部列引用路径遵循外部路径解析规则，支持三段式（使用默认 database/schema）或四段式（显式指定 database/schema），查询时系统自动从外部数据源拉取该列数据。详见 §3.5。

# 3 行为说明

## 3.1 功能范围

### 3.1.1 适用范围

- 暂只支持单独使用查询功能，不支持流计算中使用；
- 不支持订阅使用联邦查询；

### 3.1.2 支持的功能

- 支持 TSDB 除极少数无概念或类型映射外的全部查询功能，虚拟表相关查询功能高优先级。
- 支持 MySQL/PostgreSQL/InfluxDB 做外部源，高优先级支持 MySQL 和 InfluxDB。
- 支持外部数据源对象本身的创建、修改、删除、查看与刷新。
- 支持跨数据源的查询。
- 支持虚拟表列引用外部数据源的表列。
- 支持联邦查询各查询功能进行基础执行优化。
- 支持联邦查询过程的权限、审计和可观测性。

### 3.1.3 不支持的功能

- 外部系统写入。
- 外部源系统中的对象查看与操作。
- 外部源系统具备但是 TSDB 不具备的功能。
- 跨数据源强一致性与事务。

## 3.2 外部对象映射规则

外部数据源中的对象按以下规则映射到 TDengine 概念体系：

### 3.2.1 MySQL

- 数据库（Database）→ TDengine 数据库，一一对应。
- 表（Table）→ 普通表。
- 视图（View）→ 普通表。视图不受时间戳主键约束限制，其结果集可以不包含时间戳列，此时仅支持不依赖时间线的查询操作。
- 索引、存储过程、触发器等不参与联邦查询。

### 3.2.2 PostgreSQL

- 数据库（Database）+ 模式（Schema）→ TDengine 数据库（两层命名空间映射为单一命名空间）。
- 表（Table）→ 普通表。
- 视图（View）、外部表（FDW）、继承表（Inheritance）→ 普通表。视图不受时间戳主键约束限制。
- 索引、序列、触发器等不参与联邦查询。

### 3.2.3 InfluxDB v3

- Database → TDengine 数据库。
- Measurement → 超级表。
- Tag → 标签列，保留索引和分组语义。
- Field → 数据列。
- Tag Set（时间线）→ 子表，每个唯一的 Tag 组合对应一张子表。
- `time` 列 → 时间戳主键，天然满足时间戳主键约束。
- Bucket / Retention Policy 为存储策略，不影响联邦查询。

### 3.2.4 标识符大小写

- MySQL 标识符默认不区分大小写。
- PostgreSQL 标识符默认折叠为小写（引号内保留原始大小写）。
- InfluxDB v3 标识符区分大小写。

## 3.3 类型映射规则

联邦查询执行期需要将外部源列类型转换为 TDengine 内部类型，最终返回给用户的 TDengine 的数据类型，转换遵循以下规则：

- **可精确对应的类型直接映射**：如 MySQL `INT` → TDengine `INT`、PostgreSQL `double precision` → `DOUBLE`、InfluxDB `Float64` → `DOUBLE`。
- **可降级转换的类型允许映射，但可能存在精度或语义损失**：如 `DATE` 补零点转为 `TIMESTAMP`、`TIME` 转为午夜起的毫秒数存入 `BIGINT`、`SET` 序列化为逗号分隔字符串存入 `VARCHAR`、`uuid` 转为 `VARCHAR(36)` 等。降级转换时系统记录日志。
- **完全无法表达的类型拒绝映射**：报错 `TSDB_CODE_EXT_TYPE_MISMATCH`。

关键注意事项：

- `timestamptz` 等带时区类型转换时统一转换为 UTC 时区，丢弃时区信息。
- `JSON` 类型仅支持映射为 TDengine Tag 列；普通数据列中的 JSON 序列化为字符串存入 `NCHAR` / `VARCHAR`。
- `DECIMAL` / `NUMERIC` 当 precision > 38 时截断并记录日志。
- 数组、范围、复合类型等结构化类型序列化为 JSON 字符串存储，结构语义丢失。


## 3.4 外部数据源管理

### 3.4.1 创建外部数据源

#### 3.4.1.1 语法

```sql
CREATE EXTERNAL SOURCE [IF NOT EXISTS] source_name
TYPE = 'tdengine' | 'mysql' | 'postgresql' | 'influxdb'
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

#### 3.4.1.2 字段说明

| 字段 | 是否必填 | 取值/格式 | 说明 |
| --- | --- | --- | --- |
| `IF NOT EXISTS` | 否 | 固定关键字 | 当对象已存在时不报错，返回成功。 |
| `source_name` | 是 | 标识符 | 外部数据源名称，需全局唯一，不允许与 TSDB 中的库名同名。 |
| `TYPE` | 是 | `'tdengine'` / `'mysql'` / `'postgresql'` / `'influxdb'` | 外部源类型，决定外部连接器实现与路径解析规则。其中 `'tdengine'` 为预留扩展，首版不交付。 |
| `HOST` | 是 | 主机名或 IP | 外部数据源地址。 |
| `PORT` | 是 | 整数端口 | 外部数据源监听端口。 |
| `USER` | 是 | 字符串 | 外部数据源访问账号。 |
| `PASSWORD` | 是 | 字符串 | 外部数据源访问密码。 |
| `DATABASE` | 否 | 标识符 | 默认数据库名，未指定时没有默认数据库名，所有查询必须带数据库名。 |
| `SCHEMA` | 否 | 标识符 | 默认 schema 名，未指定时没有默认 schema 名，必要时所有查询必须带 schema 名。 |
| `OPTIONS` | 否 | key-value 列表 | 可选连接参数，用于传递基础字段之外的扩展配置。支持的选项分为通用选项和外部源专属选项，详见 §3.4.1.4。 |

#### 3.4.1.3 其它说明

- 所有标识符规则与现有数据库名、表名相同，即默认限制字符类型且不区分大小写，转义后放宽字符限制且区分大小写。

#### 3.4.1.4 OPTIONS 支持的选项列表

OPTIONS 子句中的所有 key 和 value 均为字符串类型。连接器在解析时按实际语义进行类型转换（如 `'true'` → 布尔值）。未识别的 key 将被忽略并记录警告日志。

**通用选项（所有 TYPE 均适用）：**

| 选项 key | 取值 / 格式 | 默认值 | 说明 |
| --- | --- | --- | --- |
| `tls_enabled` | `true` / `false` | `false` | 是否启用 TLS 加密连接。 |
| `tls_ca_cert` | PEM 文本内容 | 空（使用系统默认 CA） | CA 证书 PEM 内容，用于验证服务端证书。mnode 加密存储，随元数据分发。仅在 `tls_enabled = true` 时生效。 |
| `tls_client_cert` | PEM 文本内容 | 空 | 客户端证书 PEM 内容，用于双向 TLS 认证。加密存储。仅在 `tls_enabled = true` 时生效。 |
| `tls_client_key` | PEM 文本内容 | 空 | 客户端私钥 PEM 内容。加密存储，`SHOW`/`DESCRIBE` 时脱敏显示。仅在 `tls_enabled = true` 时生效。 |
| `connect_timeout_ms` | 正整数 | 沿用全局配置 | 单次连接建立超时（毫秒），覆盖全局 `federatedQueryConnectTimeoutMs` 配置。 |
| `read_timeout_ms` | 正整数 | 沿用全局配置 | 单次查询读取超时（毫秒），覆盖全局 `federatedQueryQueryTimeoutMs` 配置。 |

**MySQL 专属选项：**

| 选项 key | 取值 / 格式 | 默认值 | 说明 |
| --- | --- | --- | --- |
| `charset` | 字符集名称 | `utf8mb4` | 连接字符集，对应 `SET NAMES` 语句。 |
| `ssl_mode` | `disabled` / `preferred` / `required` / `verify_ca` / `verify_identity` | `preferred` | MySQL SSL 连接模式。当同时设置 `tls_enabled = true` 时，`ssl_mode` 不得为 `disabled`。 |

**PostgreSQL 专属选项：**

| 选项 key | 取值 / 格式 | 默认值 | 说明 |
| --- | --- | --- | --- |
| `sslmode` | `disable` / `allow` / `prefer` / `require` / `verify-ca` / `verify-full` | `prefer` | libpq SSL 连接模式。当同时设置 `tls_enabled = true` 时，`sslmode` 不得为 `disable`。 |
| `application_name` | 字符串 | `TDengine` | 设置 PG 会话的 `application_name`，便于在 `pg_stat_activity` 中识别来源。 |
| `search_path` | 字符串 | 空 | 设置 PG 会话的 `search_path`，多个 schema 以逗号分隔。 |

**InfluxDB 专属选项：**

| 选项 key | 取值 / 格式 | 默认值 | 说明 |
| --- | --- | --- | --- |
| `api_token` | 字符串 | 空 | InfluxDB API Token，用于身份认证。InfluxDB v3 必填（此时 `USER`/`PASSWORD` 可留空）。`SHOW`/`DESCRIBE` 时脱敏显示。 |
| `protocol` | `flight_sql` / `http` | `flight_sql` | 与 InfluxDB 通信的协议。`flight_sql` 使用 Arrow Flight SQL (gRPC)，性能更优；`http` 使用 HTTP API，兼容性更广。 |

#### 3.4.1.5 示例

```sql
-- 创建 MySQL 外部数据源，指定默认数据库
CREATE EXTERNAL SOURCE mysql_prod
TYPE = 'mysql'
HOST = '10.0.0.1'
PORT = 3306
USER = 'reader'
PASSWORD = '***'
DATABASE = power;

-- 创建 PostgreSQL 外部数据源，启用 TLS 并指定默认 schema
CREATE EXTERNAL SOURCE pg_prod
TYPE = 'postgresql'
HOST = 'pg.example.com'
PORT = 5432
USER = 'readonly'
PASSWORD = '***'
DATABASE = iot
SCHEMA = public
OPTIONS (
    'tls_enabled' = 'true',
    'tls_ca_cert' = '-----BEGIN CERTIFICATE-----
MIIDrzCCApegAwIBAgIQCDvg...
-----END CERTIFICATE-----',
    'application_name' = 'TDengine-Federation'
);

-- 创建 InfluxDB v3 外部数据源，使用 Token 认证
CREATE EXTERNAL SOURCE IF NOT EXISTS influx_prod
TYPE = 'influxdb'
HOST = 'influx.example.com'
PORT = 8086
USER = 'admin'
PASSWORD = ''
DATABASE = telegraf
OPTIONS (
    'api_token' = 'my-influx-token',
    'protocol' = 'flight_sql',
    'tls_enabled' = 'true'
);
```

### 3.4.2 查看外部数据源列表

```sql
SHOW EXTERNAL SOURCES;
```

#### 3.4.2.1 输出字段说明

| 字段 | 说明 |
| --- | --- |
| `source_name` | 外部数据源名称。 |
| `TYPE` | 外部源类型。 |
| `HOST` | 外部源地址。 |
| `PORT` | 外部源端口。 |
| `USER` | 外部源访问账号。 |
| `PASSWORD` | 外部源访问密码，脱敏显示。 |
| `DATABASE` | 默认数据库名，未配置时为空。 |
| `SCHEMA` | 默认 schema 名，未配置时为空。 |
| `OPTIONS` | 已配置的可选参数（key-value 形式展示，敏感值脱敏）。 |
| `create_time` | 外部源创建时间。 |

#### 3.4.2.2 示例

```sql
SHOW EXTERNAL SOURCES;
```

输出示例：

```
 source_name |    type    |       host        | port | user  | password | database | schema |       create_time       
-------------+------------+-------------------+------+-------+----------+----------+--------+-------------------------
 mysql_prod  | mysql      | 10.0.0.1          | 3306 | reader| ******   | power    |        | 2026-04-07 10:00:00.000
 pg_prod     | postgresql | pg.example.com    | 5432 | readonly| ******  | iot      | public | 2026-04-07 10:05:00.000
 influx_prod | influxdb   | influx.example.com| 8086 | admin | ******   | telegraf |        | 2026-04-07 10:10:00.000
```


### 3.4.3 查看外部数据源定义

```sql
DESCRIBE EXTERNAL SOURCE source_name;
```

#### 3.4.3.1 输出字段说明

| 字段 | 说明 |
| --- | --- |
| `source_name` | 外部数据源名称。 |
| `TYPE` | 外部源类型。 |
| `HOST` | 外部源地址。 |
| `PORT` | 外部源端口。 |
| `USER` | 外部源访问账号。 |
| `PASSWORD` | 外部源访问密码，脱敏显示。 |
| `DATABASE` | 默认数据库定义值，未配置时为空。 |
| `SCHEMA` | 默认 schema 定义值，未配置时为空。 |
| `OPTIONS` | 已配置的可选参数（key-value 形式展示，敏感值脱敏）。 |
| `create_time` | 外部源创建时间。 |

#### 3.4.3.2 示例

```sql
DESCRIBE EXTERNAL SOURCE mysql_prod;
```

输出示例：

```
 source_name | mysql_prod
 type        | mysql
 host        | 10.0.0.1
 port        | 3306
 user        | reader
 password    | ******
 database    | power
 schema      | 
 create_time | 2026-04-07 10:00:00.000
```

### 3.4.4 修改外部数据源

```sql
ALTER EXTERNAL SOURCE source_name
SET HOST = 'mysql-ro.example.com',
    PORT = 3306;
```

#### 3.4.4.1 字段说明

| 字段 | 是否可改 | 说明 |
| --- | --- | --- |
| `source_name` | 否（定位对象） | 指定要修改的外部数据源名称。 |
| `HOST` | 是 | 修改外部源地址。 |
| `PORT` | 是 | 修改外部源端口。 |
| `USER` | 是 | 修改访问账号。 |
| `PASSWORD` | 是 | 修改访问密码。 |
| `DATABASE` | 是 | 修改默认数据库。 |
| `SCHEMA` | 是 | 修改默认 schema。 |
| `OPTIONS` | 是 | 修改可选参数（整体替换）。 |
| `TYPE` | 否 | 不允许修改；需删除后重建。 |

#### 3.4.4.2 示例

```sql
-- 切换到只读从库地址
ALTER EXTERNAL SOURCE mysql_prod
SET HOST = 'mysql-ro.example.com',
    PORT = 3307;

-- 修改访问密码
ALTER EXTERNAL SOURCE pg_prod
SET USER = 'new_reader',
    PASSWORD = '***';

-- 修改默认数据库
ALTER EXTERNAL SOURCE mysql_prod
SET DATABASE = power_v2;
```

### 3.4.5 删除外部数据源

```sql
DROP EXTERNAL SOURCE IF EXISTS source_name;
```

#### 3.4.5.1 字段说明

| 字段 | 是否必填 | 说明 |
| --- | --- | --- |
| `IF EXISTS` | 否 | 对象不存在时不报错。 |
| `source_name` | 是 | 指定要删除的外部数据源名称。 |

#### 3.4.5.2 行为说明

- 若存在活跃查询或被对象引用，删除有可能造成当前查询失败。

#### 3.4.5.3 示例

```sql
-- 删除外部数据源（存在时才删除）
DROP EXTERNAL SOURCE IF EXISTS mysql_prod;

-- 直接删除（不存在时报错）
DROP EXTERNAL SOURCE pg_prod;
```

### 3.4.6 刷新外部源元数据

```sql
REFRESH EXTERNAL SOURCE source_name;
```

用于显式强制刷新外部源相关元数据与缓存信息，适用于外部源有变更时的强制更新处理。

#### 3.4.6.1 示例

```sql
-- 外部 MySQL 表结构变更后，强制刷新元数据缓存
REFRESH EXTERNAL SOURCE mysql_prod;
```

## 3.5 外部路径解析

外部路径在两种上下文中使用：**查询 FROM 子句**（解析到表级别）和**虚拟表 DDL 列引用 FROM 子句**（解析到列级别）。本节统一定义两种上下文的路径格式与消歧规则。

### 3.5.1 外部表路径（查询 FROM 子句）

查询语句中路径解析到表级别，按数据源类型固定：

- MySQL：`source_name.table` 或 `source_name.database.table`。
- PostgreSQL：`source_name.table` 或 `source_name.schema.table`。
- InfluxDB：`source_name.table` 或 `source_name.database.table`。
- TDengine 外部集群：`source_name.table` 或 `source_name.database.table`。

短路径（二段式）使用创建外部数据源时指定的默认 `DATABASE` / `SCHEMA`；未指定默认值时必须使用完整路径（三段式）。

### 3.5.2 外部列路径（虚拟表 DDL 列引用 FROM 子句）

虚拟表 DDL 中列引用路径解析到列级别，在外部表路径基础上追加 `.column`：

- MySQL：`source_name.table.column` 或 `source_name.database.table.column`。
- PostgreSQL：`source_name.table.column` 或 `source_name.schema.table.column`。
- InfluxDB：`source_name.table.column` 或 `source_name.database.table.column`。
- TDengine 外部集群：`source_name.table.column` 或 `source_name.database.table.column`。

短路径（三段式）使用创建外部数据源时指定的默认 `DATABASE` / `SCHEMA`；未指定默认值时必须使用完整路径（四段式）。

内部列引用路径不变：`table.column`（当前库）或 `db.table.column`（跨库）。

### 3.5.3 路径字段说明

| 路径字段 | 说明 |
| --- | --- |
| `source_name` | 外部数据源名称。 |
| `database` | 数据库名，适用于 MySQL、InfluxDB、TDengine。 |
| `schema` | schema 名，适用于 PostgreSQL。 |
| `table` | 目标表名。 |
| `column` | 目标列名，仅用于虚拟表 DDL 列引用。 |

### 3.5.4 三段式消歧规则

三段式路径 `A.B.C` 在不同上下文中含义不同，按以下规则消歧：

1. **按上下文确定解析目标：**
   - 查询 FROM 子句 → 路径解析到**表**级别，三段式为 `source_name.{database|schema}.table`，不存在歧义。
   - 虚拟表 DDL 列引用 FROM 子句 → 路径解析到**列**级别，三段式可能为外部路径 `source_name.table.column` 或内部路径 `db.table.column`，需按第 2 条规则消歧。
2. **按首段名称区分外部与内部（仅虚拟表 DDL 上下文）：**
   - 首段匹配已注册的外部数据源名称 → 外部引用 `source_name.table.column`。
   - 首段匹配本地数据库名称 → 内部引用 `db.table.column`。
   - 由于创建外部数据源时已禁止与本地库同名，两者不会冲突。
   - 首段均不匹配 → 报错。
3. **按数据源 TYPE 确定中间段含义（仅外部路径的三段式表路径和四段式列路径）：**
   - MySQL / InfluxDB / TDengine → 中间段为 `database`。
   - PostgreSQL → 中间段为 `schema`。

### 3.5.5 完整路径格式汇总

| 上下文 | 段数 | 格式 | 适用场景 | 示例 |
| --- | --- | --- | --- | --- |
| 查询 FROM | 二段 | `source.table` | 外部表，使用默认 database/schema | `mysql_prod.meters` |
| 查询 FROM | 三段 | `source.{database\|schema}.table` | 外部表，显式指定 database/schema | `pg_prod.public.devices` |
| 虚拟表 DDL | 二段 | `table.column` | 内部列引用，当前库 | `d1001.current` |
| 虚拟表 DDL | 三段 | `db.table.column` | 内部列引用，跨库 | `power.d1001.current` |
| 虚拟表 DDL | 三段 | `source.table.column` | 外部列引用，使用默认 database/schema | `mysql_prod.meters.current` |
| 虚拟表 DDL | 四段 | `source.{database\|schema}.table.column` | 外部列引用，显式指定 database/schema | `pg_prod.public.devices.temp` |

### 3.5.6 示例

```sql
-- 查询 FROM 子句（表级别）
SELECT * FROM mysql_prod.meters;                -- 二段式，使用默认 database
SELECT * FROM mysql_prod.power.meters;          -- 三段式，显式指定 database
SELECT * FROM pg_prod.devices;                  -- 二段式，使用默认 schema
SELECT * FROM pg_prod.public.devices;           -- 三段式，显式指定 schema
```

```sql
-- 虚拟表 DDL 列引用（列级别）
current   FLOAT FROM power.d1001.current                          -- 内部引用（三段式，跨库）
current   FLOAT FROM mysql_prod.meters.current                    -- 外部引用（三段式，使用默认 database）
current   FLOAT FROM mysql_prod.power.meters.current              -- 外部引用（四段式，显式指定 database）
owner     VARCHAR(64) FROM pg_prod.public.meter_asset.owner       -- 外部引用（四段式，显式指定 schema）
```

## 3.6 联邦查询行为

### 3.6.1 行为说明

- 允许本地、外部源、多个外部源在同一查询中组合。
- 所有查询语句遵循 TDengine 语义。

### 3.6.2 单源查询示例

```sql
SELECT ts, current, voltage
FROM mysql_prod.power_meters
WHERE meter_id = 1001
  AND ts >= '2026-04-01 00:00:00'
  AND ts <  '2026-04-02 00:00:00'
ORDER BY ts
LIMIT 1000;
```

### 3.6.3 本地与外部联合查询示例

```sql
SELECT l.ts, l.current, e.device_name
FROM power.meters l
JOIN pg_prod.public.device_info e
  ON l.meter_id = e.meter_id
WHERE l.ts >= '2026-04-01 00:00:00'
  AND l.ts <  '2026-04-02 00:00:00';
```

### 3.6.4 多外部源联合查询示例

```sql
SELECT l.ts, l.current, e.device_name
FROM mysql_prod.meters l
JOIN pg_prod.public.device_info e
  ON l.meter_id = e.meter_id
WHERE l.ts >= '2026-04-01 00:00:00'
  AND l.ts <  '2026-04-02 00:00:00';
```

## 3.7 查询功能支持规则

### 3.7.1 总体原则

除极少数概念或类型无法对应的场景外，其他 TDengine 查询功能在外部表上均可使用，查询结果遵循 TDengine 语义。系统采用源端执行与本地执行结合的策略：

- **优先源端执行**：当外部源具备等价语义时，系统将部分或全部查询逻辑下推到外部源执行，减少数据传输量。
- **不可源端执行则本地执行**：当外部源不具备等价语义或源端执行失败时，系统从外部源拉取必要的原始数据，在本地完成全部计算。
- **执行位置仅影响性能，不影响正确性**：用户无需关心具体的执行位置决策，查询结果始终一致。

### 3.7.2 不支持的功能

#### 3.7.2.1 `TBNAME` 伪列

外部表上使用 `TBNAME` 将报错。

原因：`TBNAME` 是 TDengine 超级表模型中返回子表名的伪列，MySQL / PostgreSQL 无超级表概念，InfluxDB 虽有 Tag Set 对应子表但无子表名概念，三方均无法提供 `TBNAME` 值。

涉及场景：`SELECT TBNAME ...`、`WHERE TBNAME = ...`、`PARTITION BY TBNAME`（MySQL / PostgreSQL）、`JOIN ON ... TBNAME` 等所有引用 `TBNAME` 的查询。

例外：InfluxDB 上 `PARTITION BY TBNAME` 可用——系统将其转换为按所有 Tag 列分组。

#### 3.7.2.2 `TAGS` 关键字查询

MySQL / PostgreSQL 外部表上使用 `SELECT TAGS ...` 将报错。

原因：`TAGS` 查询是 TDengine 超级表模型的专有操作，MySQL / PostgreSQL 无标签元数据。

例外：InfluxDB 外部表上可用，但存在语义差异——TDengine 中即使子表无数据也会返回其 Tag 值，而 InfluxDB 仅返回至少有一条数据的 Tag 组合。

#### 3.7.2.3 不可映射的外部列类型

当外部列类型无法映射为任何 TDengine 类型时，引用该列将报错 `TSDB_CODE_EXT_TYPE_MISMATCH`。

### 3.7.3 性能退化场景

以下功能可正常使用且结果正确，但因外部源无等价语义，需从外部源拉取原始数据后在本地计算，数据量大时可能出现性能退化，用户可通过缩小时间范围、增加过滤条件等方式减少拉取数据量以改善性能：

- **TDengine 专有时序功能**：所有窗口类型（状态窗口、会话窗口、事件窗口、计数窗口）、滑动时间窗口、`FILL`、`INTERP`、`SLIMIT` / `SOFFSET`、时序计算函数（`CSUM`、`DERIVATIVE`、`DIFF`、`IRATE`、`TWA` 等）、`FIRST`、`LAST` 等。这类功能依赖 TDengine 专有的时间线有序计算语义，标准 SQL 数据库无等价表达。
- **跨源 JOIN**：左右表来自不同外部源或本地表与外部表混合 JOIN 时，系统分别拉取各侧数据后在本地执行 JOIN。
- **ASOF Join / Window Join**：TDengine 专有时序 JOIN 语义，所有外部源上均在本地执行。
- **UDF（用户自定义函数）**：标量和聚合 UDF 均在本地执行。
- **其他**：其他部分无直接支持的函数、运算符、功能等。

## 3.8 虚拟表引用外部数据源

### 3.8.1 概述

虚拟表的列引用支持指向外部数据源上的表列。在现有虚拟表 DDL 中通过外部路径引用即可将某一列映射到外部数据源的某张表的某一列，具体路径格式与消歧规则详见 §3.5。查询时系统自动从外部数据源拉取数据，并与 TDengine 本地数据按时间戳归并为统一结果。

**注意：虚拟表必须创建在 TDengine 内部库中。** 虚拟表是依托于 TSDB 本地库的表对象，即使所有数据列都引用外部数据源，虚拟表本身仍必须归属于一个 TDengine 内部数据库。用户需要先 `CREATE DATABASE` 或 `USE` 一个已有库，再在该库中创建虚拟表。不能只有外部数据源而没有内部库。

### 3.8.2 虚拟表引用外部列

#### 3.8.2.1 语法

在现有虚拟表语法的基础上，列引用的 `FROM` 子句扩展为支持外部路径引用，具体格式与消歧规则详见 §3.5。

##### 3.8.2.1.1 创建虚拟普通表

```sql
CREATE VTABLE [IF NOT EXISTS] [db_name.]vtb_name
    (create_definition [, create_definition] ...)

create_definition:
    ts_col_name TIMESTAMP
  | vtb_col_name type_name [FROM column_reference]

column_reference:
    [db_name.]table_name.col_name                                -- 内部引用（二段或三段式）
  | source_name.table_name.col_name                              -- 外部引用（三段式，使用默认 database/schema）
  | source_name.{database|schema}_name.table_name.col_name       -- 外部引用（四段式，显式指定 database/schema）
```

##### 3.8.2.1.2 创建虚拟子表

```sql
CREATE VTABLE [IF NOT EXISTS] [db_name.]vtb_name
    (create_definition [, create_definition] ...)
    USING [db_name.]stb_name
    [(tag_name [, tag_name] ...)]
    TAGS (tag_value [, tag_value] ...)

create_definition:
    [stb_col_name FROM] column_reference

column_reference:
    [db_name.]table_name.col_name                                -- 内部引用
  | source_name.table_name.col_name                              -- 外部引用（三段式）
  | source_name.{database|schema}_name.table_name.col_name       -- 外部引用（四段式）

tag_value:
    const_value | table_name.tag_name
```

##### 3.8.2.1.3 创建虚拟超级表

虚拟超级表的语法不变，只定义 schema，不包含列引用。

```sql
CREATE STABLE [IF NOT EXISTS] stb_name
    (create_definition [, create_definition] ...)
    TAGS (create_definition [, create_definition] ...)
    VIRTUAL 1

create_definition:
    col_name type_name
```

##### 3.8.2.1.4 引用格式区分

列引用路径的完整格式与消歧规则详见 §3.5。

#### 3.8.2.2 示例——混合引用

时序数据来自 TDengine 内部表，台账数据来自外部 MySQL：

```sql
CREATE EXTERNAL SOURCE meter_mysql
TYPE = 'mysql'
HOST = '10.0.0.1'
PORT = 3306
USER = 'reader'
PASSWORD = '***';

CREATE VTABLE v_d1001 (
  ts        TIMESTAMP,
  current   FLOAT        FROM power.d1001.current,
  voltage   INT          FROM power.d1001.voltage,
  owner     VARCHAR(64)  FROM meter_mysql.asset_db.meter_asset.owner,
  addr      VARCHAR(128) FROM meter_mysql.asset_db.meter_asset.install_addr
);
```

### 3.8.3 外部表时间戳主键要求

被引用的外部表必须具备时间戳语义的主键或索引列，否则无法与 TDengine 的时序数据按时间戳对齐。

- MySQL：类型为 `DATETIME` 或 `TIMESTAMP` 的 `PRIMARY KEY` 或 `UNIQUE INDEX` 列。
- PostgreSQL：类型为 `TIMESTAMP` 或 `TIMESTAMPTZ` 的 `PRIMARY KEY` 列。
- 如果外部表有多个时间戳列，使用 `PRIMARY KEY` 列。

```sql
-- ✓ 合法的外部表结构：
CREATE TABLE sensor_data (
                             ts DATETIME PRIMARY KEY,
                             temperature FLOAT,
                             humidity FLOAT
);

-- ✗ 不合法的外部表结构（DDL 创建虚拟表时将报错）：
CREATE TABLE user_info (
                           id INT PRIMARY KEY,           -- 主键是整数，非时间戳
                           name VARCHAR(64),
                           email VARCHAR(128)
);
```

### 3.8.4 查询行为

#### 3.8.4.1 基本查询

含外部引用的虚拟表支持所有标准 TDengine 查询语法：

```sql
-- 简单查询
SELECT ts, current, voltage, owner
FROM v_d1001
WHERE ts >= '2026-04-01' AND ts < '2026-04-02'
ORDER BY ts;

-- 聚合查询
SELECT COUNT(*), AVG(current), owner
FROM v_d1001
WHERE ts >= '2026-04-01' AND ts < '2026-04-02'
GROUP BY owner;

-- 窗口聚合
SELECT _wstart, AVG(current), AVG(voltage)
FROM v_d1001
WHERE ts >= '2026-04-01' AND ts < '2026-04-02'
    INTERVAL(1h);
```

## 3.9 新增系统表

在系统库 `information_schema` 中新增系统表 `ins_ext_sources`，用于存储所有外部数据源的定义信息。

### 3.9.1 information_schema.ins_ext_sources

| 列名 | 类型 | 说明 |
| --- | --- | --- |
| `source_name` | VARCHAR | 外部数据源名称，全局唯一。 |
| `type` | VARCHAR | 外部源类型：`mysql` / `postgresql` / `influxdb` / `tdengine`。 |
| `host` | VARCHAR | 外部数据源地址（主机名或 IP）。 |
| `port` | INT | 外部数据源监听端口。 |
| `user` | VARCHAR | 外部数据源访问账号（仅管理员可见，普通用户查询返回 NULL）。 |
| `password` | VARCHAR | 外部数据源访问密码，始终脱敏显示为 `******`（仅管理员可见列，普通用户查询返回 NULL）。 |
| `database` | VARCHAR | 默认数据库名，未配置时为空。 |
| `schema` | VARCHAR | 默认 schema 名，未配置时为空。 |
| `options` | VARCHAR | 可选参数，JSON 格式存储（敏感值脱敏）。 |
| `create_time` | TIMESTAMP | 外部源创建时间。 |

**权限说明：**
- 普通用户可查询此表，但 `user` 和 `password` 列对非管理员用户返回 NULL。
- `password` 列始终脱敏显示，即使管理员查询也显示为 `******`。


## 3.10 新增配置字段

新增配置项详细说明如下：

| 参数名 | 说明 | 类型 | 默认值 | 最小值 | 最大值 | 参数类型 | 作用端 | 动态修改 |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `federatedQueryEnable` | 联邦查询总开关，关闭后不支持联邦查询。 | 布尔 | `false` | - | - | 全局配置参数 | 两端参数 | 支持 |
| `federatedQueryConnectTimeoutMs` | 外部连接建立超时时间，单位毫秒。 | 整数 | `30000` | `100` | `600000` | 全局配置参数 | 服务端参数 | 支持 |
| `federatedQueryMetaCacheTtlSeconds` | 外部元数据缓存有效期，单位秒。 | 整数 | `300` | `1` | `86400` | 全局配置参数 | 两端参数 | 支持 |
| `federatedQueryCapabilityCacheTtlSeconds` | 外部源能力画像缓存有效期，单位秒。 | 整数 | `300` | `1` | `86400` | 全局配置参数 | 服务端参数 | 支持 |



## 3.11 出错处理行为

联邦查询使用 TDengine 统一错误码族，分为外部源运行时错误和本地检测错误两类，确保行为稳定且便于运维统计，外部源的具体的错误信息可在错误消息中获得。

**外部源运行时错误：**

| 错误码 | 说明 |
| --- | --- |
| TSDB_CODE_EXT_CONNECT_FAILED | 外部连接建立失败或连接中断。 |
| TSDB_CODE_EXT_AUTH_FAILED | 外部源认证失败（账号/密码/令牌无效）。 |
| TSDB_CODE_EXT_ACCESS_DENIED | 外部源权限不足。 |
| TSDB_CODE_EXT_QUERY_TIMEOUT | 外部查询或网络调用超时。 |
| TSDB_CODE_EXT_OBJECT_NOT_FOUND | 外部数据库/schema/表/列等对象不存在。 |
| TSDB_CODE_EXT_SYNTAX_UNSUPPORTED | 查询语法错误、方言不兼容或使用了外部表不支持的伪列。 |
| TSDB_CODE_EXT_TYPE_MISMATCH | 类型不匹配或类型转换失败。 |
| TSDB_CODE_EXT_RESOURCE_EXHAUSTED | 外部源资源受限（并发、配额、内存、限流等）。 |
| TSDB_CODE_EXT_TXN_CONFLICT | 外部源事务或锁冲突。 |
| TSDB_CODE_EXT_REMOTE_INTERNAL | 外部源内部错误或未分类错误。 |

**本地检测错误：**

| 错误码 | 说明 |
| --- | --- |
| TSDB_CODE_EXT_SOURCE_NOT_FOUND | 引用的外部数据源在本地未注册。 |
| TSDB_CODE_EXT_SOURCE_UNAVAILABLE | 外部数据源当前不可用（已知故障，尚未尝试连接）。 |
| TSDB_CODE_EXT_CONSTRAINT_VIOLATED | 外部表不满足 TDengine 约束（如缺少时间戳主键列）。 |
| TSDB_CODE_EXT_PUSHDOWN_FAILED | 下推执行失败，客户端可发起禁用下推的重规划请求。 |

## 3.12 接口与界面行为

### 3.12.1 用户接口

当前版本以查询语句为主，不新增独立编程接口。

### 3.12.2 界面行为

- 通过 SQL 支持外部数据源查看与联邦查询执行。

## 3.13 发版与部署行为

- 本特性为企业版功能。

# 4 性能

本特性的性能影响主要来自：

- 外部访问网络时延。
- 外部结果转换为内部结果块的成本。
- 外部源的查询能力支持情况。
- 多源关联时的数据重排与汇聚开销。

性能目标：

- 在可下推场景下显著减少远端读取量。

若当前部署无需外部访问，本特性可保持关闭，对本地性能无影响。

# 5 安全

安全要求如下：

- 密码必须加密保存，展示与日志必须脱敏。
- 权限校验覆盖对象管理与查询执行全过程。
- 外部通信支持加密传输与证书校验。
- 错误路径必须避免敏感信息泄露。
- 对异常返回数据要做边界校验，避免崩溃和资源泄漏。

# 6 兼容性

本特性不应破坏既有本地查询行为。

兼容性要求：

- 默认关闭时，现有行为不变。
- 开启后仅新增对象与行为，不改变原有对象语义。
- 不支持语义必须显式报错，禁止返回不确定结果。

# 7 运维

运维影响如下：

- 需要维护外部数据源配置与密码。
- 需要持续观察连接状态、超时率、失败率、缓存命中率。
- 需要维护外部连接器版本矩阵与兼容性说明。
- 多节点环境需校验外部连接器一致性。

# 8 使用场景

## 8.1 场景一：本地与外部时序统一查询

用户在一个查询语句中同时访问 TDengine 本地数据与外部数据，避免重复同步。

```sql
-- TDengine 本地存储最近 7 天的高频采集数据，
-- MySQL 存储历史归档数据。
-- 通过 UNION ALL 统一查询一个月内的设备数据。

SELECT ts, meter_id, current, voltage
FROM power.meters
WHERE ts >= '2026-03-10' AND ts < '2026-04-10'

UNION ALL

SELECT ts, meter_id, current, voltage
FROM mysql_prod.power_meters
WHERE ts >= '2026-03-10' AND ts < '2026-03-17'
ORDER BY ts;
```

## 8.2 场景二：外部数据快速分析

用户按时间范围直接分析外部时序数据，并复用本地聚合与排序能力。

```sql
-- 直接对 InfluxDB 中的传感器数据做聚合分析，
-- 无需预先同步到 TDengine。

SELECT _wstart AS ts,
       AVG(temperature) AS avg_temp,
       MAX(temperature) AS max_temp,
       MIN(temperature) AS min_temp
FROM influx_prod.telegraf.sensor_readings
WHERE ts >= '2026-04-01' AND ts < '2026-04-08'
INTERVAL(1h)
ORDER BY ts;
```

## 8.3 场景三：跨系统关联分析

本地事实数据与外部维度数据在查询期关联，支持设备、组织、资产等联合分析。

```sql
-- TDengine 本地存储实时采集的电力数据，
-- PostgreSQL 存储设备台账与区域信息。
-- 查询期关联，按区域统计平均功率。

SELECT e.region, e.device_name,
       AVG(l.current * l.voltage) AS avg_power
FROM power.meters l
JOIN pg_prod.public.device_info e
  ON l.meter_id = e.meter_id
WHERE l.ts >= '2026-04-01' AND l.ts < '2026-04-02'
GROUP BY e.region, e.device_name
ORDER BY avg_power DESC
LIMIT 20;
```

## 8.4 场景四：诊断与治理

运维通过可观测指标和审计日志定位下推失败、超时和权限问题。

```sql
-- 查看所有已注册的外部数据源及其配置状态
SHOW EXTERNAL SOURCES;

-- 通过系统表查看特定外部源详情
SELECT source_name, type, host, port, database, create_time
FROM information_schema.ins_ext_sources
WHERE type = 'mysql';

-- 查看外部数据源的表定义，确认元数据可达
DESCRIBE EXTERNAL SOURCE mysql_prod;
```

## 8.5 场景五：虚拟表融合本地时序与外部台账

通过虚拟表将 TDengine 本地的实时采集数据（电流、电压、温度等）与外部 MySQL/PostgreSQL 中的设备台账数据（设备名称、安装地址、负责人等）融合为一张统一的逻辑表。用户查询时无需感知数据的物理存储位置。

```sql
CREATE EXTERNAL SOURCE asset_mysql
TYPE = 'mysql' HOST = '10.0.0.1' PORT = 3306
USER = 'reader' PASSWORD = '***';

CREATE VTABLE v_d1001 (
  ts        TIMESTAMP,
  current   FLOAT        FROM power.d1001.current,
  voltage   INT          FROM power.d1001.voltage,
  owner     VARCHAR(64)  FROM asset_mysql.asset_db.meter_asset.owner,
  addr      VARCHAR(128) FROM asset_mysql.asset_db.meter_asset.install_addr
);

SELECT ts, current, voltage, owner, addr
FROM v_d1001
WHERE ts >= '2026-04-01' AND ts < '2026-04-02';
```

# 9 约束和限制

## 9.1 约束

- 外部访问必须通过外部数据源对象完成。
- 路径解析必须遵循数据源类型约束。
- TSDB 自身功能限制和约束同等适用于联邦查询。

## 9.2 限制

- 不支持外部源DDL 操作、写入、事务、非查询语句。
- 外部源中参与查询的表必须有且只有一个可转换为 TDengine `TIMESTAMP` 类型的主键列；视图不受此约束，但不含时间戳列的视图仅支持不依赖时间线的查询操作。
- 部分复杂查询在外部源能力不足时会性能退化。
- `TBNAME` 伪列在外部表上不可用。
- `TAGS` 关键字查询仅 InfluxDB 外部源可用（MySQL / PostgreSQL 不支持）。
- 类型无法映射的外部列不可参与查询。
- 外部源数据库有明确的版本支持限制，只支持和测试部分外部源版本。参考《安装和卸载一节》。

# 10 常见错误和排查

## 10.1 外部连接失败

排查方向：

- 检查主机、端口、账号、密码是否正确。
- 检查网络连通性与防火墙策略。
- 检查目标数据库是否允许当前来源访问。

## 10.2 路径解析失败

排查方向：

- 检查路径层级是否符合数据源类型规则。
- 检查默认命名空间是否配置完整。
- 检查对象名拼写与大小写。

## 10.3 结构不一致

排查方向：

- 检查外部对象是否发生结构变化。
- 执行 `REFRESH EXTERNAL SOURCE ...` 后重试。
- 检查类型映射是否可用。

## 10.4 查询超时

排查方向：

- 调整超时和并发配置。
- 缩小时间范围和返回规模。
- 检查外部源慢查询与索引状态。

## 10.5 虚拟表创建失败

排查方向：

- 检查外部列引用路径中的外部数据源名称是否正确（`SHOW EXTERNAL SOURCES`）。
- 检查外部 database、表、列是否存在。
- 检查虚拟表声明的列类型是否与外部列类型兼容。
- 检查外部表是否有时间戳主键。
- 检查是否能连接到外部数据源（网络/密码）。

# 11 可观测性

建议提供以下可观测能力：

- 外部请求次数、失败率、超时率。
- 下推命中率与回退比例。
- 元数据缓存命中率与刷新耗时。
- 按查询标识串联解析、规划、执行、外部连接器日志。
- 展示最近错误与外部源健康状态。

# 12 安装和卸载

安装要求：

- 安装包包含联邦核心模块与外部连接器。
- 初始化脚本完成目录创建、配置落盘、默认校验。
- 升级脚本处理外部数据源元数据迁移。

外部数据库客户端库依赖：

| 数据库 | 客户端库 | 最低版本 | 协议 | 备注 |
| --- | --- | --- | --- | --- |
| MySQL | libmysqlclient | 5.7+ | TCP/SSL | 推荐 8.0+ |
| PostgreSQL | libpq | 9.6+ | TCP/SSL | |
| InfluxDB | HTTP API / Apache Arrow Flight | v2.x+ / v3.x+ | HTTP/gRPC | v3.x 推荐使用 Flight SQL 获得更好性能 |

外部数据源服务端版本要求：

| 数据库 | 支持版本 | 备注 |
| --- | --- | --- |
| MySQL | 5.7、8.0、8.4 | 推荐 8.0+；MariaDB 10.x 兼容但不做专项测试 |
| PostgreSQL | 12、13、14、15、16 | 推荐 14+；需支持标准 SQL 特性集 |
| InfluxDB | v3.x | v3.x 推荐；v1.x v2.x 不支持 |

卸载要求：

- 停止联邦相关服务与任务。
- 清理外部连接器组件与配置。
- 不得误删业务数据。

# 13 文档

需要更新企业版文档和官网文档，至少包括：

- 功能介绍与适用边界。
- 外部数据源语法与路径规则。
- 虚拟表引用外部列的 DDL 语法说明。
- 权限、安全与审计说明。
- 常见错误与排查手册。
- 可观测指标与运维建议。

发布前需完成文档评审与合并。

# 14 参考文档



# 15 附录


