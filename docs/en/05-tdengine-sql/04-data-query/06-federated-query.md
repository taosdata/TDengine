---
sidebar_label: Federated Query
title: Federated Query
toc_max_heading_level: 3
---

Federated Query allows TDengine to read external data sources at query execution time and combine them with local TDengine data in a single SQL statement for filtering, aggregation, sorting, joins, and window computations. Federated Query eliminates the need to migrate external data into TDengine beforehand.

External access is defined through External Source objects. These objects store the connection information required to access external systems and select the appropriate connector for each data source type to retrieve metadata, read data, and convert results.

## Feature Scope

Federated Query is available only for standalone `SELECT` queries and supports MySQL, PostgreSQL, and InfluxDB v3 as external data sources. Except where there is no corresponding concept or types cannot be mapped, external tables support the full range of TDengine query capabilities and can be combined with local tables or tables from other external sources.

### Supported Capabilities

Federated Query supports the following capabilities:

- Creating, viewing, modifying, deleting, and refreshing external data sources.
- Cross-source queries and combined queries of local tables with external tables.
- Virtual tables that reference columns from external data sources.
- Permissions, auditing, and observability throughout query execution.

### Unsupported Scenarios

The following scenarios cannot reference external tables:

- Data subscriptions, such as `CREATE TOPIC ... AS SELECT ... FROM external_table`, or `CREATE TOPIC ... AS DATABASE/STABLE` involving external tables.
- Write subqueries, such as `INSERT INTO local_table SELECT ... FROM external_table`.
- Writes to external systems, DDL operations on external objects, and cross-source strongly consistent transactions.
- Features available in external systems but not in TDengine.

### Compatibility

This feature is available in the Enterprise Edition. When disabled by default, existing local query behavior is unchanged; when enabled, only external data source objects and federated query behavior are added. Unsupported semantics return explicit errors rather than indeterminate results. The current version provides external data source management and federated query capabilities through SQL statements and does not introduce separate programming interfaces.

## Prerequisites

Before use, enable `federatedQueryEnable` on both the client and server. Connection, timeout, connection pool, and cache settings for federated query are managed through component configuration:

- For server parameters, see [taosd federated query configuration](../../12-operations-and-tooling/03-components/01-taosd.md#federated-query-related).
- For client parameters, see [taosc federated query configuration](../../12-operations-and-tooling/03-components/02-taosc.md#federated-query-related).

**Note**: Federated query parameters supported on both client and server should be kept consistent. Whether parameter changes take effect immediately depends on the "Dynamic Modification" section in the component configuration documentation.

### Federated Query Runtime Libraries

Before using Federated Query, you must install the federated query runtime library package that matches your TDengine version on every server that actually executes federated queries. This package provides the runtime libraries required by external connectors: MariaDB Connector/C for MySQL, `libpq` for PostgreSQL, and Apache Arrow Flight SQL runtime libraries for InfluxDB.

**Note**: Without the corresponding runtime libraries, you cannot create or query external data sources of the respective types. When upgrading TDengine, upgrade the federated query runtime library package to a matching version as well.

### Federated Query Plugin Package (Third-Party Runtime Libraries)

In addition to client and server configuration, you can install the Federated Query plugin package (containing third-party runtime libraries) separately to supply external connector dependencies.

- Download name: `TDengine TSDB Federated Query Plugin`.
- Download channel: Available from the download center starting with `v3.4.3.0`.
- Currently supported platforms: Linux x64, Linux ARM64; other platforms are not supported at this time.

The plugin package typically has the following structure after extraction:

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

**Notes**

- `libmariadb.so`, `libpq.so`, and Arrow/Parquet related libraries are runtime dependencies for external connectors.
- After installation, ensure the dynamic library path can be loaded by `taosd` and `taosc` processes.
- Appendix: The current internal download URL (intranet) is `http://192.168.1.131/data/nas/TDengine/smoking/v3.4.2.4.0805/enterprise/tdengine-tsdb-enterprise-fq-runtime-3.4.2.4.0805-linux-x64.tar.gz`.
- This URL is a temporary intranet address; after `v3.4.3.0` is released, use the official link provided by the download center.
- Detailed installation and startup steps will be added later.

### Execution Node Requirements

- Pure federated queries—that is, queries that read only external data sources without reading local TSDB tables—are not affected by `queryPolicy` and must execute on a qnode. Ensure the cluster has qnodes deployed before use; otherwise, queries return an error.
- When a query involves local TSDB tables, local scans are handled by the relevant vnodes; for example, virtual table queries with external column references access their associated vnodes.

### Permissions and Security

Passwords are stored with AES-CBC encryption and masked in display and logs. External source access permissions are controlled by the external database itself: the `USER`, `PASSWORD`, or `api_token` specified when creating an external data source represents the access credentials for that external source, and the external database determines the accessible scope accordingly. Only the root user can manage external data source objects in TDengine.

**Note**: External communication supports encrypted transport and certificate validation. Do not expose plaintext credentials in SQL, logs, or application configuration.

## Creating External Data Sources

Use `CREATE EXTERNAL SOURCE` to create an external data source. Create and alter operations save metadata only and do not verify network connectivity or account credentials; the first actual query establishes the external connection. Connection or authentication failures return the corresponding error.

### Syntax

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

### Supported Types

Available external source types and their namespaces are as follows:

| Type         | Default Namespace              | Description |
| ------------ | ------------------------------ | ----------- |
| `mysql`      | `DATABASE`                     | Access MySQL databases and tables. |
| `postgresql` | `DATABASE` and optional `SCHEMA` | `DATABASE` is required; `SCHEMA` can serve as the default schema. |
| `influxdb`   | `DATABASE`                     | Access InfluxDB databases and Measurements. |

### Field Descriptions and Constraints

| Field           | Required | Value or Limit | Description |
| --------------- | -------- | -------------- | ----------- |
| `IF NOT EXISTS` | No       | Fixed keyword  | Does not return an error if the object already exists. |
| `source_name`   | Yes      | Maximum 64 bytes | External data source name, globally unique, and must not share a name with a local database. |
| `TYPE`          | Yes      | `mysql`, `postgresql`, `influxdb` | External source type, case-insensitive, determines connector and path resolution rules. |
| `HOST`          | Yes      | Hostname or IP, maximum 256 bytes | External data source address; supports full FQDN. |
| `PORT`          | Yes      | `1` to `65535` | External data source port. |
| `USER`          | Yes      | Maximum 128 bytes | External data source access account. |
| `PASSWORD`      | Yes      | Maximum 128 bytes | External data source access password; encrypted when stored and masked when displayed. |
| `DATABASE`      | Optional for MySQL and InfluxDB; required for PostgreSQL | Maximum 64 bytes | Default database. When no default database is set, queries must explicitly specify the database. PostgreSQL connections cannot switch databases after establishment. |
| `SCHEMA`        | No       | Maximum 64 bytes | Default schema. When not set, queries that require it must explicitly specify the schema. |
| `OPTIONS`       | No       | Key maximum 64 bytes, value maximum 4095 bytes, overall JSON maximum 4095 bytes | Connection extension parameters. |

**Note**: All identifiers follow TDengine database and table naming rules: by default, character types are restricted and names are case-insensitive; escaped identifiers relax character restrictions and are case-sensitive.

### OPTIONS Parameters

Keys and values in `OPTIONS` are both strings. Connectors perform type conversion according to parameter semantics, for example converting `'true'` to a boolean value.

#### Common Parameters

All external sources support the following common parameters:

| Parameter            | Value              | Default | Description |
| -------------------- | ------------------ | ------- | ----------- |
| `tls_enabled`        | `true` or `false`  | `false` | Whether to enable TLS encrypted connections. |
| `tls_ca_cert`        | PEM text           | Empty; uses system default CA | Used to verify the server certificate; effective only when `tls_enabled = true`. |
| `tls_client_cert`    | PEM text           | Empty   | Client certificate for mutual TLS; effective only when `tls_enabled = true`; must be configured together with `tls_client_key`. |
| `tls_client_key`     | PEM text           | Empty   | Client private key for mutual TLS; effective only when `tls_enabled = true`; must be configured together with `tls_client_cert`. |
| `connect_timeout_ms` | `0` or `100` to `600000` | Uses global configuration | Single connection establishment timeout in milliseconds. `0` means wait indefinitely; `1` to `99` are invalid values. This value overrides `federatedQueryConnectTimeoutMs`. |
| `read_timeout_ms`    | `0` or `100` to `600000` | Uses global configuration | Single query read timeout in milliseconds. `0` means wait indefinitely; `1` to `99` are invalid values. This value overrides `federatedQueryQueryTimeoutMs`. |

#### MySQL Parameters

MySQL-specific parameters are as follows:

| Parameter  | Value | Default | Description |
| ---------- | ----- | ------- | ----------- |
| `charset`  | Character set name | `utf8mb4` | Connection character set; corresponds to `SET NAMES`. |
| `ssl_mode` | `disabled`, `preferred`, `required`, `verify_ca`, `verify_identity` | `preferred` | MySQL SSL connection mode. Must not be `disabled` when `tls_enabled = true`. |

#### PostgreSQL Parameters

PostgreSQL-specific parameters are as follows:

| Parameter | Value | Default | Description |
| --------- | ----- | ------- | ----------- |
| `sslmode` | `disable`, `allow`, `prefer`, `require`, `verify-ca`, `verify-full` | `prefer` | libpq SSL connection mode. Must not be `disable` when `tls_enabled = true`. |

#### InfluxDB Parameters

InfluxDB-specific parameters are as follows:

| Parameter   | Value | Default | Description |
| ----------- | ----- | ------- | ----------- |
| `api_token` | String | Empty | Required InfluxDB authentication token. When this parameter is set, `USER` and `PASSWORD` may be empty. Masked when displayed. |
| `protocol`  | `flight_sql` or `http` | `flight_sql` | Protocol for communicating with InfluxDB. `flight_sql` uses Arrow Flight SQL (gRPC); `http` uses the HTTP API. Both protocols support common TLS parameters. |

#### Parameter Behavior and Notes

**Note**: Explicitly setting `connect_timeout_ms` or `read_timeout_ms`, including setting them to `0`, takes precedence over global configuration; global configuration is used only when the key is not set.

**Note**: Unknown keys return `TSDB_CODE_PAR_SYNTAX_ERROR` during the DDL phase and are not saved. `ALTER EXTERNAL SOURCE ... SET OPTIONS(...)` merges incrementally; keys not specified retain their original values; setting a value to an empty string `''` deletes any existing key. In `SHOW` and `DESCRIBE`, `password`, `api_token`, `tls_client_cert`, and `tls_client_key` are masked.

#### Examples

The following examples create three external data sources:

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

The following examples illustrate timeout, unknown parameter, and parameter deletion behavior:

```sql
-- Valid: explicitly set to wait indefinitely
ALTER EXTERNAL SOURCE pg_prod
SET OPTIONS('read_timeout_ms' = '0');

-- Invalid: 1 to 99 are not in the allowed range
ALTER EXTERNAL SOURCE pg_prod
SET OPTIONS('read_timeout_ms' = '99');

-- Invalid: unknown parameters return TSDB_CODE_PAR_SYNTAX_ERROR
ALTER EXTERNAL SOURCE mysql_prod
SET OPTIONS('unknown_opt' = 'x');

-- Valid: delete an existing parameter
ALTER EXTERNAL SOURCE mysql_prod
SET OPTIONS('ssl_mode' = '');
```

## Managing External Data Sources

### Viewing the External Data Source List

Use `SHOW EXTERNAL SOURCES` to view registered external data sources:

#### Syntax

```sql
SHOW EXTERNAL SOURCES;
```

#### Returned Fields

Returned fields are as follows:

| Field         | Description |
| ------------- | ----------- |
| `source_name` | External data source name. |
| `TYPE`        | External source type. |
| `HOST`        | External source address. |
| `PORT`        | External source port. |
| `USER`        | External source access account. |
| `PASSWORD`    | External source access password; masked. |
| `DATABASE`    | Default database; empty when not configured. |
| `SCHEMA`      | Default schema; empty when not configured. |
| `OPTIONS`     | Configured key-value parameters; sensitive values masked. |
| `create_time` | External source creation time. |

**Note**: In `SHOW` and `DESCRIBE`, `password`, `api_token`, `tls_client_cert`, and `tls_client_key` are masked.

### Viewing External Data Source Details

Use `DESCRIBE EXTERNAL SOURCE source_name` to view the definition of a specific external data source:

#### Syntax

```sql
DESCRIBE EXTERNAL SOURCE mysql_prod;
```

#### Description

This command returns the same definition fields as `SHOW EXTERNAL SOURCES`; `PASSWORD` and sensitive `OPTIONS` values are always masked.

### Querying the External Data Source System Table

External data source definitions can also be queried from `information_schema.ins_ext_sources`:

#### Example

```sql
SELECT source_name, type, host, port, database, schema, create_time
FROM information_schema.ins_ext_sources
WHERE type = 'mysql';
```

Fields in `information_schema.ins_ext_sources` are as follows:

| Column        | Type      | Description |
| ------------- | --------- | ----------- |
| `source_name` | VARCHAR   | Globally unique external data source name. |
| `type`        | VARCHAR   | External source type. |
| `host`        | VARCHAR   | External data source address. |
| `port`        | INT       | External data source port. |
| `user`        | VARCHAR   | External data source access account; visible to all users. |
| `password`    | VARCHAR   | External data source password; all users see only `******`. |
| `database`    | VARCHAR   | Default database; empty when not configured. |
| `schema`      | VARCHAR   | Default schema; empty when not configured. |
| `options`     | VARCHAR   | Optional parameters in JSON format; sensitive values masked. |
| `create_time` | TIMESTAMP | External source creation time. |

**Note**: All users can query this system table but cannot obtain the original password through it.

### Modifying External Data Sources

Use `ALTER EXTERNAL SOURCE` to modify connection information:

#### Syntax

```sql
ALTER EXTERNAL SOURCE source_name
SET HOST = 'hostname',
    PORT = port_number;
```

#### Modifiable Fields

| Field                             | Modifiable | Description |
| --------------------------------- | ---------- | ----------- |
| `source_name`                     | No         | Used only to locate the external data source. |
| `HOST`, `PORT`, `USER`, `PASSWORD` | Yes        | Modify connection address, port, and access credentials. |
| `DATABASE`, `SCHEMA`              | Yes        | Modify the default namespace. |
| `OPTIONS`                         | Yes        | Incrementally add or overwrite parameters; unspecified keys are retained; empty string deletes a key. |
| `TYPE`                            | No         | Cannot be modified; delete and recreate instead. |

#### Notes

- Alter operations also do not check external connectivity; new configuration is validated on subsequent queries.
- `ALTER EXTERNAL SOURCE ... SET OPTIONS(...)` merges incrementally; keys not specified retain their original values; setting a value to an empty string `''` deletes any existing key.

#### Examples

```sql
-- Switch to a read-only replica
ALTER EXTERNAL SOURCE mysql_prod
SET HOST = 'mysql-ro.example.com',
    PORT = 3307;

-- Modify PostgreSQL account and password
ALTER EXTERNAL SOURCE pg_prod
SET USER = 'new_reader',
    PASSWORD = 'new_password';

-- Modify the default database
ALTER EXTERNAL SOURCE mysql_prod
SET DATABASE = power_v2;

-- Incrementally add read timeout; existing parameters are retained
ALTER EXTERNAL SOURCE mysql_prod
SET OPTIONS('read_timeout_ms' = '3000');
```

### Refreshing External Data Sources

After external object structures change, use `REFRESH EXTERNAL SOURCE` to force a refresh of external metadata and caches:

#### Syntax

```sql
REFRESH EXTERNAL SOURCE mysql_prod;
```

**Note**: After external table structures, column definitions, or other metadata change, refresh before querying again.

### Deleting External Data Sources

Use `DROP EXTERNAL SOURCE` to delete an external data source:

#### Syntax

```sql
DROP EXTERNAL SOURCE IF EXISTS mysql_prod;
```

**Note**: `IF EXISTS` prevents an error when the object does not exist. Deleting an external data source that is being queried or referenced by other objects may cause current queries to fail.

## External Objects and Type Mapping

### External Object Mapping

| External Source | External Object | TDengine Query Object |
| --------------- | --------------- | --------------------- |
| MySQL           | Database        | Database. |
| MySQL           | Table, View     | Regular table. Views may omit a timestamp column; see [External Tables Without a Timeline](#external-tables-without-a-timeline) for rules when no timeline is present. |
| PostgreSQL      | Database and Schema | A single query namespace. |
| PostgreSQL      | Table, View, FDW, Inheritance | Regular table. Views are not subject to timestamp primary key constraints; see [External Tables Without a Timeline](#external-tables-without-a-timeline) for rules when no timeline is present. |
| InfluxDB        | Database        | Database. |
| InfluxDB        | Measurement     | Supertable. |
| InfluxDB        | Tag             | Tag column; retains index and grouping semantics. |
| InfluxDB        | Field           | Data column. |
| InfluxDB        | Tag Set         | Subtable; each unique Tag combination corresponds to one subtable. |
| InfluxDB        | `time`          | Timestamp primary key. |

Indexes, stored procedures, and triggers in MySQL, and indexes, sequences, and triggers in PostgreSQL, do not participate in federated query. InfluxDB Buckets and Retention Policies are storage policies and do not affect federated query.

**Note**: Identifier case follows external database rules: MySQL is case-insensitive by default; PostgreSQL folds to lowercase by default and preserves original case when quoted; InfluxDB is case-sensitive.

### Timestamp Primary Key Requirements

When participating in federated query and performing **timeline-dependent** operations, external tables (Table / Measurement) must have a primary key time column mappable to TDengine `TIMESTAMP`. This column is used for time range filtering, sorting, windows, and time-series functions.

- MySQL timestamp primary key columns must be of type `DATETIME` or `TIMESTAMP`.
- PostgreSQL timestamp primary key columns must be of type `TIMESTAMP` or `TIMESTAMPTZ`.
- The `time` column of an InfluxDB Measurement naturally satisfies this requirement.

**Note**: The timestamp primary key may be one column in a composite primary key; other columns in the composite primary key and other indexes may exist. However, the same external table cannot have a second primary key column mappable to TDengine `TIMESTAMP`. **MySQL / PostgreSQL views (View) are not subject to timestamp primary key constraints**; see the next section for rules when the result set contains no timestamp column.

### External Tables Without a Timeline

Refers to regular tables in MySQL / PostgreSQL that lack a primary key time column mappable to `TIMESTAMP`, or external views whose result sets contain no timestamp column. Typical scenarios include device registries and asset dimension tables. InfluxDB Measurements all contain `time` and are not covered by this section.

| Scenario | Supported | Description |
| -------- | --------- | ----------- |
| Single-table query | **Partially supported** | Supports queries that do not depend on a timeline, such as `COUNT`; external views can be queried directly |
| Used in virtual tables | **Not a supported usage** | Virtual tables with external column references sort by the external table's primary time column and merge by timestamp; external regular tables must have a primary key time column mappable to `TIMESTAMP`; external views cannot be referenced as virtual table columns |
| `JOIN` with timeline tables | **Not supported** | `ON` conditions for external table `JOIN` must include the primary timestamp column; external tables without a timeline have no usable primary timestamp column and cannot be joined on business keys alone |

Examples of **queries that do not depend on a timeline**:

```sql
-- Count rows in an external table without a timestamp primary key
SELECT COUNT(*) FROM pg_prod.public.device_info;

-- Query an external view that contains no timestamp column
SELECT * FROM mysql_prod.v_device_dim LIMIT 100;
```

External tables without a timeline cannot be used with `INTERVAL`, state/session/event/count windows, `FILL`, `INTERP`, time-series difference/integral functions, ASOF JOIN, Window JOIN, or virtual table column references. In the current version, external regular tables with non-timestamp primary keys may pass virtual table DDL validation, but that path does not have tested time-merge semantics and should not be used in production. To associate external dimension data with TDengine time-series data, first provide join results with a timestamp primary key in the external system, or synchronize dimension data into TDengine before joining.

### Type Mapping

#### Mapping Rules

External columns are mapped to TDengine data types at query time according to the following rules:

- Types with exact correspondence are mapped directly, for example MySQL `INT` maps to `INT`, PostgreSQL `double precision` maps to `DOUBLE`, and InfluxDB `Float64` maps to `DOUBLE`.
- Types that can be downgraded are allowed to map, but precision or semantic loss may occur. For example, `DATE` is converted to `TIMESTAMP` at midnight, `TIME` is converted to milliseconds from midnight and stored in `BIGINT`, `SET` is serialized as a comma-separated string, and `uuid` is converted to `VARCHAR(36)`. Downgrade conversions are logged.
- Structured types such as arrays, ranges, composite types, and `hstore` are serialized to JSON or text strings and stored in `NCHAR` or `VARCHAR`; structural semantics are not preserved, and a log entry is recorded.
- Unrecognized or unsupported external type codes result in errors; if `SELECT *` expands to unmappable columns, the entire query fails.

#### Special Types and Precision

**Note**: `JSON`, `JSONB`, and string columns in external sources are uniformly serialized as strings and stored in `NCHAR`; they are not mapped to the TDengine native JSON type. Using JSON subfield operators `->` or `CONTAINS` on such columns returns an error.

When declared length, precision, scale, or container capacity of known external types exceeds TDengine limits, the query does not fail solely due to declared limits. Result columns use the maximum range supported by TDengine; errors occur at execution time only when actual values cannot be represented losslessly—there is no silent truncation, rounding, substitution with `NULL`, or skipping.

Timezone and precision rules for external timestamp columns are as follows:

- Timezone-aware types such as PostgreSQL `timestamptz` and MySQL `TIMESTAMP` are interpreted according to their own timezone.
- Timezone-naive types such as PostgreSQL `timestamp`, MySQL `DATETIME`, and `DATE` are interpreted according to the connection timezone or client timezone.
- In single-database queries, TDengine databases use their `PRECISION`; MySQL and PostgreSQL use microsecond precision; InfluxDB uses nanosecond precision.
- In multi-database queries, result timestamps use the highest precision among participating databases; lower-precision values are zero-padded. For example, a TDengine millisecond database joined with MySQL yields microsecond precision; joined with InfluxDB, nanosecond precision.

## External Paths and USE

### External Table Paths

#### Description

In the `FROM` clause of a query, external table paths resolve to the table level:

| External Source | Using Default Namespace | Explicit Namespace |
| --------------- | ----------------------- | ------------------ |
| MySQL           | `source_name.table`     | `source_name.database.table` |
| PostgreSQL      | `source_name.table`     | `source_name.schema.table` |
| InfluxDB        | `source_name.table`     | `source_name.database.table` |

Two-part paths use the default `DATABASE` or `SCHEMA` specified when creating the external data source; when no default is set, the full three-part path must be used.

### Virtual Table External Column Paths

#### Description

External column references in virtual table DDL append the column name after the table path:

| External Source | Using Default Namespace | Explicit Namespace |
| --------------- | ----------------------- | ------------------ |
| MySQL           | `source_name.table.column` | `source_name.database.table.column` |
| PostgreSQL      | `source_name.table.column` | `source_name.schema.table.column` |
| InfluxDB        | `source_name.table.column` | `source_name.database.table.column` |

**Note**: Internal column paths remain `table.column` or `db.table.column`. For external paths, `source_name`, `database`, `schema`, and `column` have a maximum of 64 bytes; `table` has a maximum of 192 bytes. Objects exceeding these limits return an error directly—there is no truncation, renaming, or prefix matching.

**Note**: Three-part paths `A.B.C` in virtual table DDL are disambiguated by the first segment: when the first segment matches a registered external data source, it resolves to `source_name.table.column`; when it matches a local database, it resolves to `db.table.column`. External data source names must not share a name with a local database, so there is no conflict. Three-part paths in query `FROM` clauses always resolve to `source_name.{database|schema}.table`.

```sql
-- Query external tables
SELECT * FROM mysql_prod.meters;
SELECT * FROM mysql_prod.power.meters;
SELECT * FROM pg_prod.devices;
SELECT * FROM pg_prod.public.devices;

-- Reference external columns in virtual table DDL
current FLOAT FROM mysql_prod.meters.current
current FLOAT FROM mysql_prod.power.meters.current
owner VARCHAR(64) FROM pg_prod.public.meter_asset.owner
```

### Using USE to Switch External Namespaces

#### Syntax

`USE` can switch the current session to an external data source namespace so subsequent queries use single-segment table names:

```sql
USE source_name;
USE source_name.database;
USE source_name.schema;
```

#### Description

`USE source_name` requires the external source to have a default namespace set: `DATABASE` for MySQL and InfluxDB, `SCHEMA` for PostgreSQL. When not set, `TSDB_CODE_EXT_DEFAULT_NS_MISSING` is returned. `USE source_name.database` explicitly specifies a database for MySQL or InfluxDB; `USE source_name.schema` explicitly specifies a schema for PostgreSQL—the database for PostgreSQL remains the `DATABASE` specified when creating the external source.

**Note**: When resolving names in `USE`, the system first matches registered external data sources, then local databases; because the two are prohibited from sharing names, there is no ambiguity.

#### Examples

```sql
-- Use the MySQL default database
USE mysql_prod;
SELECT * FROM meters LIMIT 10;

-- Explicitly select a MySQL database
USE mysql_prod.power;
SELECT * FROM meters LIMIT 10;

-- Explicitly select a PostgreSQL schema
USE pg_prod.public;
SELECT * FROM devices LIMIT 10;

-- Switch back to a local database and clear external context
USE power;
SELECT * FROM meters LIMIT 10;
```

## Querying External Data

### Query Examples

```sql
-- Single external source query
SELECT ts, current, voltage
FROM mysql_prod.power_meters
WHERE meter_id = 1001
  AND ts >= '2026-04-01 00:00:00'
  AND ts < '2026-04-02 00:00:00'
ORDER BY ts
LIMIT 1000;

-- Join two external time-series tables that both have timestamp primary keys (ON must include the primary timestamp column)
SELECT m.ts, m.current, a.voltage
FROM mysql_prod.meters m
JOIN pg_prod.public.archive_meters a
  ON m.ts = a.ts AND m.meter_id = a.meter_id
WHERE m.ts >= '2026-04-01 00:00:00'
  AND m.ts < '2026-04-02 00:00:00';
```

Additional examples:

```sql
-- InfluxDB external table window aggregation (grouped by Tag combination)
SELECT _wstart AS ts,
       location,
       AVG(temperature) AS avg_temp
FROM influx_prod.telegraf.sensor_readings
WHERE ts >= '2026-04-01 00:00:00'
  AND ts < '2026-04-02 00:00:00'
PARTITION BY location
INTERVAL(10m)
ORDER BY ts;

-- Grouped statistics on an external time-series table
SELECT location,
       COUNT(*) AS point_cnt,
       AVG(temperature) AS avg_temperature
FROM influx_prod.telegraf.sensor_readings
WHERE ts >= '2026-04-01 00:00:00'
  AND ts < '2026-04-02 00:00:00'
GROUP BY location
ORDER BY point_cnt DESC;

-- UNION ALL across local and external archive databases for unified analysis
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

When the external source supports equivalent semantics, the system prefers to execute equivalent computations at the source to reduce data transfer. When the external source lacks equivalent semantics or source-side execution fails, the system reads the necessary raw data and completes computation locally. Execution location affects performance only and does not change the results of successfully completed queries.

### Feature Limitations

#### TBNAME

External tables do not support the `TBNAME` pseudo-column, including `SELECT TBNAME`, `WHERE TBNAME = ...`, `JOIN ON ... TBNAME`, and similar usages. `PARTITION BY TBNAME` on InfluxDB is an exception and is equivalent to grouping by all Tag columns.

#### TAGS

MySQL and PostgreSQL external tables do not support `SELECT TAGS ...`; InfluxDB external tables support `TAGS` but return only Tag combinations that have at least one data point. In TDengine, Tag values can be returned even for subtables with no data—the semantics differ.

#### JOIN Constraints

Once an external table is referenced in a query, **JOIN `ON` conditions must include a TIMESTAMP column** (for example, `a.ts = b.ts`). You cannot join a local time-series table with a timeline-free external dimension table using business keys alone (such as `meter_id`). For such scenarios, synchronize dimension data into TDengine first, or have the external system provide join results with a timestamp primary key. A two-table JOIN involving external tables whose `ON` condition does not include a TIMESTAMP column returns the error `External source JOIN requires primary timestamp column in ON condition`.

#### Column, Tag, and Timestamp Constraints

The total number of columns and Tags on an external table or Measurement may exceed TDengine limits. An error is returned only when the number of columns or Tags actually referenced by the current statement exceeds the limit; columns or Tags referenced by `SELECT *`, filtering, grouping, sorting, joins, and functions all count toward this limit.

External access must go through external data source objects; paths must follow data source type constraints; existing TDengine feature limitations apply equally to federated query.

### Performance Degradation Scenarios

The following queries are supported but may read more raw data and compute locally when the external source lacks equivalent semantics, which can cause performance degradation at large data volumes:

- TDengine-specific time-series features, such as certain window computations and functions.
- JOINs between local and external tables, or between different external sources.
- ASOF JOIN and Window JOIN.
- Scalar and aggregate UDFs.
- Other functions, operators, and features without direct equivalents in the external source.

**Note**: Reduce external read volume by narrowing time ranges, adding filter conditions, and reducing returned columns; also check slow queries and index status on the external database.

## Virtual Tables Referencing External Columns

### Usage

Virtual table column references can point to table columns in external data sources. Virtual tables themselves must be created in TDengine internal databases; even when all data columns come from external sources, you must first create or `USE` a local database.

When querying a virtual table with external references, the system reads external columns and merges them with local columns by timestamp into a unified result. External tables must have a primary key time column mappable to `TIMESTAMP`; external views cannot be referenced as virtual table columns.

### Creating Virtual Regular Tables

When creating virtual regular tables, external column reference syntax is as follows:

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

### Creating Virtual Subtables

When creating virtual subtables, the same external paths can be used in column references:

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

### Creating Virtual Supertables

Virtual supertable syntax is unchanged; it defines schema only and does not include external column references:

```sql
CREATE STABLE [IF NOT EXISTS] stb_name
    (create_definition [, create_definition] ...)
    TAGS (create_definition [, create_definition] ...)
    VIRTUAL 1;
```

### Examples

The following example merges local time-series data and external MySQL time-series data into one virtual table:

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

### Timestamp Key Requirements

Virtual tables merge internal and external data by timestamp:

- The virtual table itself must declare a `TIMESTAMP` column (for example, `ts TIMESTAMP`); this column cannot directly reference an external column using `FROM`.
- Referenced **external time-series tables** must satisfy the [Timestamp Primary Key Requirements](#timestamp-primary-key-requirements).
- External views and external dimension/registry tables without a timestamp primary key cannot be referenced as virtual table columns.

If a virtual table lacks a valid primary time column, or referenced external tables do not satisfy timestamp primary key requirements, data cannot be correctly aligned with TDengine time-series data.

### Query Behavior

Virtual tables with external references support all standard TDengine query syntax, including regular queries, aggregation, and window aggregation:

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

Federated Query supports the `EXPLAIN` command for plan display and execution analysis.

```sql
EXPLAIN
SELECT ts, voltage, current
FROM mysql_prod.power.sensor_data
WHERE ts >= '2024-01-01'
ORDER BY ts
LIMIT 1000;
```

Example output:

```text
FederatedScan on mysql_prod.power.sensor_data
  Remote SQL: SELECT ts, voltage, current FROM sensor_data WHERE ts >= '2024-01-01' ORDER BY ts LIMIT 1000
  Output: rows=1000, width=24
```

## Error Codes and Troubleshooting

### External Source Runtime Errors

External source runtime errors are as follows:

| Error Code                         | Description |
| ---------------------------------- | ----------- |
| `TSDB_CODE_EXT_CONNECT_FAILED`     | External connection establishment failed or connection interrupted. |
| `TSDB_CODE_EXT_AUTH_FAILED`        | External source authentication failed; account, password, or token invalid. |
| `TSDB_CODE_EXT_ACCESS_DENIED`      | Insufficient permissions on the external source. |
| `TSDB_CODE_EXT_QUERY_TIMEOUT`      | External query or network call timed out. |
| `TSDB_CODE_EXT_OBJECT_NOT_FOUND`   | External database, schema, table, or column does not exist. |
| `TSDB_CODE_EXT_SYNTAX_UNSUPPORTED` | Query syntax error, dialect incompatibility, or use of pseudo-columns not supported on external tables. |
| `TSDB_CODE_EXT_TYPE_NOT_MAPPABLE`  | External column type cannot be mapped. |
| `TSDB_CODE_EXT_RESOURCE_EXHAUSTED` | External source limited by concurrency, quota, memory, throttling, or other resource constraints. |
| `TSDB_CODE_EXT_TXN_CONFLICT`       | External source transaction or lock conflict. |
| `TSDB_CODE_EXT_REMOTE_INTERNAL`    | Internal or uncategorized error on the external source. |

### Locally Detected Errors

Locally detected errors are as follows:

| Error Code                          | Description |
| ----------------------------------- | ----------- |
| `TSDB_CODE_EXT_SOURCE_NOT_FOUND`    | Referenced external data source is not registered. |
| `TSDB_CODE_EXT_CONSTRAINT_VIOLATED` | External table does not satisfy TDengine constraints, for example missing a timestamp primary key column. |
| `TSDB_CODE_EXT_PUSHDOWN_FAILED`     | External source execution failed; corresponding source-side execution could not be completed. |

### Common Issues

Troubleshooting approaches for common issues:

| Issue | Troubleshooting |
| ----- | --------------- |
| Cannot connect to external source | Check host, port, account, password, or token; check network, firewall, and whether the target database allows access from the current source. |
| Path resolution failure | Check path hierarchy, default database or schema, object name spelling, and case. |
| External object structure changed | Run `REFRESH EXTERNAL SOURCE source_name` and retry; check type mapping. |
| Query timeout | Adjust timeout and concurrency settings; narrow time range and result size; check slow queries and indexes on the external source. |
| Virtual table creation failed | Check external data source name, external database, table and columns, declared type compatibility, timestamp primary key, and external connectivity. |

## Version and Compatibility

### External Database Versions

Supported and tested external database versions are as follows:

| Database   | Supported Versions | Tested Version | Notes |
| ---------- | ------------------ | -------------- | ----- |
| MySQL      | 5.7, 8.x           | 8.0            | Version 8.0 and above recommended. MariaDB 10.x is compatible but not specifically tested. |
| PostgreSQL | 14 and above       | 16             | Requires support for the standard SQL feature set. |
| InfluxDB   | v3.x               | v3.0           | v1.x and v2.x are not supported. |

### Installation, Upgrade, and Uninstall

**Note**: In addition to the main TDengine installation package, you must install the federated query runtime library package (or the Federated Query plugin package from the download center) that matches your TDengine version. This package provides runtime libraries for external connectors and should be installed on every machine that executes federated queries and hosts `taosc`; upgrade it in sync with TDengine.

Verify the following in order:

1. Confirm that `federatedQueryEnable` is enabled on both `taosd` and `taosc`.
2. Confirm that the federated query runtime library or plugin package version matches the TDengine version.
3. Confirm that the target machine architecture is supported (currently Linux x64 and Linux ARM64).
4. Run `SHOW EXTERNAL SOURCES`, `DESCRIBE EXTERNAL SOURCE ...`, and a minimal query example to verify runtime dependencies are available.

## Use Cases

### Unified Query of Local and External Time Series

When the local database stores recent high-frequency data and MySQL stores archived data, use `UNION ALL` for unified analysis:

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

### Direct Analysis of External Time-Series Data

Without synchronizing data, you can apply TDengine window aggregation to InfluxDB Measurements:

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

### Cross-System Correlation Analysis

Tables participating in external `JOIN` must have a primary key time column mappable to `TIMESTAMP`, and `ON` conditions must include that time column. For example, you can correlate local TDengine time series with external PostgreSQL time-series archives by timestamp and device identifier:

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
