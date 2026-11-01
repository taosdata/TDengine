---
sidebar_label: Federated Query
title: Federated Query
toc_max_heading_level: 3
---

Federated Query allows TDengine to read external data sources at query execution time and combine them with local TDengine data in a single SQL statement for filtering, aggregation, sorting, joins, and window computations. Federated Query eliminates the need to migrate external data into TDengine beforehand.

External access is defined through External Source objects. These objects store the connection information required to access external systems and select the appropriate connector for each data source type to retrieve metadata, read data, and convert results.

## Feature Scope

Federated Query is available only for standalone `SELECT` queries and supports MySQL, PostgreSQL, InfluxDB v3, SQL Server (2019 / 2022), and Oracle (11g Release 2 and later) as external data sources. Except where there is no corresponding concept or types cannot be mapped, external tables support the full range of TDengine query capabilities and can be combined with local tables or tables from other external sources.

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

Before using Federated Query, you must install the federated query runtime library package that matches your TDengine version on every server that actually executes federated queries. This package provides the runtime libraries required by external connectors: MariaDB Connector/C for MySQL, `libpq` for PostgreSQL, Apache Arrow Flight SQL runtime libraries for InfluxDB, unixODBC plus Microsoft ODBC Driver 18 for SQL Server for SQL Server, and unixODBC plus Oracle Instant Client for Oracle (two client generations, 19c and 23ai; the installer selects the active one for the server version, see [Version and Compatibility](#external-database-versions)).

**Note**: Without the corresponding runtime libraries, you cannot create or query external data sources of the respective types. When upgrading TDengine, upgrade the federated query runtime library package to a matching version as well.

### Federated Query Plugin Package (Third-Party Runtime Libraries)

In addition to client and server configuration, you can install the Federated Query plugin package (containing third-party runtime libraries) separately to supply external connector dependencies.

- Download name: `TDengine TSDB Federated Query Plugin`.
- Download channel: Available from the download center starting with `v3.4.3.0`.
- Currently supported platforms: Linux x64, Linux ARM64, and Windows x64. The directory layout below is the Linux plugin package.

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
- SQL Server requires unixODBC and Microsoft ODBC Driver 18 for SQL Server, which are shipped with the federated query runtime package. They do not need to be installed separately or registered with the system. If they are missing, creating or querying an external source with `TYPE = 'sqlserver'` returns an external source type unsupported error.
- Oracle requires unixODBC and Oracle Instant Client (19c / 23ai), which are shipped with the federated query runtime package. They do not need to be installed separately or registered with the system. If they are missing, creating or querying an external source with `TYPE = 'oracle'` returns an external source type unsupported error.
- After installation, ensure the dynamic library path can be loaded by `taosd` and `taosc` processes.
- On Windows x64, the MySQL and PostgreSQL client libraries `libmariadb.dll` and `libpq.dll` are placed in the same directory as `taosd`. InfluxDB uses HTTP and does not depend on Arrow Flight SQL. SQL Server uses ODBC. Installation steps for the Windows plugin package are not yet described in this document.
- Appendix: The current internal download URL (intranet) is `http://192.168.1.131/data/nas/TDengine/smoking/v3.4.2.4.0805/enterprise/tdengine-tsdb-enterprise-fq-runtime-3.4.2.4.0805-linux-x64.tar.gz`.
- This URL is a temporary intranet address; after `v3.4.3.0` is released, use the official link provided by the download center.
- Detailed installation and startup steps will be added later.

### Execution Node Requirements

- Pure federated queries—that is, queries that read only external data sources without reading local TSDB tables—are not affected by `queryPolicy` and must execute on a qnode. Ensure the cluster has qnodes deployed before use; otherwise, queries return an error.
- When a query involves local TSDB tables, local scans are handled by the relevant vnodes; for example, virtual table queries with external column references access their associated vnodes.

### Permissions and Security

Passwords are stored with AES-CBC encryption and masked in display and logs. External source access permissions are controlled by the external database itself: the `USER`, `PASSWORD`, or `api_token` specified when creating an external data source represents the access credentials for that external source, and the external database determines the accessible scope accordingly. Only the root user can manage external data source objects in TDengine. SQL Server external sources support SQL Server Authentication only; Windows integrated authentication, Kerberos, NTLM, and Microsoft Entra ID are not supported. Oracle external sources support database account and password authentication only; operating-system authentication, Kerberos, and other external authentication methods are not supported.

**Note**: External communication supports encrypted transport and certificate validation. Do not expose plaintext credentials in SQL, logs, or application configuration.

## Creating External Data Sources

Use `CREATE EXTERNAL SOURCE` to create an external data source. Create and alter operations save metadata only and do not verify network connectivity or account credentials; the first actual query establishes the external connection. Connection or authentication failures return the corresponding error.

### Syntax

```sql
CREATE EXTERNAL SOURCE [IF NOT EXISTS] source_name
TYPE = 'mysql' | 'postgresql' | 'influxdb' | 'sqlserver' | 'oracle'
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
| `sqlserver`  | `DATABASE` and `SCHEMA`, both optional | Access SQL Server databases, schemas, and tables. When resolving a short path or `USE`, both components must be present after combination; `dbo` is not injected and the login's default schema is not used as a fallback. |
| `oracle`     | `SCHEMA` | Access Oracle schemas and tables. Oracle qualifies objects with a single schema level; `DATABASE` is the service name used for the connection and never appears in an object path. With multiple PDBs, create one external source per PDB. |

### Field Descriptions and Constraints

| Field           | Required | Value or Limit | Description |
| --------------- | -------- | -------------- | ----------- |
| `IF NOT EXISTS` | No       | Fixed keyword  | Does not return an error if the object already exists. |
| `source_name`   | Yes      | Maximum 64 bytes | External data source name, globally unique, and must not share a name with a local database. |
| `TYPE`          | Yes      | `mysql`, `postgresql`, `influxdb`, `sqlserver`, `oracle` | External source type, case-insensitive, determines connector and path resolution rules. |
| `HOST`          | Yes      | Hostname or IP, maximum 256 bytes | External data source address; supports full FQDN. |
| `PORT`          | Yes      | `1` to `65535` | External data source port. |
| `USER`          | Yes      | Maximum 128 bytes | External data source access account. |
| `PASSWORD`      | Yes      | Maximum 128 bytes | External data source access password; encrypted when stored and masked when displayed. |
| `DATABASE`      | Optional for MySQL, InfluxDB, and SQL Server; required for PostgreSQL and Oracle | Maximum 64 bytes | Default database. When no default database is set, queries must explicitly specify the database. PostgreSQL connections cannot switch databases after establishment. For Oracle, `DATABASE` is the service name; a SID, instance name, or TNS alias does not work. |
| `SCHEMA`        | No       | Maximum 64 bytes | Default schema. When not set, queries that require it must explicitly specify the schema. SQL Server does not inject `dbo` as a default; for Oracle, the schema must be given explicitly when no default is set. |
| `OPTIONS`       | No       | Key maximum 64 bytes, value maximum 4095 bytes, overall JSON maximum 4095 bytes | Connection extension parameters. |

**Note**: All identifiers follow TDengine database and table naming rules: by default, character types are restricted and names are case-insensitive; escaped identifiers relax character restrictions and are case-sensitive.

**SQL Server namespaces**: `DATABASE` and `SCHEMA` are combined from the source defaults and the values explicitly specified by the query. Components given in the path take precedence; the rest come from the source defaults. If either component is still empty after combination, the query returns `TSDB_CODE_EXT_DEFAULT_NS_MISSING`; there is no implicit fallback—neither `dbo` nor the login's default schema is injected. A three-part column path therefore requires both `DATABASE` and `SCHEMA` defaults on the source, a four-part column path requires the `DATABASE` default, and a five-part column path requires neither. SQL Server only supports databases on the same instance that the login can access; cross-instance access is not supported.

**Oracle namespaces**: Oracle qualifies objects with a single schema level, and the service name never appears in a path. The default schema comes from the `SCHEMA` in the source definition; an explicit schema in the path or `USE` takes precedence. If the combined schema is still empty, the query returns `TSDB_CODE_EXT_DEFAULT_NS_MISSING`. The service name is fixed when the connection is established, so an `ALTER EXTERNAL SOURCE` that changes `DATABASE` affects only new connections. With multiple PDBs, create one external source per PDB; switching containers within a session is not supported.

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

#### SQL Server Parameters

SQL Server-specific parameters are as follows:

| Parameter | Value | Default | Description |
| -------------------------- | ----- | ------- | ----------- |
| `trust_server_certificate` | `true` or `false` | `false` | Whether to verify the server certificate. When `true`, certificate chain and host name verification are skipped; this is meaningful only when `tls_enabled = true` and should be used only in test environments. When `false`, the server certificate chain and the host name in the certificate are verified. |

The common TLS and timeout parameters also apply to SQL Server. The first release supports only Microsoft ODBC Driver 18 for SQL Server and does not use `Encrypt=strict` (TDS 8.0 is available only on SQL Server 2022); encryption is controlled by `tls_enabled`.

#### Oracle Parameters

Oracle has no dedicated parameters and supports only the common `connect_timeout_ms` and `read_timeout_ms`:

- TLS parameters are not supported: setting `tls_enabled = true` or any certificate parameter (`tls_ca_cert`, `tls_client_cert`, `tls_client_key`) returns `TSDB_CODE_PAR_SYNTAX_ERROR` during the DDL phase and the external source is not created.
- Connections use plain TCP. When the server enables Oracle Net native encryption (`SQLNET.ENCRYPTION_SERVER` set to `required` or `requested`), the connection negotiates native encryption without any client configuration.
- Wallets, `sqlnet.ora`, and `tnsnames.ora` are not read; TCPS and certificate authentication are not supported, and an instance that listens on TCPS only cannot be connected.

#### Parameter Behavior and Notes

**Note**: Explicitly setting `connect_timeout_ms` or `read_timeout_ms`, including setting them to `0`, takes precedence over global configuration; global configuration is used only when the key is not set.

**Note**: Unknown keys return `TSDB_CODE_PAR_SYNTAX_ERROR` during the DDL phase and are not saved. `ALTER EXTERNAL SOURCE ... SET OPTIONS(...)` merges incrementally; keys not specified retain their original values; setting a value to an empty string `''` deletes any existing key. In `SHOW` and `DESCRIBE`, `password`, `api_token`, `tls_client_cert`, and `tls_client_key` are masked.

#### Examples

The following examples create five external data sources:

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

-- SQL Server 2022: default database and schema, TLS with server certificate verification
CREATE EXTERNAL SOURCE sqlserver_prod
TYPE = 'sqlserver'
HOST = 'sqlserver.example.com'
PORT = 1433
USER = 'td_reader'
PASSWORD = 'your_password'
DATABASE = Telemetry
SCHEMA = dbo
OPTIONS (
    'tls_enabled' = 'true',
    'connect_timeout_ms' = '5000',
    'read_timeout_ms' = '60000'
);

-- Test environment: encrypted but without certificate verification
CREATE EXTERNAL SOURCE sqlserver_test
TYPE = 'sqlserver'
HOST = '192.0.2.10'
PORT = 1433
USER = 'readonly'
PASSWORD = 'your_password'
DATABASE = Telemetry
OPTIONS (
    'tls_enabled' = 'true',
    'trust_server_certificate' = 'true'
);

-- Oracle: DATABASE is the service name and SCHEMA is the default schema
CREATE EXTERNAL SOURCE oracle_prod
TYPE = 'oracle'
HOST = 'oracle.example.com'
PORT = 1521
USER = 'td_reader'
PASSWORD = 'your_password'
DATABASE = ORCLPDB1
SCHEMA = TELEMETRY
OPTIONS (
    'connect_timeout_ms' = '5000',
    'read_timeout_ms' = '60000'
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
| SQL Server      | Database and Schema | A single query namespace. |
| SQL Server      | Table, View     | Regular table. Views are not required to contain a timestamp column; see [External Tables Without a Timeline](#external-tables-without-a-timeline) for rules when no timeline is present. |
| Oracle          | Schema          | A single query namespace. |
| Oracle          | Table, View, Materialized View | Regular table. Views are not required to contain a timestamp column; see [External Tables Without a Timeline](#external-tables-without-a-timeline) for rules when no timeline is present. |

Indexes, stored procedures, and triggers in MySQL, indexes, sequences, and triggers in PostgreSQL, and synonyms, stored procedures, triggers, indexes, constraints, and SQL Server Agent objects in SQL Server, do not participate in federated query. InfluxDB Buckets and Retention Policies are storage policies and do not affect federated query. Oracle synonyms, sequences, PL/SQL packages/procedures/functions, triggers, indexes, constraints, and scheduler objects do not participate in federated query; a synonym in a path is treated as a missing object.

**Note**: Identifier case follows external database rules: MySQL is case-insensitive by default; PostgreSQL folds to lowercase by default and preserves original case when quoted; InfluxDB is case-sensitive; SQL Server follows the collation of the database containing the object, which may differ across databases, and identifiers quoted with brackets or double quotes preserve their original names; Oracle folds unquoted identifiers to upper case and preserves the original case when double quotes are used.

### Timestamp Primary Key Requirements

When participating in federated query and performing **timeline-dependent** operations, external tables (Table / Measurement) must have a primary key time column mappable to TDengine `TIMESTAMP`. This column is used for time range filtering, sorting, windows, and time-series functions.

- MySQL timestamp primary key columns must be of type `DATETIME` or `TIMESTAMP`.
- PostgreSQL timestamp primary key columns must be of type `TIMESTAMP` or `TIMESTAMPTZ`.
- The `time` column of an InfluxDB Measurement naturally satisfies this requirement.
- SQL Server timestamp primary key columns must be of type `datetime2`, `datetimeoffset`, `datetime`, `smalldatetime`, or `date`; `timestamp` / `rowversion` are row-version binary values and `time` represents only a time of day without a date, so neither can be used as a timestamp primary key.
- Oracle timestamp primary key columns must be of type `DATE` or `TIMESTAMP[(n)]`; `TIMESTAMP(n) WITH TIME ZONE` and `TIMESTAMP(n) WITH LOCAL TIME ZONE` cannot be primary keys at all (`ORA-02329`) and therefore cannot serve as the timestamp primary key.

**Note**: The timestamp primary key may be one column in a composite primary key; other columns in the composite primary key and other indexes may exist. However, the same external table cannot have a second primary key column mappable to TDengine `TIMESTAMP`. **MySQL / PostgreSQL / SQL Server / Oracle views (View) are not subject to timestamp primary key constraints**; see the next section for rules when the result set contains no timestamp column.

### External Tables Without a Timeline

Refers to regular tables in MySQL / PostgreSQL / SQL Server / Oracle that lack a primary key time column mappable to `TIMESTAMP`, or external views whose result sets contain no timestamp column. Typical scenarios include device registries and asset dimension tables. InfluxDB Measurements all contain `time` and are not covered by this section.

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

-- SQL Server dimension table without a timestamp primary key
SELECT * FROM sqlserver_prod.dbo.device_info;

-- Oracle dimension table without a timestamp primary key
SELECT * FROM oracle_prod.TELEMETRY.device_info;
```

External tables without a timeline cannot be used with `INTERVAL`, state/session/event/count windows, `FILL`, `INTERP`, time-series difference/integral functions, ASOF JOIN, Window JOIN, or virtual table column references. In the current version, external regular tables with non-timestamp primary keys may pass virtual table DDL validation, but that path does not have tested time-merge semantics and should not be used in production. To associate external dimension data with TDengine time-series data, first provide join results with a timestamp primary key in the external system, or synchronize dimension data into TDengine before joining.

### Type Mapping

#### Mapping Rules

External columns are mapped to TDengine data types at query time according to the following rules:

- Types with exact correspondence are mapped directly, for example MySQL `INT` maps to `INT`, PostgreSQL `double precision` maps to `DOUBLE`, InfluxDB `Float64` maps to `DOUBLE`, SQL Server `int` maps to `INT`, and `float` maps to `DOUBLE`; Oracle `NUMBER(p,0)` with p ≤ 18 maps to `BIGINT` and `BINARY_DOUBLE` maps to `DOUBLE`.
- Types that can be downgraded are allowed to map, but precision or semantic loss may occur. For example, `DATE` is converted to `TIMESTAMP` at midnight, `TIME` is converted to milliseconds from midnight and stored in `BIGINT`, `SET` is serialized as a comma-separated string, and `uuid` is converted to `VARCHAR(36)`; Oracle stores the empty string `''` as `NULL` and reads `CHAR(n)` values with their padding spaces. Downgrade conversions are logged.
- Structured types such as arrays, ranges, composite types, and `hstore` are serialized to JSON or text strings and stored in `NCHAR` or `VARCHAR`; structural semantics are not preserved, and a log entry is recorded.
- Unrecognized or unsupported external type codes result in errors; if `SELECT *` expands to unmappable columns, the entire query fails.

#### Special Types and Precision

**Note**: `JSON`, `JSONB`, and string columns in external sources are uniformly serialized as strings and stored in `NCHAR`; they are not mapped to the TDengine native JSON type. Using JSON subfield operators `->` or `CONTAINS` on such columns returns an error. Oracle stores JSON data in `VARCHAR2` / `CLOB` / `BLOB` columns, which are likewise read as text.

When declared length, precision, scale, or container capacity of known external types exceeds TDengine limits, the query does not fail solely due to declared limits. Result columns use the maximum range supported by TDengine; errors occur at execution time only when actual values cannot be represented losslessly—there is no silent truncation, rounding, substitution with `NULL`, or skipping.

Timezone and precision rules for external timestamp columns are as follows:

- Timezone-aware types such as PostgreSQL `timestamptz`, MySQL `TIMESTAMP`, and SQL Server `datetimeoffset` are interpreted according to their own timezone.
- Timezone-naive types such as PostgreSQL `timestamp`, MySQL `DATETIME` and `DATE`, SQL Server `datetime2` / `datetime` / `smalldatetime` / `date`, and Oracle `DATE` and `TIMESTAMP[(n)]` are interpreted according to the connection timezone or client timezone.
- Oracle `TIMESTAMP(n) WITH TIME ZONE` and `TIMESTAMP(n) WITH LOCAL TIME ZONE` are timezone-aware and are interpreted by their own offset or absolute instant.
- In single-database queries, TDengine databases use their `PRECISION`; MySQL, PostgreSQL, and SQL Server use microsecond precision; InfluxDB uses nanosecond precision; Oracle uses microsecond or nanosecond precision according to the maximum precision of the time columns in the table (nanosecond when a `TIMESTAMP(7-9)` column exists; the second precision of `DATE` is zero-padded to milliseconds).
- In multi-database queries, result timestamps use the highest precision among participating databases; lower-precision values are zero-padded. For example, a TDengine millisecond database joined with MySQL yields microsecond precision; joined with InfluxDB, nanosecond precision.
- SQL Server external timestamps have microsecond precision. When creating a virtual table that references SQL Server external columns, the virtual table's database precision must not be lower than microseconds (for example, `CREATE DATABASE ... PRECISION 'us'`).
- Oracle `DATE` has second precision (zero-padded to milliseconds) and `TIMESTAMP` defaults to `TIMESTAMP(6)`. When creating a virtual table that references Oracle external columns, the virtual table's database precision must not be lower than the declared precision of that timestamp column: `DATE` and `TIMESTAMP(0-3)` are milliseconds, `TIMESTAMP(4-6)` is microseconds, and `TIMESTAMP(7-9)` is nanoseconds; otherwise the statement returns `TSDB_CODE_EXT_SCHEMA_CHANGED`.

## External Paths and USE

### External Table Paths

#### Description

In the `FROM` clause of a query, external table paths resolve to the table level:

| External Source | Using Default Namespace | Explicit Namespace |
| --------------- | ----------------------- | ------------------ |
| MySQL           | `source_name.table`     | `source_name.database.table` |
| PostgreSQL      | `source_name.table`     | `source_name.schema.table` |
| InfluxDB        | `source_name.table`     | `source_name.database.table` |
| SQL Server      | `source_name.table`     | `source_name.schema.table`, `source_name.database.schema.table` |
| Oracle          | `source_name.table`     | `source_name.schema.table` |

Two-part paths use the default `DATABASE` or `SCHEMA` specified when creating the external data source; when no default is set, the full path must be used.

**SQL Server path notes**: In a three-part path `source_name.schema.table`, the middle segment is the schema and the database comes from the default value; a four-part path `source_name.database.schema.table` specifies the database and schema in order. Four-part paths can only access other databases on the same SQL Server instance that the login can access; cross-instance access is not supported.

**Oracle path notes**: In a three-part path `source_name.schema.table`, the middle segment is the schema. The service name never appears in a table path, and four-part or longer table paths are not supported.

### Virtual Table External Column Paths

#### Description

External column references in virtual table DDL append the column name after the table path:

| External Source | Using Default Namespace | Explicit Namespace |
| --------------- | ----------------------- | ------------------ |
| MySQL           | `source_name.table.column` | `source_name.database.table.column` |
| PostgreSQL      | `source_name.table.column` | `source_name.schema.table.column` |
| InfluxDB        | `source_name.table.column` | `source_name.database.table.column` |
| SQL Server      | `source_name.table.column` (requires default `DATABASE` and `SCHEMA`), `source_name.schema.table.column` (requires default `DATABASE`) | `source_name.database.schema.table.column` |
| Oracle          | `source_name.table.column` (requires default `SCHEMA`) | `source_name.schema.table.column` |

**SQL Server column path note**: SQL Server external column references support three forms:

| Form | Path | Required source defaults |
| ---- | ---- | ------------------------ |
| Three-part | `source_name.table.column` | `DATABASE` + `SCHEMA` |
| Four-part | `source_name.schema.table.column` | `DATABASE` |
| Five-part | `source_name.database.schema.table.column` | None |

All three forms work in `CREATE VTABLE` column definitions, `ALTER VTABLE ... ADD COLUMN ... FROM`, `ALTER VTABLE ... ALTER COLUMN ... SET`, and virtual child table (`CREATE VTABLE ... USING vstb`) column definitions. Relative forms (three-part and four-part) are expanded to the full five-part namespace when written to virtual table metadata, so `SHOW CREATE VTABLE` and `information_schema.ins_columns.col_source` always display `source.database.schema.table.column`; a later `ALTER EXTERNAL SOURCE` changing the default namespace does not change the binding of an existing virtual table.

**Oracle column path note**: Three-part `source_name.table.column` and four-part `source_name.schema.table.column` are supported. A relative form is expanded to the four-part form when written to virtual table metadata, so `SHOW CREATE VTABLE` and `information_schema.ins_columns.col_source` always display `source.schema.table.column`.

**Note**: Internal column paths remain `table.column` or `db.table.column`. For external paths, `source_name`, `database`, `schema`, and `column` have a maximum of 64 bytes; `table` has a maximum of 192 bytes. Objects exceeding these limits return an error directly—there is no truncation, renaming, or prefix matching.

**Note**: Three-part paths `A.B.C` in virtual table DDL are disambiguated by the first segment: when the first segment matches a registered external data source, it resolves to `source_name.table.column`; when it matches a local database, it resolves to `db.table.column`. External data source names must not share a name with a local database, so there is no conflict. Three-part paths in query `FROM` clauses always resolve to `source_name.{database|schema}.table`.

```sql
-- Query external tables
SELECT * FROM mysql_prod.meters;
SELECT * FROM mysql_prod.power.meters;
SELECT * FROM pg_prod.devices;
SELECT * FROM pg_prod.public.devices;
SELECT * FROM sqlserver_prod.sensor_data;
SELECT * FROM sqlserver_prod.telemetry.sensor_data;
SELECT * FROM sqlserver_prod.Archive.telemetry.sensor_data;
SELECT * FROM oracle_prod.sensor_data;
SELECT * FROM oracle_prod.TELEMETRY.sensor_data;

-- Reference external columns in virtual table DDL
current FLOAT FROM mysql_prod.meters.current
current FLOAT FROM mysql_prod.power.meters.current
owner VARCHAR(64) FROM pg_prod.public.meter_asset.owner
-- SQL Server three-part (source requires default database and schema)
value FLOAT FROM sqlserver_prod.sensor_data.value
-- SQL Server four-part (source requires a default database; schema from the path)
value FLOAT FROM sqlserver_prod.telemetry.sensor_data.value
-- SQL Server five-part (full namespace)
value FLOAT FROM sqlserver_prod.Archive.telemetry.sensor_data.value
-- Oracle three-part (source requires a default schema)
value FLOAT FROM oracle_prod.sensor_data.value
-- Oracle four-part (explicit schema)
value FLOAT FROM oracle_prod.TELEMETRY.sensor_data.value
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

`USE source_name` requires the external source to have a default namespace set: `DATABASE` for MySQL and InfluxDB, `SCHEMA` for PostgreSQL, and both `DATABASE` and `SCHEMA` for SQL Server after combination. When not set, `TSDB_CODE_EXT_DEFAULT_NS_MISSING` is returned. `USE source_name.database` explicitly specifies a database for MySQL or InfluxDB; `USE source_name.schema` explicitly specifies a schema for PostgreSQL—the database for PostgreSQL remains the `DATABASE` specified when creating the external source.

SQL Server `USE` semantics: `USE source_name` uses the default database and default schema; `USE source_name.database` switches the database and keeps the default schema; `USE source_name.database.schema` switches both. The first release does not provide a schema-only `USE` form; use a three-part table path `source_name.schema.table` when only the schema needs to change. `USE` affects only short-path resolution in the current TDengine session; it does not change the external source definition or the external column origins of existing virtual tables.

Oracle `USE` semantics: `USE source_name` uses the default schema, and `USE source_name.schema` switches the schema. The service name is not a namespace and cannot be switched with `USE source_name.database`.

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

-- Use the SQL Server default namespace
USE sqlserver_prod;
SELECT * FROM sensor_data LIMIT 10;

-- Explicitly switch the SQL Server database (default schema retained)
USE sqlserver_prod.Archive;
SELECT * FROM sensor_data LIMIT 10;

-- Switch both the SQL Server database and schema
USE sqlserver_prod.Archive.telemetry;
SELECT * FROM sensor_data LIMIT 10;

-- Use the Oracle default schema
USE oracle_prod;
SELECT * FROM sensor_data LIMIT 10;

-- Explicitly select an Oracle schema
USE oracle_prod.TELEMETRY;
SELECT * FROM sensor_data LIMIT 10;

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
  AND ts ≥ '2026-04-01 00:00:00'
  AND ts < '2026-04-02 00:00:00'
ORDER BY ts
LIMIT 1000;

-- Join two external time-series tables that both have timestamp primary keys (ON must include the primary timestamp column)
SELECT m.ts, m.current, a.voltage
FROM mysql_prod.meters m
JOIN pg_prod.public.archive_meters a
  ON m.ts = a.ts AND m.meter_id = a.meter_id
WHERE m.ts ≥ '2026-04-01 00:00:00'
  AND m.ts < '2026-04-02 00:00:00';

-- SQL Server external table range query
SELECT ts, value
FROM sqlserver_prod.telemetry.sensor_data
WHERE ts ≥ '2026-01-01 00:00:00'
  AND ts < '2026-01-02 00:00:00'
ORDER BY ts;

-- Oracle external table range query
SELECT ts, value
FROM oracle_prod.TELEMETRY.sensor_data
WHERE ts ≥ '2026-01-01 00:00:00'
  AND ts < '2026-01-02 00:00:00'
ORDER BY ts;
```

Additional examples:

```sql
-- InfluxDB external table window aggregation (grouped by Tag combination)
SELECT _wstart AS ts,
       location,
       AVG(temperature) AS avg_temp
FROM influx_prod.telegraf.sensor_readings
WHERE ts ≥ '2026-04-01 00:00:00'
  AND ts < '2026-04-02 00:00:00'
PARTITION BY location
INTERVAL(10m)
ORDER BY ts;

-- Grouped statistics on an external time-series table
SELECT location,
       COUNT(*) AS point_cnt,
       AVG(temperature) AS avg_temperature
FROM influx_prod.telegraf.sensor_readings
WHERE ts ≥ '2026-04-01 00:00:00'
  AND ts < '2026-04-02 00:00:00'
GROUP BY location
ORDER BY point_cnt DESC;

-- UNION ALL across local and external archive databases for unified analysis
SELECT ts, meter_id, current
FROM power.meters
WHERE ts ≥ '2026-03-01 00:00:00'
  AND ts < '2026-03-15 00:00:00'
UNION ALL
SELECT ts, meter_id, current
FROM mysql_prod.power_archive
WHERE ts ≥ '2026-03-01 00:00:00'
  AND ts < '2026-03-15 00:00:00'
ORDER BY ts;
```

When the external source supports equivalent semantics, the system prefers to execute equivalent computations at the source to reduce data transfer. When the external source lacks equivalent semantics or source-side execution fails, the system reads the necessary raw data and completes computation locally. Execution location affects performance only and does not change the results of successfully completed queries.

### Feature Limitations

#### TBNAME

External tables do not support the `TBNAME` pseudo-column, including `SELECT TBNAME`, `WHERE TBNAME = ...`, `JOIN ON ... TBNAME`, and similar usages. `PARTITION BY TBNAME` on InfluxDB is an exception and is equivalent to grouping by all Tag columns.

#### TAGS

MySQL, PostgreSQL, SQL Server, and Oracle external tables do not support `SELECT TAGS ...`; InfluxDB external tables support `TAGS` but return only Tag combinations that have at least one data point. In TDengine, Tag values can be returned even for subtables with no data—the semantics differ.

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

### InfluxDB SERIES Column References

For InfluxDB, a `SERIES` declaration can be appended when creating a virtual regular table or virtual subtable. It binds an alias to a specific series selected by tag conditions:

```sql
SERIES series_alias AS source_name.database_name.measurement_name
    (tag_name = 'tag_value' [, tag_name = 'tag_value'] ...)
```

This syntax has the following constraints:

- It is supported only for InfluxDB external data sources, not MySQL, PostgreSQL, or SQL Server.
- The `SERIES` target must use the complete three-part path `source_name.database_name.measurement_name`; the external source name and database name cannot be omitted.
- At least one tag condition is required. Conditions support only `tag_name = 'tag_value'` and, at declaration time, must include every tag of the measurement exactly once. Missing, unknown, or duplicate tags cause an error.
- `series_alias.field_name` represents a measurement field constrained by the declared tag conditions. An alias must be unique within the virtual table and cannot be used to reference an InfluxDB tag.

The following example binds `s1` to the series where `device = 'd1'`. `s1.value` expands to an external `influx_source.metrics.meters.value` column reference with that tag condition:

```sql
CREATE VTABLE v_a1 (
  ts TIMESTAMP,
  value DOUBLE FROM s1.value
)
SERIES s1 AS influx_source.metrics.meters (device = 'd1');
```

Use `ALTER VTABLE` to add a `SERIES` to an existing virtual table, bind a column to it, remove the binding, and then remove the `SERIES`:

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

`ADD SERIES` uses the same target-path and tag-condition constraints as a `SERIES` declaration at creation time. Before removing a `SERIES`, use `ALTER COLUMN ... SET NULL` or point the column to another reference so that no virtual table column still references the alias.

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
WHERE ts ≥ '2026-04-01' AND ts < '2026-04-02'
ORDER BY ts;

SELECT COUNT(*), AVG(current), AVG(temperature)
FROM v_d1001
WHERE ts ≥ '2026-04-01' AND ts < '2026-04-02'
GROUP BY humidity;

SELECT _wstart, AVG(current), AVG(voltage)
FROM v_d1001
WHERE ts ≥ '2026-04-01' AND ts < '2026-04-02'
INTERVAL(1h);
```

## EXPLAIN

Federated Query supports the `EXPLAIN` command for plan display and execution analysis.

```sql
EXPLAIN
SELECT ts, voltage, current
FROM mysql_prod.power.sensor_data
WHERE ts ≥ '2024-01-01'
ORDER BY ts
LIMIT 1000;
```

Example output:

```text
FederatedScan on mysql_prod.power.sensor_data
  Remote SQL: SELECT ts, voltage, current FROM sensor_data WHERE ts ≥ '2024-01-01' ORDER BY ts LIMIT 1000
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
| SQL Server cannot connect | Check whether TCP/IP is enabled on SQL Server, whether `PORT` is a fixed TCP port, whether the firewall allows access, and whether SQL Server Authentication is enabled. For a named instance, configure a fixed port instead of relying on SQL Browser. |
| SQL Server TLS or certificate error | Check the combination of `tls_enabled`, `tls_ca_cert`, and `trust_server_certificate`; in production, use a certificate issued by a trusted CA and do not keep `trust_server_certificate = true` enabled long-term. |
| SQL Server database or schema error | Check the path order: a three-part table path is `source.schema.table`, and a four-part path is `source.database.schema.table`. Confirm that the login has a user in the target database (or that `guest` is enabled) and has read permission on objects in the target schema. |
| Oracle cannot connect | Check whether the listener is reachable, whether `PORT` is correct, whether the firewall allows access, and whether `DATABASE` is a service name exposed by the listener (a SID or TNS alias does not work). An instance that listens on TCPS only is not supported. |
| Oracle client and server versions do not match | Re-run the runtime package installer and select the matching server version family (11g / 12c / 18c use the 19c client; 19c and later use the 23ai client), then restart taosd and all client processes. |
| Oracle object missing or not authorized | Oracle returns `ORA-00942` for both a missing object and a missing privilege, and the two cannot be distinguished. Verify the object name and schema first, then run `SELECT COUNT(*) FROM <owner>.<table>` in Oracle with the same account to confirm privileges. |

## Version and Compatibility

### External Database Versions

Supported and tested external database versions are as follows:

| Database   | Supported Versions | Tested Version | Notes |
| ---------- | ------------------ | -------------- | ----- |
| MySQL      | 5.7, 8.x           | 8.0            | Version 8.0 and above recommended. MariaDB 10.x is compatible but not specifically tested. |
| PostgreSQL | 14 and above       | 16             | Requires support for the standard SQL feature set. |
| InfluxDB   | v3.x               | v3.0           | v1.x and v2.x are not supported. |
| SQL Server | 2019 (15.x), 2022 (16.x) | 2022 | Requires Microsoft ODBC Driver 18 for SQL Server; only on-premises deployments and self-managed IaaS instances are supported. |
| Oracle     | 11.2.0.3 / 11.2.0.4, 12c, 18c, 19c, 21c, 23ai, 26ai | 11.2.0.4, 26ai | The runtime package ships both Instant Client 19c and 23ai; the installer selects the active client for the server version, and only one client is active per deployment at a time. |

**SQL Server support scope**: Only SQL Server Authentication is supported; Windows integrated authentication, Kerberos, NTLM, and Microsoft Entra ID are not supported. Azure SQL Database, Azure SQL Managed Instance, Azure Synapse Analytics, SQL Server Analysis Services, SQL Server 2017 and earlier, SQL Server Compact, and SQL Server 2025 are outside the support scope of this version. SQL Server installed on a cloud virtual machine (IaaS) is treated as an on-premises deployment. When using a named instance, configure a fixed port for that instance and specify it in `PORT`; SQL Browser discovery is not used.

**Oracle support scope**: Oracle 11g Release 1 (11.1) and earlier are not supported. Only database account and password authentication is supported; operating-system authentication, Kerberos, and other external authentication methods are not supported. An instance that listens on TCPS only is outside the support scope. To access 11g and 26ai from the same deployment, split them into two deployments with different active clients.

### Installation, Upgrade, and Uninstall

**Note**: In addition to the main TDengine installation package, you must install the federated query runtime library package (or the Federated Query plugin package from the download center) that matches your TDengine version. This package provides runtime libraries for external connectors (unixODBC and Microsoft ODBC Driver 18 for SQL Server are also shipped in the package and do not need separate registration; the Oracle Instant Client 19c / 23ai is shipped in the package as well) and should be installed on every machine that executes federated queries and hosts `taosc`; upgrade it in sync with TDengine.

Verify the following in order:

1. Confirm that `federatedQueryEnable` is enabled on both `taosd` and `taosc`.
2. Confirm that the federated query runtime library or plugin package version matches the TDengine version.
3. Confirm that the target machine architecture is supported (Linux x64, Linux ARM64, and Windows x64).
4. Run `SHOW EXTERNAL SOURCES`, `DESCRIBE EXTERNAL SOURCE ...`, and a minimal query example to verify runtime dependencies are available.

## Use Cases

### Unified Query of Local and External Time Series

When the local database stores recent high-frequency data and MySQL stores archived data, use `UNION ALL` for unified analysis:

```sql
SELECT ts, meter_id, current, voltage
FROM power.meters
WHERE ts ≥ '2026-03-10' AND ts < '2026-04-10'

UNION ALL

SELECT ts, meter_id, current, voltage
FROM mysql_prod.power_meters
WHERE ts ≥ '2026-03-10' AND ts < '2026-03-17'
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
WHERE ts ≥ '2026-04-01' AND ts < '2026-04-08'
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
WHERE l.ts ≥ '2026-04-01' AND l.ts < '2026-04-02'
GROUP BY l.meter_id
ORDER BY avg_power DESC
LIMIT 20;
```
