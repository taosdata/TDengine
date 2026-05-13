# Examples

## docs

Official website sample code. These examples are embedded in the TDengine documentation and must maintain backward compatibility -- do not modify their interfaces or behavior.

| Directory | Description |
|-----------|-------------|
| `connect/afconn` | Native connection using the `af` package |
| `connect/cgoexample` | Native connection using the `taosSql` driver via `database/sql` |
| `connect/connpool` | Connection pooling using the `taosSql` driver |
| `connect/restexample` | RESTful connection using the `taosRestful` driver |
| `connect/wsexample` | WebSocket connection using the `taosWS` driver |
| `insert/sql` | Standard SQL INSERT via the `taosRestful` driver |
| `insert/stmt` | Parameter binding insert using the native Statement API |
| `insert/line` | Schemaless insert using InfluxDB Line Protocol |
| `insert/telnet` | Schemaless insert using OpenTSDB Telnet Protocol |
| `insert/json` | Schemaless insert using JSON format |
| `sqlquery` | SQL query (create database/table, insert, select) using the `taosSql` driver |
| `queryreqid` | SQL query with request ID tracing using the `taosSql` driver |
| `stmt/native` | Native Statement (STMT) API for parameter binding inserts |
| `stmt/ws` | WebSocket Statement API via the `ws/stmt` package |
| `stmt2/native` | Native Statement 2 (STMT2) API for improved parameter binding |
| `schemaless/native` | Native schemaless ingestion (Line, Telnet, JSON) via the `af` package |
| `schemaless/ws` | WebSocket schemaless ingestion via the `ws/schemaless` package |
| `tmq/native` | Native TMQ data subscription and consumption |
| `tmq/ws` | WebSocket TMQ data subscription and consumption |
| `sub` | Data subscription using the native `af/tmq` package |

## all_type_query

Query all supported TDengine data types through different connection methods.

| Directory | Description |
|-----------|-------------|
| `native` | Query all data types using the native `taosSql` driver |
| `rest` | Query all data types using the `taosRestful` driver |
| `ws` | Query all data types using the `taosWS` driver |

## all_type_stmt

Bind and insert all supported TDengine data types using the Statement API.

| Directory | Description |
|-----------|-------------|
| `native` | All data types via the native STMT API |
| `ws` | All data types via the WebSocket unified STMT API |

## schemaless

Schemaless ingestion examples with full connection management.

| Directory | Description |
|-----------|-------------|
| `native` | Native schemaless ingestion (Line, Telnet, JSON) |
| `ws` | WebSocket schemaless ingestion via the `ws/unified` client |

## tmq

TMQ consumer examples with full setup, subscription, and polling.

| Directory | Description |
|-----------|-------------|
| `native` | Native TMQ consumer |
| `ws` | WebSocket TMQ consumer |

## failover

Multi-endpoint failover examples with `autoReconnect` enabled. Each example puts an unavailable endpoint first in the DSN to demonstrate automatic endpoint fallback on initial connection, and enables `autoReconnect=true` for mid-stream reconnection failover.

| Directory | Description |
|-----------|-------------|
| `query` | SQL query failover using the `taosWS` driver via `database/sql` |
| `schemaless` | Schemaless insert failover using the `ws/unified` client |
| `stmt` | Statement (STMT2) insert failover using the `ws/unified` client |
| `tmq` | TMQ consumer failover with `ws.autoReconnect` enabled |

## slog

| Directory | Description |
|-----------|-------------|
| `slog` | WebSocket SQL example with driver logs routed to Go `log/slog` |

## platform

| Directory | Description |
|-----------|-------------|
| `platform` | Connect to TDengine Cloud using the `taosRestful` driver with token authentication |

## Running

All examples (except `failover` and `platform`) can be run via:

```bash
cd examples
./go.sh
```
