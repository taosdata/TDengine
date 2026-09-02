---
sidebar_label: Product Roadmap
title: Product Roadmap
description: TDengine TSDB 2026 product roadmap
---

The 2026 roadmap for TDengine TSDB is as follows.

## Product Capabilities

### 2026 Q1

1. Storage: batch tag modification and dynamic adjustment of the data-cache LRU
2. Query: subqueries, external windows, `ANY`/`SOME`/`ALL`/`EXISTS` operators, window and interpolation enhancements, and `EXPLAIN` and `SHOW QUERIES` optimization
3. Virtual tables: virtual-supertable query performance, subscription to virtual-table metadata changes, and reference validation between virtual and source tables
4. Stream processing: triggers by natural week/month/quarter/year, event-trigger condition enhancements, grouped-computation performance, and virtual-supertable triggers for subtable additions, deletions, and changes
5. taosX: TSDB privilege management, Windows support, Transform parsing extensions, export/import order consistency, ForceControl real-time database, KingHistorian data-source improvements, MQTT multi-broker support, and more
6. Security: mandatory access control, three-admin roles, and protection against SQL injection / denial of service / overflow attacks; force password change for the default `root` password, audit records that bypass taosKeeper, and forced overwrite after sensitive-data deletion; privilege-control improvements; storage encryption and encrypted-cluster upgrades; TOTP / TOKEN authentication and notifications; security hardening and vulnerability fixes for taosExplorer / connectors / taosX, and protection against configuration-file tampering

### 2026 Q2

1. Query: natural week/month/quarter/year, timezone improvements, and multiple state columns for state windows; window functions and the `OVER` clause, `COUNT(DISTINCT)`, function-category cleanup, and time-series degradation handling
2. Virtual tables: references to columns and tag columns in other virtual tables; virtual-table inheritance; virtual-supertable column renaming
3. Stream processing: leaf-level aggregation and multilevel subevents
4. Federated queries: MySQL, PostgreSQL, and InfluxDB support
5. Cluster: transactional metadata changes, rapid data recovery, and CPU affinity
6. Connectors: broad multi-language support for taosAdapter high availability, STMT2, BLOB, Decimal, transfer compression, and benchmark tools
7. Tools: taosExplorer configuration improvements and a taosdump upgrade
8. Security: mandatory access control and three-admin role improvements; end-to-end authentication, transport security, high availability, and a security deployment guide; dynamic JWT token secret management; SBOM generation for releases and CI memory-safety prechecks

### 2026 Q3

1. Query: scalar correlated subqueries, Hash Join, and complex-query performance improvements; more than 20 new query functions and window-function performance improvements
2. Virtual tables: tag columns on virtual regular tables, custom columns on virtual subtables, and references across databases with different precision
3. Stream processing: performance improvements for historical computation, multiple measurement points, multiple groups, recomputation, and virtual-table triggering, plus maintainability improvements
4. Federated queries: query pushdown and stream-processing optimization, dynamic third-party library loading, and plugin packaging
5. Data subscription: continuous queries for subscribing to virtual-table time-series data
6. Cluster: full and incremental backup, broader metadata-transaction coverage, and less write impact from cluster operations
7. taosX: high availability, observability, and stability improvements

### 2026 Q4

1. Storage: `TEXT`, variable-length strings, database renaming, and column renaming
2. Query: query parallelization and additional observability metrics; MySQL operators and functions, and UDF framework refactoring
3. Connectors: broader ecosystem integrations and improvements for Python, Rust, C#, and ODBC connectors
4. taosX: logical backup and restore performance improvements, and fuller type support
5. Resource control: memory control for taosX and the engine
6. Best practices: end-to-end compression and end-to-end load balancing
