---
sidebar_label: Security Guide
title: Security Guide
description: Full-trace layered guide for authentication, transport, reliability, and availability, plus client connector security, data-at-rest protection, audit, hardening, and security advisories
---

TDengine provides multi-layer security and reliability capabilities for production environments. This chapter is organized by a **full-trace layered** structure to show how authentication, transport encryption, compression, persisted-data protection, and high availability connect across `taosX-Agent`, `taosX`, `taosAdapter`, and `taosd`. Enterprise edition provides complete capabilities for user privileges (including RBAC / separation of duties), IP allowlists, audit, transparent encryption, and tokens. Community edition provides only basic capabilities; see each topic for details.

## Reading Order

| # | Document | Topic |
|---|---|---|
| 01 | [Full-Trace Authentication](./01-full-trace-auth.md) | Identity authentication and access control |
| 02 | [Full-Trace Transport Security and Compression](./02-full-trace-transport.md) | TLS/SASL + transport compression + data-at-rest compression |
| 03 | [Full-Trace Reliability](./03-full-trace-reliability.md) | Checkpoint + WAL + backup and restore |
| 04 | [Full-Trace Availability](./04-full-trace-availability.md) | Automatic failover + load distribution + active-active disaster recovery |
| 05 | [Client and Connector Security](./05-client-connector-security.md) | Token, client TLS, and dynamic rotation at the entry layer |
| 06 | [Data-at-Rest Protection](./06-data-security.md) | TDE, key management, and `SECURE_DELETE` |
| 07 | [Audit and Compliance](./07-audit-and-compliance.md) | Audit log configuration and viewing |
| 08 | [Security Hardening](./08-security-hardening.md) | Component exposure surfaces, hardening, and gateways |
| 09 | [Security Advisories](./09-security-advisories.md) | Known vulnerabilities, affected versions, and fixed versions |

For syntax details, see [Users](../05-tdengine-sql/07-user-and-privilege/01-user.md) and [Privileges](../05-tdengine-sql/07-user-and-privilege/02-grant.md).

## Unified Layered Framework

The following layers are the common narrative order for the full-trace topics:

```text
+------------------------------------------------------------------+
| 1. Entry layer                                                   |
|    A. Programmatic entry: applications / language connectors     |
|       (C / Python / JDBC / ODBC / Rust / Go / CSharp / Node.js)  |
|    B. Web UI entry: taosExplorer in a browser                    |
|    C. CLI tools: taos / taosX / taosBenchmark / taosdump         |
+------------------------------------------------------------------+
                              |
                              v
+------------------------------------------------------------------+
| 2. Access layer                                                  |
|    Access layer = protocol/proxy layer after entry and before    |
|    taosd                                                         |
|    Path A: client -> taosAdapter (WebSocket / REST)              |
|    Path B: client embeds taosc (native connection)               |
|    Both paths eventually enter taosd through taosc               |
+------------------------------------------------------------------+
                              |
                              v
+------------------------------------------------------------------+
| 3. Data ingestion path (background tasks)                        |
|    External source -> taosX-Agent (optional) -> taosX ->         |
|    taosAdapter -> taosd                                          |
|    taosX-Agent deployment does not change checkpoint semantics   |
|    Kafka / Pulsar can rely on upstream persistent buffering      |
|    Device-direct / edge-disconnect scenarios should use          |
|    taosX-Agent local buffering                                   |
+------------------------------------------------------------------+
                              |
                              v
+------------------------------------------------------------------+
| 4. Cluster internals                                             |
|    taosd cluster (multiple DNodes / replicas / Leader switch)    |
+------------------------------------------------------------------+
                              |
                              v
+------------------------------------------------------------------+
| 5. Observability access                                          |
|    taosKeeper receives metrics from components and writes them   |
|    into the log database                                         |
+------------------------------------------------------------------+
                              |
                              v
+------------------------------------------------------------------+
| 6. Storage and audit persistence                                 |
|    Business data / audit logs -> WAL / snapshots / backup and    |
|    restore / TDE                                                 |
+------------------------------------------------------------------+
```

### Layer Notes

- **1. Entry layer**: user and application touchpoints, grouped as programmatic access, Web UI, and CLI.
- **2. Access layer**: the two physical paths from entry to `taosd`. Path A uses `taosAdapter` for WebSocket/REST. Path B uses native `taosc`. Both eventually reach `taosd` through `taosc`.
- **3. Data ingestion path**: background data-flow tasks triggered by SQL in `taosd`. The recommended model is "external source -> taosX-Agent (optional) -> taosX -> taosAdapter -> taosd".
- **4. Cluster internals**: multi-node, multi-replica, and Leader-switch mechanisms inside a `taosd` cluster.
- **5. Observability access**: `taosKeeper` receives externally exposed metrics from components and writes them back to the `log` database.
- **6. Storage and audit persistence**: compression, encryption, logs, and backup mechanisms when business data and audit logs are persisted.

### Key Terms

- **Checkpoint resume**: by default, checkpoint on the **data source side**, used to recover ingestion progress after task restart. It is different from the persistent queue used when `taosX` writes downstream.
- **Task restart**: stopping and restarting a `taosX` task after all automatic fault-recovery mechanisms are exhausted. It is different from data source connection retry.
- **Node naming**: product roles are written as `DNode`, `MNode`, and `VGroup`; host examples such as `dnode1` are only address examples.

## Applicable Versions and Conventions

- TDengine TSDB v3.4+
- Account examples use `tduser` / `SecurePass123!` instead of the default `root/taosdata`.
- Domain examples use `example.com`; certificate paths use `/etc/taos/certs/`.

For configuration and hardening, see [Security Hardening](./08-security-hardening.md). For vulnerability disclosure and fixed versions, see [Security Advisories](./09-security-advisories.md).
