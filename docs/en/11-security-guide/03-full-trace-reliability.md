---
sidebar_label: Full-Trace High Reliability
title: Full-Trace High Reliability
description: Reliability related to checkpoints, WAL, backup and restore, and data persistence
toc_max_heading_level: 3
---

This document describes the complete reliability path in TDengine TSDB, from user access to data persistence. It follows a unified six-layer architecture: entry layer -> access layer -> data ingestion path -> cluster internals -> observability access -> storage and audit persistence. Each layer is described with its reliability mechanisms.

> Terminology
>
> - Access layer: the protocol/proxy layer after the entry layer and before taosd, including the two physical paths taosAdapter and taosc.
> - Resumable transfer: by default, this refers to the checkpoint on the data source side, which is used to restore ingestion progress after a task restart. It is not the same as the persistent queue that taosX uses when writing downstream.
> - Task restart: shutting down and starting the taosX task again after all automatic failure recovery mechanisms have been exhausted. It is different from a data source connection retry.

---

## Overview

```mermaid
graph TD
    classDef client fill:#f39c12,color:#000,stroke:#d68910
    classDef svc    fill:#1a6ea8,color:#fff,stroke:#0d4f7d
    classDef core   fill:#2c3e50,color:#fff,stroke:#1a252f
    classDef ops    fill:#16a085,color:#fff,stroke:#0e6655
    classDef disk   fill:#7f8c8d,color:#fff,stroke:#566573

    APP(["Applications / Connectors"])
    SRC(["External Data Sources"])
    AGT(["taosX-Agent (Optional)<br/>Local Cache"])
    TX(["taosX<br/>Checkpoint + Persistent Queue"])
    GATE(["Access Layer<br/>taosAdapter / taosc"])
    CORE(["taosd Cluster<br/>Multi-Replica + Leader Switch"])
    OBS(["taosKeeper<br/>Metrics / Audit (Classic Path)"])
    STORE[("Business Data / Audit Logs<br/>WAL + Snapshot + TDE + Backup")]

    SRC --> TX

    SRC .-> AGT

    AGT .-> TX

    APP --> GATE

    TX --> GATE
    GATE --> CORE
    CORE --> STORE
    CORE -. Metrics and audit side path .-> OBS
    OBS -. Write back to log / audit database .-> STORE

    class SRC,APP client
    class AGT,TX,GATE svc
    class CORE core
    class OBS ops
    class STORE disk
```

### Six Lines of Defense for Reliability

| Line of Defense | Mechanism | Failure Coverage | Layer |
|------|------|---------|--------|
| L1 Data ingestion | Failover + resumable transfer + cache queue | Network or service interruption | Section 3 |
| L2 Multiple instances | Multi-instance load balancing / failover | Single instance failure | Section 2 access layer |
| L3 WAL | Persist WAL before write ACK | Process crash, sudden power loss | Section 6 |
| L4 Multi-replica | Raft 3 replicas across nodes | Single-node disk/host failure | Section 4 |
| L5 TDE | Transparent encryption at the storage layer | Disk theft, physical media leakage | Section 6 |
| L6 Backup and restore | taosdump / taosX | Accidental deletion, data center-level disaster | Section 6 |

### Reliability Overview by Layer

| Layer | Key Mechanisms |
|------|---------|
| ① Entry layer | Connector built-in reconnection + application retry; Explorer multi-instance fronted by LB; CLI resume |
| ② Access layer | taosAdapter connection pool / rate limiting / memory protection; taosc `firstEp` / `secondEp` automatic probing |
| ③ Data ingestion path | Data source-side checkpoint + Sink-side persistent queue + task restart; optional taosX-Agent local cache |
| ④ Cluster internals | Raft multi-replica (VGroup/MNode) + automatic Leader election |
| ⑤ Observability access | taosKeeper metrics buffering / backfill push; default path also carries audit reporting; failure does not affect business reads and writes |
| ⑥ Storage and audit persistence | WAL fsync + snapshot + TDE + backup and restore + audit log privilege separation |

---

## 1. Entry Layer Reliability

The entry layer is the first hop through which users and applications reach TDengine. This layer provides reliability through three types of mechanisms: **connector built-in reconnection + application-layer retry + CLI resume**. Connections can recover automatically after transient interruptions, and long-running tasks can resume from checkpoints after partial failure without manual user intervention.

### 1.1 Programmatic Entry (Applications / Language Connectors)

Applications access TDengine through WebSocket/REST or native TCP. At the connector layer, configure **connection pools + timeouts + retries**, and wrap idempotent retry logic at the application layer.

**Java / JDBC + HikariCP:**

```java
HikariConfig config = new HikariConfig();
config.setJdbcUrl("jdbc:TAOS-RS://lb-vip:6041/db");
config.setUsername("tduser");
config.setPassword("SecurePass123!");
config.setConnectionTimeout(30_000);
config.setMaximumPoolSize(10);
config.setConnectionTestQuery("SELECT server_status()");
```

**Go:**

```go
db, _ := sql.Open("taosWS", "tduser:SecurePass123!@ws(lb-vip:6041)/db?readTimeout=30s")
db.SetMaxOpenConns(20)
db.SetConnMaxLifetime(10 * time.Minute)
```

**Python:**

```python
import taosws
conn = taosws.connect("taos+ws://tduser:SecurePass123!@lb-vip:6041/db")
# Recommended: wrap retry logic at the application layer (exponential backoff + idempotent writes)
```

> **Application-layer retry recommendation**: For idempotent operations (INSERT with explicit timestamps, SELECT), use exponential backoff, for example starting at 200 ms, doubling up to 10 s, with at most 5 retries. DDL and non-idempotent writes must be paired with business-layer deduplication (`INSERT ... ON DUPLICATE KEY UPDATE` or timestamp primary key overwrite).

### 1.2 Web UI Entry (taosExplorer)

taosExplorer provides a web console through the browser and focuses on two reliability scenarios:

- **Session invalidation**: After the login session expires or the backend restarts, the Explorer frontend automatically redirects to the login page for re-authentication. Users do not need to refresh manually. Unsubmitted editor content is retained in browser local state as long as the page is not refreshed.
- **Long queries / long tasks**: After a browser tab disconnects, SQL queries are canceled on the taosd side because taosAdapter detects the closed connection. Dashboard panels use short-connection polling, so network jitter does not affect data that has already been persisted.

Production recommendation: deploy Explorer as **stateless multiple instances** behind Nginx/HAProxy. After a single instance fails, the browser can reconnect to another instance. Business data itself is not affected.

### 1.3 Command-Line Tool Entry (Operations CLI)

Operations CLI tools need **interruptible and resumable** capabilities when performing large-scale data movement or long-running pressure tests.

#### 1.3.1 `taos` (Interactive SQL Shell)

`taos` connects directly to taosd on :6030 through taosc. taosc has built-in automatic reconnection: when the current DNode is unreachable, taosc attempts to relocate the MNode through `firstEp` / `secondEp` and obtain the latest cluster topology (see Section 2.2). In the shell, users experience this as automatic recovery after a short stall.

#### 1.3.2 `taosX` (Data Pipeline CLI)

The core reliability capability of `taosX` is **resumable transfer**. After a task is interrupted, for example the process is killed, the network disconnects, or the target side becomes unavailable, restarting the same task continues from the last data source-side checkpoint, without duplicate consumption or missing ingestion (see Section 3.2).

```bash
# After an abnormal task exit, rerun the same command to resume
taosX run \
  -f "taos+ws://tduser:SecurePass123!@src:6041/srcdb" \
  -t "taos+ws://tduser:SecurePass123!@dst:6041/dstdb"
```

#### 1.3.3 `taosBenchmark` (Pressure Testing)

`taosBenchmark` provides `--retry` parameters. When transient errors occur during writes, it can retry automatically to avoid full task failure caused by brief network jitter or rate limiting.

```bash
# Automatically retry failed writes (example configuration)
taosBenchmark -u tduser -p'SecurePass123!' \
  -R 3 \          # Maximum retry count per record
  -S 1000         # Retry interval (milliseconds)
```

#### 1.3.4 `taosdump` (Backup / Restore)

`taosdump` controls the time range through `-S` (start time) / `-E` (end time), enabling **incremental backup** and **failed-run resume**. If a full backup fails midway, retry with a narrower range based on the completed time range.

```bash
# Full backup
taosdump -h localhost -u tduser -p'SecurePass123!' -o /backup

# Incremental backup (export only the specified time range)
taosdump -h localhost -u tduser -p'SecurePass123!' -D mydb \
  -S "2024-01-01 00:00:00" -E "2024-01-02 00:00:00" \
  -o /backup/incremental/2024-01-01
```

> **Resume strategy**: Perform incremental backups by daily slices. A failed day can be rerun independently. Full backups should be completed in one run during off-peak hours. If interrupted, rerun the full backup to a new directory.

---

## 2. Access Layer Reliability

> **About taosc**: taosc is the native TDengine client library. As an **independent component**, it provides C API and DSN connection interfaces upward, communicates with the taosd cluster downward through a **private protocol** (TCP :6030), and implements reliability mechanisms at the protocol layer, such as `firstEp` / `secondEp` dual-entry probing, cluster topology refresh, and transparent retry after Leader switching. Path A (WebSocket/REST) calls taosc inside taosAdapter on the server side to complete the final hop to taosd, so Adapter's rate limiting and connection pool protection are layered on top of taosc automatic reconnection. Path B connects applications or CLIs directly to taosd by embedding the taosc dynamic library, so its reliability mechanisms come entirely from taosc. Both paths eventually enter taosd through taosc.

When data moves from the entry layer to the access layer, two problems must be solved: "server-side self-protection" and "client-side automatic reconnection." This layer provides reliability through **taosAdapter rate limiting / timeout / memory protection** (Path A) and **taosc node probing / automatic reconnection** (Path B). During server overload or node failure, it avoids both cascading overload and indefinitely hung clients.

### 2.1 Path A: WebSocket / REST (taosAdapter Self-Protection)

taosAdapter is the server-side gateway for WebSocket/REST traffic. It protects taosd from overload through multiple mechanisms, including connection pools, query rate limiting, and memory watermarks.

**Connection pool configuration** (`/etc/taos/taosadapter.toml`):

```toml
[pool]
maxConnect  = 0       # Maximum connections (default: CPU cores x 2)
maxIdle     = 0       # Maximum idle connections
idleTimeout = "0s"    # Idle timeout
waitTimeout = 60      # Connection wait timeout (seconds), returns 503 on timeout
maxWait     = 0       # Maximum wait queue (0 = unlimited)
```

**Memory protection (backpressure)**:

```toml
[monitor]
disable                    = false
collectDuration            = "3s"
pauseQueryMemoryThreshold  = 70    # Query pause threshold (%)
pauseAllMemoryThreshold    = 80    # Pause-all threshold (%)
```

The health check `/-/ping` returns 503 when thresholds are exceeded. **The upstream load balancer automatically removes the node based on this signal**, creating a backpressure signal that flows back to clients. Connectors retry after receiving 503.

**Query rate limiting**:

```toml
[request]
queryLimitEnable = true

[request.default]
queryLimit       = 0     # Default concurrent query count (0 = unlimited)
queryWaitTimeout = 900

[request.users.readonly_user]
queryLimit       = 10
queryWaitTimeout = 60
```

**SQL rejection regex** (to prevent misoperations from expanding the fault radius):

```toml
rejectQuerySqlRegex = [
  '(?i)^drop\\s+database\\s+.*',
  '(?i)^alter\\s+table\\s+.*'
]
```

### 2.2 Path B: taosc -> taosd Private Protocol (Automatic Reconnection)

In the native path, taosc acts as an independent client library embedded in the application process. It communicates with taosd through the TDengine **private protocol** (TCP :6030) and is responsible for node probing and failover:

- **`firstEp` / `secondEp` dual entry**: The client configures two initial access points. If connection to `firstEp` fails, it automatically switches to `secondEp`, avoiding a single point of failure.
- **Topology refresh**: After connecting to any available DNode, taosc obtains the latest cluster topology from the MNode, including which VGroup Leaders are on which DNodes. Subsequent writes are routed directly to the correct Leader.
- **Transparent Leader switching**: When a VGroup Leader changes (see Section 4.4), taosc automatically refreshes routing and retries after the next request receives a `Not Leader` error. This is transparent to applications.

```ini
# Client-side taos.cfg
firstEp  dnode1.example.com:6030
secondEp dnode2.example.com:6030
```

---

## 3. Data Ingestion Path Reliability

Data enters the cluster from external data sources through the path "**external data source -> taosX-Agent (optional) -> taosX -> taosAdapter -> taosd**". Whether taosX-Agent is deployed does not change the core semantics of resumable transfer: the **data source-side checkpoint** is what restores ingestion progress. After data has already been read by taosX but has not yet been successfully written downstream, protection is provided by the **taosX persistent queue**.

### 3.1 Ingestion Path and Cache Boundaries

taosX-Agent is needed only for edge ingestion, weak-network transmission, or upstream sources that do not provide reliable replay. Not all data sources require local caching.

| Scenario | taosX-Agent Local Cache Recommended | Reliability Boundary |
|------|------------------------|-----------|
| Kafka / Pulsar / TMQ | Usually not needed | Can rely on Broker / upstream persistent cache; taosX still needs to record checkpoints |
| MQTT QoS 0 / serial port / direct device ingestion | Recommended | Upstream usually has no reliable replay capability; persist locally in taosX-Agent |
| OPC-UA / OPC-DA / industrial edge networks | Recommended | Edge network jitter is common; local cache supports resumable transfer across network outages |
| MySQL / PostgreSQL / MSSQL incremental pull | Depends on scenario | Usually relies on source table retention window + incremental column; no extra local cache required |

> If the data source can connect directly to taosX, the path can be simplified to "external data source -> taosX -> taosAdapter -> taosd". In this case, using taosX-Agent only affects the cache location, not the definition of a checkpoint.

### 3.2 Data Source-Side Resumable Transfer (Checkpoint)

Each taosX task periodically writes the **data source consumption progress** into a checkpoint. After the task restarts, the recovery logic first reads this checkpoint and then determines where to continue ingestion:

- **Position-based sources** (Kafka / Pulsar / TMQ): record offset or cursor.
- **Window-based sources** (MySQL / PostgreSQL / TDengine queries): record completed time windows or incremental columns.
- **Edge ingestion sources** (through taosX-Agent): record the latest acknowledged sequence number, timestamp, or position.

Do not confuse checkpoints with persistent queues:

| Capability | Records | Problem Solved | Typical Trigger |
|------|---------|-----------|---------|
| Checkpoint | Data source-side offset / time window / incremental column | Where to continue "reading" after the task is shut down | Task restart |
| Persistent queue | Data already read but not yet successfully written downstream | How to "avoid losing writes" when downstream is temporarily unavailable | Sink-side jitter / target unreachable |

### 3.3 taosX Persistent Queue

taosX uses a persistent queue to temporarily store data that **has already been read from the data source but has not yet been successfully written to taosAdapter / taosd**. When the Sink side is temporarily unavailable, data is first persisted to local disk and then sent after the downstream recovers.

| Parameter | Default | Description |
|------|--------|------|
| Queue segment size | 1 GB | Maximum size of each persistent file |
| Maximum batch read | 1000 records | Maximum number of messages read from the queue at one time |
| Write sync interval | 3 seconds | Interval for flushing persistent data to disk |
| Cleanup interval | 30 seconds | Automatic cleanup cycle for consumed queue segments |

### 3.4 taosX Task Restart

In the data ingestion path, **automatic recovery happens first**: data source connection retry, taosAdapter reconnection, persistent queue replay, and so on. Only after these mechanisms are exhausted does the task enter `Failed`. Then the scheduler or operations team performs a **task restart**, that is, shutting down and starting the entire taosX task again.

| Concept | Trigger Timing | Scope | Shuts Down Task |
|------|---------|---------|-------------|
| Data source connection retry | A single connection interruption | Current Source connection | No |
| Task restart | After all automatic recovery mechanisms fail | Entire taosX task | Yes |

The recovery sequence after a task restart is usually:

1. Read the data source-side checkpoint and confirm the position from which ingestion should continue.
2. Replay the persistent queue that has not yet been cleared, first completing data that was already read but not fully written.
3. Re-establish Source / Sink connections and resume normal processing.

### 3.5 taosX-Agent Local Buffering + Disconnection Reconnection

taosX-Agent is deployed on the edge side, close to the data source. It has a built-in message cache queue. When the network connection to central taosX is interrupted, it temporarily stores data locally and automatically resumes transfer after recovery. For configuration and behavior, see [Store and Forward](../12-operations-and-tooling/03-components/07-taosx-agent/store-and-forward.md).

```toml
# /etc/taos/agent.toml
in_memory_cache_capacity = 64     # In-memory cache queue capacity
keep_online = true                # Keep running after disconnection
data_dir = "/var/lib/taos/taosX-agent"  # Persistence directory
```

`keep_online = true` (default) ensures that the Agent continues running and caching data when disconnected from taosX. It applies to unstable network environments such as factory intranets and satellite links.

### 3.6 External Data Source Connection Retry

The entry side from external data sources to taosX / Agent also requires reconnection, which is built into each Source connector. Only after the retry threshold is exceeded does the task enter `Failed`, after which the task restart mechanism in Section 3.4 takes over.

| Data Source Type | Minimum Interval | Maximum Interval | Maximum Attempts |
|-----------|---------|---------|---------|
| MQTT | 100 ms | 10 s | 10 |
| Kafka | - | 300 s | 3 |
| Pulsar | - | 300 s | 3 |
| Traditional connectors | - | - | 5 |

| Data Source | Reliability Mechanism |
|--------|-----------|
| MQTT | Client automatic reconnection (reconnects to Broker with exponential backoff after disconnection); QoS 1/2 guarantees at-least-once delivery |
| Kafka | Consumer Group rebalancing; resumes from committed offset after consumer restart; messages are ordered within a single partition |
| Pulsar | Resumable transfer based on subscription cursor; Broker switching is transparent to consumers |
| JDBC Source (MySQL/PG/MSSQL) | Retries failed queries by time window; idempotent pull based on incremental column (timestamp / auto-increment ID) |
| OPC-UA / OPC-DA | Agent automatically reconnects after disconnection; in subscription mode, changed values are cached on the server until resubscription |

> Configuration point: all external Sources should explicitly specify the **checkpoint position** (offset / timestamp / incremental column) to avoid relying on defaults that may cause restart from the beginning or missed data.

---

## 4. Cluster Internal Reliability

After data enters taosd, this layer provides reliability through three elements: **Raft multi-replica protocol + automatic Leader election + highly reliable MNode metadata**. A single-node failure does not interrupt writes or lose data; Leader switching completes within seconds; metadata such as users, databases, and table schemas receives the same level of replica protection as data.

### 4.1 Multi-Replica Raft Protocol

TDengine uses the Raft protocol for multi-replica replication of both VNode (time-series data) and MNode (metadata). The Leader receives writes, synchronizes them to Followers through AppendEntries RPC, and commits after majority acknowledgement.

### 4.2 Replica Architecture (1 / 2 / 3 Replicas)

`REPLICA` can be `1`, `2`, or `3`. Different databases can choose as needed. Production environments should prefer three replicas, while cost-sensitive scenarios can use two replicas.

**Three replicas (Raft majority)**

```text
VGroup 1 (replica=3)
  ├── VNode @ DNode-1  [Leader]   <- Receives writes
  ├── VNode @ DNode-2  [Follower] <- Real-time synchronization
  └── VNode @ DNode-3  [Follower] <- Real-time synchronization
```

```sql
CREATE DATABASE db REPLICA 3 VGROUPS 10;
ALTER DATABASE db REPLICA 3;
```

Three replicas commit by Raft majority. One replica can fail while reads and writes continue. Conversion to or from two replicas is not supported.

**Two replicas (Enterprise, Mnode arbitration)**

Two replicas reduce storage cost while providing a certain level of reliability and availability. Time-series data is stored in only two copies. Leader election is not determined by an internal Raft majority within the VGroup; instead, a highly available Mnode acts as the Arbitrator. When one Vnode fails and data has already been synchronized, another Vnode can be designated as the Assigned Leader to continue service. The cluster must have at least three nodes, typically two data nodes plus one arbitration node. The arbitration node can set `supportVnodes` to `0`.

```text
VGroup 1 (replica=2)
  ├── VNode @ DNode-1  [Leader / Assigned Leader]
  ├── VNode @ DNode-2  [Follower]
  └── Arbitrator @ Mnode (does not store time-series data)
```

```sql
CREATE DATABASE db REPLICA 2 VGROUPS 10;
ALTER DATABASE db REPLICA 2;   -- Supports conversion only with single replica
```

Fault tolerance boundary: recovery is possible when a single service fails and no continuous failures occur. If both data replicas are unavailable at the same time, or another failure occurs before synchronization completes, service cannot continue. Operations such as force assigning a Leader are described below.

For deployment constraints, exception scenarios, and `ASSIGN LEADER FORCE`, see [Two-Replica Solution](../12-operations-and-tooling/02-operations/11-ha/02-replica2.md).

**Single replica**

`REPLICA 1` has no redundancy. A node failure makes it unavailable, so it is suitable only for development and testing.

### 4.3 Write Consistency

Leader receives write -> append WAL -> send AppendEntries in parallel -> majority ACK -> Commit -> reply to client. Performance cost: write throughput decreases by &lt; 15%, and latency increases by &lt; 5 ms.

### 4.4 Failover

| Event | Automatic Handling | Switch Time |
|------|---------|---------|
| Leader crash | Follower initiates election | &lt; 30 seconds |
| DNode network interruption | Remaining two replicas continue service | Immediate |
| DNode disk damage | `RESTORE DNODE` rebuilds from replicas | Depends on data volume |

### 4.5 Node Recovery

```sql
RESTORE DNODE <dnode_id>;
RESTORE MNODE ON DNODE <dnode_id>;
RESTORE VNODE ON DNODE <dnode_id>;
```

### 4.6 Replica Status Monitoring

```sql
SHOW VGROUPS;
-- status: leader / follower / offline / candidate
```

### 4.7 Highly Reliable MNode Metadata

MNode stores cluster metadata, including users, databases, tables, and the DNode list. Production deployments require three MNodes to form a Raft group:

```sql
SHOW MNODES;
CREATE MNODE ON DNODE 2;
CREATE MNODE ON DNODE 3;
```

When the Leader fails, a new Leader is elected automatically (&lt; 30 seconds), transparently to clients. DDL statements being executed during that period may fail briefly, but business reads and writes that are routed to VGroup Leaders are not affected.

---

## 5. Observability Access Reliability

taosKeeper writes monitoring metrics exported by taosd / taosAdapter / taosX / taosExplorer back to the `log` database. In the default deployment (`auditSaveInSelf = 0`), Enterprise audit logs are also written through Keeper to an audit database with `IS_AUDIT`. When `auditSaveInSelf = 1` (`v3.4.1.0+`), audit is written directly by the local cluster and does not pass through Keeper. For audit configuration, see [Audit and Compliance](./07-audit-and-compliance.md). This section focuses on metrics-path reliability and explains the dependency of the default audit path on Keeper.

### 5.1 taosKeeper Metrics Collection Reliability

taosKeeper receives metrics pushed by each component and writes them to the TDengine `log` database through WebSocket. In the classic path, it also receives audit reports from `taosd` and writes them to the audit database. Key reliability points:

- **Collector-side buffering**: Components such as taosd and taosAdapter locally cache the most recent batch of metrics. Temporary unavailability of taosKeeper or taosAdapter does not block the main path. Metrics collection failure does not affect business reads and writes.
- **Write retry**: taosKeeper automatically reconnects after the WebSocket connection to taosAdapter is disconnected. Metrics waiting to be written are temporarily stored in memory and written after reconnection.
- **Degradation tolerance**: A brief monitoring path interruption causes a small number of missing metric points, leaving gaps in monitoring charts, but business data is not affected. In the default audit path, long-term Keeper unavailability also interrupts audit persistence, although business reads and writes remain unaffected. Use `auditSaveInSelf` when audit needs to be decoupled from Keeper.

```toml
# /etc/taos/taoskeeper.toml
[tdengine]
host     = "localhost"
port     = 6041
username = "keeper_writer"
password = "KeeperPass123!"
usessl   = false
```

### 5.2 Impact of Keeper Failure

- taosKeeper is not on the business request path, so its failure does not affect business reads and writes.
- New metrics generated during the failure first remain in component-side buffers. After Keeper recovers, backfill push continues.
- If the interruption exceeds the buffer limit, monitoring gaps appear and observability is affected, but business data is not retroactively affected.
- In the default audit path, audit cannot be persisted through Keeper while Keeper is down. After recovery, it depends on the taosd-side buffering and retry window. If the window is exceeded, audit gaps may appear. Direct local-cluster audit writing (`auditSaveInSelf`) is not affected by this.

Production recommendation: a single instance is sufficient for small clusters. If continuous observability and audit continuity through Keeper are required, deploy two instances and distribute traffic through a frontend LB.

---

## 6. Storage and Audit Persistence Reliability

Data is ultimately persisted on the taosd side. This layer provides reliability through five mechanisms: **WAL write-ahead logging + snapshots + TDE transparent encryption + backup and restore + audit log privilege separation**. These mechanisms prevent data loss after process crashes, prevent leakage if disks are physically stolen, support recovery from accidental deletion or disasters, and make audit logs tamper-resistant.

### 6.1 WAL (Write-Ahead Log)

Each VNode maintains an independent WAL. Write requests are first appended to the WAL and fsynced to disk, then replicated to Followers. The client receives ACK only after majority confirmation.

```sql
-- WAL mode 1: memory cache (highest performance, latest data may be lost on crash)
CREATE DATABASE db WAL_LEVEL 1;

-- WAL mode 2: fsync to disk (recommended for production)
CREATE DATABASE db WAL_LEVEL 2 WAL_FSYNC_PERIOD 3000;
```

```ini
# taos.cfg
walRetentionPeriod 3600   # WAL retention duration (seconds)
walRetentionSize   0      # WAL retention size (bytes)
```

### 6.2 Snapshot (Snapshot / STT)

After WAL exceeds the retention threshold, TDengine merges committed data into STT (Snapshot Tier) files and cleans up old WAL. Snapshots are also used for:

- **Accelerating node recovery**: Newly added Followers or Followers offline for a long time catch up directly through snapshots instead of replaying all WAL.
- **Raft log compaction**: Prevents unlimited WAL growth.

Snapshots are triggered automatically by the system and generally require no manual intervention. Use `walRetentionPeriod` / `walRetentionSize` to control the WAL retention window. Only increments within this window can be consumed by TMQ/taosX subscriptions.

### 6.3 Transparent Data Encryption (TDE)

For operational details, including key hierarchy, rotation, and encrypted database creation, see [Data-at-Rest Protection](./06-data-security.md).

> TDE protects **storage-layer** data, including persisted data files, WAL, and snapshots. For transport-layer encryption, see [Full-Trace Transport Security and Compression](./02-full-trace-transport.md).

**Key hierarchy:**

```text
SVR_KEY -> DB_KEY -> CFG_KEY -> META_KEY -> DATA_KEY
```

**Encryption algorithms:**

| Algorithm | Applicable Scenario |
|------|---------|
| SM4-CBC | Chinese commercial cryptography compliance (commonly used) |
| AES-128-CBC | International standard |

**Enable example (`v3.4+`, must first use `taosk` to generate keys containing `DATA_KEY`):**

```sql
CREATE DATABASE db ENCRYPT_ALGORITHM 'SM4-CBC';
SHOW ENCRYPT_STATUS;
```

### 6.4 Data Backup and Restore

#### 6.4.1 taosdump Logical Backup (Full + Incremental)

```bash
# Full backup
taosdump -h localhost -u tduser -P SecurePass123! -o /backup

# Incremental backup
taosdump -h localhost -u tduser -P SecurePass123! -D mydb \
  -S "2024-01-01 00:00:00" -o /backup/incremental

# Restore
taosdump -h localhost -u tduser -P SecurePass123! -i /backup/mydb
```

#### 6.4.2 taosX Data Migration and Replication (Continuous Synchronization)

```bash
taosX run \
  --from "taos://source-host:6030/mydb" \
  --to   "taos://backup-host:6030/mydb_backup"
```

Based on the checkpoint + persistent queue + task restart mechanisms described in Section 3, the taosX replication path itself also supports resumable transfer and automatic recovery.

#### 6.4.3 Backup Strategy Recommendations

| Strategy | Method | Frequency | Coverage |
|------|------|------|---------|
| Multi-replica | `REPLICA 3` (or Enterprise `REPLICA 2`) | Real time, within the cluster | Single-node failure (for the fault tolerance boundary of two replicas, see Section 4.2) |
| Real-time replication | taosX | Continuous synchronization to another site | Data center-level disaster |
| Incremental backup | taosdump + time range | Daily | Accidental deletion, logical corruption |
| Full backup | taosdump | Weekly | Last-resort cold backup |

### 6.5 Audit Log Tamper Resistance (Privilege Separation)

For audit database configuration, operation lists, and the role model, see [Audit and Compliance](./07-audit-and-compliance.md) and [Privilege Management - Audit Database](../05-tdengine-sql/07-user-and-privilege/02-grant.md#audit-database).

Audit logs are written to an audit database with `IS_AUDIT`, whose default name is commonly `audit`, not the `log` database used for monitoring. When creating an audit database in `v3.4.0.0+`, the server enforces constraints such as `VGROUPS 1`, `WAL_LEVEL 2`, `PRECISION ns`, `ENCRYPT_ALGORITHM` not being `none`, and `KEEP >= 1825d`. RBAC reduces the risk of retroactive tampering:

- Only `SYSAUDIT_LOG` can write to the audit database. Only `SYSAUDIT` can view audit table data.
- Audit tables and their data rows cannot be deleted or modified. Audit databases default to `ALLOW_DROP = 0`. Before deletion, it must be changed to `1`, and only `SYSAUDIT` can delete or modify audit databases.
- Business accounts should not have write privileges to the audit database. Use an auditor role for viewing, separated from business reads and writes.
- When `auditLevel >= 5`, queries against audit tables may also generate new audit events.
- Through the Keeper path, audit can be written to a remote target cluster. You can also combine it with the taosX replication in Section 6.4 to synchronize the audit database to an independent secure cluster, further reducing the risk of tampering in a single cluster.

---

## 7. Common Troubleshooting

| Error Code | Description | Handling |
|--------|------|------|
| 0x80000903 | Sync timeout | Check the network and retry after election completes |
| 0x8000090C | Sync leader unreachable | Check DNode status and network |
| 0x80000911 | Sync not ready | Wait for node recovery to complete |
| 0x80000914 | Sync leader restoring | Wait for the new Leader log replay to complete |
| 0x80000916 | Sync buffer full | Reduce write concurrency |
| 0x80000917 | Sync write stall | Check disk I/O |

---

## 8. Deployment Checklist

### 8.1 Entry Layer

- [ ] Connection pools + timeouts are configured for each language connector
- [ ] Application-layer exponential backoff retries are wrapped around idempotent operations
- [ ] taosExplorer frontend is deployed as &gt;= 2 instances + load balancing
- [ ] Operations scripts use `taosX` resumable transfer / `taosdump` time-sliced resume

### 8.2 Access Layer

- [ ] taosAdapter query rate limiting is enabled (`queryLimitEnable = true`)
- [ ] taosAdapter memory protection thresholds are configured (`pauseQueryMemoryThreshold` / `pauseAllMemoryThreshold`)
- [ ] Clients are configured with `firstEp` + `secondEp` dual entries
- [ ] The load balancer uses taosAdapter `/-/ping` health checks

### 8.3 Data Ingestion Path

- [ ] Sufficient disk space is reserved for the taosX persistent queue (recommended &gt;= 50 GB)
- [ ] Data source checkpoint positions are explicitly configured and resumable transfer has been verified
- [ ] Automatic recovery thresholds and task restart paths have been rehearsed
- [ ] taosX-Agent `keep_online = true` is enabled if taosX-Agent is used

### 8.4 Cluster Internals

- [ ] The cluster has at least 3 DNodes; production databases prefer `REPLICA 3` (cost-sensitive scenarios can use Enterprise `REPLICA 2`; see [Two-Replica Solution](../12-operations-and-tooling/02-operations/11-ha/02-replica2.md))
- [ ] 3 MNodes are distributed on different DNodes
- [ ] `RESTORE DNODE` / `RESTORE VNODE` recovery workflows have been rehearsed
- [ ] `SHOW VGROUPS` is used for regular replica status inspection

### 8.5 Observability Access

- [ ] taosKeeper is running normally, and metrics continue to be written to the `log` database
- [ ] Component-side monitoring buffer capacity matches the acceptable disconnection duration
- [ ] Key environments have evaluated whether dual Keeper + frontend LB is needed
- [ ] If audit goes through Keeper: Keeper high availability and audit continuity have been evaluated, or `auditSaveInSelf` has been used

### 8.6 Storage and Audit Persistence

- [ ] Production databases use `WAL_LEVEL 2`, and `WAL_FSYNC_PERIOD` is tuned according to RPO
- [ ] The WAL retention window meets TMQ / taosX subscription requirements
- [ ] TDE is enabled if compliance requires it
- [ ] taosdump daily incremental + weekly full scheduled tasks are configured
- [ ] taosX remote real-time replication is running
- [ ] An `IS_AUDIT` audit database has been created (`VGROUPS 1`, encryption, `WAL_LEVEL 2`, `PRECISION ns`, `KEEP >= 1825d`) and audit is enabled
- [ ] Audit viewing uses `SYSAUDIT`; business accounts have no write privileges to the audit database
