---
sidebar_label: Full-Trace High Availability
title: Full-Trace High Availability
description: High availability capabilities such as multi-instance deployment, failover, load distribution, and active-active disaster recovery
toc_max_heading_level: 3
---

This document describes the high availability path of TDengine TSDB under a unified six-layer framework: entry layer, access layer, data ingestion path, cluster internals, observability access, and storage layer. It explains automatic failover and load distribution section by section. The original "full-trace load balancing" content has been incorporated into the corresponding sections.

> Terminology
>
> - Access layer: the protocol/proxy layer after the entry layer and before taosd, including the two physical paths taosAdapter and taosc.
> - Task restart: shutting down and starting the taosX task again after all automatic recovery mechanisms have been exhausted. It is different from a data source connection retry.
> - Checkpoint: by default, the data source-side progress position. Whether taosX-Agent is used only affects the local cache location and does not change the recovery semantics of checkpoints.

---

## Overview

```mermaid
graph TD
    classDef client fill:#f39c12,color:#000,stroke:#d68910
    classDef svc    fill:#1a6ea8,color:#fff,stroke:#0d4f7d
    classDef core   fill:#2c3e50,color:#fff,stroke:#1a252f
    classDef ops    fill:#16a085,color:#fff,stroke:#0e6655
    classDef store  fill:#7f8c8d,color:#fff,stroke:#566573
    classDef ext    fill:#27ae60,color:#fff,stroke:#1e8449

    ENTRY(["① Entry Layer<br/>Programmatic Entry / Web UI / CLI"])
    ACCESS(["② Access Layer<br/>Nginx / HAProxy + taosAdapter / taosc"])
    INGEST(["③ Data Ingestion Path<br/>External Data Source -> taosX-Agent (Optional) -> taosX / XNode"])
    CORE(["④ Cluster Internals<br/>taosd Cluster: Multiple DNodes + Multi-Replica + Leader Election"])
    OBS(["⑤ Observability Access<br/>taosKeeper Multi-Instance + LB"])
    STORE(["⑥ Storage Layer<br/>VGroup / WAL / Snapshot / Backup and Restore"])
    DR(["Extension: Active-Active Cross-Cluster DR"])

    ENTRY --> ACCESS

    ACCESS --> INGEST

    INGEST --> CORE

    CORE --> OBS

    OBS --> STORE

    STORE -.-> DR

    class ENTRY client
    class ACCESS,INGEST svc
    class CORE core
    class OBS ops
    class STORE store
    class DR ext
```

> The diagram is organized by document hierarchy rather than the runtime main write path. taosKeeper belongs to the side-path observability flow, and active-active disaster recovery is listed separately as an extension beyond the six layers.

### High Availability Layer Overview

| Layer | Component | HA Mechanism | Description |
|------|------|---------|------|
| ① Entry layer | Connectors / taosExplorer / CLI | Multi-node URLs, multiple instances, client failover | Avoids single points during connection establishment |
| ② Access layer | taosAdapter / taosc | Nginx/HAProxy + health checks; `firstEp` / `secondEp` + Leader redirection | Requests can be routed to any healthy taosd |
| ③ Data ingestion | taosX / XNode / taosX-Agent | Multiple XNode instances, task migration, task restart, optional local cache | Tasks continue without interruption or missed ingestion |
| ④ Cluster internals | taosd cluster | Multi-replica + Leader election | Single-node failure does not interrupt writes |
| ⑤ Observability access | taosKeeper | Multiple instances + LB | Metrics and default-path audit continue; failure does not block business |
| ⑥ Storage layer | VGroup / WAL / snapshot | Multi-replica + catch-up + backup and restore | Replica recovery and disaster recovery fallback |

---

## 1. Entry Layer High Availability

The HA goal of the entry layer is that when any entry instance fails, clients can still establish connections and send requests to healthy backends. The key mechanisms are **multi-node / multi-instance** deployment and **client failover**.

### 1.1 Programmatic Entry (Applications / Language Connectors)

#### 1.1.1 Multi-Node URL Configuration

Production environments must not hard-code a single node. Application-side connection strings should point to an LB VIP or a multi-node list.

```java
// Java - points to LB VIP (WebSocket/REST path)
String url = "jdbc:TAOS-RS://lb-vip:6041/db";
```

```go
// Go (WebSocket)
db, _ := sql.Open("taosWS", "tduser:SecurePass123!@ws(lb-vip:6041)/db")
```

```python
# Python (WebSocket)
conn = taosws.connect("taos+ws://tduser:SecurePass123!@lb-vip:6041/db")
```

The taosc private protocol path connects directly to taosd in a `firstEp,secondEp` multi-endpoint form (see Section 2.2):

```java
// JDBC (embedded taosc, private protocol with multiple EPs)
String url = "jdbc:TAOS://dnode1:6030,dnode2:6030/db";
```

```go
// Go (embedded taosc, private protocol with multiple EPs)
cfg := taos.NewConfig()
cfg.Addr = "dnode1:6030,dnode2:6030"
```

#### 1.1.2 How Automatic Failover Works After Configuration

**WebSocket / REST path (through taosAdapter)**:

1. The connector connects only to `lb-vip:6041` and is unaware of individual taosAdapter instances.
2. The frontend Nginx / HAProxy removes failed instances through `/-/ping`.
3. If the current connection has been disconnected, the connector or application layer re-establishes the connection according to the reconnection strategy.
4. The new connection automatically lands on a healthy taosAdapter. That instance then routes to a healthy taosd through its internal taosc.

**Native taosc path (direct connection to taosd)**:

1. The client configures `firstEp` / `secondEp` or a multi-EP DSN.
2. When the first access point is unreachable, taosc automatically switches to the next entry.
3. After obtaining the cluster topology, requests are routed directly to the corresponding Leader.
4. When the Leader switches, taosc refreshes routing based on the return code and retries transparently.

#### 1.1.3 Connection Pool and Load Distribution Recommendations

Connection pools are used not only for concurrency and performance, but also to determine how reconnected connections are redistributed across multiple taosAdapter instances.

| Language | Library | Maximum Connections | Recommendation |
|------|-----|----------|------|
| Java | HikariCP | `maximumPoolSize=10` | Use with `connectionTestQuery` to quickly evict bad connections after failure |
| Go | `database/sql` | `SetMaxOpenConns(20)` | Use with `SetConnMaxLifetime()` to periodically rebuild connections |
| Python | Application-managed | N/A | Rebuild connections after catching exceptions and return them to the connection pool |
| Node.js | `taos.sqlConnect()` | N/A | Transparently rebuild long connections on exception |

### 1.2 Web UI Entry (taosExplorer Multiple Instances)

taosExplorer itself is a stateless web frontend. Multiple instances can be deployed on multiple hosts, with high availability implemented through a frontend Nginx / HAProxy reverse proxy.

```text
explorer-1: taosExplorer:6060
explorer-2: taosExplorer:6060
```

```nginx
upstream tdengine_explorer {
    ip_hash;
    server explorer1.example.com:6060 max_fails=3 fail_timeout=10s;
    server explorer2.example.com:6060 max_fails=3 fail_timeout=10s;
}

server {
    listen 443 ssl;
    server_name explorer.example.com;
    ssl_certificate     /etc/taos/certs/explorer.crt;
    ssl_certificate_key /etc/taos/certs/explorer.key;

    location / {
        proxy_pass http://tdengine_explorer;
        proxy_http_version 1.1;
        proxy_set_header Upgrade $http_upgrade;
        proxy_set_header Connection "upgrade";
        proxy_next_upstream error timeout http_502 http_503 http_504;
    }
}
```

How high availability is implemented:

1. The browser always accesses the unified `explorer.example.com`.
2. Nginx forwards new requests only to healthy instances. After an instance fails, it automatically switches to another replica.
3. taosExplorer sessions are stateless, but each node must be registered. Restarting a single instance does not create a cluster-level single point.
4. taosExplorer depends on long WebSocket sessions and connections should remain as stable as possible, so the `ip_hash` strategy is recommended.

### 1.3 Command-Line Tool Entry

Operations CLI tools must be able to connect to other nodes when any cluster node fails.

**`taos` shell (embedded taosc, private protocol)**: Implements multi-endpoint failover through `firstEp` / `secondEp` in `taos.cfg`:

```ini
# /etc/taos/taos.cfg
firstEp  dnode1:6030
secondEp dnode2:6030
```

When `taos` starts, it first attempts `firstEp`, then automatically switches to `secondEp` on failure. After obtaining the complete DNode list, subsequent requests are routed by Leader.

**`taosX` CLI (data pipeline)**: Source and target DSNs can both be written as LB VIPs or multiple nodes:

```bash
taosX run \
  -f "mqtt://mqtt.example.com:1883" \
  -t "taos+ws://tduser:SecurePass123!@lb-vip:6041/db"
```

**`taosdump` (backup and restore)**: Specify the LB VIP through `-h` or connect to `firstEp`. If a single node is unreachable, retry against another DNode:

```bash
taosdump -h lb-vip -P 6030 -u tduser -p'SecurePass123!' -D db -o /backup
```

---

## 2. Access Layer High Availability

> **About taosc**: taosc is the native TDengine client library. As an **independent component**, it provides C API and DSN connection interfaces upward and communicates with the taosd cluster downward through a **private protocol** (TCP :6030). Its built-in `firstEp` / `secondEp` multi-EP probing and Leader redirection are key HA mechanisms of the access layer. Path A (WebSocket/REST) calls taosc inside taosAdapter on the server side to complete the final hop to taosd: multiple Adapter instances are fronted by LB, and taosc embedded in each Adapter performs node failover. Path B embeds the taosc dynamic library directly in applications or CLIs, with taosc handling node switching entirely. Both paths eventually enter taosd through taosc.

The HA goal of the access layer is: **entry-layer requests can be routed to any healthy taosd node**. Load distribution and failover are implemented together at this layer.

### 2.1 Path A: WebSocket / REST -> taosAdapter

#### 2.1.1 taosAdapter Multi-Instance Deployment

Multiple taosAdapter instances form a stateless cluster:

```text
adapter-1: taosAdapter:6041
adapter-2: taosAdapter:6041
adapter-3: taosAdapter:6041
```

Any taosAdapter instance can receive requests and forward them to taosd, so an LB can be placed in front for health checks and traffic distribution.

#### 2.1.2 Nginx / HAProxy Configuration

```nginx
upstream tdengine_adapter {
    least_conn;
    server dnode1:6041 max_fails=3 fail_timeout=10s;
    server dnode2:6041 max_fails=3 fail_timeout=10s;
    server dnode3:6041 max_fails=3 fail_timeout=10s;
}

server {
    listen 6041;

    location / {
        proxy_pass http://tdengine_adapter;
        proxy_http_version 1.1;
        proxy_set_header Upgrade $http_upgrade;
        proxy_set_header Connection "upgrade";
        proxy_connect_timeout 5s;
        proxy_read_timeout 300s;
        proxy_next_upstream error timeout http_502 http_503 http_504;
    }

    location /-/ping {
        proxy_pass http://tdengine_adapter/-/ping;
    }
}
```

```haproxy
defaults
    mode http
    timeout connect 5s
    timeout client  300s
    timeout server  300s

frontend tdengine_fe
    bind *:6041
    default_backend tdengine_be

backend tdengine_be
    balance leastconn
    option httpchk GET /-/ping
    server ta1 dnode1:6041 check inter 3s fall 3 rise 2
    server ta2 dnode2:6041 check inter 3s fall 3 rise 2
    server ta3 dnode3:6041 check inter 3s fall 3 rise 2
```

#### 2.1.3 How High Availability Is Implemented

1. The LB keeps only healthy taosAdapter instances through `/-/ping`.
2. New requests are distributed to healthy instances by `least_conn` / round-robin strategies, spreading daily traffic. This differs from taosExplorer.
3. When an instance returns 503, times out, or disconnects, the LB stops forwarding traffic to that node.
4. taosAdapter internally calls taosc to continue routing requests to the healthy taosd Leader.

#### 2.1.4 Strategy Selection

| Scenario | Recommended Strategy | Description |
|------|---------|------|
| Stateless REST | Round-robin / `least_conn` | New requests are spread evenly; stickiness is not required |
| WebSocket long connections | `ip_hash` / `balance source` | Keep the same client on the same instance as much as possible |
| Mixed workload | `least_conn` | Avoid long connections piling up on a single node |

### 2.2 Path B: taosc -> taosd Private Protocol (Multi-EP Probing)

Native connections embed taosc as an independent client library, connect directly to taosd through the TDengine **private protocol** (TCP :6030), and use `firstEp` / `secondEp` for node probing and automatic failover:

```ini
# taos.cfg
firstEp  dnode1:6030
secondEp dnode2:6030
```

- taosc first connects to `firstEp` to obtain the complete MNode / DNode list.
- When `firstEp` is unreachable, it automatically switches to `secondEp`.
- After obtaining the list, it automatically distributes requests to the corresponding Leader.
- After Leader switching, taosc identifies the change by return code and automatically updates routing.

**Connector DSN support for multiple EPs**:

```java
// JDBC (embedded taosc, private protocol)
String url = "jdbc:TAOS://dnode1:6030,dnode2:6030/db";
```

```go
// Go (embedded taosc, private protocol)
cfg := taos.NewConfig()
cfg.Addr = "dnode1:6030,dnode2:6030"
```

> Production recommendation: `firstEp` and `secondEp` must be on **different hosts / different racks**. Otherwise, a single-host or single-rack failure can invalidate both at the same time.

---

## 3. Data Ingestion Path High Availability

The data ingestion path works as "**external data source -> taosX-Agent (optional) -> taosX / XNode -> taosAdapter -> taosd**". The HA goals are: **tasks do not interrupt, ingestion is not missed, and failures can be automatically taken over**.

### 3.1 Multiple XNode Instances and Task Migration

taosX manages multiple XNode instances. When a task fails, it can be automatically scheduled to a healthy node:

```text
taosX
  ├── XNode-1 @ Edge-A  [Running]
  ├── XNode-2 @ Edge-B  [Running]
  └── XNode-3 @ Edge-C  [Standby]
```

XNodes are managed uniformly by the taosX scheduler. A single XNode outage does not affect global task execution.

### 3.2 Automatic Recovery and Task Restart

The automatic recovery sequence of taosX is usually:

1. Source connection retry;
2. Sink (taosAdapter) reconnection;
3. Replay of the local persistent queue;
4. If all of the above mechanisms fail, trigger **task restart**.

The checkpoint records the **data source-side position** and is used to restore ingestion progress after task restart. It is not the same mechanism as the Sink-side persistent queue.

| Concept | Scope | Triggered By | Shuts Down Task |
|------|---------|-------|-------------|
| Data source connection retry | Current Source connection | Source connector | No |
| Sink reconnection | Current downstream connection | taosX / taosAdapter | No |
| Task restart | Entire task | Scheduler / operations | Yes |

### 3.3 Is taosX-Agent Required?

Whether taosX-Agent is deployed only affects the edge cache strategy:

| Data Source Scenario | Agent Recommended | Reason |
|-----------|---------------|------|
| Kafka / Pulsar / TMQ | Usually not needed | Can rely on upstream persistent cache and offset/cursor |
| MQTT QoS 0 / direct device ingestion | Recommended | Upstream usually cannot replay reliably |
| OPC-UA / OPC-DA / industrial edge networks | Recommended | Weak networks are common and require local cache for resumable transfer across outages |
| MySQL / PostgreSQL incremental pull | Depends on scenario | Often relies on source table retention window and incremental column |

### 3.4 taosX-Agent Disconnection Reconnection

taosX-Agent is deployed on the remote edge and maintains a long connection to taosX. When network jitter occurs or the taosX instance switches:

- Agent automatically reconnects and restores the session;
- The ingestion buffer (local persistent queue) ensures that data is not lost during reconnection;
- After reconnection succeeds, it continues uploading from the acknowledged position.

For Store and Forward configuration details, see [Store and Forward](../12-operations-and-tooling/03-components/07-taosx-agent/store-and-forward.md).

---

## 4. Cluster Internal High Availability

The core of internal cluster HA is: **multiple DNodes + multi-replica + Leader election**. The diagram does not expand MNode / VGroup / Raft details, but actual failover depends on these mechanisms.

### 4.1 MNode Metadata High Availability

MNode manages cluster metadata, including database and table definitions, user privileges, and VGroup distribution. It uses Raft multi-replica, with 3 replicas recommended:

```sql
SHOW MNODES;
CREATE MNODE ON DNODE 2;
CREATE MNODE ON DNODE 3;
```

| Node | Role | Status |
|------|------|------|
| DNode-1 | Leader | Ready |
| DNode-2 | Follower | Ready |
| DNode-3 | Follower | Ready |

### 4.2 VGroup High Availability Overview

VGroups use multiple replicas to carry time-series data:

```sql
CREATE DATABASE db REPLICA 3 VGROUPS 10;
SHOW db.VGROUPS;
```

**Write routing**: taosc automatically sends writes to the VNode Leader. After the Leader switches, taosc automatically updates routing.

**Read consistency**:

| Read Mode | Parameter | Description |
|---------|------|------|
| Eventual consistency | `queryPolicy=1` | Reads Followers, lower latency |
| Strong consistency | `queryPolicy=3` | Reads Leader, guarantees latest data |

### 4.3 Leader Election Mechanism

Both MNode and VGroup use Raft:

- Leader sends heartbeats periodically. If a Follower does not receive heartbeats within the election timeout, it starts a new election.
- A new Leader takes effect after majority (N/2+1) votes.
- During election, usually &lt; 30 seconds, the corresponding Raft group is read-only and not writable.
- The new Leader compares term and LogIndex to ensure committed logs are not lost.

---

## 5. Observability Access High Availability

taosKeeper receives monitoring metrics exported by taosd / taosAdapter / taosX / taosExplorer and writes them back to the `log` database. Under the default audit path (`auditSaveInSelf = 0`), Enterprise audit also writes to the audit database through taosKeeper. When `auditSaveInSelf = 1` (`v3.4.1.0+`), audit does not pass through Keeper. For details, see [Audit and Compliance](./07-audit-and-compliance.md).

### 5.1 taosKeeper Multi-Instance Deployment

```text
keeper-1: taosKeeper:6043
keeper-2: taosKeeper:6043
```

```nginx
upstream tdengine_keeper {
    least_conn;
    server keeper1.example.com:6043 max_fails=3 fail_timeout=10s;
    server keeper2.example.com:6043 max_fails=3 fail_timeout=10s;
}

server {
    listen 6043;
    location / {
        proxy_pass http://tdengine_keeper;
        proxy_next_upstream error timeout http_502 http_503 http_504;
    }
}
```

The `monitorFqdn` / `keeperUrl` of each component, and the audit reporting target when routed through Keeper, should point to the LB VIP rather than a single instance.

### 5.2 Failure Impact

- taosKeeper is not on the business request path, so its failure **does not affect business reads and writes**.
- New metrics generated during the failure first remain in component-side buffers. After Keeper recovers, backfill push continues.
- If the interruption exceeds the buffer limit, monitoring gaps appear and observability is affected, but business data is not rolled back and writes do not fail.
- In the default audit path, Keeper failure also interrupts audit persistence. Use `auditSaveInSelf` when audit needs to be decoupled from Keeper.

---

## 6. Storage Layer High Availability

The core of storage-layer HA is **VGroup multi-replica placement** + **new replica catch-up**.

### 6.1 VGroup Replica Placement Strategy

Each VGroup contains N VNodes (N = `REPLICA`) distributed across different DNodes:

- Multiple VNodes in the same VGroup must be distributed across different DNodes. This is enforced by the system.
- Production recommendation: distribute DNodes across different hosts / racks / cabinets.
- `REPLICA 3` tolerates one DNode failure. Plan higher replica counts according to business requirements.

```sql
-- View VGroup replica distribution
SHOW db.VGROUPS;
```

### 6.2 Snapshot + WAL Catch-Up for New Replicas

When a Follower replica is disconnected beyond the window, or when a new replica is added, the Leader catches it up through **snapshot + incremental WAL**:

1. The Leader triggers a snapshot (SNAPSHOT) and transfers it to the lagging replica;
2. The lagging replica loads the snapshot into the state machine;
3. The Leader continues sending WAL logs after the snapshot point for replay one by one;
4. After catching up, the replica enters Ready status and starts participating in majority voting.

This mechanism ensures automatic data recovery when a DNode is replaced after outage or replicas are added during scale-out. For details, see the WAL / snapshot sections in [Full-Trace High Reliability](./03-full-trace-reliability.md).

---

## 7. Active-Active Cross-Cluster Disaster Recovery

> 3.3.6+ Enterprise

Two clusters back up each other. Each accepts writes and synchronizes data bidirectionally in real time. This is used for cross-data-center / cross-region disaster recovery.

```text
Cluster A (Beijing) <-> Cluster B (Shanghai)
  taosd ①②③  --taosX-->  taosd ④⑤⑥
  taosd ①②③  <--taosX--  taosd ④⑤⑥
```

### 7.1 Configuration Steps

1. Deploy two clusters independently (`REPLICA 3`).
2. Deploy taosX synchronization tasks (A -> B and B -> A).
3. Synchronization tasks use database subscription (TMQ) to obtain incremental data.
4. Configure conflict resolution strategy (default: timestamp priority).

### 7.2 Switch Strategy

| Mode | Switch Time | Data Loss |
|------|---------|---------|
| Planned switch | 0 | 0 |
| Failover | &lt; 1 minute | &lt;= synchronization delay |

Applications switch traffic to the standby cluster through DNS / GSLB / frontend LB. Only the VIP in the connection string needs to be changed.

---

## 8. Deployment Checklist

### 8.1 Entry Layer

- [ ] Application connector URLs point to LB VIPs or multi-EP lists, with no hard-coded single nodes
- [ ] Connection pool parameters are configured and bad connections can be quickly evicted
- [ ] taosExplorer is deployed as &gt;= 2 instances and distributed by frontend Nginx / HAProxy
- [ ] CLIs such as `taos` / `taosX` / `taosdump` are configured with multiple EPs or point to LB

### 8.2 Access Layer

- [ ] taosAdapter is deployed as &gt;= 2 instances
- [ ] Nginx / HAProxy is configured with `/-/ping` health checks
- [ ] Round-robin, `least_conn`, or sticky strategy has been selected according to REST / WebSocket scenarios
- [ ] `firstEp` / `secondEp` are not on the same host

### 8.3 Data Ingestion Path

- [ ] XNode &gt;= 2 for critical data ingestion tasks
- [ ] Checkpoint recovery, Sink reconnection, and task restart paths have been verified
- [ ] taosX-Agent local persistent queue capacity matches the network disconnection duration if taosX-Agent is used

### 8.4 Cluster Internals

- [ ] DNode &gt;= 3, distributed across different hosts / racks
- [ ] 3 MNodes are distributed on different DNodes
- [ ] Automatic Leader election after Leader failure has been rehearsed

### 8.5 Observability Access

- [ ] taosKeeper &gt;= 2 instances in key environments
- [ ] Each component's `monitorFqdn` / `keeperUrl` points to the VIP
- [ ] The acceptable monitoring gap duration during Keeper failure has been evaluated

### 8.6 Storage Layer

- [ ] `REPLICA 3` is used for all production databases
- [ ] VGroup replicas are distributed across hosts / racks
- [ ] Replica catch-up after DNode outage has been rehearsed

### 8.7 Disaster Recovery

- [ ] Active-active cross-cluster deployment is used if RPO &lt; 5 minutes is required
- [ ] Scheduled taosdump backups are configured if compliance requires them
- [ ] The failover process has been rehearsed
