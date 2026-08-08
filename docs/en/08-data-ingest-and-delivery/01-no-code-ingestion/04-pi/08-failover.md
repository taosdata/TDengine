---
title: "PI Connector High Availability and Failover"
sidebar_label: "High Availability and Failover"
toc_max_heading_level: 4
---

This page describes how PI connector tasks are scheduled and fail over when multiple taosX instances are deployed and managed as Xnodes in the engine. It also covers data integrity during migration windows, to help you evaluate redundant deployments for production.

## taosX and Xnode

Both names refer to the same class of data-ingestion execution capability; which term you use depends on the context:

| Context | Name | Description |
| --- | --- | --- |
| External deployment and operations | taosX | Independently installed data-ingestion component that runs as a service or process; listens on gRPC and related ports for Agents and task execution |
| In-engine cluster management | Xnode | Data-ingestion execution node registered and managed in TDengine; brought into scheduling via SQL such as `CREATE XNODE`, `SHOW XNODES`, and `DRAIN XNODE` |

When you create an Xnode, `url` points to the corresponding taosX instance’s gRPC address (default port `6055`). The `xnoded` daemon handles connectivity and scheduling coordination between that execution node and `taosd`. For concepts and SQL, see [Data Ingestion (Xnode)](../../../05-tdengine-sql/08-cluster-management/02-xnode.md). For component deployment, see [taosX Reference](../../../12-operations-and-tooling/03-components/06-taosx.md).

In the rest of this page, **taosX** is used for deploying services, patching, and configuring Agent addresses; **Xnode** is used for in-cluster registration, scheduling, draining, and node status.

## Overview

In a cluster with multiple registered Xnodes, a PI connector task can continue on another taosX (mapped to another Xnode) after a taosX instance fails or undergoes planned maintenance. The scheduling model is one task instance on one Xnode at a time; high availability relies mainly on rescheduling after failure.

Migration or downtime causes a short collection interruption. Real-time tasks can backfill a recent window via [Restart Compensation Time](./05-realtime-guide.md#restart-compensation-time). Data beyond that window, or in-flight data not yet flushed, should be handled per your requirements using [Historical Data Backfill](./04-backfill-guide.md) and related procedures.

| Capability | Description |
| --- | --- |
| Continue or resume the task on another node when one taosX / Xnode is unavailable | Supported; see scheduling and failover below |
| Live values, Snapshot updates, and out-of-order late values in the migration window | Depend on the restart compensation window and follow-up backfill |
| Same PI task running on two nodes at once | Scheduler and worker mechanisms help prevent dual runs |
| Rolling upgrade of a single taosX | Drain the task with `DRAIN XNODE` before upgrading |

:::note
The behavior below is based on general Xnode scheduling and the current PI connector implementation. PI-specific high-availability testing for data-ingestion tasks is still being refined.
:::

## Scheduling Model

A PI task is scheduled as a single Job onto the current “best” Xnode (online, required Agent attached, more free memory, and so on), and runs on one Xnode at a time.

- This is single-instance scheduling, not active-standby dual-instance collection.
- After a node failure, the scheduler restarts the task on an available Xnode.
- You can run this in a cluster with 2, 3, or more registered Xnodes; the node count does not change the single-instance semantics.

## Failover Behavior

When the taosX that hosts the running task is stopped, patched and rebooted, or crashes:

1. The Xnode side heartbeats taosX about every 5 seconds.
2. After failure, the node is marked offline; after about 6 backoff retries, rebalancing selects the best available Xnode for related Jobs and restarts them.
3. Typical detect-and-migrate latency is about 10–30 seconds (depending on network and cluster load).

For planned maintenance, migrate tasks proactively instead of waiting for failure timeout:

```sql
DRAIN XNODE <id>;
```

This command reassigns existing tasks on the specified Xnode to other Xnodes. See [Drain Xnode](../../../05-tdengine-sql/08-cluster-management/02-xnode.md#drain-xnode).

### Failover Prerequisites

Configure taosX-Agent so it can reach all related taosX endpoints (the gRPC addresses registered for each Xnode). If the Agent points to only one taosX, the task may have nowhere to fail over when that instance is unavailable. See [Configuration Reference](../../../12-operations-and-tooling/03-components/07-taosx-agent/configuration.md).

## Subscription, Checkpoint, and State During Failover

Failover interrupts the PI DataPipe subscription:

- The subscription on the original taosX ends when the process exits.
- The task instance on the new Xnode (another taosX) creates a new DataPipe and re-subscribes the relevant points.

In the current implementation:

- PI real-time collection tasks do not yet carry a migratable checkpoint / sync state with the task.
- [Restart Compensation Time](./05-realtime-guide.md#restart-compensation-time) (MaxBackfillRange) backfills recent historical data after restart according to the configured window, which suits short interruptions.
- When using restart compensation, note its bounds:
  - In-flight data not yet flushed at crash time may be lost;
  - Late data older than the compensation window is not covered by this mechanism.

## Handling Data in the Interruption Window

Whether data produced or updated in the interruption window is written to TDengine depends on interruption length and restart compensation settings:

- Historical data within the restart compensation window can be backfilled after the task starts on the new Xnode.
- Live values, PI Snapshot updates, and out-of-order / late values beyond the compensation window should be recovered with a [PI backfill task](./04-backfill-guide.md) after you confirm the interruption range, then validated with volume checks and spot checks.

## Preventing the Same Task on Two Nodes

Scheduler and worker constraints reduce the risk of duplicate writes from dual runs of the same PI task:

- Xnode scheduling runs on the mnode leader; a worker acknowledges only one scheduler connection tagged with a connection-id, and a new scheduler disconnects the old one.
- The data-ingestion worker scheduler rejects duplicate starts of the same `(task_id, job_id)`.

## Rolling Upgrades and Planned Maintenance

You can patch or upgrade taosX hosts one at a time without deliberately stopping overall collection. Recommended flow:

1. Run `DRAIN XNODE <id>` for the Xnode that maps to that taosX, so tasks move away.
2. Patch / upgrade / reboot that taosX.
3. After the corresponding Xnode is online again, it can take new tasks under the scheduling policy.

Additional notes:

- Planned migration has a short collection gap similar to failover; data integrity behaves as described above.
- If the upgraded node also hosts the mnode leader, tasks across the cluster may pause briefly during `xnoded` switchover, then resume automatically.

## Recommended Production Architecture

For aggregating multiple PI systems into a central TDengine with redundancy on the data-ingestion side:

| Component | Recommendation |
| --- | --- |
| TDengine | 3 dnodes, three-replica mnode |
| taosX / Xnode | Deploy 2–3 taosX instances and register corresponding Xnodes in the engine |
| Storage | Shared storage (per Enterprise HA deployment requirements) |
| taosX-Agent | On Windows hosts that can reach PI; configure all related taosX endpoints |
| PI dependencies | Licensed PI AF SDK (and appropriate Windows service account permissions) |

For network and Agent proxy topology, see [Deployment Architecture](./02-deployment-architecture.md). For storage-side replica HA, see [High Availability](../../../12-operations-and-tooling/02-operations/11-ha/index.md) (a different layer from Xnode task scheduling).

## Version, Components, and Licensing

Using the capabilities above typically requires:

- TDengine TSDB Enterprise v3.4.0.0 or later
- Enterprise license
- Deployed and licensed PI AF SDK (on the Windows environment hosting taosX / taosX-Agent)
- Shared storage that meets deployment requirements
- Corresponding Xnodes created, with taosX-Agent pointing at all related taosX endpoints in the cluster

## Related Documentation

- [Deployment Architecture](./02-deployment-architecture.md)
- [Real-time Data Sync](./05-realtime-guide.md) (restart compensation time)
- [Historical Data Backfill](./04-backfill-guide.md)
- [Data Ingestion (Xnode)](../../../05-tdengine-sql/08-cluster-management/02-xnode.md)
- [taosX Reference](../../../12-operations-and-tooling/03-components/06-taosx.md)
- [Configuration Reference](../../../12-operations-and-tooling/03-components/07-taosx-agent/configuration.md) (taosX-Agent)
