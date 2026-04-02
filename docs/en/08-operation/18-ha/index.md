---
sidebar_label: High Availability
title: High Availability
---

TDengine TSDB is a distributed time-series database with built-in high availability (HA). By default, it uses a standard three-replica architecture based on the RAFT protocol. To better fit different deployment scenarios, TDengine also offers a two-replica RAFT-based option, as well as an active-active solution using WAL data synchronization for users who prefer a traditional primary-standby setup.

- Three-replica architecture: Stores three copies of each dataset, delivering the highest level of availability at the highest cost.
- Dual-replica with arbitrator: Stores two copies of each dataset, but requires at least three nodes in the cluster. This reduces storage costs compared to three replicas while still ensuring strong consistency and high availability.
- Active-active: Runs with just two nodes. This setup provides good availability but only guarantees eventual consistency.

The key differences between these three approaches are outlined below:

| # | Three-Replica | Dual-Replica | Active-Active |
|:--|:----------|:----------|:--------|
| Cluster setup | Single cluster | Single cluster | Two independent clusters |
| Minimum nodes | Three data nodes | Two data nodes and one arbitrator node | Two data nodes |
| Leader election   | Raft algorithm | Arbitrator-managed | Not applicable |
| Replication method   | Raft algorithm | Raft algorithm | taosX |
| Sync latency   | None | None | Depends on taosX; typically seconds |
| Data durability | No data loss | No data loss | Depends on WAL retention |
| Data consistency | Strong (Raft) | Strong (Raft) | Eventual |
| Availability   | Service unaffected if any single node fails | Service unaffected if a single node fails, but cannot tolerate consecutive failures | Service remains available as long as one instance is alive |
