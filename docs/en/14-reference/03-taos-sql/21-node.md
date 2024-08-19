---
title: Cluster
sidebar_label: Cluster
description: This document describes the SQL statements related to cluster management in TDengine.
---

The physical entities that form TDengine clusters are known as data nodes (dnodes). Each dnode is a process running on the operating system of the physical machine. Dnodes can contain virtual nodes (vnodes), which store time-series data. Virtual nodes are formed into vgroups, which have 1 or 3 vnodes depending on the replica setting. If you want to enable replication on your cluster, it must contain at least three nodes. Dnodes can also contain management nodes (mnodes). Each cluster has up to three mnodes. Finally, dnodes can contain query nodes (qnodes), which compute time-series data, thus separating compute from storage. A single dnode can contain a vnode, qnode, and mnode.

## Create a Dnode

```sql
CREATE DNODE {dnode_endpoint | dnode_host_name PORT port_val}
```

Enter the dnode_endpoint in hostname:port format. You can also specify the hostname and port as separate parameters.

Create the dnode before starting the corresponding dnode process. The dnode can then join the cluster based on the value of the firstEp parameter. Each dnode is assigned an ID after it joins a cluster.

## View Dnodes

```sql
SHOW DNODES;
```

The preceding SQL command shows all dnodes in the cluster with the ID, endpoint, and status.

## Delete a DNODE

```sql
DROP DNODE dnode_id
```

Note that deleting a dnode does not stop its process. You must stop the process after the dnode is deleted.

## Modify Dnode Configuration

```sql
ALTER DNODE dnode_id dnode_option

ALTER ALL DNODES dnode_option

dnode_option: {
    'resetLog'
  | 'balance' 'value'
  | 'monitor' 'value'
  | 'debugFlag' 'value'
  | 'monDebugFlag' 'value'
  | 'vDebugFlag' 'value'
  | 'mDebugFlag' 'value'
  | 'cDebugFlag' 'value'
  | 'httpDebugFlag' 'value'
  | 'qDebugflag' 'value'
  | 'sdbDebugFlag' 'value'
  | 'uDebugFlag' 'value'
  | 'tsdbDebugFlag' 'value'
  | 'sDebugflag' 'value'
  | 'rpcDebugFlag' 'value'
  | 'dDebugFlag' 'value'
  | 'mqttDebugFlag' 'value'
  | 'wDebugFlag' 'value'
  | 'tmrDebugFlag' 'value'
  | 'cqDebugFlag' 'value'
}
```

The parameters that you can modify through this statement are the same as those located in the dnode configuration file. Modifications that you make through this statement take effect immediately, while modifications to the configuration file take effect when the dnode restarts.

`value` is the value of the parameter, which needs to be in character format. For example, modify the log output level of dnode 1 to debug:

```sql
ALTER DNODE 1 'debugFlag' '143';
```

## Add an Mnode

```sql
CREATE MNODE ON DNODE dnode_id
```

TDengine automatically creates an mnode on the firstEp node. You can use this statement to create more mnodes for higher system availability. A cluster can have a maximum of three mnodes. Each dnode can contain only one mnode.

## View Mnodes

```sql
SHOW MNODES;
```

This statement shows all mnodes in the cluster with the ID, dnode, and status.

## Delete an Mnode

```sql
DROP MNODE ON DNODE dnode_id;
```

This statement deletes the mnode located on the specified dnode.

## Create a Qnode

```sql
CREATE QNODE ON DNODE dnode_id;
```

TDengine does not automatically create qnodes on startup. You can create qnodes as necessary for compute/storage separation. Each dnode can contain only one qnode. If a qnode is created on a dnode whose supportVnodes parameter is not 0, a vnode and qnode may coexist on the dnode. Each dnode can have a maximum of one vnode, one qnode, and one mnode. However, you can configure your cluster so that vnodes, qnodes, and mnodes are located on separate dnodes. If you set supportVnodes to 0 for a dnode, you can then decide whether to deploy an mnode or a qnode on it. In this way you can physically separate virtual node types.

## View Qnodes

```sql
SHOW QNODES;
```

This statement shows all qnodes in the cluster with the ID and dnode.

## Delete a Qnode

```sql
DROP QNODE ON DNODE dnode_id;
```

This statement deletes the mnode located on the specified dnode. This does not affect the status of the dnode.

## Modify Client Configuration

The client configuration can also be modified in a similar way to other cluster components.

```sql
ALTER LOCAL local_option

local_option: {
    'resetLog'
  | 'rpcDebugFlag' 'value'
  | 'tmrDebugFlag' 'value'
  | 'cDebugFlag' 'value'
  | 'uDebugFlag' 'value'
  | 'debugFlag' 'value'
}
```

The parameters that you can modify through this statement are the same as those located in the client configuration file. Modifications that you make through this statement take effect immediately, while modifications to the configuration file take effect when the client restarts.

## View Client Configuration

```sql
SHOW LOCAL VARIABLES;
```
