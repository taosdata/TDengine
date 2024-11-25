---
title: Manage Nodes
description: Detailed analysis of SQL commands for managing cluster nodes
slug: /tdengine-reference/sql-manual/manage-nodes
---

The physical entities that make up a TDengine cluster are called dnodes (short for data nodes), which are processes running on the operating system. Within a dnode, virtual nodes (vnodes) can be created to handle time-series data storage. In a multi-node cluster environment, when a database has a replica of 3, each vgroup in that database consists of 3 vnodes. When a database has a replica of 1, each vgroup consists of 1 vnode. To configure a database for multiple replicas, the number of dnodes in the cluster must be at least 3. A dnode can also create management nodes (mnodes), with a maximum of three mnodes per cluster. In TDengine version 3.0.0.0, a new logical node type called qnode (query node) was introduced to support the separation of computing and storage. Qnodes and vnodes can coexist in the same dnode or be completely separated onto different dnodes.

## Create Data Node

```sql
CREATE DNODE {dnode_endpoint | dnode_host_name PORT port_val}
```

Here, `dnode_endpoint` is in the format of `hostname:port`, and you can also specify the hostname and port separately.

In practice, it is recommended to create the dnode first and then start the corresponding dnode process so that it can immediately join the cluster based on the firstEP in its configuration file. Each dnode is assigned an ID after successfully joining.

## View Data Nodes

```sql
SHOW DNODES;
```

This command lists all the data nodes in the cluster, including fields such as the dnode ID, endpoint, and status.

## Delete Data Node

```sql
DROP DNODE dnode_id
```

Note that deleting a dnode does not stop the corresponding process. It is recommended to delete a dnode before stopping its associated process.

## Modify Data Node Configuration

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

The above syntax allows modifying these configuration items in a way similar to how they are configured in the dnode configuration file, with the difference being that the modifications take effect immediately and do not require restarting the dnode.

The value is the parameter value, which should be in character format. For example, to modify the log output level of dnode 1 to debug:

```sql
ALTER DNODE 1 'debugFlag' '143';
```

## Add Management Node

```sql
CREATE MNODE ON DNODE dnode_id
```

By default, a MNODE is created on the firstEP node when the system starts. Users can use this command to create additional MNODEs to improve system availability. A maximum of three MNODEs can exist in a cluster, and only one MNODE can be created on each DNODE.

## View Management Nodes

```sql
SHOW MNODES;
```

This command lists all the management nodes in the cluster, including their ID, the DNODE they are on, and their status.

## Delete Management Node

```sql
DROP MNODE ON DNODE dnode_id;
```

This command deletes the MNODE on the specified DNODE.

## Create Query Node

```sql
CREATE QNODE ON DNODE dnode_id;
```

By default, there are no QNODEs in the system, and users can create QNODEs to achieve separation of computing and storage. Only one QNODE can be created on each DNODE. If the `supportVnodes` parameter of a DNODE is not 0, and a QNODE is created on it, that DNODE will have both vnodes for storage management and qnodes for query computation. Additionally, if an MNODE is created on the same DNODE, all three types of logical nodes can coexist. However, through configuration, these nodes can also be completely separated. Setting the `supportVnodes` parameter of a DNODE to 0 allows for the creation of either an MNODE or a QNODE, achieving a physical separation of the three logical nodes.

## View Query Nodes

```sql
SHOW QNODES;
```

This command lists all the query nodes in the cluster, including their ID and the DNODE they are on.

## Delete Query Node

```sql
DROP QNODE ON DNODE dnode_id;
```

This command deletes the QNODE on the specified DNODE, but it does not affect the status of that DNODE.

## Query Cluster Status

```sql
SHOW CLUSTER ALIVE;
```

This command checks whether the current cluster is available, returning values: 0 for unavailable, 1 for fully available, and 2 for partially available (some nodes in the cluster are offline, but others can still operate normally).

## Modify Client Configuration

If the client is viewed as part of the broader cluster, you can dynamically modify client configuration parameters with the following command.

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

The parameters in the above syntax are the same as those configured in the client configuration file, but they take effect immediately without restarting the client.

## View Client Configuration

```sql
SHOW LOCAL VARIABLES;
```
