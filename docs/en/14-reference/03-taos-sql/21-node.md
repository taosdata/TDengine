---
title: Manage Nodes
slug: /tdengine-reference/sql-manual/manage-nodes
---

The physical entities that make up a TDengine cluster are dnodes (short for data nodes), which are processes running on top of the operating system. Within a dnode, vnodes (virtual nodes) can be established for storing time-series data. In a multi-node cluster environment, when the replica of a database is 3, each vgroup in that database consists of 3 vnodes; when the replica is 1, each vgroup consists of 1 vnode. To configure a database with multiple replicas, there must be at least 3 dnodes in the cluster. In a dnode, an mnode (management node) can also be created, with a maximum of three mnodes in a single cluster. In TDengine 3.0.0.0, to support separation of storage and computation, a new logical node called qnode (query node) was introduced, which can either coexist with a vnode in the same dnode or be completely separated on different dnodes.

## Create Data Node

```sql
CREATE DNODE {dnode_endpoint | dnode_host_name PORT port_val}
```

Where `dnode_endpoint` is in the format `hostname:port`. You can also specify hostname and port separately.

In practice, it is recommended to first create a dnode and then start the corresponding dnode process, so that the dnode can immediately join the cluster according to the firstEP in its configuration file. Each dnode is assigned an ID upon successful joining.

## View Data Nodes

```sql
SHOW DNODES;
```

This lists all the data nodes in the cluster, with fields including the dnode's ID, endpoint, and status.

## Delete Data Node

```sql
DROP DNODE dnode_id [force] [unsafe]
```

Note that deleting a dnode does not stop the corresponding process. It is recommended to stop the process after deleting a dnode.

Only online nodes can be deleted. To forcibly delete an offline node, you need to perform a forced deletion operation, i.e., specify the force option.

If there is a single replica on the node and the node is offline, to forcibly delete the node, you need to perform an unsafe deletion, i.e., specify unsafe, and the data cannot be recovered.

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

The modifiable configuration items in the syntax above are configured in the same way as in the dnode configuration file, the difference being that modifications are dynamic, take immediate effect, and do not require restarting the dnode.

`value` is the value of the parameter, which needs to be in string format. For example, to change the log output level of dnode 1 to debug:

```sql
ALTER DNODE 1 'debugFlag' '143';
```

## Add Management Node

```sql
CREATE MNODE ON DNODE dnode_id
```

The system by default creates an MNODE on the firstEP node upon startup. Users can use this statement to create more MNODEs to improve system availability. A cluster can have a maximum of three MNODEs, and only one MNODE can be created on a DNODE.

## View Management Nodes

```sql
SHOW MNODES;
```

List all management nodes in the cluster, including their ID, the DNODE they are on, and their status.

## Delete Management Node

```sql
DROP MNODE ON DNODE dnode_id;
```

Delete the MNODE on the DNODE specified by dnode_id.

## Create Query Node

```sql
CREATE QNODE ON DNODE dnode_id;
```

By default, there are no QNODEs when the system starts. Users can create QNODEs to achieve separation of computation and storage. Only one QNODE can be created on a DNODE. If a DNODE's `supportVnodes` parameter is not 0 and a QNODE is also created on it, then the dnode will have both a vnode responsible for storage management and a qnode responsible for query computation. If an mnode is also created on that dnode, then up to three types of logical nodes can exist on one dnode. However, through configuration, they can also be completely separated. Setting a dnode's `supportVnodes` to 0 allows choosing to create either an mnode or a qnode on it, thus achieving complete physical separation of the three types of logical nodes.

## View Query Nodes

```sql
SHOW QNODES;
```

List all query nodes in the cluster, including their ID and the DNODE they are on.

## Delete Query Node

```sql
DROP QNODE ON DNODE dnode_id;
```

Delete the QNODE on the DNODE with ID dnode_id, but this does not affect the status of that dnode.

## Query Cluster Status

```sql
SHOW CLUSTER ALIVE;
```

Query whether the current cluster status is available, return values: 0: Not available 1: Fully available 2: Partially available (some nodes in the cluster are offline, but other nodes can still be used normally)

## Modify Client Configuration

If the client is also considered as part of the cluster in a broader sense, the following command can be used to dynamically modify client configuration parameters.

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

The parameters in the syntax above are used in the same way as in the configuration file for the client, but do not require a restart of the client, and the changes take effect immediately.

## View Client Configuration

```sql
SHOW LOCAL VARIABLES;
```
