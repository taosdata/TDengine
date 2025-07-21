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
```

For configuration parameters that support dynamic modification, you can use the ALTER DNODE or ALTER ALL DNODES syntax to modify the values of configuration parameters in a dnode. Starting from version 3.3.4.0, the modified configuration parameters will be automatically persisted and will remain effective even after the database service is restarted.

To check whether a configuration parameter supports dynamic modification, please refer to the following page: [taosd Reference](/tdengine-reference/components/taosd/)

The value is the parameter's value and needs to be in character format. For example, to change the log output level of dnode 1 to debug:

```sql
ALTER DNODE 1 'debugFlag' '143';
```

### Additional Notes

Configuration parameters in a dnode are divided into global configuration parameters and local configuration parameters. You can check the category field in SHOW VARIABLES or SHOW DNODE dnode_id VARIABLE to determine whether a configuration parameter is a global configuration parameter or a local configuration parameter:

Local configuration parameters: You can use ALTER DNODE or ALTER ALL DNODES to update the local configuration parameters of a specific dnode or all dnodes.
Global configuration parameters: Global configuration parameters require consistency across all dnodes, so you can only use ALTER ALL DNODES to update the global configuration parameters of all dnodes.
There are three cases for whether a configuration parameter can be dynamically modified:

Supports dynamic modification, effective immediately
Supports dynamic modification, effective after restart
Does not support dynamic modification
For configuration parameters that take effect after a restart, you can see the modified values through SHOW VARIABLES or SHOW DNODE dnode_id VARIABLE, but you need to restart the database service to make them effective.

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
```

You can use the above syntax to modify the client's configuration parameters, and there is no need to restart the client. The changes take effect immediately.

To check whether a configuration parameter supports dynamic modification, please refer to the following page:[taosc Reference](/tdengine-reference/components/taosc/)

## View Client Configuration

```sql
SHOW LOCAL VARIABLES;
```
