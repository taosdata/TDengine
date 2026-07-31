---
sidebar_label: Manage Nodes
title: Manage Nodes
description: SQL commands to manage dnodes, mnodes, qnodes, bnodes, and vnodes
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
DROP DNODE {dnode_id | dnode_endpoint} [FORCE | UNSAFE]
```

Note that deleting a dnode does not stop the corresponding process. It is recommended to stop the process after deleting a dnode.

Only online nodes can be deleted. To forcibly delete an offline node, specify `FORCE`.

If there is a single replica on the node and the node is offline, to forcibly delete the node, specify `UNSAFE`; the data cannot be recovered. `FORCE` and `UNSAFE` are mutually exclusive—do not specify both.

## Modify Data Node Configuration

```sql
ALTER DNODE dnode_id dnode_option

ALTER ALL DNODES dnode_option
```

For configuration parameters that support dynamic modification, you can use the ALTER DNODE or ALTER ALL DNODES syntax to modify the values of configuration parameters in a dnode. Starting from version 3.3.4.0, the modified configuration parameters will be automatically persisted and will remain effective even after the database service is restarted.

To check whether a configuration parameter supports dynamic modification, please refer to the following page: [taosd Reference](../../12-operations-and-tooling/03-components/01-taosd.md)

The value is the parameter's value and needs to be in character format. For example, to change the log output level of dnode 1 to debug:

```sql
ALTER DNODE 1 'debugFlag' '143';
```

### Additional Notes

Configuration parameters in a dnode are divided into global configuration parameters and local configuration parameters. You can check the category field in SHOW VARIABLES or SHOW DNODE dnode_id VARIABLES to determine whether a configuration parameter is a global configuration parameter or a local configuration parameter:

Local configuration parameters: You can use ALTER DNODE or ALTER ALL DNODES to update the local configuration parameters of a specific dnode or all dnodes.
Global configuration parameters: Global configuration parameters require consistency across all dnodes, so you can only use ALTER ALL DNODES to update the global configuration parameters of all dnodes.
There are three cases for whether a configuration parameter can be dynamically modified:

Supports dynamic modification, effective immediately
Supports dynamic modification, effective after restart
Does not support dynamic modification
For configuration parameters that take effect after a restart, you can see the modified values through SHOW VARIABLES or SHOW DNODE dnode_id VARIABLES, but you need to restart the database service to make them effective.

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

## Create Subscription Node

```sql
CREATE BNODE ON DNODE dnode_id [PROTOCOL protocol];
```

By default, there are no BNODEs when the system starts. Users can create BNODEs to start subscription services. Only one BNODE can be created on a DNODE. The `PROTOCOL` is optional, and the default is “mqtt”, if not provided; Other protocols will be added later. After bnode is created successfully, dnode will start the subprocess `taosmqtt` to provide subscription services.

## View Subscription Nodes

```sql
SHOW BNODES;
```

List all subscription nodes in the cluster, including their ID, protocol, create time, and the DNODE they are on.

## Delete Subscription Node

```sql
DROP BNODE ON DNODE dnode_id;
```

Delete the BNODE on the DNODE with ID dnode_id,  and the `taosmqtt` subprocess on this dnode will exit to stop the subscription service.

## Close Virtual Node

```sql
CLOSE VNODE vgroup_id ON DNODE dnode_id;
```

Close the vnode of the specified vgroup on the specified dnode. The close operation gracefully flushes data and stops the vnode from running, but does not delete any data files. The closed vnode will appear as `offline` in `SHOW VNODES`.

This command requires superuser privileges.

**Usage notes**:

- A closed vnode no longer participates in data read/write or Raft replication, reducing the effective replica count
- If the closed vnode is the leader replica, a Raft re-election will be triggered (writes temporarily unavailable, typically < 1s)
- The closed state does not persist across taosd restarts — the vnode automatically reopens after restart
- It is recommended to verify the state change via `SHOW VNODES` after the operation

**Example**:

```sql
-- Close the vnode of vgroup 2 on dnode 1
CLOSE VNODE 2 ON DNODE 1;

-- Verify the status
SHOW VNODES;
```

## Open Virtual Node

```sql
OPEN VNODE vgroup_id ON DNODE dnode_id;
```

Reopen a vnode that was previously closed via `CLOSE VNODE` on the specified dnode, restoring it to normal operation. After opening, the vnode will rejoin the Raft replication group and participate in data read/write.

This command requires superuser privileges.

**Usage notes**:

- Can only be executed on vnodes previously closed via `CLOSE VNODE`
- The open operation involves vnode initialization and WAL replay; duration depends on WAL size
- After opening, the vnode needs to catch up on replication progress and may temporarily be unable to serve reads

**Example**:

```sql
-- Reopen the vnode of vgroup 2 on dnode 1
OPEN VNODE 2 ON DNODE 1;

-- Verify the recovery
SHOW VNODES;
```

**Common errors**:

| Error Message | Cause | Solution |
| --- | --- | --- |
| Vgroup does not exist | The specified vgroupId does not exist | Run `SHOW VGROUPS` to verify |
| Vgroup not found on specified dnode | The vgroup has no replica on the specified dnode | Run `SHOW VGROUPS` to check replica distribution |
| Vnode is already closed | The vnode is already in closed state | No need to close again |
| Vnode is not in closed state | The vnode is not closed; cannot execute OPEN | Check if the vnode was closed, or if it was automatically restored after restart |

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

To check whether a configuration parameter supports dynamic modification, please refer to the following page:[taosc Reference](../../12-operations-and-tooling/03-components/02-taosc.md)

Connection-level parameter modification commands `SET TIMEZONE` and `SET FIRST_DAY_OF_WEEK` are documented in [Timezone and Natural Time Units](../10-time/01-timezone.md).

## View Client Configuration

```sql
SHOW LOCAL VARIABLES;
```
