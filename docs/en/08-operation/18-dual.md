---
title: Active-Active Deployment
slug: /operations-and-maintenance/active-active-deployment
---

import Image from '@theme/IdealImage';
import imgDual from '../assets/active-active-01.png';

import Enterprise from '../assets/resources/_enterprise.mdx';

<Enterprise/>

You can deploy TDengine in active-active mode to achieve high availability and reliability with limited resources. Active-active mode is also used in disaster recovery strategies to maintain offsite replicas of the database.

In active-active mode, you create two separate TDengine deployments, one acting as the primary node and the other as the secondary node. Data is replicated in real time between the primary and secondary nodes via TDengine's built-in data subscription component. Note that each node in an active-active deployment can be a single TDengine instance or a cluster.

In the event that the primary node cannot provide service, the client driver fails over to the secondary node. This failover is automatic and transparent to the business layer.

Replicated data is specially marked to avoid infinite loops. The architecture of an active-active deployment is described in the following figure.

<figure>
<Image img={imgDual} alt=""/>
<figcaption>Figure 1. TDengine in active-active mode</figcaption>
</figure>

## Limitations

The following limitations apply to active-active deployments:

1. You cannot use the data subscription APIs when active-active mode is enabled.
1. You cannot use the parameter binding interface while active-active mode is enabled.
1. The primary and secondary nodes must be identical. Database names, all configuration parameters, usernames, passwords, and permission settings must be exactly the same.
1. You can connect to an active-active deployment only through the Java client library in WebSocket mode.
1. Do not use the `USE <database>` statement to set a context. Instead, specify the database in the connection parameters.

## Cluster Configuration

It is not necessary to configure your cluster specifically for active-active mode. However, note that the WAL retention period affects the fault tolerance of an active-active deployment. This is because data loss will occur If the secondary node is unreachable for a period of time exceeding the configured WAL retention period. Data lost in this manner can only be recovered manually.

## Enable Active-Active Mode

1. Create two identical TDengine deployments. For more information, see [Get Started](../../get-started/).
1. Ensure that the taosd and taosx service are running on both deployments.
1. On the deployment that you have designated as the primary node, run the following command to start the replication service:

   ```shell
   taosx replica start -f <source-endpoint> -t <sink-endpoint> [database]
   ```

   - The source endpoint is the FQDN of TDengine on the primary node.
   - The sink endpoint is the FQDN of TDengine on the secondary node.
   - You can use the native connection (port 6030) or WebSocket connection (port 6041).
   - You can specify one or more databases to replicate only the data contained in those databases. If you do not specify a database, all databases on the node are replicated except for `information_schema`, `performance_schema`, `log`, and `audit`.
   - New databases in both sides will be detected periodically to start replication, with optional `--new-database-checking-interval <SECONDS>` argument.
   - New databases checking will be disabled with `--no-new-databases`.

   When the command is successful, the replica ID is displayed. You can use this ID to add other databases to the replication task if necessary.

1. Run the same command on the secondary node, specifying the FQDN of TDengine on the secondary node as the source endpoint and the FQDN of TDengine on the primary node as the sink endpoint.

## Client Configuration

Active-active mode is supported in the Java client library in WebSocket connection mode. The following is an example configuration:

```java
url = "jdbc:TAOS-RS://" + host + ":6041/?user=root&password=taosdata";
Properties properties = new Properties();
properties.setProperty(TSDBDriver.PROPERTY_KEY_BATCH_LOAD, "true");
properties.setProperty(TSDBDriver.PROPERTY_KEY_SLAVE_CLUSTER_HOST, "192.168.1.11");
properties.setProperty(TSDBDriver.PROPERTY_KEY_SLAVE_CLUSTER_PORT, "6041");
properties.setProperty(TSDBDriver.PROPERTY_KEY_ENABLE_AUTO_RECONNECT, "true");
properties.setProperty(TSDBDriver.PROPERTY_KEY_RECONNECT_INTERVAL_MS, "2000");
properties.setProperty(TSDBDriver.PROPERTY_KEY_RECONNECT_RETRY_COUNT, "3");
connection = DriverManager.getConnection(url, properties);
```

These parameters are described as follows:

| Property Name                      | Meaning                                                      |
| ---------------------------------- | ------------------------------------------------------------ |
| PROPERTY_KEY_SLAVE_CLUSTER_HOST    | Enter the hostname or IP address of the secondary node.      |
| PROPERTY_KEY_SLAVE_CLUSTER_PORT    | Enter the port number of the secondary node.                 |
| PROPERTY_KEY_ENABLE_AUTO_RECONNECT | Specify whether to enable automatic reconnection. For active-active mode, set the value of this parameter to true. |
| PROPERTY_KEY_RECONNECT_INTERVAL_MS | Enter the interval in milliseconds at which reconnection is attempted. The default value is 2000. You can enter 0 to attempt to reconnect immediately. There is no maximum limit. |
| PROPERTY_KEY_RECONNECT_RETRY_COUNT | Enter the maximum number of retries per node. The default value is 3. There is no maximum limit. |

## Command Reference

You can manage your active-active deployment with the following commands:

1. Use an existing replica ID to add databases to an existing replication task:

   ```shell
   taosx replica start -i <id> [database...]
   ```

   :::note
   - This command cannot create duplicate tasks. It only adds the specified databases to the specified task.
   - The replica ID is globally unique within a taosX instance and is independent of the source/sink combination.
   :::

1. Check the status of a task:

   ```shell
   taosx replica status [id...]
   ```

   This command returns the list and status of active-active synchronization tasks created on the current machine. You can specify one or more replica IDs to obtain their task lists and status. An example output is as follows:

   ```shell
   +---------+----------+----------+----------+------+-------------+----------------+
   | replica | task     | source   | sink     | database | status      | note           |
   +---------+----------+----------+----------+------+-------------+----------------+
   | a       | 2        | td1:6030 | td2:6030 | opc      | running     |                |
   | a       | 3        | td2:6030 | td2:6030 | test     | interrupted | Error reason   |
   ```

1. Stop a replication task:

   ```shell
   taosx replica stop [id [db...]]
   ```

   If you specify a database, replication for that database is stopped. If you do not specify a database, all replication tasks on the ID are stopped. If you do not specify an ID, all replication tasks on the instance are stopped.

   Use `--no-new-databases` to not stop new-databases checking.

1. Restart a replication task:

   ```shell
   taosx replica restart [id [db...]]
   ```

   If you specify a database, replication for that database is restarted. If you do not specify a database, all replication tasks in the instance are restarted. If you do not specify an ID, all replication tasks on the instance are restarted.

1. Update new databases checking interval:

   ```shell
   taosx replica update id --new-database-checking-interval <SECONDS>
   ```

   This command will only update the checking interval for new databases.

1. Check the progress of a replication task:

   ```shell
   taosx replica diff [id [db....]]
   ```

  This command outputs the difference between the subscribed offset in the current active-active replication task and the latest WAL (not representing row counts), for example:

   ```shell
   +---------+----------+----------+----------+-----------+---------+---------+------+
   | replica | database | source   | sink     | vgroup_id | current | latest  | diff |
   +---------+----------+----------+----------+-----------+---------+---------+------+
   | a       | opc      | td1:6030 | td2:6030 | 2         | 17600   | 17600   | 0    |
   | ad      | opc      | td2:6030 | td2:6030 | 3         | 17600   | 17600   | 0    |
   ```

1. Delete a replication task.

   ```shell
   taosx replica remove [id] [--force]
   ```

   This command deletes all stopped replication tasks on the specified ID. If you do not specify an ID, all stopped replication tasks on the instance are deleted. You can include the `--force` argument to delete all tasks without stopping them first.
