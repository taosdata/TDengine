---
title: Edge–Cloud Synchronization
---

TDengine TSDB supports multiple methods for automated synchronization between edge and cloud nodes.

- Data can be pushed by the edge node to the cloud node or pulled by the cloud node from the edge node.
- Edge–cloud synchronization can be implemented through TDengine TSDB data subscription or through queries.

Before enabling edge–cloud synchronization, determine which method is appropriate for your environment.

- If your environment has few edge nodes, and the cloud node can access the edge nodes directly, configure the cloud node to pull data from edge nodes.
- If your environment has many edge nodes, or inbound access to edge nodes is not permitted, configure the edge nodes to push data to the cloud node.
- If you require near-real-time synchronization, use data subscription.
- If you require periodic or on-demand synchronization, use querying.

## Procedure

### Edge Push + Subscription

1. On the cloud node, create a database to which edge data will be replicated.
1. Access TDengine TSDB-Explorer on the edge node in a web browser.
1. From the main menu on the left, select **Management**.
1. Open the **Data Replication** tab and click **Add New Replication**.
1. Select the database that you want to synchronize to the cloud.
1. In the **Target DSN** field, enter the DSN of the cloud node.
1. Click **Confirm**.

The edge node pushes data from the specified database to the specified cloud node. You can repeat this procedure to synchronize additional databases or to synchronize data to multiple cloud nodes.

### Edge Push + Data Query

1. On the cloud node, create a database to which edge data will be replicated.
1. On the edge node, open a terminal.
1. Run the following command to push data to the cloud node:

   ```sql
   taosx run -f 'taos://<edge-user>:<edge-password>@<edge-ip>:<edge-port>/<edge-db>' -t 'taos://<cloud-user>:<cloud-password>@<cloud-ip>:<cloud-port>/<cloud-db>' -v
   ```

   For example, the following command synchronizes data from database `sync_test` on an edge node deployed at 192.0.2.1:6030 to database `edge_data` on a cloud node deployed at 198.51.100.1:6030, using the default username and password.

   ```sql
   taosx run -f 'taos://root:taosdata@192.0.2.1:6030/sync_test' -t 'taos://root:taosdata@198.51.100.1:6030/edge_data' -v
   ```

For more information, see [Migrating Data from Older Versions](../14-reference/01-components/04-taosx.md#migrating-data-from-older-versions).

### Cloud Pull + Subscription

1. Access TDengine TSDB-Explorer on the edge node in a web browser.
1. From the main menu on the left, select **Topics**.
1. In the list displayed, locate the database that you want to synchronize to the cloud.
1. In the **Get DSN** column, click **Copy**.
1. Record this DSN for later use.
1. Access TDengine TSDB-Explorer on the cloud node in a web browser.
1. From the main menu on the left, select **Data In**.
1. Open the **Data In Task** tab and click **Add Source**.
1. Enter a name for the task.
1. From the **Type** drop-down menu, select **TDengine Data Subscription**.
1. From the **Target** drop-down menu, select the database to which you want to synchronize edge data. If you do not have an appropriate database, click **Create Database**.
1. Under **Connection Configuration**, copy the DSN you recorded earlier into the **Topic DSN** field.
1. Click **Check Connection** to verify that the cloud node can access the edge node.
1. Under **Subscribe Options**, enter a unique identifier in the **Client ID** field.
1. Configure other options as needed. You can retain the default values if desired.
1. Click **Submit**.

The cloud node subscribes to the specified database on the edge node and synchronizes its data to the specified database on the cloud node. You can repeat this procedure to create additional tasks to synchronize data from more databases or more cloud nodes.

For more information about TDengine Data Subscription tasks, see [TDengine Data Subscription](../06-advanced/05-data-in/02-tmq.md).

### Cloud Pull + Query

1. Access TDengine TSDB-Explorer on the cloud node in a web browser.
1. From the main menu on the left, select **Data In**.
1. Open the **Data In Task** tab and click **Add Source**.
1. Enter a name for the task.
1. From the **Type** drop-down menu, select **TDengine Query**.
1. From the **Target** drop-down menu, select the database to which you want to synchronize edge data. If you do not have an appropriate database, click **Create Database**.
1. Under **Connection Configuration**, enter the following:
   - **Protocol:** Select **WS**.
   - **Host:** Enter the IP address or hostname of the taosAdapter instance for the edge node.
   - **Port:** Enter the port number for the taosAdapter instance for the edge node.
   - **Database:** Enter the database on the edge node that you want to synchronize.
1. Under **Authentication**, enter the following:
   - **Username:** Enter a username on the edge node that has access to the database that you want to synchronize.
   - **Password:** Enter the password for the specified user on the edge node.
1. Click **Check Connection** to verify that the cloud node can access the edge node.
1. Configure other options as needed. You can retain the default values if desired.
1. Click **Submit**.

The cloud node queries the specified database on the edge node and synchronizes its data to the specified database on the cloud node. You can repeat this procedure to create additional tasks to synchronize data from more databases or more cloud nodes.

For more information about TDengine Query tasks, see [TDengine Query](../06-advanced/05-data-in/01-migrate.md).
