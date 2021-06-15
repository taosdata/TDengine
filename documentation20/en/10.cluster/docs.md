# TDengine Cluster Management

Multiple TDengine servers, that is, multiple running instances of taosd, can form a cluster to ensure the highly reliable operation of TDengine and provide scale-out features. To understand cluster management in TDengine 2.0, it is necessary to understand the basic concepts of clustering. Please refer to the chapter "Overall Architecture of TDengine 2.0". And before installing the cluster, please follow the chapter ["Getting started"](https://www.taosdata.com/en/documentation/getting-started/) to install and experience the single node function.

Each data node of the cluster is uniquely identified by End Point, which is composed of FQDN (Fully Qualified Domain Name) plus Port, such as [h1.taosdata.com](http://h1.taosdata.com/):6030. The general FQDN is the hostname of the server, which can be obtained through the Linux command `hostname -f` (how to configure FQDN, please refer to: [All about FQDN of TDengine](https://www.taosdata.com/blog/2020/09/11/1824.html)). Port is the external service port number of this data node. The default is 6030, but it can be modified by configuring the parameter serverPort in taos.cfg. A physical node may be configured with multiple hostnames, and TDengine will automatically get the first one, but it can also be specified through the configuration parameter fqdn in taos.cfg. If you are accustomed to direct IP address access, you can set the parameter fqdn to the IP address of this node.

The cluster management of TDengine is extremely simple. Except for manual intervention in adding and deleting nodes, all other tasks are completed automatically, thus minimizing the workload of operation. This chapter describes the operations of cluster management in detail.

Please refer to the [video tutorial](https://www.taosdata.com/blog/2020/11/11/1961.html) for cluster building.

## <a class="anchor" id="prepare"></a> Preparation

**Step 0:** Plan FQDN of all physical nodes in the cluster, and add the planned FQDN to /etc/hostname of each physical node respectively; modify the /etc/hosts of each physical node, and add the corresponding IP and FQDN of all cluster physical nodes. [If DNS is deployed, contact your network administrator to configure it on DNS]

**Step 1:** If the physical nodes have previous test data, installed with version 1. x, or installed with other versions of TDengine, please delete it first and drop all data. For specific steps, please refer to the blog "[Installation and Uninstallation of Various Packages of TDengine](https://www.taosdata.com/blog/2019/08/09/566.html)"

**Note 1:** Because the information of FQDN will be written into a file, if FQDN has not been configured or changed before, and TDengine has been started, be sure to clean up the previous data （`rm -rf /var/lib/taos/*`）on the premise of ensuring that the data is useless or backed up;

**Note 2:** The client also needs to be configured to ensure that it can correctly parse the FQDN configuration of each node, whether through DNS service or Host file.

**Step 2:** It is recommended to close the firewall of all physical nodes, and at least ensure that the TCP and UDP ports of ports 6030-6042 are open. It is **strongly recommended** to close the firewall first and configure the ports after the cluster is built;

**Step 3:** Install TDengine on all physical nodes, and the version must be consistent, **but do not start taosd**. During installation, when prompted to enter whether to join an existing TDengine cluster, press enter for the first physical node directly to create a new cluster, and enter the FQDN: port number (default 6030) of any online physical node in the cluster for the subsequent physical nodes;

**Step 4:** Check the network settings of all data nodes and the physical nodes where the application is located:

1. Execute command `hostname -f` on each physical node, and check and confirm that the hostnames of all nodes are different (the node where the application driver is located does not need to do this check).
2. Execute `ping host` on each physical node, wherein host is that hostname of other physical node, and see if other physical nodes can be communicated to; if not, you need to check the network settings, or the /etc/hosts file (the default path for Windows systems is C:\ Windows\ system32\ drivers\ etc\ hosts), or the configuration of DNS. If it fails to ping, then we cann't build the cluster. 
3. From the physical node where the application runs, ping the data node where taosd runs. If the ping fails, the application cannot connect to taosd. Please check the DNS settings or hosts file of the physical node where the application is located;
4. The End Point of each data node is the output hostname plus the port number, for example, [h1.taosdata.com](http://h1.taosdata.com/): 6030

**Step 5:** Modify the TDengine configuration file (the file/etc/taos/taos.cfg for all nodes needs to be modified). Assume that the first data node End Point to be started is [h1.taosdata.com](http://h1.taosdata.com/): 6030, and its parameters related to cluster configuration are as follows:

```
// firstEp is the first data node connected after each data node’s first launch
firstEp        h1.taosdata.com:6030
// Must configure it as the FQDN of this data node. If this machine has only one hostname, you can comment out this configuration
fqdn         h1.taosdata.com  
// Configure the port number of this data node, the default is 6030
serverPort      6030
// For application scenarios, please refer to the section “Use of Arbitrator”
arbitrator      ha.taosdata.com:6042
```

The parameters that must be modified are firstEp and fqdn. At each data node, every firstEp needs to be configured to be the same, **but fqdn must be configured to the value of the data node where it is located**. Other parameters may not be modified unless you have clear reasons.

**The data node dnode added to the cluster must be exactly the same as the 11 parameters in the following table related to the cluster, otherwise it cannot be successfully added to the cluster.**



| **#** | **Configuration Parameter Name** | **Description**                                              |
| ----- | -------------------------------- | ------------------------------------------------------------ |
| 1     | numOfMnodes                      | Number of management nodes in system                         |
| 2     | mnodeEqualVnodeNum               | A mnode equals to the number of vnodes consumed              |
| 3     | offlineThreshold                 | Offline threshold of dnode to judge if the dnode is offline  |
| 4     | statusInterval                   | The interval for dnode to report its status to mnode         |
| 5     | arbitrator                       | The end point of the arbitrator in system                    |
| 6     | timezone                         | Time zone                                                    |
| 7     | locale                           | Location information and coding format of system             |
| 8     | charset                          | Character set encoding                                       |
| 9     | balance                          | Whether to start load balancing                              |
| 10    | maxTablesPerVnode                | The maximum number of tables that can be created in each vnode |
| 11    | maxVgroupsPerDb                  | The maximum number of vgroups that can be used per DB        |

## <a class="anchor" id="node-one"></a> Launch the First Data Node

Follow the instructions in "[Getting started](https://www.taosdata.com/en/documentation/getting-started/)", launch the first data node, such as [h1.taosdata.com](http://h1.taosdata.com/), then execute taos, start the taos shell, and execute command "show dnodes" from the shell; ", as follows:

```
Welcome to the TDengine shell from Linux, Client Version:2.0.0.0
Copyright (c) 2017 by TAOS Data, Inc. All rights reserved.

taos> show dnodes;
 id |       end_point    | vnodes | cores | status | role |      create_time        |
=====================================================================================
  1 |  h1.taos.com:6030  |      0 |     2 |  ready |  any | 2020-07-31 03:49:29.202 |
Query OK, 1 row(s) in set (0.006385s)

taos>
```

In the above command, you can see that the End Point of the newly launched data node is: [h1.taos.com](http://h1.taos.com/): 6030, which is the firstEP of the new cluster.

## <a class="anchor" id="node-other"></a>Launch Subsequent Data Nodes

To add subsequent data nodes to the existing cluster, there are the following steps:

1. Start taosd at each physical node according to the chapter "[Getting started](https://www.taosdata.com/en/documentation/getting-started/)";

2. On the first data node, use CLI program taos to log in to TDengine system and execute the command:

    ```
      CREATE DNODE "h2.taos.com:6030"; 
    ```

Add the End Point of the new data node (learned in Step 4 of the preparation) to the cluster's EP list. **"fqdn: port" needs to be enclosed in double quotation marks**, otherwise an error will occur. Notice that the example "[h2.taos.com](http://h2.taos.com/): 6030" is replaced with the End Point for this new data node.

3. And then execute the command

1. ```
      SHOW DNODES;
   ```

2. Check to see if the new node was successfully joined. If the added data node is offline, then check:

1. - Check whether the taosd of this data node is working properly. If it is not working properly, you need to check the reason first
   - Check the first few lines of the data node taosd log file taosdlog.0 (usually in the /var/log/taos directory) to see if the data node fqdn and port number output in the log are the just added End Point. If not, you need to add the correct End Point.

According to the above steps, new data nodes can be continuously added to the cluster.

**Tips**:

- Any data node that has joined the cluster online can be used as the firstEP of the subsequent node to be joined.
- firstEp is only effective when the data node joins the cluster for the first time. After joining the cluster, the data node will save the latest End Point list of mnode and no longer rely on this parameter.
- The two dnode data nodes dnode that are not configured with the firstEp parameter will run independently after startup. At this time, one data node cannot be added to another data node to form a cluster. **You cannot merge two independent clusters into a new cluster**.

## <a class="anchor" id="management"></a> Data Node Management

The above has already introduced how to build clusters from scratch. After the cluster is formed, new data nodes can be added at any time for expansion, or data nodes can be deleted, and the current status of the cluster can be checked.

### Add data nodes

Execute CLI program taos, log in to the system using root account, and execute:

```
CREATE DNODE "fqdn:port"; 
```

Add the End Point for the new data node to the cluster's EP list. **"fqdn: port" needs to be enclosed in double quotation marks**, otherwise an error will occur. The fqdn and port of a data node's external service can be configured through the configuration file taos.cfg, which is automatically obtained by default. [It is strongly not recommended to configure FQDN with automatic acquisition, which may cause the End Point of the generated data node to be not expected]

### Delete data nodes

Execute the CLI program taos, log in to the TDengine system using the root account, and execute:

```
DROP DNODE "fqdn:port";
```

Where fqdn is the FQDN of the deleted node, and port is the port number of its external server.

<font color=green>**【Note】**</font>

- Once a data node is dropped, it cannot rejoin the cluster. This node needs to be redeployed (emptying the data folder). The cluster migrates the data from the dnode before it completes the drop dnode operation.
- Note that dropping a dnode and stopping the taosd process are two different concepts. Don't be confused: the data migration operation must be performed before deleting a dnode, thus the deleted dnode must remain online. The taosd process cannot be stopped until the delete operation is completed.
- After a data node is dropped, other nodes will perceive the deletion of this dnodeID, and no node in any cluster will receive the request of the dnodeID.
- dnodeID is automatically assigned by the cluster and cannot be specified manually. It is incremented at the time of generation and does not repeat.

### View data nodes

Execute the CLI program taos, log in to the TDengine system using the root account, and execute:

```
SHOW DNODES;
```

All dnodes, fqdn: port for each dnode, status (ready, offline, etc.), number of vnodes, number of unused vnodes in the cluster will be listed. You can use this command to view after adding or deleting a data node.

### View virtual node group

In order to make full use of multi-core technology and provide scalability, data needs to be processed in partitions. Therefore, TDengine will split the data of a DB into multiple parts and store them in multiple vnodes. These vnodes may be distributed in multiple data node dnodes, thus realizing scale-out. A vnode belongs to only one DB, but a DB can have multiple vnodes. vnode is allocated automatically by mnode according to the current system resources without any manual intervention.

Execute the CLI program taos, log in to the TDengine system using the root account, and execute:

```
SHOW VGROUPS;
```

## <a class="anchor" id="high-availability"></a> High-availability of vnode

TDengine provides high-availability of system through a multi-replica mechanism, including high-availability of vnode and mnode.

The number of replicas of vnode is associated with DB. There can be multiple DBs in a cluster. Each DB can be configured with different replicas according to operational requirements. When creating a database, specify the number of replicas with parameter replica (the default is 1). If the number of replicas is 1, the reliability of the system cannot be guaranteed. As long as the node where the data is located goes down, the service cannot be provided. The number of nodes in the cluster must be greater than or equal to the number of replicas, otherwise the error "more dnodes are needed" will be returned when creating a table. For example, the following command will create a database demo with 3 replicas:

```
CREATE DATABASE demo replica 3;
```

The data in a DB will be partitioned and splitted into multiple vnode groups. The number of vnodes in a vnode group is the number of replicas of the DB, and the data of each vnode in the same vnode group is completely consistent. In order to ensure high-availability, the vnodes in a vnode group must be distributed in different dnode data nodes (in actual deployment, they need to be on different physical machines). As long as more than half of the vnodes in a vgroup are working, the vgroup can be normally serving.

There may be data from multiple DBs of data in a data node dnode, so when a dnode is offline, it may affect multiple DBs. If half or more of the vnodes in a vnode group do not work, then the vnode group cannot serve externally and cannot insert or read data, which will affect the reading and writing operations of some tables in the DB to which it belongs.

Because of the introduction of vnode, it is impossible to simply draw a conclusion: "If more than half of the data nodes in the cluster work in dnode, the cluster should work." But for simple cases, it is easier to judge. For example, if the number of replicas is 3 and there are only 3 dnodes, the whole cluster can still work normally if only one node does not work, but if two data nodes do not work, the whole cluster cannot work normally.

## <a class="anchor" id="mnode"></a> High-availability of mnode

TDengine cluster is managed by mnode (a module of taosd, management node). In order to ensure the high-availability of mnode, multiple mnode replicas can be configured. The number of replicas is determined by system configuration parameter numOfMnodes, and the effective range is 1-3. In order to ensure the strong consistency of metadata, mnode replicas are duplicated synchronously.

A cluster has multiple data node dnodes, but a dnode runs at most one mnode instance. In the case of multiple dnodes, which dnode can be used as an mnode? This is automatically specified by the system according to the resource situation on the whole. User can execute the following command in the console of TDengine through the CLI program taos:

```
SHOW MNODES;
```

To view the mnode list, which lists the End Point and roles (master, slave, unsynced, or offline) of the dnode where the mnode is located. When the first data node in the cluster starts, the data node must run an mnode instance, otherwise the dnode of the data node cannot work properly because a system must have at least one mnode. If numOfMnodes is configured to 2, when the second dnode is started, the latter will also run an mnode instance.

To ensure the high-availability of mnode service, numOfMnodes must be set to 2 or greater. Because the metadata saved by mnode must be strongly consistent, if numOfMnodes is greater than 2, the duplication parameter quorum is automatically set to 2, that is to say, at least two replicas must be guaranteed to write the data successfully before notifying the client application of successful writing.

**Note:** A TDengine highly-available system, whether vnode or mnode, must be configured with multiple replicas.

## <a class="anchor" id="load-balancing"></a> Load Balancing

There are three situations in which load balancing will be triggered, and no manual intervention is required.

- When a new data node is added to the cluster, the system will automatically trigger load balancing, and the data on some nodes will be automatically migrated to the new data node without any manual intervention.
- When a data node is removed from the cluster, the system will automatically migrate the data on the data node to other data nodes without any manual intervention.
- If a data node is overheated (too large amount of data), the system will automatically load balance and migrate some vnodes of the data node to other nodes.

When the above three situations occur, the system will start a load computing of each data node to decide how to migrate.

**[Tip] Load balancing is controlled by parameter balance, which determines whether to start automatic load balancing.**

## <a class="anchor" id="offline"></a> Offline Processing of Data Nodes

If a data node is offline, the TDengine cluster will automatically detect it. There are two detailed situations:

- If the data node is offline for more than a certain period of time (configuration parameter offlineThreshold in taos.cfg controls the duration), the system will automatically delete the data node, generate system alarm information and trigger the load balancing process. If the deleted data node is online again, it will not be able to join the cluster, and the system administrator will need to add it to the cluster again.
- After offline, the system will automatically start the data recovery process if it goes online again within the duration of offlineThreshold. After the data is fully recovered, the node will start to work normally.

**Note:** If each data node belonging to a virtual node group (including mnode group) is in offline or unsynced state, Master can only be elected after all data nodes in the virtual node group are online and can exchange status information, and the virtual node group can serve externally. For example, the whole cluster has 3 data nodes with 3 replicas. If all 3 data nodes go down and then 2 data nodes restart, it will not work. Only when all 3 data nodes restart successfully can serve externally again.

## <a class="anchor" id="arbitrator"></a> How to Use Arbitrator

If the number of replicas is even, it is impossible to elect a master from a vnode group when half of the vnodes are not working. Similarly, when half of the mnodes are not working, the master of the mnode cannot be elected because of the "split brain" problem. To solve this problem, TDengine introduced the concept of Arbitrator. Arbitrator simulates a vnode or mnode working, but is simply responsible for networking, and does not handle any data insertion or access. As long as more than half of the vnodes or mnodes, including the Arbitrator, work, the vnode group or mnode group can normally provide data insertion or query services. For example, in the case of 2 replicas, if one node A is offline, but the other node B is normal on and can connect to the Arbitrator, then node B can work normally.

In a word, under the current version, TDengine recommends configuring Arbitrator in double-replica environment to improve the availability.

The name of the executable for Arbitrator is tarbitrator. The executable has almost no requirements for system resources, just need to ensure a network connection, with any Linux server to run it. The following briefly describes the steps to install the configuration:



1. Click [Package Download](https://www.taosdata.com/cn/all-downloads/), and in the TDengine Arbitrator Linux section, select the appropriate version to download and install.
2. The command line parameter  -p of this application can specify the port number of its external service, and the default is 6042.
3. Modify the configuration file of each taosd instance, and set parameter arbitrator to the End Point corresponding to the tarbitrator in taos.cfg. (If this parameter is configured, when the number of replicas is even, the system will automatically connect the configured Arbitrator. If the number of replicas is odd, even if the Arbitrator is configured, the system will not establish a connection.)
4. The Arbitrator configured in the configuration file will appear in the return result of instruction `SHOW DNODES`; the value of the corresponding role column will be "arb".

