---
title: Manual Deployment and Management
sidebar_label: Manual Deployment
description: This document describes how to deploy TDengine on a server.
---

## Prerequisites

### Step 0

The FQDN of all hosts must be setup properly. For e.g. FQDNs may have to be configured in the /etc/hosts file on each host. You must confirm that each FQDN can be accessed from any other host. For e.g. you can do this by using the `ping` command. If you have a DNS server on your network, contact your network administrator for assistance.

### Step 1

If any previous version of TDengine has been installed and configured on any host, the installation needs to be removed and the data needs to be cleaned up. To clean up the data, please use `rm -rf /var/lib/taos/\*` assuming the `dataDir` is configured as `/var/lib/taos`.

:::note
FQDN information is written to file. If you have started TDengine without configuring or changing the FQDN, ensure that data is backed up or no longer needed before running the `rm -rf /var/lib\taos/\*` command.
:::

:::note
- The host where the client program runs also needs to be configured properly for FQDN, to make sure all hosts for client or server can be accessed from any other. In other words, the hosts where the client is running are also considered as a part of the cluster.
:::

### Step 2

- Please ensure that your firewall rules do not block TCP/UDP on ports 6030-6042 on all hosts in the cluster.

### Step 3

Now it's time to install TDengine on all hosts but without starting `taosd`. Note that the versions on all hosts should be same. If you are prompted to input the existing TDengine cluster, simply press carriage return to ignore the prompt.

### Step 4

Now each physical node (referred to, hereinafter, as `dnode` which is an abbreviation for "data node") of TDengine needs to be configured properly.

To get the hostname on any host, the command `hostname -f` can be executed.

`ping <FQDN>` command can be executed on each host to check whether any other host is accessible from it. If any host is not accessible, the network configuration, like /etc/hosts or DNS configuration, needs to be checked and revised, to make any two hosts accessible to each other. Hosts that are not accessible to each other cannot form a cluster.

On the physical machine running the application, ping the dnode that is running taosd. If the dnode is not accessible, the application cannot connect to taosd. In this case, verify the DNS and hosts settings on the physical node running the application.

The end point of each dnode is the output hostname and port, such as h1.tdengine.com:6030.

### Step 5

Modify the TDengine configuration file `/etc/taos/taos.cfg` on each node. Assuming the first dnode of TDengine cluster is "h1.tdengine.com:6030", its `taos.cfg` is configured as following.

```c
// firstEp is the end point to connect to when any dnode starts
firstEp               h1.tdengine.com:6030

// must be configured to the FQDN of the host where the dnode is launched
fqdn                  h1.tdengine.com

// the port used by the dnode, default is 6030
serverPort            6030

```

`firstEp` and `fqdn` must be configured properly. In `taos.cfg` of all dnodes in TDengine cluster, `firstEp` must be configured to point to same address, i.e. the first dnode of the cluster. `fqdn` and `serverPort` compose the address of each node itself. Retain the default values for other parameters.

For all the dnodes in a TDengine cluster, the below parameters must be configured exactly the same, any node whose configuration is different from dnodes already in the cluster can't join the cluster.

| **#** | **Parameter**    | **Definition**                                                                |
| ----- | ---------------- | ----------------------------------------------------------------------------- |
| 1     | statusInterval   | The interval by which dnode reports its status to mnode                       |
| 2     | timezone         | Timezone                                                                      |
| 3     | locale           | System region and encoding                                                    |
| 4     | charset          | Character set                                                                 |
| 5     | ttlChangeOnWrite | Whether the ttl expiration time changes with the table modification operation |

## Start Cluster

The first dnode can be started following the instructions in [Get Started](../../get-started/). Then TDengine CLI `taos` can be launched to execute command `show dnodes`, the output is as following for example:

```
taos> show dnodes;
id | endpoint | vnodes | support_vnodes | status | create_time | note |
============================================================================================================================================
1 | h1.tdengine.com:6030 | 0 | 1024 | ready | 2022-07-16 10:50:42.673 | |
Query OK, 1 rows affected (0.007984s)


```

From the above output, it is shown that the end point of the started dnode is "h1.tdengine.com:6030", which is the `firstEp` of the cluster.

## Add DNODE

There are a few steps necessary to add other dnodes in the cluster.

Second, we can start `taosd` as instructed in [Get Started](../../get-started/).

Then, on the first dnode i.e. h1.tdengine.com in our example, use TDengine CLI `taos` to execute the following command:

```sql
CREATE DNODE "h2.taos.com:6030";
````

This adds the end point of the new dnode (from Step 4) into the end point list of the cluster. In the command "fqdn:port" should be quoted using double quotes. Change `"h2.taos.com:6030"` to the end point of your new dnode.

Then on the first dnode h1.tdengine.com, execute `show dnodes` in `taos`

```sql
SHOW DNODES;
```

to show whether the second dnode has been added in the cluster successfully or not. If the status of the newly added dnode is offline, please check:

- Whether the `taosd` process is running properly or not
- In the log file `taosdlog.0` to see whether the fqdn and port are correct and add the correct end point if not.
The above process can be repeated to add more dnodes in the cluster.

:::tip

Any node that is in the cluster and online can be the firstEp of new nodes.
Nodes use the firstEp parameter only when joining a cluster for the first time. After a node has joined the cluster, it stores the latest mnode in its end point list and no longer makes use of firstEp.

However, firstEp is used by clients that connect to the cluster. For example, if you run TDengine CLI `taos` without arguments, it connects to the firstEp by default.

Two dnodes that are launched without a firstEp value operate independently of each other. It is not possible to add one dnode to the other dnode and form a cluster. It is also not possible to form two independent clusters into a new cluster.

:::

## Show DNODEs

The below command can be executed in TDengine CLI `taos`

```sql
SHOW DNODES;
```

to list all dnodes in the cluster, including ID, end point (fqdn:port), status (ready, offline), number of vnodes, number of free vnodes and so on. We recommend executing this command after adding or removing a dnode.

Below is the example output of this command.

```
taos> show dnodes;
   id   |            endpoint            | vnodes | support_vnodes |   status   |       create_time       |              note              |
============================================================================================================================================
      1 | trd01:6030                     |    100 |           1024 | ready      | 2022-07-15 16:47:47.726 |                                |
Query OK, 1 rows affected (0.006684s)
```

## Show VGROUPs

To utilize system resources efficiently and provide scalability, data sharding is required. The data of each database is divided into multiple shards and stored in multiple vnodes. These vnodes may be located on different dnodes. One way of scaling out is to add more vnodes on dnodes. Each vnode can only be used for a single DB, but one DB can have multiple vnodes. The allocation of vnode is scheduled automatically by mnode based on system resources of the dnodes.

Launch TDengine CLI `taos` and execute below command:

```sql
USE SOME_DATABASE;
SHOW VGROUPS;
```

The example output is below:

```
taos> use db;
Database changed.

taos> show vgroups;
  vgroup_id  |            db_name             |   tables    |  v1_dnode   | v1_status  |  v2_dnode   | v2_status  |  v3_dnode   | v3_status  |    status    |   nfiles    |  file_size  | tsma |
================================================================================================================================================================================================
           2 | db                             |           0 |           1 | leader     |        NULL | NULL       |        NULL | NULL       | NULL         |        NULL |        NULL |    0 |
           3 | db                             |           0 |           1 | leader     |        NULL | NULL       |        NULL | NULL       | NULL         |        NULL |        NULL |    0 |
           4 | db                             |           0 |           1 | leader     |        NULL | NULL       |        NULL | NULL       | NULL         |        NULL |        NULL |    0 |
Query OK, 8 row(s) in set (0.001154s)
```

## Drop DNODE

Before running the TDengine CLI, ensure that the taosd process has been stopped on the dnode that you want to delete.

```sql
DROP DNODE dnodeId;
```

to drop or remove a dnode from the cluster. In the command, you can get `dnodeId` from `show dnodes`.

:::warning

- Once a dnode is dropped, it can't rejoin the cluster. To rejoin, the dnode needs to deployed again after cleaning up the data directory. Before dropping a dnode, the data belonging to the dnode MUST be migrated/backed up according to your data retention, data security or other SOPs.
- Please note that `drop dnode` is different from stopping `taosd` process. `drop dnode` just removes the dnode out of TDengine cluster. Only after a dnode is dropped, can the corresponding `taosd` process be stopped.
- Once a dnode is dropped, other dnodes in the cluster will be notified of the drop and will not accept the request from the dropped dnode.
- dnodeID is allocated automatically and can't be manually modified. dnodeID is generated in ascending order without duplication.

:::
