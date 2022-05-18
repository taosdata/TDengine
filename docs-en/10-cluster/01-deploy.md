---
title: Deployment
---

## Prerequisites

### Step 1

The FQDN of all hosts need to be setup properly, all the FQDNs need to be configured in the /etc/hosts of each host. It must be guaranteed that each FQDN can be accessed (by ping, for example) from any other hosts.

On each host command `hostname -f` can be executed to get the hostname. `ping` command can be executed on each host to check whether any other host is accessible from it. If any host is not accessible, the network configuration, like /etc/hosts or DNS configuration, need to be checked and revised to make any two hosts accessible to each other.

:::note

- The host where the client program runs also needs to configured properly for FQDN, to make sure all hosts for client or server can be accessed from any other. In other words, the hosts where the client is running are also considered as a part of the cluster.

- It's suggested to disable the firewall for all hosts in the cluster. At least TCP/UDP for port 6030~6042 need to be open if firewall is enabled.

:::

### Step 2

If any previous version of TDengine has been installed and configured on any host, the installation needs to be removed and the data needs to be cleaned up. For details about uninstalling please refer to [Install and Uninstall](/operation/pkg-install). To clean up the data, please use `rm -rf /var/lib/taos/\*` assuming the `dataDir` is configured as `/var/lib/taos`.

### Step 3

Now it's time to install TDengine on all hosts without starting `taosd`, the versions on all hosts should be same. If it's prompted to input the existing TDengine cluster, simply press carriage return to ignore it. `install.sh -e no` can also be used to disable this prompt. For details please refer to [Install and Uninstall](/operation/pkg-install).

### Step 4

Now each physical node (referred to as `dnode` hereinafter, it's abbreviation for "data node") of TDengine need to be configured properly. Please be noted that one dnode doesn't stand for one host, multiple TDengine nodes can be started on single host as long as they are configured properly without conflicting. More specifically each instance of the configuration file `taos.cfg` stands for a dnode. Assuming the first dnode of TDengine cluster is "h1.taosdata.com:6030", its `taos.cfg` is configured as following.

```c
// firstEp is the end point to connect to when any dnode starts
firstEp               h1.taosdata.com:6030

// must be configured to the FQDN of the host where the dnode is launched
fqdn                  h1.taosdata.com

// the port used by the dnode, default is 6030
serverPort            6030

// only necessary when replica is configured to an even number
#arbitrator            ha.taosdata.com:6042
```

`firstEp` and `fqdn` must be configured properly. In `taos.cfg` of all dnodes in TDengine cluster, `firstEp` must be configured to point to same address, i.e. the first dnode of the cluster. `fqdn` and `serverPort` compose the address of each node itself. If you want to start multiple TDengine dnodes on a single host, please also make sure all other configurations like `dataDir`, `logDir`, and other resources related parameters are not conflicting.

For all the dnodes in a TDengine cluster, below parameters must be configured as exactly same, any node whose configuration is different from dnodes already in the cluster can't join the cluster.

| **#** | **Parameter**      | **Definition**                                                                    |
| ----- | ------------------ | --------------------------------------------------------------------------------- |
| 1     | numOfMnodes        | The number of management nodes in the cluster                                     |
| 2     | mnodeEqualVnodeNum | The ratio of resource consuming of mnode to vnode                                 |
| 3     | offlineThreshold   | The threshold of dnode offline, once it's reached the dnode is considered as down |
| 4     | statusInterval     | The interval by which dnode reports its status to mnode                           |
| 5     | arbitrator         | End point of the arbitrator component in the cluster                              |
| 6     | timezone           | Timezone                                                                          |
| 7     | balance            | Enable load balance automatically                                                 |
| 8     | maxTablesPerVnode  | Maximum number of tables that can be created in each vnode                        |
| 9     | maxVgroupsPerDb    | Maximum number vgroups that can be used by each DB                                |

:::note
Prior to version 2.0.19.0, besides the above parameters, `locale` and `charset` must be configured as same too for each dnode.

:::

## Start Cluster

### Start The First DNODE

The first dnode can be started following the instructions in [Get Started](/get-started/), for example h1.taosdata.com. Then TDengine CLI `taos` can be launched to execute command `show dnodes`, the output is as following for example:

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

From the above output, it is shown that the end point of the started dnode is "h1.taos.com:6030", which is the `firstEp` of the cluster.

### Start Other DNODEs

There are a few steps necessary to add other dnodes in the cluster.

Firstly, start `taosd` as instructed in [Get Started](/get-started/), assuming it's for the second dnode. Before starting `taosd`, please making sure the configuration is correct, especially `firstEp`, `FQDN` and `serverPort`, `firstEp` must be same as the dnode shown in the section "Start First DNODE", i.e. "h1.taosdata.com" in this example.

Then, on the first dnode, use TDengine CLI `taos` to execute below command to add the end point of the dnode in the cluster. In the command "fqdn:port" should be quoted using double quotes.

```sql
CREATE DNODE "h2.taos.com:6030";
```

Then on the first dnode, execute `show dnodes` in `taos` to show whether the second dnode has been added in the cluster successfully or not.

```sql
SHOW DNODES;
```

If the status of the newly added dnode is offlie, please check:

- Whether the `taosd` process is running properly or not
- In the log file `taosdlog.0` to see whether the fqdn and port are correct or not 查

The above process can be repeated to add more dnodes in the cluster.
