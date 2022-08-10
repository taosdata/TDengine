---
title: Deployment
---

## Prerequisites

### Step 1

The FQDN of all hosts must be setup properly. All FQDNs need to be configured in the /etc/hosts file on each host. You must confirm that each FQDN can be accessed from any other host, you can do this by using the `ping` command.

The command `hostname -f` can be executed to get the hostname on any host. `ping <FQDN>` command can be executed on each host to check whether any other host is accessible from it. If any host is not accessible, the network configuration, like /etc/hosts or DNS configuration, needs to be checked and revised, to make any two hosts accessible to each other.

:::note

- The host where the client program runs also needs to be configured properly for FQDN, to make sure all hosts for client or server can be accessed from any other. In other words, the hosts where the client is running are also considered as a part of the cluster.

- Please ensure that your firewall rules do not block TCP/UDP on ports 6030-6042 on all hosts in the cluster.

:::

### Step 2

If any previous version of TDengine has been installed and configured on any host, the installation needs to be removed and the data needs to be cleaned up. For details about uninstalling please refer to [Install and Uninstall](/operation/pkg-install). To clean up the data, please use `rm -rf /var/lib/taos/\*` assuming the `dataDir` is configured as `/var/lib/taos`.

:::note

As a best practice, before cleaning up any data files or directories, please ensure that your data has been backed up correctly, if required by your data integrity, backup, security, or other standard operating protocols (SOP).

:::

### Step 3

Now it's time to install TDengine on all hosts but without starting `taosd`. Note that the versions on all hosts should be same. If you are prompted to input the existing TDengine cluster, simply press carriage return to ignore the prompt. `install.sh -e no` can also be used to disable this prompt. For details please refer to [Install and Uninstall](/operation/pkg-install).

### Step 4

Now each physical node (referred to, hereinafter, as `dnode` which is an abbreviation for "data node") of TDengine needs to be configured properly. Please note that one dnode doesn't stand for one host. Multiple TDengine dnodes can be started on a single host as long as they are configured properly without conflicting. More specifically each instance of the configuration file `taos.cfg` stands for a dnode. Assuming the first dnode of TDengine cluster is "h1.taosdata.com:6030", its `taos.cfg` is configured as following.

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

`firstEp` and `fqdn` must be configured properly. In `taos.cfg` of all dnodes in TDengine cluster, `firstEp` must be configured to point to same address, i.e. the first dnode of the cluster. `fqdn` and `serverPort` compose the address of each node itself. If you want to start multiple TDengine dnodes on a single host, please make sure all other configurations like `dataDir`, `logDir`, and other resources related parameters are not conflicting.

For all the dnodes in a TDengine cluster, the below parameters must be configured exactly the same, any node whose configuration is different from dnodes already in the cluster can't join the cluster.

| **#** | **Parameter**  | **Definition**                                                |
| ----- | -------------- | ------------------------------------------------------------- |
| 1     | statusInterval | The time interval for which dnode reports its status to mnode |
| 2     | timezone       | Time Zone where the server is located                         |
| 3     | locale         | Location code of the system                                   |
| 4     | charset        | Character set of the system                                   |

## Start Cluster

In the following example we assume that first dnode has FQDN h1.taosdata.com and the second dnode has FQDN h2.taosdata.com.

### Start The First DNODE

Start the first dnode following the instructions in [Get Started](/get-started/). Then launch TDengine CLI `taos` and execute command `show dnodes`, the output is as following for example:

```
Welcome to the TDengine shell from Linux, Client Version:3.0.0.0
Copyright (c) 2022 by TAOS Data, Inc. All rights reserved.

Server is Enterprise trial Edition, ver:3.0.0.0 and will never expire.

taos> show dnodes;
   id   |            endpoint            | vnodes | support_vnodes |   status   |       create_time       |              note              |
============================================================================================================================================
      1 | h1.taosdata.com:6030                     |      0 |           1024 | ready      | 2022-07-16 10:50:42.673 |                                |
Query OK, 1 rows affected (0.007984s)

taos>
```

From the above output, it is shown that the end point of the started dnode is "h1.taosdata.com:6030", which is the `firstEp` of the cluster.

### Start Other DNODEs

There are a few steps necessary to add other dnodes in the cluster.

Let's assume we are starting the second dnode with FQDN, h2.taosdata.com. Firstly we make sure the configuration is correct.

```c
// firstEp is the end point to connect to when any dnode starts
firstEp               h1.taosdata.com:6030

// must be configured to the FQDN of the host where the dnode is launched
fqdn                  h2.taosdata.com

// the port used by the dnode, default is 6030
serverPort            6030

```

Secondly, we can start `taosd` as instructed in [Get Started](/get-started/).

Then, on the first dnode i.e. h1.taosdata.com in our example, use TDengine CLI `taos` to execute the following command to add the end point of the dnode in the cluster. In the command "fqdn:port" should be quoted using double quotes.

```sql
CREATE DNODE "h2.taos.com:6030";
```

Then on the first dnode h1.taosdata.com, execute `show dnodes` in `taos` to show whether the second dnode has been added in the cluster successfully or not.

```sql
SHOW DNODES;
```

If the status of the newly added dnode is offline, please check:

- Whether the `taosd` process is running properly or not
- In the log file `taosdlog.0` to see whether the fqdn and port are correct

The above process can be repeated to add more dnodes in the cluster.
