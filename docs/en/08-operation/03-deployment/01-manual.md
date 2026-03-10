---
title: Manual Deployment
slug: /operations-and-maintenance/deploy-your-cluster/manual-deployment
---

You can deploy TDengine manually on a physical or virtual machine.

## Deploying taosd

taosd is the most important service component in the TDengine cluster. This section describes the steps to manually deploy a taosd cluster.

### 1. Clear Data

If the physical nodes for setting up the cluster contain previous test data or have had other versions of TDengine installed (such as 1.x/2.x), please delete them and clear all data first.

### 2. Check Environment

Before deploying the TDengine cluster, it is crucial to thoroughly check the network settings of all dnodes and the physical nodes where the applications are located. Here are the steps to check:

- Step 1: Execute the `hostname -f` command on each physical node to view and confirm that all node hostnames are unique. This step can be omitted for nodes where application drivers are located.
- Step 2: Execute the `ping host` command on each physical node, where host is the hostname of other physical nodes. This step aims to detect the network connectivity between the current node and other physical nodes. If you cannot ping through, immediately check the network and DNS settings. For Linux operating systems, check the `/etc/hosts` file; for Windows operating systems, check the `C:\Windows\system32\drivers\etc\hosts` file. Network issues will prevent the formation of a cluster, so be sure to resolve this issue.
- Step 3: Repeat the above network detection steps on the physical nodes where the application is running. If the network is found to be problematic, the application will not be able to connect to the taosd service. At this point, carefully check the DNS settings or hosts file of the physical node where the application is located to ensure it is configured correctly.
- Step 4: Check ports to ensure that all hosts in the cluster can communicate over TCP on port 6030.

By following these steps, you can ensure that all nodes communicate smoothly at the network level, laying a solid foundation for the successful deployment of the TDengine cluster.

### 3. Installation

To ensure consistency and stability within the cluster, install the same version of TDengine on all physical nodes.

### 4. Modify Configuration

Modify the configuration file of TDengine (the configuration files of all nodes need to be modified). Assuming the endpoint of the first dnode to be started is `h1.tdengine.com:6030`, the cluster-related parameters are as follows.

```shell
# firstEp is the first dnode that each dnode connects to after the initial startup
firstEp h1.tdengine.com:6030
# Must be configured to the FQDN of this dnode, if there is only one hostname on this machine, you can comment out or delete the following line
fqdn h1.tdengine.com
# Configure the port of this dnode, default is 6030
serverPort 6030
```

The parameters that must be modified are firstEp and fqdn. For each dnode, the firstEp configuration should remain consistent, but fqdn must be set to the value of the dnode it is located on. Other parameters do not need to be modified unless you are clear on why they should be changed.

For dnodes wishing to join the cluster, it is essential to ensure that the parameters related to the TDengine cluster listed in the table below are set identically. Any mismatch in parameters may prevent the dnode from successfully joining the cluster.

| Parameter Name   | Meaning                                                   |
|:----------------:|:---------------------------------------------------------:|
| statusInterval   | Interval at which dnode reports status to mnode           |
| timezone         | Time zone                                                 |
| locale           | System locale information and encoding format             |
| charset          | Character set encoding                                    |
| ttlChangeOnWrite | Whether ttl expiration changes with table modification    |

### 5. Start

Start the first dnode, such as `h1.tdengine.com`, following the steps mentioned above. Then execute taos in the terminal to start TDengine CLI program taos, and execute the `show dnodes` command within it to view all dnode information in the current cluster.

```shell
taos> show dnodes;
 id | endpoint | vnodes|support_vnodes|status| create_time | note |
===================================================================================
 1| h1.tdengine.com:6030 | 0| 1024| ready| 2022-07-16 10:50:42.673 | |
```

You can see that the endpoint of the dnode node that has just started is `h1.tdengine.com:6030`. This address is the first Ep of the new cluster.

### 6. Adding dnode

Follow the steps mentioned earlier, start taosd on each physical node. Each dnode needs to configure the firstEp parameter in the taos.cfg file to the endpoint of the first node of the new cluster, which in this case is `h1.tdengine.com:6030`. On the machine where the first dnode is located, run taos in the terminal, open TDengine CLI program taos, then log into the TDengine cluster, and execute the following SQL.

```shell
create dnode "h2.tdengine.com:6030"
```

Add the new dnode's endpoint to the cluster's endpoint list. You need to put `fqdn:port` in double quotes, otherwise, it will cause an error when running. Please note to replace the example h2.tdengine.com:6030 with the endpoint of this new dnode. Then execute the following SQL to see if the new node has successfully joined. If the dnode you want to join is currently offline, please refer to the "Common Issues" section later in this chapter for a solution.

```shell
show dnodes;
```

In the logs, please confirm that the fqdn and port of the output dnode are consistent with the endpoint you just tried to add. If they are not consistent, correct it to the correct endpoint. By following the steps above, you can continuously add new dnodes to the cluster one by one, thereby expanding the scale of the cluster and improving overall performance. Make sure to follow the correct process when adding new nodes, which helps maintain the stability and reliability of the cluster.

:::note

- Any dnode that has joined the cluster can serve as the firstEp for subsequent nodes to be added. The firstEp parameter only functions when that dnode first joins the cluster. After joining, the dnode will save the latest mnode's endpoint list, and subsequently, it no longer depends on this parameter. The firstEp parameter in the configuration file is mainly used for client connections, and if no parameters are set for TDengine CLI, it will default to connecting to the node specified by firstEp.
- Two dnodes that have not configured the firstEp parameter will run independently after starting. At this time, it is not possible to join one dnode to another to form a cluster.
- TDengine does not allow merging two independent clusters into a new cluster.

:::

### 7. Adding mnode

When creating a TDengine cluster, the first dnode automatically becomes the mnode of the cluster, responsible for managing and coordinating the cluster. To achieve high availability of mnode, subsequent dnodes need to manually create mnode. Please note that a cluster can create up to 3 mnodes, and only one mnode can be created on each dnode. When the number of dnodes in the cluster reaches or exceeds 3, you can create mnode for the existing cluster. In the first dnode, first log into TDengine through TDengine CLI program taos, then execute the following SQL.

```shell
create mnode on dnode <dnodeId>
```

Please note to replace the dnodeId in the example above with the serial number of the newly created dnode (which can be obtained by executing the `show dnodes` command). Finally, execute the following `show mnodes` to see if the newly created mnode has successfully joined the cluster.

:::note

During the process of setting up a TDengine cluster, if a new node always shows as offline after executing the create dnode command to add a new node, please follow these steps for troubleshooting.

- Step 1, check whether the taosd service on the new node has started normally. You can confirm this by checking the log files or using the ps command.
- Step 2, if the taosd service has started, next check whether the new node's network connection is smooth and confirm whether the firewall has been turned off. Network issues or firewall settings may prevent the node from communicating with other nodes in the cluster.
- Step 3, use the taos -h fqdn command to try to connect to the new node, then execute the show dnodes command. This will display the running status of the new node as an independent cluster. If the displayed list is inconsistent with that shown on the main node, it indicates that the new node may have formed a single-node cluster on its own. To resolve this issue, follow these steps. First, stop the taosd service on the new node. Second, clear all files in the dataDir directory specified in the taos.cfg configuration file on the new node. This will delete all data and configuration information related to that node. Finally, restart the taosd service on the new node. This will reset the new node to its initial state, ready to rejoin the main cluster.

:::

## Deploying taosAdapter

This section discusses how to deploy taosAdapter, which provides RESTful and WebSocket access capabilities for the TDengine cluster, thus playing a very important role in the cluster.

1. Installation

After the installation of TDengine Enterprise is complete, taosAdapter can be used. If you want to deploy taosAdapter on different servers, TDengine Enterprise needs to be installed on these servers.

1. Single Instance Deployment

Deploying a single instance of taosAdapter is very simple. For specific commands and configuration parameters, please refer to the taosAdapter section in the manual.

1. Multiple Instances Deployment

The main purposes of deploying multiple instances of taosAdapter are as follows:

- To increase the throughput of the cluster and prevent taosAdapter from becoming a system bottleneck.
- To enhance the robustness and high availability of the cluster, allowing requests entering the business system to be automatically routed to other instances when one instance fails.

When deploying multiple instances of taosAdapter, it is necessary to address load balancing issues to avoid overloading some nodes while others remain idle. During the deployment process, multiple single instances need to be deployed separately, and the deployment steps for each instance are exactly the same as those for deploying a single instance. The next critical part is configuring Nginx. Below is a verified best practice configuration; you only need to replace the endpoint with the correct address in the actual environment. For the meanings of each parameter, please refer to the official Nginx documentation.

```json
user root;
worker_processes auto;
error_log /var/log/nginx_error.log;


events {
        use epoll;
        worker_connections 1024;
}

http {

    access_log off;

    map $http_upgrade $connection_upgrade {
        default upgrade;
        ''      close;
    }

    server {
        listen 6041;
        location ~* {
            proxy_pass http://dbserver;
            proxy_read_timeout 600s;
            proxy_send_timeout 600s;
            proxy_connect_timeout 600s;
            proxy_next_upstream error http_502 non_idempotent;
            proxy_http_version 1.1;
            proxy_set_header Upgrade $http_upgrade;
            proxy_set_header Connection $http_connection;
        }
    }
    server {
        listen 6043;
        location ~* {
            proxy_pass http://keeper;
            proxy_read_timeout 60s;
            proxy_next_upstream error  http_502 http_500  non_idempotent;
        }
    }

    server {
        listen 6060;
        location ~* {
            proxy_pass http://explorer;
            proxy_read_timeout 60s;
            proxy_next_upstream error  http_502 http_500  non_idempotent;
        }
    }
    upstream dbserver {
        least_conn;
        server 172.16.214.201:6041 max_fails=0;
        server 172.16.214.202:6041 max_fails=0;
        server 172.16.214.203:6041 max_fails=0;
    }
    upstream keeper {
        ip_hash;
        server 172.16.214.201:6043 ;
        server 172.16.214.202:6043 ;
        server 172.16.214.203:6043 ;
    }
    upstream explorer{
        ip_hash;
        server 172.16.214.201:6060 ;
        server 172.16.214.202:6060 ;
        server 172.16.214.203:6060 ;
    }
}
```

## Deploying taosKeeper

To use the monitoring capabilities of TDengine, taosKeeper is an essential component. For monitoring, please refer to [TDinsight](../../../tdengine-reference/components/tdinsight), and for details on deploying taosKeeper, please refer to the [taosKeeper Reference Manual](../../../tdengine-reference/components/taoskeeper).

## Deploying taosX

To utilize the data ingestion capabilities of TDengine, it is necessary to deploy the taosX service. For detailed explanations and deployment, please refer to the TSDB-Enterprise reference manual.

## Deploying taosX-Agent

For some data sources such as Pi, OPC, etc., due to network conditions and data source access restrictions, taosX cannot directly access the data sources. In such cases, a proxy service, taosX-Agent, needs to be deployed. For detailed explanations and deployment, please refer to the TSDB-Enterprise reference manual.

## Deploying taos-Explorer

TDengine provides the capability to visually manage TDengine clusters. To use the graphical interface, the taos-Explorer service needs to be deployed. For detailed explanations and deployment, please refer to the [taos-Explorer Reference Manual](../../../tdengine-reference/components/taosexplorer/)
