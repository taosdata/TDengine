---
title: Deploy Your Cluster
slug: /operations-and-maintenance/deploy-your-cluster
---

Since TDengine was designed from the outset with a distributed architecture, it has powerful horizontal scalability to meet the growing data processing demands. Therefore, TDengine supports clusters and open-sources this core functionality. Users can choose from four deployment methods based on their actual environment and needs—manual deployment, Docker deployment, Kubernetes deployment, and Helm deployment.

## Manual Deployment

### Deploying taosd

taosd is the main service component in the TDengine cluster. This section describes the steps for manually deploying a taosd cluster.

#### 1. Clear Data

If there is any previous test data or if another version (such as 1.x/2.x) of TDengine has been installed on the physical nodes where the cluster is being set up, please delete it and clear all data.

#### 2. Check Environment

Before deploying the TDengine cluster, it is crucial to thoroughly check the network settings of all dnodes and the physical nodes where the applications reside. Here are the checking steps:

- **Step 1:** Execute the command `hostname -f` on each physical node to check and confirm that all node hostnames are unique. This step can be skipped for the node where the application driver resides.
- **Step 2:** Execute the command `ping host` on each physical node, where `host` is the hostname of other physical nodes. This step aims to detect network connectivity between the current node and other physical nodes. If any nodes cannot be pinged, immediately check the network and DNS settings. For Linux operating systems, check the `/etc/hosts` file; for Windows operating systems, check the `C:\Windows\system32\drivers\etc\hosts` file. Poor network connectivity will prevent the cluster from being formed, so be sure to resolve this issue.
- **Step 3:** Repeat the above network check steps on the physical nodes running the applications. If network connectivity is poor, the application will not be able to connect to the taosd service. At this time, carefully check the DNS settings or hosts file on the physical node where the application resides to ensure correct configuration.
- **Step 4:** Check the ports to ensure that all hosts in the cluster can communicate over TCP on port 6030.

By following these steps, you can ensure smooth network communication between all nodes, thereby laying a solid foundation for successfully deploying the TDengine cluster.

#### 3. Installation

To ensure consistency and stability across the physical nodes in the cluster, please install the same version of TDengine on all physical nodes.

#### 4. Modify Configuration

Modify the TDengine configuration file (all nodes' configuration files need to be modified). Suppose the endpoint of the first dnode to be started is `h1.tdengine.com:6030`, its related cluster configuration parameters are as follows.

```shell
# firstEp is the first dnode to connect after each dnode starts
firstEp h1.tdengine.com:6030
# Must be configured to the FQDN of this dnode. If the machine only has one hostname, this line can be commented out or deleted
fqdn h1.tdengine.com
# Configure the port for this dnode, default is 6030
serverPort 6030
```

The parameters that must be modified are `firstEp` and `fqdn`. The `firstEp` configuration should be consistent for each dnode, but the `fqdn` must be configured to the value of the respective dnode. Other parameters need not be modified unless you are very clear about why changes are necessary.

For dnodes that wish to join the cluster, it is essential to ensure that the parameters related to the TDengine cluster listed in the table below are set to be completely consistent. Any mismatch in parameters may prevent the dnode from successfully joining the cluster.

| Parameter Name         | Meaning                                                       |
|:----------------------:|:------------------------------------------------------------:|
| statusInterval         | Interval at which the dnode reports status to the mnode     |
| timezone               | Time zone                                                    |
| locale                 | System locale information and encoding format               |
| charset                | Character set encoding                                       |
| ttlChangeOnWrite      | Whether the TTL expiration time changes with the table modification |

#### 5. Start

Follow the previous steps to start the first dnode, for example, `h1.tdengine.com`. Then execute `taos` in the terminal to start the TDengine CLI program and execute the command `show dnodes` to view information about all dnodes currently in the cluster.

```shell
taos> show dnodes;
 id | endpoint          | vnodes | support_vnodes | status | create_time          | note |
=================================================================================
 1 | h1.tdengine.com:6030 | 0 | 1024 | ready | 2022-07-16 10:50:42.673 | |
```

You can see that the endpoint of the newly started dnode is `h1.tdengine.com:6030`. This address is the first Ep of the newly created cluster.

#### 6. Add dnode

Following the previous steps, start taosd on each physical node. Each dnode needs to have its `firstEp` parameter in the `taos.cfg` file set to the endpoint of the first node of the new cluster, which in this case is `h1.tdengine.com:6030`. On the machine where the first dnode resides, execute `taos` in the terminal to open the TDengine CLI program, then log into the TDengine cluster and execute the following SQL.

```shell
create dnode "h2.tdengine.com:6030"
```

This command adds the endpoint of the new dnode to the cluster's endpoint list. You must enclose `fqdn:port` in double quotes; otherwise, a runtime error will occur. Remember to replace the example `h2.tdengine.com:6030` with the endpoint of this new dnode. Then execute the following SQL to check whether the new node has successfully joined. If the dnode you wish to add is currently offline, please refer to the "Common Issues" section at the end of this section for resolution.

```shell
show dnodes;
```

In the logs, please verify that the fqdn and port of the output dnode match the endpoint you just attempted to add. If they do not match, please correct it to the correct endpoint. By following the above steps, you can continue to add new dnodes one by one to expand the scale of the cluster and improve overall performance. Ensuring that the correct process is followed when adding new nodes helps maintain the stability and reliability of the cluster.

:::tip

- Any dnode that has already joined the cluster can serve as the `firstEp` for subsequent nodes to join. The `firstEp` parameter only takes effect when that dnode first joins the cluster; after joining, that dnode will retain the latest endpoint list of mnode and will no longer depend on this parameter. The `firstEp` parameter in the configuration file is primarily used for client connections. If no parameters are set for the TDengine CLI, the default connection will be to the node specified by `firstEp`.
- Two dnodes that have not configured the `firstEp` parameter will operate independently after startup. In this case, it will not be possible to join one dnode to another to form a cluster.
- TDengine does not allow merging two independent clusters into a new cluster.

:::

#### 7. Add mnode

When creating the TDengine cluster, the first dnode will automatically become the mnode, responsible for managing and coordinating the cluster. To achieve high availability of the mnode, subsequent dnodes need to manually create mnode. Note that a maximum of three mnodes can be created for one cluster, and only one mnode can be created on each dnode. When the number of dnodes in the cluster reaches or exceeds three, you can create an mnode for the existing cluster. On the first dnode, log into TDengine using the TDengine CLI program `taos`, then execute the following SQL.

```shell
create mnode on dnode <dnodeId>
```

Be sure to replace the dnodeId in the example above with the sequence number of the newly created dnode (this can be obtained by executing the `show dnodes` command). Finally, execute the following `show mnodes` command to check whether the newly created mnode has successfully joined the cluster.

:::tip

During the process of building a TDengine cluster, if the new node always shows as offline after executing the `create dnode` command to add a new node, please follow these steps for troubleshooting.

- **Step 1:** Check whether the `taosd` service on the new node has started correctly. You can confirm this by checking the log files or using the `ps` command.
- **Step 2:** If the `taosd` service has started, please check whether the network connection on the new node is smooth and whether the firewall is disabled. Poor network connectivity or firewall settings may prevent the node from communicating with other nodes in the cluster.
- **Step 3:** Use the command `taos -h fqdn` to try connecting to the new node, then execute the `show dnodes` command. This will display the operating status of the new node as an independent cluster. If the displayed list is inconsistent with what is shown on the main node, it indicates that the new node may have formed a single-node cluster independently. To resolve this issue, first stop the `taosd` service on the new node. Next, clear all files in the dataDir directory specified in the `taos.cfg` configuration file on the new node. This will remove all data and configuration information related to that node. Finally, restart the `taosd` service on the new node. This will restore the new node to its initial state and prepare it to rejoin the main cluster.

:::

### Deploying taosAdapter

This section describes how to deploy taosAdapter, which provides RESTful and WebSocket access capabilities for the TDengine cluster, thus playing a critical role in the cluster.

1. **Installation**

After completing the installation of TDengine Enterprise, you can use taosAdapter. If you want to deploy taosAdapter on different servers, TDengine Enterprise must be installed on those servers as well.

2. **Single Instance Deployment**

Deploying a single instance of taosAdapter is straightforward; please refer to the manual for the commands and configuration parameters in the taosAdapter section.

3. **Multi-instance Deployment**

The primary purposes of deploying multiple instances of taosAdapter are:

- To enhance the throughput of the cluster and avoid taosAdapter becoming a bottleneck in the system.
- To improve the robustness and high availability of the cluster, so that when one instance fails to provide service for some reason, incoming requests to the business system can be automatically routed to other instances.

When deploying multiple instances of taosAdapter, you need to address the load balancing issue to avoid overloading one node while others remain idle. During deployment, you need to deploy multiple single instances separately, with the deployment steps for each instance being identical to that of a single instance. The critical part next is configuring Nginx. Below is a validated best practice configuration; you only need to replace the endpoint with the correct address for your actual environment. For the meanings of each parameter, please refer to the official Nginx documentation.

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
            proxy_next_upstream error http_502 http_500 non_idempotent;
        }
    }

    server {
        listen 6060;
        location ~* {
            proxy_pass http://explorer;
            proxy_read_timeout 60s;
            proxy_next_upstream error http_502 http_500 non_idempotent;
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
    upstream explorer {
        ip_hash;
        server 172.16.214.201:6060 ;
        server 172.16.214.202:6060 ;
        server 172.16.214.203:6060 ;
    }
}
```

### Deploying taosKeeper

If you want to use the monitoring capabilities of TDengine, taosKeeper is a necessary component. Please refer to [TDinsight](../../tdengine-reference/components/tdinsight/) for monitoring details and refer to the [taosKeeper Reference Manual](../../tdengine-reference/components/taoskeeper/) for details on deploying taosKeeper.

### Deploying taosX

If you want to use TDengine's data access capabilities, you need to deploy the taosX service. For detailed descriptions and deployment instructions, please refer to the Enterprise Edition Reference Manual.

### Deploying taosX-Agent

Some data sources, such as Pi and OPC, may have network conditions and access restrictions that prevent taosX from directly accessing the data sources. In such cases, it is necessary to deploy a proxy service, taosX-Agent. For detailed descriptions and deployment instructions, please refer to the Enterprise Edition Reference Manual.

### Deploying taos-Explorer

TDengine provides capabilities to visually manage the TDengine cluster. To use the graphical interface, you need to deploy the taos-Explorer service. For detailed descriptions and deployment instructions, please refer to the [taos-Explorer Reference Manual](../../tdengine-reference/components/taosexplorer/).

## Docker Deployment

This section will introduce how to start the TDengine service in a Docker container and access it. You can use environment variables in the `docker run` command line or the `docker-compose` file to control the behavior of the services in the container.

### Starting TDengine

The TDengine image activates the HTTP service by default upon startup. You can create a containerized TDengine environment with HTTP service using the following command.

```shell
docker run -d --name tdengine \
-v ~/data/taos/dnode/data:/var/lib/taos \
-v ~/data/taos/dnode/log:/var/log/taos \
-p 6041:6041 tdengine/tdengine
```

The detailed parameter explanations are as follows:

- `/var/lib/taos`: Default directory for TDengine data files, which can be modified in the configuration file.
- `/var/log/taos`: Default directory for TDengine log files, which can also be modified in the configuration file.

The above command starts a container named `tdengine` and maps port 6041 of the HTTP service within the container to port 6041 on the host. You can use the following command to verify whether the HTTP service provided in that container is available.

```shell
curl -u root:taosdata -d "show databases" localhost:6041/rest/sql
```

Run the following command to access TDengine within the container.

```shell
$ docker exec -it tdengine taos

taos> show databases;
              name              |
=================================
 information_schema             |
 performance_schema             |
Query OK, 2 rows in database (0.033802s)
```

In the container, the TDengine CLI or various connectors (such as JDBC-JNI) establish connections with the server using the container's hostname. Accessing TDengine from outside the container is relatively complex; using RESTful/WebSocket connection methods is the simplest approach.

### Starting TDengine in Host Network Mode

You can run the following command to start TDengine in host network mode, allowing connections to be established using the host's FQDN instead of the container's hostname.

```shell
docker run -d --name tdengine --network host tdengine/tdengine
```

This method has the same effect as starting TDengine using the `systemctl` command on the host. If the TDengine client is already installed on the host, you can directly access the TDengine service using the command below.

```shell
$ taos

taos> show dnodes;
     id      |            endpoint            | vnodes | support_vnodes |   status   |       create_time       |              note              |
=================================================================================================================================================
           1 | vm98:6030                      |      0 |             32 | ready      | 2022-08-19 14:50:05.337 |                                |
Query OK, 1 rows in database (0.010654s)
```

### Starting TDengine with Specified Hostname and Port

You can use the following command to set the `TAOS_FQDN` environment variable or the `fqdn` configuration item in `taos.cfg` to have TDengine establish a connection using the specified hostname. This approach provides greater flexibility for deploying TDengine.

```shell
docker run -d \
   --name tdengine \
   -e TAOS_FQDN=tdengine \
   -p 6030:6030 \
   -p 6041-6049:6041-6049 \
   -p 6041-6049:6041-6049/udp \
   tdengine/tdengine
```

First, the above command starts a TDengine service in the container that listens on the hostname `tdengine`, mapping the container's port 6030 to port 6030 on the host and mapping the container's port range [6041, 6049] to the host's port range [6041, 6049]. If the port range on the host is already occupied, you can modify the above command to specify a free port range on the host.

Secondly, ensure that the hostname `tdengine` is resolvable in the `/etc/hosts` file. You can save the correct configuration information to the hosts file using the command below.

```shell
echo 127.0.0.1 tdengine |sudo tee -a /etc/hosts
```

Finally, you can access the TDengine service using the TDengine CLI with `tdengine` as the server address using the following command.

```shell
taos -h tdengine -P 6030
```

If `TAOS_FQDN` is set to the same value as the hostname of the host, the effect will be the same as "starting TDengine in host network mode".

## Kubernetes Deployment

As a time-series database designed for cloud-native architecture, TDengine natively supports Kubernetes deployment. This section describes how to create a high-availability TDengine cluster for production use step-by-step using YAML files, focusing on common operations for TDengine in a Kubernetes environment. This subsection requires readers to have a certain understanding of Kubernetes and be familiar with common `kubectl` commands, as well as concepts such as `statefulset`, `service`, `pvc`, etc. Readers who are unfamiliar with these concepts can refer to the official Kubernetes website for study.

To meet the high-availability requirements, the cluster must meet the following criteria:

- **Three or more dnodes:** Multiple vnodes from the same vgroup in TDengine cannot be distributed across a single dnode at the same time. Therefore, if creating a database with three replicas, the number of dnodes must be greater than or equal to three.
- **Three mnodes:** The mnode is responsible for managing the entire cluster. By default, TDengine starts with one mnode. If this mnode's dnode goes offline, the entire cluster becomes unavailable.
- **Three replicas of the database:** The replica configuration of TDengine is at the database level. Therefore, having three replicas of the database allows the cluster to continue functioning normally even if one of the dnodes goes offline. If two dnodes go offline, the cluster becomes unavailable, as RAFT cannot complete the election. (Enterprise Edition: In disaster recovery scenarios, any node with damaged data files can be recovered by restarting a dnode.)

### Prerequisites

To deploy and manage a TDengine cluster using Kubernetes, the following preparations must be made:

- This article is applicable to Kubernetes v1.19 and above.
- The installation of the `kubectl` tool for deployment is required; ensure the relevant software is installed.
- Kubernetes must be installed, deployed, and accessible, or the necessary container repositories or other services must be updated.

### Configure Service

Create a service configuration file: `taosd-service.yaml`. The service name `metadata.name` (here as "taosd") will be used in the next step. First, add the ports used by TDengine, and then set the selector to confirm the label `app` (here as “tdengine”).

```yaml
---
apiVersion: v1
kind: Service
metadata:
  name: "taosd"
  labels:
    app: "tdengine"
spec:
  ports:
    - name: tcp6030
      protocol: "TCP"
      port: 6030
    - name: tcp6041
      protocol: "TCP"
      port: 6041
  selector:
    app: "tdengine"
```

### Stateful Service StatefulSet

According to Kubernetes's instructions for various deployments, we will use `StatefulSet` as the deployment resource type for TDengine. Create a file `tdengine.yaml`, where `replicas` defines the number of nodes in the cluster to be 3. The timezone for the nodes is set to China (Asia/Shanghai), and each node is allocated 5G standard storage (you can modify this according to your actual situation).

Pay special attention to the configuration of `startupProbe`. When the Pod of the dnode goes offline for a period and then restarts, the newly launched dnode may be temporarily unavailable. If the `startupProbe` configuration is too small, Kubernetes will consider that Pod to be in an abnormal state and will attempt to restart it frequently, causing the dnode's Pod to repeatedly restart and never recover to a normal state.

```yaml
---
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: "tdengine"
  labels:
    app: "tdengine"
spec:
  serviceName: "taosd"
  replicas: 3
  updateStrategy:
    type: RollingUpdate
  selector:
    matchLabels:
      app: "tdengine"
  template:
    metadata:
      name: "tdengine"
      labels:
        app: "tdengine"
    spec:
      containers:
        - name: "tdengine"
          image: "tdengine/tdengine:3.2.3.0"
          imagePullPolicy: "IfNotPresent"
          ports:
            - name: tcp6030
              protocol: "TCP"
              containerPort: 6030
            - name: tcp6041
              protocol: "TCP"
              containerPort: 6041
          env:
            # POD_NAME for FQDN config
            - name: POD_NAME
              valueFrom:
                fieldRef:
                  fieldPath: metadata.name
            # SERVICE_NAME and NAMESPACE for fqdn resolve
            - name: SERVICE_NAME
              value: "taosd"
            - name: STS_NAME
              value: "tdengine"
            - name: STS_NAMESPACE
              valueFrom:
                fieldRef:
                  fieldPath: metadata.namespace
            # TZ for timezone settings, we recommend to always set it.
            - name: TZ
              value: "Asia/Shanghai"
            # Environment variables with prefix TAOS_ will be parsed and converted into corresponding parameter in taos.cfg. For example, serverPort in taos.cfg should be configured by TAOS_SERVER_PORT when using K8S to deploy
            - name: TAOS_SERVER_PORT
              value: "6030"
            # Must set if you want a cluster.
            - name: TAOS_FIRST_EP
              value: "$(STS_NAME)-0.$(SERVICE_NAME).$(STS_NAMESPACE).svc.cluster.local:$(TAOS_SERVER_PORT)"
            # TAOS_FQND should always be set in k8s env.
            - name: TAOS_FQDN
              value: "$(POD_NAME).$(SERVICE_NAME).$(STS_NAMESPACE).svc.cluster.local"
          volumeMounts:
            - name: taosdata
              mountPath: /var/lib/taos
          startupProbe:
            exec:
              command:
                - taos-check
            failureThreshold: 360
            periodSeconds: 10
          readinessProbe:
            exec:
              command:
                - taos-check
            initialDelaySeconds: 5
            timeoutSeconds: 5000
          livenessProbe:
            exec:
              command:
                - taos-check
            initialDelaySeconds: 15
            periodSeconds: 20
  volumeClaimTemplates:
    - metadata:
        name: taosdata
      spec:
        accessModes:
          - "ReadWriteOnce"
        storageClassName: "standard"
        resources:
          requests:
            storage: "5Gi"
```

### Use kubectl Commands to Deploy the TDengine Cluster

First, create the corresponding namespace `dengine-test` and `pvc`, ensuring that the `storageClassName` is `standard` and that there is enough remaining space. Then execute the following commands in order:

```shell
kubectl apply -f taosd-service.yaml -n tdengine-test
```

The above configuration will generate a three-node TDengine cluster. The dnode will be automatically configured, and you can use the command `show dnodes` to check the current nodes in the cluster:

```shell
kubectl exec -it tdengine-0 -n tdengine-test -- taos -s "show dnodes"
kubectl exec -it tdengine-1 -n tdengine-test -- taos -s "show dnodes"
kubectl exec -it tdengine-2 -n tdengine-test -- taos -s "show dnodes"
```

The output is as follows:

```shell
taos show dnodes
     id      | endpoint         | vnodes | support_vnodes |   status   |       create_time       |       reboot_time       |              note              |          active_code           |         c_active_code          |
=============================================================================================================================================================================================================================================
           1 | tdengine-0.ta... |      0 |             16 | ready      | 2023-07-19 17:54:18.552 | 2023-07-19 17:54:18.469 |                                |                                |                                |
           2 | tdengine-1.ta... |      0 |             16 | ready      | 2023-07-19 17:54:37.828 | 2023-07-19 17:54:38.698 |                                |                                |                                |
           3 | tdengine-2.ta... |      0 |             16 | ready      | 2023-07-19 17:55:01.141 | 2023-07-19 17:55:02.039 |                                |                                |                                |
Query OK, 3 row(s) in set (0.001853s)
```

Check the current mnode

```shell
kubectl exec -it tdengine-1 -n tdengine-test -- taos -s "show mnodes\G"
taos> show mnodes\G
*************************** 1.row ***************************
         id: 1
   endpoint: tdengine-0.taosd.tdengine-test.svc.cluster.local:6030
       role: leader
     status: ready
create_time: 2023-07-19 17:54:18.559
reboot_time: 2023-07-19 17:54:19.520
Query OK, 1 row(s) in set (0.001282s)
```

Create mnode

```shell
kubectl exec -it tdengine-0 -n tdengine-test -- taos -s "create mnode on dnode 2"
kubectl exec -it tdengine-0 -n tdengine-test -- taos -s "create mnode on dnode 3"
```

Check mnode

```shell
kubectl exec -it tdengine-1 -n tdengine-test -- taos -s "show mnodes\G"

taos> show mnodes\G
*************************** 1.row ***************************
         id: 1
   endpoint: tdengine-0.taosd.tdengine-test.svc.cluster.local:6030
       role: leader
     status: ready
create_time: 2023-07-19 17:54:18.559
reboot_time: 2023-07-20 09:19:36.060
*************************** 2.row ***************************
         id: 2
   endpoint: tdengine-1.taosd.tdengine-test.svc.cluster.local:6030
       role: follower
     status: ready
create_time: 2023-07-20 09:22:05.600
reboot_time: 2023-07-20 09:22:12.838
*************************** 3.row ***************************
         id: 3
   endpoint: tdengine-2.taosd.tdengine-test.svc.cluster.local:6030
       role: follower
     status: ready
create_time: 2023-07-20 09:22:20.042
reboot_time: 2023-07-20 09:22:23.271
Query OK, 3 row(s) in set (0.003108s)
```

### Port Forwarding

Using the `kubectl port-forward` feature allows applications to access the TDengine cluster running in the Kubernetes environment.

```shell
kubectl port-forward -n tdengine-test tdengine-0 6041:6041 &
```

Use the `curl` command to verify the availability of the TDengine REST API on port 6041.

```shell
curl -u root:taosdata -d "show databases" 127.0.0.1:6041/rest/sql
{"code":0,"column_meta":[["name","VARCHAR",64]],"data":[["information_schema"],["performance_schema"],["test"],["test1"]],"rows":4}
```

### Cluster Expansion

TDengine supports cluster expansion:

```shell
kubectl scale statefulsets tdengine  -n tdengine-test --replicas=4
```

In the above command, the parameter `--replica=4` indicates that the TDengine cluster should be expanded to 4 nodes. After execution, first check the status of the PODs:

```shell
kubectl get pod -l app=tdengine -n tdengine-test  -o wide
```

The output is as follows:

```text
NAME                       READY   STATUS    RESTARTS        AGE     IP             NODE     NOMINATED NODE   READINESS GATES
tdengine-0   1/1     Running   4 (6h26m ago)   6h53m   10.244.2.75    node86   <none>           <none>
tdengine-1   1/1     Running   1 (6h39m ago)   6h53m   10.244.0.59    node84   <none>           <none>
tdengine-2   1/1     Running   0               5h16m   10.244.1.224   node85   <none>           <none>
tdengine-3   1/1     Running   0               3m24s   10.244.2.76    node86   <none>           <none>
```

At this point, the status of the PODs is still Running. The dnode status in the TDengine cluster can only be seen after the POD status is ready:

```shell
kubectl exec -it tdengine-3 -n tdengine-test -- taos -s "show dnodes"
```

The list of dnodes in the expanded four-node TDengine cluster:

```text
taos> show dnodes
     id      | endpoint         | vnodes | support_vnodes |   status   |       create_time       |       reboot_time       |              note              |          active_code           |         c_active_code          |
=============================================================================================================================================================================================================================================
           1 | tdengine-0.ta... |     10 |             16 | ready      | 2023-07-19 17:54:18.552 | 2023-07-20 09:39:04.297 |                                |                                |                                |
           2 | tdengine-1.ta... |     10 |             16 | ready      | 2023-07-19 17:54:37.828 | 2023-07-20 09:28:24.240 |                                |                                |                                |
           3 | tdengine-2.ta... |     10 |             16 | ready      | 2023-07-19 17:55:01.141 | 2023-07-20 10:48:43.445 |                                |                                |                                |
           4 | tdengine-3.ta... |      0 |             16 | ready      | 2023-07-20 16:01:44.007 | 2023-07-20 16:01:44.889 |                                |                                |                                |
Query OK, 4 row(s) in set (0.003628s)
```

### Cleaning the Cluster

:::warning

When deleting PVCs, it is important to note the `pv persistentVolumeReclaimPolicy` strategy. It is recommended to change it to `Delete`, so that when the PVC is deleted, the PV is automatically cleaned, along with the underlying CSI storage resources. If the strategy to automatically clean PVs upon PVC deletion is not configured, then after deleting the PVC, the corresponding CSI storage resources for the PV may not be released when manually cleaning PVs.

:::

To completely remove the TDengine cluster, you need to clear the `statefulset`, `svc`, and `pvc` separately, and finally delete the namespace.

```shell
kubectl delete statefulset -l app=tdengine -n tdengine-test
kubectl delete svc -l app=tdengine -n tdengine-test
kubectl delete pvc -l app=tdengine -n tdengine-test
kubectl delete namespace tdengine-test
```

### Cluster Disaster Recovery Capability

For TDengine's high availability and reliability in the Kubernetes environment, regarding hardware damage and disaster recovery, it can be discussed on two levels:

- The underlying distributed block storage has disaster recovery capabilities. With multiple replicas of block storage, popular distributed block storage solutions like Ceph possess multi-replica capabilities, extending storage replicas to different racks, cabinets, rooms, or data centers (or directly using the block storage services provided by public cloud vendors).
- The disaster recovery capability of TDengine itself: In TDengine Enterprise, it can automatically restore the work of a dnode if it goes offline permanently (such as due to physical disk damage or data loss) by restarting a blank dnode.

## Using Helm to Deploy the TDengine Cluster

Helm is the package manager for Kubernetes.
While the operations of deploying the TDengine cluster using Kubernetes are already simple, Helm can provide even more powerful capabilities.

### Installing Helm

```shell
curl -fsSL -o get_helm.sh \
  https://raw.githubusercontent.com/helm/helm/master/scripts/get-helm-3
chmod +x get_helm.sh
./get_helm.sh
```

Helm will use `kubectl` and the kubeconfig configuration to operate Kubernetes; you can refer to the Rancher installation of Kubernetes for configuration.

### Installing the TDengine Chart

The TDengine Chart has not yet been released to the Helm repository. Currently, it can be directly downloaded from GitHub:

```shell
wget https://github.com/taosdata/TDengine-Operator/raw/3.0/helm/tdengine-3.0.2.tgz
```

Get the current Kubernetes storage classes:

```shell
kubectl get storageclass
```

In `minikube`, the default is `standard`. Then use the `helm` command to install:

```shell
helm install tdengine tdengine-3.0.2.tgz \
  --set storage.className=<your storage class name> \
  --set image.tag=3.2.3.0
```

In the `minikube` environment, you can set a smaller capacity to avoid exceeding the available disk space:

```shell
helm install tdengine tdengine-3.0.2.tgz \
  --set storage.className=standard \
  --set storage.dataSize=2Gi \
  --set storage.logSize=10Mi \
  --set image.tag=3.2.3.0
```

After successful deployment, the TDengine Chart will output instructions for operating TDengine:

```shell
export POD_NAME=$(kubectl get pods --namespace default \
  -l "app.kubernetes.io/name=tdengine,app.kubernetes.io/instance=tdengine" \
  -o jsonpath="{.items[0].metadata.name}")
kubectl --namespace default exec $POD_NAME -- taos -s "show dnodes; show mnodes"
kubectl --namespace default exec -it $POD_NAME -- taos
```

You can create a table for testing:

```shell
kubectl --namespace default exec $POD_NAME -- \
  taos -s "create database test;
    use test;
    create table t1 (ts timestamp, n int);
    insert into t1 values(now, 1)(now + 1s, 2);
    select * from t1;"
```

### Configuring Values

TDengine supports `values.yaml` customization.
You can use `helm show values` to get the complete list of values supported by the TDengine Chart:

```shell
helm show values tdengine-3.0.2.tgz
```

You can save the output as `values.yaml`, then modify the parameters such as the number of replicas, storage class name, capacity size, TDengine configuration, etc., and install the TDengine cluster using the command below:

```shell
helm install tdengine tdengine-3.0.2.tgz -f values.yaml
```

The full parameters are as follows:

```yaml
# Default values for tdengine.
# This is a YAML-formatted file.
# Declare variables to be passed into helm templates.

replicaCount: 1

image:
  prefix: tdengine/tdengine
  #pullPolicy: Always
  # Overrides the image tag whose default is the chart appVersion.
#  tag: "3.0.2.0"

service:
  # ClusterIP is the default service type, use NodeIP only if you know what you are doing.
  type: ClusterIP
  ports:
    # TCP range required
    tcp: [6030, 6041, 6042, 6043, 6044, 6046, 6047, 6048, 6049, 6060]
    # UDP range
    udp: [6044, 6045]


# Set timezone here, not in taoscfg
timezone: "Asia/Shanghai"

resources:
  # We usually recommend not to specify default resources and to leave this as a conscious
  # choice for the user. This also increases chances charts run on environments with little
  # resources, such as Minikube. If you do want to specify resources, uncomment the following
  # lines, adjust them as necessary, and remove the curly braces after 'resources:'.
  # limits:
  #   cpu: 100m
  #   memory: 128Mi
  # requests:
  #   cpu: 100m
  #   memory: 128Mi

storage:
  # Set storageClassName for pvc. K8s use default storage class if not set.
  #
  className: ""
  dataSize: "100Gi"
  logSize: "10Gi"

nodeSelectors:
  taosd:
    # node selectors

clusterDomainSuffix: ""
# Config settings in taos.cfg file.
#
# The helm/k8s support will use environment variables for taos.cfg,
# converting an upper-snake-cased variable like `TAOS_DEBUG_FLAG`,
# to a camelCase taos config variable `debugFlag`.
#
# Note:
# 1. firstEp/secondEp: should not be set here, it's auto generated at scale-up.
# 2. serverPort: should not be set, we'll use the default 6030 in many places.
# 3. fqdn: will be auto generated in kubernetes, user should not care about it.
# 4. role: currently role is not supported - every node is able to be mnode and vnode.
#
# Btw, keep quotes "" around the value like below, even the value will be number or not.
taoscfg:
  # Starts as cluster or not, must be 0 or 1.
  #   0: all pods will start as a separate TDengine server
  #   1: pods will start as TDengine server cluster. [default]
  CLUSTER: "1"

  # number of replications, for cluster only
  TAOS_REPLICA: "1"


  # TAOS_NUM_OF_RPC_THREADS: number of threads for RPC
  #TAOS_NUM_OF_RPC_THREADS: "2"

  #
  # TAOS_NUM_OF_COMMIT_THREADS: number of threads to commit cache data
  #TAOS_NUM_OF_COMMIT_THREADS: "4"

  # enable/disable installation / usage report
  #TAOS_TELEMETRY_REPORTING: "1"

  # time interval of system monitor, seconds
  #TAOS_MONITOR_INTERVAL: "30"

  # time interval of dnode status reporting to mnode, seconds, for cluster only
  #TAOS_STATUS_INTERVAL: "1"

  # time interval of heart beat from shell to dnode, seconds
  #TAOS_SHELL_ACTIVITY_TIMER: "3"

  # minimum sliding window time, milli-second
  #TAOS_MIN_SLIDING_TIME: "10"

  # minimum time window, milli-second
  #TAOS_MIN_INTERVAL_TIME: "1"

  # the compressed rpc message, option:
  #  -1 (no compression)
  #   0 (all message compressed),
  # > 0 (rpc message body which larger than this value will be compressed)
  #TAOS_COMPRESS_MSG_SIZE: "-1"

  # max number of connections allowed in dnode
  #TAOS_MAX_SHELL_CONNS: "50000"

  # stop writing logs when the disk size of the log folder is less than this value
  #TAOS_MINIMAL_LOG_DIR_G_B: "0.1"

  # stop writing temporary files when the disk size of the tmp folder is less than this value
  #TAOS_MINIMAL_TMP_DIR_G_B: "0.1"

  # if disk free space is less than this value, taosd service exit directly within startup process
  #TAOS_MINIMAL_DATA_DIR_G_B: "0.1"

  # One mnode is equal to the number of vnode consumed
  #TAOS_MNODE_EQUAL_VNODE_NUM: "4"

  # enbale/disable http service
  #TAOS_HTTP: "1"

  # enable/disable system monitor
  #TAOS_MONITOR: "1"

  # enable/disable async log
  #TAOS_ASYNC_LOG: "1"

  #
  # time of keeping log files, days
  #TAOS_LOG_KEEP_DAYS: "0"

  # The following parameters are used for debug purpose only.
  # debugFlag 8 bits mask: FILE-SCREEN-UNUSED-HeartBeat-DUMP-TRACE_WARN-ERROR
  # 131: output warning and error
  # 135: output debug, warning and error
  # 143: output trace, debug, warning and error to log
  # 199: output debug, warning and error to both screen and file
  # 207: output trace, debug, warning and error to both screen and file
  #
  # debug flag for all log type, take effect when non-zero value
  #TAOS_DEBUG_FLAG: "143"

  # generate core file when service crash
  #TAOS_ENABLE_CORE_FILE: "1"
```

### Expansion

For expansion, refer to the explanation in the previous section. There are some additional operations needed to obtain from the Helm deployment.
First, obtain the name of the StatefulSet from the deployment.

```shell
export STS_NAME=$(kubectl get statefulset \
  -l "app.kubernetes.io/name=tdengine" \
  -o jsonpath="{.items[0].metadata.name}")
```

The expansion operation is straightforward; just increase the replica count. The following command expands TDengine to three nodes:

```shell
kubectl scale --replicas 3 statefulset/$STS_NAME
```

Use the commands `show dnodes` and `show mnodes` to check whether the expansion was successful.

### Cleaning the Cluster

With Helm management, the cleanup operation has also become simple:

```shell
helm uninstall tdengine
```

However, Helm will not automatically remove PVCs; you need to manually retrieve and delete them.
