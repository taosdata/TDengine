---
title: Deploying Your Cluster
slug: /operations-and-maintenance/deploy-your-cluster
---

Since TDengine was designed with a distributed architecture from the beginning, it has powerful horizontal scaling capabilities to meet the growing data processing needs. Therefore, TDengine supports clustering and has open-sourced this core functionality. Users can choose from four deployment methods according to their actual environment and needs—manual deployment, Docker deployment, Kubernetes deployment, and Helm deployment.

## Manual Deployment

### Deploying taosd

taosd is the most important service component in the TDengine cluster. This section describes the steps to manually deploy a taosd cluster.

#### 1. Clear Data

If the physical nodes for setting up the cluster contain previous test data or have had other versions of TDengine installed (such as 1.x/2.x), please delete them and clear all data first.

#### 2. Check Environment

Before deploying the TDengine cluster, it is crucial to thoroughly check the network settings of all dnodes and the physical nodes where the applications are located. Here are the steps to check:

- Step 1: Execute the `hostname -f` command on each physical node to view and confirm that all node hostnames are unique. This step can be omitted for nodes where application drivers are located.
- Step 2: Execute the `ping host` command on each physical node, where host is the hostname of other physical nodes. This step aims to detect the network connectivity between the current node and other physical nodes. If you cannot ping through, immediately check the network and DNS settings. For Linux operating systems, check the `/etc/hosts` file; for Windows operating systems, check the `C:\Windows\system32\drivers\etc\hosts` file. Network issues will prevent the formation of a cluster, so be sure to resolve this issue.
- Step 3: Repeat the above network detection steps on the physical nodes where the application is running. If the network is found to be problematic, the application will not be able to connect to the taosd service. At this point, carefully check the DNS settings or hosts file of the physical node where the application is located to ensure it is configured correctly.
- Step 4: Check ports to ensure that all hosts in the cluster can communicate over TCP on port 6030.

By following these steps, you can ensure that all nodes communicate smoothly at the network level, laying a solid foundation for the successful deployment of the TDengine cluster.

#### 3. Installation

To ensure consistency and stability within the cluster, install the same version of TDengine on all physical nodes.

#### 4. Modify Configuration

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

#### 5. Start

Start the first dnode, such as `h1.tdengine.com`, following the steps mentioned above. Then execute taos in the terminal to start TDengine's CLI program taos, and execute the `show dnodes` command within it to view all dnode information in the current cluster.

```shell
taos> show dnodes;
 id | endpoint | vnodes|support_vnodes|status| create_time | note |
===================================================================================
 1| h1.tdengine.com:6030 | 0| 1024| ready| 2022-07-16 10:50:42.673 | |
```

You can see that the endpoint of the dnode node that has just started is `h1.tdengine.com:6030`. This address is the first Ep of the new cluster.

#### 6. Adding dnode

Follow the steps mentioned earlier, start taosd on each physical node. Each dnode needs to configure the firstEp parameter in the taos.cfg file to the endpoint of the first node of the new cluster, which in this case is `h1.tdengine.com:6030`. On the machine where the first dnode is located, run taos in the terminal, open TDengine's CLI program taos, then log into the TDengine cluster, and execute the following SQL.

```shell
create dnode "h2.tdengine.com:6030"
```

Add the new dnode's endpoint to the cluster's endpoint list. You need to put `fqdn:port` in double quotes, otherwise, it will cause an error when running. Please note to replace the example h2.tdengine.com:6030 with the endpoint of this new dnode. Then execute the following SQL to see if the new node has successfully joined. If the dnode you want to join is currently offline, please refer to the "Common Issues" section later in this chapter for a solution.

```shell
show dnodes;
```

In the logs, please confirm that the fqdn and port of the output dnode are consistent with the endpoint you just tried to add. If they are not consistent, correct it to the correct endpoint. By following the steps above, you can continuously add new dnodes to the cluster one by one, thereby expanding the scale of the cluster and improving overall performance. Make sure to follow the correct process when adding new nodes, which helps maintain the stability and reliability of the cluster.

**Tips**

- Any dnode that has joined the cluster can serve as the firstEp for subsequent nodes to be added. The firstEp parameter only functions when that dnode first joins the cluster. After joining, the dnode will save the latest mnode's endpoint list, and subsequently, it no longer depends on this parameter. The firstEp parameter in the configuration file is mainly used for client connections, and if no parameters are set for TDengine's CLI, it will default to connecting to the node specified by firstEp.
- Two dnodes that have not configured the firstEp parameter will run independently after starting. At this time, it is not possible to join one dnode to another to form a cluster.
- TDengine does not allow merging two independent clusters into a new cluster.

#### 7. Adding mnode

When creating a TDengine cluster, the first dnode automatically becomes the mnode of the cluster, responsible for managing and coordinating the cluster. To achieve high availability of mnode, subsequent dnodes need to manually create mnode. Please note that a cluster can create up to 3 mnodes, and only one mnode can be created on each dnode. When the number of dnodes in the cluster reaches or exceeds 3, you can create mnode for the existing cluster. In the first dnode, first log into TDengine through the CLI program taos, then execute the following SQL.

```shell
create mnode on dnode <dnodeId>
```

Please note to replace the dnodeId in the example above with the serial number of the newly created dnode (which can be obtained by executing the `show dnodes` command). Finally, execute the following `show mnodes` to see if the newly created mnode has successfully joined the cluster.

**Tips**

During the process of setting up a TDengine cluster, if a new node always shows as offline after executing the create dnode command to add a new node, please follow these steps for troubleshooting.

- Step 1, check whether the taosd service on the new node has started normally. You can confirm this by checking the log files or using the ps command.
- Step 2, if the taosd service has started, next check whether the new node's network connection is smooth and confirm whether the firewall has been turned off. Network issues or firewall settings may prevent the node from communicating with other nodes in the cluster.
- Step 3, use the taos -h fqdn command to try to connect to the new node, then execute the show dnodes command. This will display the running status of the new node as an independent cluster. If the displayed list is inconsistent with that shown on the main node, it indicates that the new node may have formed a single-node cluster on its own. To resolve this issue, follow these steps. First, stop the taosd service on the new node. Second, clear all files in the dataDir directory specified in the taos.cfg configuration file on the new node. This will delete all data and configuration information related to that node. Finally, restart the taosd service on the new node. This will reset the new node to its initial state, ready to rejoin the main cluster.

### Deploying taosAdapter

This section discusses how to deploy taosAdapter, which provides RESTful and WebSocket access capabilities for the TDengine cluster, thus playing a very important role in the cluster.

1. Installation

After the installation of TDengine Enterprise is complete, taosAdapter can be used. If you want to deploy taosAdapter on different servers, TDengine Enterprise needs to be installed on these servers.

2. Single Instance Deployment

Deploying a single instance of taosAdapter is very simple. For specific commands and configuration parameters, please refer to the taosAdapter section in the manual.

3. Multiple Instances Deployment

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

### Deploying taosKeeper

To use the monitoring capabilities of TDengine, taosKeeper is an essential component. For monitoring, please refer to [TDinsight](../../tdengine-reference/components/tdinsight), and for details on deploying taosKeeper, please refer to the [taosKeeper Reference Manual](../../tdengine-reference/components/taoskeeper).

### Deploying taosX

To utilize the data ingestion capabilities of TDengine, it is necessary to deploy the taosX service. For detailed explanations and deployment, please refer to the enterprise edition reference manual.

### Deploying taosX-Agent

For some data sources such as Pi, OPC, etc., due to network conditions and data source access restrictions, taosX cannot directly access the data sources. In such cases, a proxy service, taosX-Agent, needs to be deployed. For detailed explanations and deployment, please refer to the enterprise edition reference manual.

### Deploying taos-Explorer

TDengine provides the capability to visually manage TDengine clusters. To use the graphical interface, the taos-Explorer service needs to be deployed. For detailed explanations and deployment, please refer to the [taos-Explorer Reference Manual](../../tdengine-reference/components/taosexplorer/)

## Docker Deployment

This section will explain how to start TDengine services in Docker containers and access them. You can use environment variables in the docker run command line or docker-compose file to control the behavior of services in the container.

### Starting TDengine

The TDengine image is launched with HTTP service activated by default. Use the following command to create a containerized TDengine environment with HTTP service.

```shell
docker run -d --name tdengine \
-v ~/data/taos/dnode/data:/var/lib/taos \
-v ~/data/taos/dnode/log:/var/log/taos \
-p 6041:6041 tdengine/tdengine
```

Detailed parameter explanations are as follows:

- /var/lib/taos: Default data file directory for TDengine, can be modified through the configuration file.
- /var/log/taos: Default log file directory for TDengine, can be modified through the configuration file.

The above command starts a container named tdengine and maps the HTTP service's port 6041 to the host port 6041. The following command can verify if the HTTP service in the container is available.

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

Within the container, TDengine CLI or various connectors (such as JDBC-JNI) connect to the server via the container's hostname. Accessing TDengine inside the container from outside is more complex, and using RESTful/WebSocket connection methods is the simplest approach.

### Starting TDengine in host network mode

Run the following command to start TDengine in host network mode, which allows using the host's FQDN to establish connections, rather than using the container's hostname.

```shell
docker run -d --name tdengine --network host tdengine/tdengine
```

This method is similar to starting TDengine on the host using the systemctl command. If the TDengine client is already installed on the host, you can directly use the following command to access the TDengine service.

```shell
$ taos

taos> show dnodes;
     id      |            endpoint            | vnodes | support_vnodes |   status   |       create_time       |              note              |
=================================================================================================================================================
           1 | vm98:6030                      |      0 |             32 | ready      | 2022-08-19 14:50:05.337 |                                |
Query OK, 1 rows in database (0.010654s)
```

### Start TDengine with a specified hostname and port

Use the following command to establish a connection on a specified hostname using the TAOS_FQDN environment variable or the fqdn configuration item in taos.cfg. This method provides greater flexibility for deploying TDengine.

```shell
docker run -d \
   --name tdengine \
   -e TAOS_FQDN=tdengine \
   -p 6030:6030 \
   -p 6041-6049:6041-6049 \
   -p 6041-6049:6041-6049/udp \
   tdengine/tdengine
```

First, the above command starts a TDengine service in the container, listening on the hostname tdengine, and maps the container's port 6030 to the host's port 6030, and the container's port range [6041, 6049] to the host's port range [6041, 6049]. If the port range on the host is already in use, you can modify the command to specify a free port range on the host.

Secondly, ensure that the hostname tdengine is resolvable in /etc/hosts. Use the following command to save the correct configuration information to the hosts file.

```shell
echo 127.0.0.1 tdengine |sudo tee -a /etc/hosts
```

Finally, you can access the TDengine service using the TDengine CLI with tdengine as the server address, as follows.

```shell
taos -h tdengine -P 6030
```

If TAOS_FQDN is set to the same as the hostname of the host, the effect is the same as "starting TDengine in host network mode".

## Kubernetes Deployment

As a time-series database designed for cloud-native architectures, TDengine inherently supports Kubernetes deployment. This section introduces how to step-by-step create a highly available TDengine cluster for production use using YAML files, with a focus on common operations of TDengine in a Kubernetes environment. This subsection requires readers to have a certain understanding of Kubernetes, be proficient in running common kubectl commands, and understand concepts such as statefulset, service, and pvc. Readers unfamiliar with these concepts can refer to the Kubernetes official website for learning.
To meet the requirements of high availability, the cluster needs to meet the following requirements:

- 3 or more dnodes: Multiple vnodes in the same vgroup of TDengine should not be distributed on the same dnode, so if creating a database with 3 replicas, the number of dnodes should be 3 or more.
- 3 mnodes: mnodes are responsible for managing the entire cluster, with TDengine defaulting to one mnode. If the dnode hosting this mnode goes offline, the entire cluster becomes unavailable.
- 3 replicas of the database: TDengine's replica configuration is at the database level, so 3 replicas can ensure that the cluster remains operational even if any one of the 3 dnodes goes offline. If 2 dnodes go offline, the cluster becomes unavailable because RAFT cannot complete the election. (Enterprise edition: In disaster recovery scenarios, if the data files of any node are damaged, recovery can be achieved by restarting the dnode.)

### Prerequisites

To deploy and manage a TDengine cluster using Kubernetes, the following preparations need to be made.

- This article applies to Kubernetes v1.19 and above.
- This article uses the kubectl tool for installation and deployment, please install the necessary software in advance.
- Kubernetes has been installed and deployed and can normally access or update necessary container repositories or other services.

### Configure Service

Create a Service configuration file: taosd-service.yaml, the service name metadata.name (here "taosd") will be used in the next step. First, add the ports used by TDengine, then set the determined labels app (here "tdengine") in the selector.

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

### Stateful Services StatefulSet

According to Kubernetes' descriptions of various deployment types, we will use StatefulSet as the deployment resource type for TDengine. Create the file tdengine.yaml, where replicas define the number of cluster nodes as 3. The node timezone is set to China (Asia/Shanghai), and each node is allocated 5G of standard storage, which you can modify according to actual conditions.

Please pay special attention to the configuration of startupProbe. After a dnode's Pod goes offline for a period of time and then restarts, the newly online dnode will be temporarily unavailable. If the startupProbe configuration is too small, Kubernetes will consider the Pod to be in an abnormal state and attempt to restart the Pod. This dnode's Pod will frequently restart and never return to a normal state.

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
      affinity:
        podAntiAffinity:
          preferredDuringSchedulingIgnoredDuringExecution:
            - weight: 100
              podAffinityTerm:
                labelSelector:
                  matchExpressions:
                    - key: app
                      operator: In
                      values:
                        - tdengine
                topologyKey: kubernetes.io/hostname
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

### Deploying TDengine Cluster Using kubectl Command

First, create the corresponding namespace `dengine-test`, as well as the PVC, ensuring that there is enough remaining space with `storageClassName` set to `standard`. Then execute the following commands in sequence:

```shell
kubectl apply -f taosd-service.yaml -n tdengine-test
```

The above configuration will create a three-node TDengine cluster, with `dnode` automatically configured. You can use the `show dnodes` command to view the current cluster nodes:

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

View the current mnode:

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

View mnode

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

Using kubectl port forwarding feature allows applications to access the TDengine cluster running in the Kubernetes environment.

```shell
kubectl port-forward -n tdengine-test tdengine-0 6041:6041 &
```

Use the curl command to verify the TDengine REST API using port 6041.

```shell
curl -u root:taosdata -d "show databases" 127.0.0.1:6041/rest/sql
{"code":0,"column_meta":[["name","VARCHAR",64]],"data":[["information_schema"],["performance_schema"],["test"],["test1"]],"rows":4}
```

### Cluster Expansion

TDengine supports cluster expansion:

```shell
kubectl scale statefulsets tdengine  -n tdengine-test --replicas=4
```

The command line argument `--replica=4` indicates that the TDengine cluster is to be expanded to 4 nodes. After execution, first check the status of the POD:

```shell
kubectl get pod -l app=tdengine -n tdengine-test  -o wide
```

Output as follows:

```text
NAME                       READY   STATUS    RESTARTS        AGE     IP             NODE     NOMINATED NODE   READINESS GATES
tdengine-0   1/1     Running   4 (6h26m ago)   6h53m   10.244.2.75    node86   <none>           <none>
tdengine-1   1/1     Running   1 (6h39m ago)   6h53m   10.244.0.59    node84   <none>           <none>
tdengine-2   1/1     Running   0               5h16m   10.244.1.224   node85   <none>           <none>
tdengine-3   1/1     Running   0               3m24s   10.244.2.76    node86   <none>           <none>
```

At this point, the Pod status is still Running. The dnode status in the TDengine cluster can be seen after the Pod status changes to ready:

```shell
kubectl exec -it tdengine-3 -n tdengine-test -- taos -s "show dnodes"
```

The dnode list of the four-node TDengine cluster after expansion:

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

### Cleaning up the Cluster

**Warning**
When deleting PVCs, pay attention to the PV persistentVolumeReclaimPolicy. It is recommended to set it to Delete, so that when the PVC is deleted, the PV will be automatically cleaned up, along with the underlying CSI storage resources. If the policy to automatically clean up PVs when deleting PVCs is not configured, after deleting the PVCs, manually cleaning up the PVs may not release the corresponding CSI storage resources.

To completely remove the TDengine cluster, you need to clean up the statefulset, svc, pvc, and finally delete the namespace.

```shell
kubectl delete statefulset -l app=tdengine -n tdengine-test
kubectl delete svc -l app=tdengine -n tdengine-test
kubectl delete pvc -l app=tdengine -n tdengine-test
kubectl delete namespace tdengine-test
```

### Cluster Disaster Recovery Capabilities

For high availability and reliability of TDengine in a Kubernetes environment, in terms of hardware damage and disaster recovery, it is discussed on two levels:

- The disaster recovery capabilities of the underlying distributed block storage, which includes multiple replicas of block storage. Popular distributed block storage like Ceph has multi-replica capabilities, extending storage replicas to different racks, cabinets, rooms, and data centers (or directly using block storage services provided by public cloud vendors).
- TDengine's disaster recovery, in TDengine Enterprise, inherently supports the recovery of a dnode's work by launching a new blank dnode when an existing dnode permanently goes offline (due to physical disk damage and data loss).

## Deploying TDengine Cluster with Helm

Helm is the package manager for Kubernetes.
The previous section on deploying the TDengine cluster with Kubernetes was simple enough, but Helm can provide even more powerful capabilities.

### Installing Helm

```shell
curl -fsSL -o get_helm.sh \
  https://raw.githubusercontent.com/helm/helm/master/scripts/get-helm-3
chmod +x get_helm.sh
./get_helm.sh
```

Helm operates Kubernetes using kubectl and kubeconfig configurations, which can be set up following the Rancher installation configuration for Kubernetes.

### Installing TDengine Chart

The TDengine Chart has not yet been released to the Helm repository, it can currently be downloaded directly from GitHub:

```shell
wget https://github.com/taosdata/TDengine-Operator/raw/3.0/helm/tdengine-3.0.2.tgz
```

Retrieve the current Kubernetes storage class:

```shell
kubectl get storageclass
```

In minikube, the default is standard. Then, use the helm command to install:

```shell
helm install tdengine tdengine-3.0.2.tgz \
  --set storage.className=<your storage class name> \
  --set image.tag=3.2.3.0

```

In a minikube environment, you can set a smaller capacity to avoid exceeding disk space:

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

### Configuring values

TDengine supports customization through `values.yaml`.
You can obtain the complete list of values supported by the TDengine Chart with helm show values:

```shell
helm show values tdengine-3.0.2.tgz
```

You can save the results as `values.yaml`, then modify various parameters in it, such as the number of replicas, storage class name, capacity size, TDengine configuration, etc., and then use the following command to install the TDengine cluster:

```shell
helm install tdengine tdengine-3.0.2.tgz -f values.yaml
```

All parameters are as follows:

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
  # debug flag for all log type, take effect when non-zero value\
  #TAOS_DEBUG_FLAG: "143"

  # generate core file when service crash
  #TAOS_ENABLE_CORE_FILE: "1"
```

### Expansion

For expansion, refer to the explanation in the previous section, with some additional operations needed from the helm deployment.
First, retrieve the name of the StatefulSet from the deployment.

```shell
export STS_NAME=$(kubectl get statefulset \
  -l "app.kubernetes.io/name=tdengine" \
  -o jsonpath="{.items[0].metadata.name}")
```

The expansion operation is extremely simple, just increase the replica. The following command expands TDengine to three nodes:

```shell
kubectl scale --replicas 3 statefulset/$STS_NAME
```

Use the commands `show dnodes` and `show mnodes` to check if the expansion was successful.

### Cleaning up the Cluster

Under Helm management, the cleanup operation also becomes simple:

```shell
helm uninstall tdengine
```

However, Helm will not automatically remove PVCs, you need to manually retrieve and then delete the PVCs.
