---
title: Kubernetes Deployment
slug: /operations-and-maintenance/deploy-your-cluster/kubernetes-deployment
---

You can use kubectl or Helm to deploy TDengine in Kubernetes.

## Deploy TDengine with kubectl

As a time-series database designed for cloud-native architectures, TDengine inherently supports Kubernetes deployment. This section introduces how to step-by-step create a highly available TDengine cluster for production use using YAML files, with a focus on common operations of TDengine in a Kubernetes environment. This subsection requires readers to have a certain understanding of Kubernetes, be proficient in running common kubectl commands, and understand concepts such as statefulset, service, and pvc. Readers unfamiliar with these concepts can refer to the Kubernetes official website for learning.
To meet the requirements of high availability, the cluster needs to meet the following requirements:

- 3 or more dnodes: Multiple vnodes in the same vgroup of TDengine should not be distributed on the same dnode, so if creating a database with 3 replicas, the number of dnodes should be 3 or more.
- 3 mnodes: mnodes are responsible for managing the entire cluster, with TDengine defaulting to one mnode. If the dnode hosting this mnode goes offline, the entire cluster becomes unavailable.
- 3 replicas of the database: TDengine's replica configuration is at the database level, so 3 replicas can ensure that the cluster remains operational even if any one of the 3 dnodes goes offline. If 2 dnodes go offline, the cluster becomes unavailable because RAFT cannot complete the election. (TSDB-Enterprise: In disaster recovery scenarios, if the data files of any node are damaged, recovery can be achieved by restarting the dnode.)

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

## Deploy TDengine with Helm

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
wget https://github.com/taosdata/TDengine-Operator/raw/refs/heads/3.0/helm/tdengine-3.5.0.tgz
```

Follow the steps below to install the TDengine Chart:

```shell
# Edit the values.yaml file to set the topology of the cluster
vim values.yaml
helm install tdengine tdengine-3.5.0.tgz -f values.yaml
```

If you are using community images, you can use the following command to install TDengine with Helm Chart:

<details>
<summary>Helm Chart Use Cases for Community</summary>

#### Community Case 1: Simple 1-node Deployment

The following is a simple example of deploying a single-node TDengine cluster using Helm.

```yaml
# This example is a simple deployment with one server replica.
name: "tdengine"

image:
  repository: # Leave a trailing slash for the repository, or "" for no repository
  server: tdengine/tdengine:latest

# Set timezone here, not in taoscfg
timezone: "Asia/Shanghai"

labels:
  app: "tdengine"
  # Add more labels as needed.

services:
  server:
    type: ClusterIP
    replica: 1
    ports:
      # TCP range required
      tcp: [6041, 6030, 6060]
      # UDP range, optional
      udp:
    volumes:
      - name: data
        mountPath: /var/lib/taos
        spec:
          storageClassName: "local-path"
          accessModes: [ "ReadWriteOnce" ]
          resources:
            requests:
              storage: "10Gi"
      - name: log
        mountPath: /var/log/taos/
        spec:
          storageClassName: "local-path"
          accessModes: [ "ReadWriteOnce" ]
          resources:
            requests:
              storage: "10Gi"
    files:
      - name: cfg # must be lower case.
        mountPath: /etc/taos/taos.cfg
        content: |
          dataDir /var/lib/taos/
          logDir /var/log/taos/
```

Let's explain the above configuration:

- name: The name of the deployment, here it is "tdengine".
- image:
  - repository: The image repository address, remember to leave a trailing slash for the repository, or set it to an empty string to use docker.io.
  - server: The specific name and tag of the server image. You need to ask your business partner for the TDengine Enterprise image.
- timezone: Set the timezone, here it is "Asia/Shanghai".
- labels: Add labels to the deployment, here is an app label with the value "tdengine", more labels can be added as needed.
- services:
  - server: Configure the server service.
    - type: The service type, here it is **ClusterIP**.
    - replica: The number of replicas, here it is 1.
    - ports: Configure the ports of the service.
      - tcp: The required TCP port range, here it is [6041, 6030, 6060].
      - udp: The optional UDP port range, which is not configured here.
    - volumes: Configure the volumes.
      - name: The name of the volume, here there are two volumes, data and log.
      - mountPath: The mount path of the volume.
      - spec: The specification of the volume.
        - storageClassName: The storage class name, here it is **local-path**.
        - accessModes: The access mode, here it is **ReadWriteOnce**.
        - resources.requests.storage: The requested storage size, here it is **10Gi**.
    - files: Configure the files to mount in TDengine server.
      - name: The name of the file, here it is **cfg**.
      - mountPath: The mount path of the file, which is **taos.cfg**.
      - content: The content of the file, here the **dataDir** and **logDir** are configured.

After configuring the values.yaml file, use the following command to install the TDengine Chart:

```shell
helm install simple tdengine-3.5.0.tgz -f values.yaml
```

After installation, you can see the instructions to see the status of the TDengine cluster:

```shell
NAME: simple
LAST DEPLOYED: Sun Feb  9 13:40:00 2025 default
STATUS: deployed
REVISION: 1
TEST SUITE: None
NOTES:
1. Get first POD name:
                                                                                                           
export POD_NAME=$(kubectl get pods --namespace default \
  -l "app.kubernetes.io/name=tdengine,app.kubernetes.io/instance=simple" -o jsonpath="{.items[0].metadata.name}")

2. Show dnodes/mnodes:

kubectl --namespace default exec $POD_NAME -- taos -s "show dnodes; show mnodes"

3. Run into TDengine CLI:

kubectl --namespace default exec -it $POD_NAME -- taos
```

Follow the instructions to check the status of the TDengine cluster:

```shell
root@u1-58:/data1/projects/helm# kubectl --namespace default exec $POD_NAME -- taos -s "show dnodes; show mnodes"
Welcome to the TDengine Command Line Interface, Client Version:3.3.5.8
Copyright (c) 2023 by TDengine, all rights reserved.

taos> show dnodes; show mnodes
     id      |            endpoint            | vnodes | support_vnodes |    status    |       create_time       |       reboot_time       |              note              |
=============================================================================================================================================================================
           1 | oss-tdengine-0.oss-tdengine... |      0 |             21 | ready        | 2025-03-12 19:05:42.224 | 2025-03-12 19:05:42.044 |                                |
Query OK, 1 row(s) in set (0.002545s)

     id      |            endpoint            |      role      |   status    |       create_time       |        role_time        |
==================================================================================================================================
           1 | oss-tdengine-0.oss-tdengine... | leader         | ready       | 2025-03-12 19:05:42.239 | 2025-03-12 19:05:42.137 |
Query OK, 1 row(s) in set (0.001343s)
```

To clean up the TDengine cluster, use the following command:

```shell
helm uninstall simple
kubectl delete pvc -l app.kubernetes.io/instance=simple
```

#### Community Case 2: 3-replica Deployment with Single taosX

```yaml
# This example shows how to deploy a 3-replica TDengine cluster with separate taosx/explorer service.
# Users should know that the explorer/taosx service is not cluster-ready, so it is recommended to deploy it separately.
name: "tdengine"

image:
  repository: # Leave a trailing slash for the repository, or "" for no repository
  server: tdengine/tdengine:latest

# Set timezone here, not in taoscfg
timezone: "Asia/Shanghai"

labels:
  # Add more labels as needed.

services:
  server:
    type: ClusterIP
    replica: 3
    ports:
      # TCP range required
      tcp: [6041, 6030]
      # UDP range, optional
      udp:
    volumes:
      - name: data
        mountPath: /var/lib/taos
        spec:
          storageClassName: "local-path"
          accessModes: ["ReadWriteOnce"]
          resources:
            requests:
              storage: "10Gi"
      - name: log
        mountPath: /var/log/taos/
        spec:
          storageClassName: "local-path"
          accessModes: ["ReadWriteOnce"]
          resources:
            requests:
              storage: "10Gi"
```

You can see that the configuration is similar to the first one, with the addition of the taosx configuration. The taosx service is configured with similar storage configuration as the server service, and the server service is configured with 3 replicas. Since the taosx service is not cluster-ready, it is recommended to deploy it separately.

After configuring the values.yaml file, use the following command to install the TDengine Chart:

```shell
helm install replica3 tdengine-3.5.0.tgz -f values.yaml
```

To clean up the TDengine cluster, use the following command:

```shell
helm uninstall replica3
kubectl delete pvc -l app.kubernetes.io/instance=replica3
```

You can use the following command to expose the explorer service to the outside world with ingress:

```shell
tee replica3-ingress.yaml <<EOF
# This is a helm chart example for deploying 3 replicas of TDengine Explorer
apiVersion: networking.k8s.io/v1
kind: Ingress
metadata:
  name: replica3-ingress
  namespace: default
spec:
  rules:
    - host: replica3.local.tdengine.com
      http:
        paths:
          - path: /
            pathType: Prefix
            backend:
              service:
                name:  replica3-tdengine-taosx
                port:
                  number: 6060
EOF

kubectl apply -f replica3-ingress.yaml
```

Use `kubectl get ingress` to view the ingress service.

```shell
root@server:/data1/projects/helm# kubectl get ingress
NAME               CLASS   HOSTS                         ADDRESS        PORTS   AGE
replica3-ingress   nginx   replica3.local.tdengine.com   192.168.1.58   80      48m
```

You can configure the domain name resolution to point to the ingress service's external IP address. For example, add the following line to the hosts file:

```conf
192.168.1.58    replica3.local.tdengine.com
```

Now you can access the explorer service through the domain name `replica3.local.tdengine.com`.

```shell
curl http://replica3.local.tdengine.com
```

</details>

With TDengine Enterprise images, you can use the following command to install TDengine with Helm Chart:

<details>
<summary>Helm Chart Use Cases for Enterprise</summary>

#### Enterprise Case 1: Simple 1-node Deployment

The following is a simple example of deploying a single-node TDengine cluster using Helm.

```yaml
# This example is a simple deployment with one server replica.
name: "tdengine"

image:
  repository:
  server: tdengine/tdengine-ee

# Set timezone here, not in taoscfg
timezone: "Asia/Shanghai"

labels:
  app: "tdengine"
  # Add more labels as needed.

services:
  server:
    type: ClusterIP
    replica: 1
    ports:
      # TCP range required
      tcp: [6041, 6030, 6060]
      # UDP range, optional
      udp:
    volumes:
      - name: data
        mountPath: /var/lib/taos
        spec:
          storageClassName: "local-path"
          accessModes: [ "ReadWriteOnce" ]
          resources:
            requests:
              storage: "10Gi"
      - name: log
        mountPath: /var/log/taos/
        spec:
          storageClassName: "local-path"
          accessModes: [ "ReadWriteOnce" ]
          resources:
            requests:
              storage: "10Gi"
    files:
      - name: cfg # must be lower case.
        mountPath: /etc/taos/taos.cfg
        content: |
          dataDir /var/lib/taos/
          logDir /var/log/taos/
```

Let's explain the above configuration:

- name: The name of the deployment, here it is "tdengine".
- image:
  - repository: The image repository address, remember to leave a trailing slash for the repository, or set it to an empty string to use docker.io.
  - server: The specific name and tag of the server image. You need to ask your business partner for the TDengine Enterprise image.
- timezone: Set the timezone, here it is "Asia/Shanghai".
- labels: Add labels to the deployment, here is an app label with the value "tdengine", more labels can be added as needed.
- services:
  - server: Configure the server service.
    - type: The service type, here it is **ClusterIP**.
    - replica: The number of replicas, here it is 1.
    - ports: Configure the ports of the service.
      - tcp: The required TCP port range, here it is [6041, 6030, 6060].
      - udp: The optional UDP port range, which is not configured here.
    - volumes: Configure the volumes.
      - name: The name of the volume, here there are two volumes, data and log.
      - mountPath: The mount path of the volume.
      - spec: The specification of the volume.
        - storageClassName: The storage class name, here it is **local-path**.
        - accessModes: The access mode, here it is **ReadWriteOnce**.
        - resources.requests.storage: The requested storage size, here it is **10Gi**.
    - files: Configure the files to mount in TDengine server.
      - name: The name of the file, here it is **cfg**.
      - mountPath: The mount path of the file, which is **taos.cfg**.
      - content: The content of the file, here the **dataDir** and **logDir** are configured.

After configuring the values.yaml file, use the following command to install the TDengine Chart:

```shell
helm install simple tdengine-3.5.0.tgz -f values.yaml
```

After installation, you can see the instructions to see the status of the TDengine cluster:

```shell
NAME: simple
LAST DEPLOYED: Sun Feb  9 13:40:00 2025 default
STATUS: deployed
REVISION: 1
TEST SUITE: None
NOTES:
1. Get first POD name:
                                                                                                           
export POD_NAME=$(kubectl get pods --namespace default \
  -l "app.kubernetes.io/name=tdengine,app.kubernetes.io/instance=simple" -o jsonpath="{.items[0].metadata.name}")

2. Show dnodes/mnodes:

kubectl --namespace default exec $POD_NAME -- taos -s "show dnodes; show mnodes"

3. Run into TDengine CLI:

kubectl --namespace default exec -it $POD_NAME -- taos
```

Follow the instructions to check the status of the TDengine cluster:

```shell
root@u1-58:/data1/projects/helm# kubectl --namespace default exec $POD_NAME -- taos -s "show dnodes; show mnodes"
Welcome to the TDengine Command Line Interface, Client Version:3.3.5.1
Copyright (c) 2023 by TDengine, all rights reserved.

taos> show dnodes; show mnodes
     id      |            endpoint            | vnodes | support_vnodes |    status    |       create_time       |       reboot_time       |              note              |         machine_id         |
==========================================================================================================================================================================================================
           1 | simple-tdengine-0.simple-td... |      0 |             85 | ready        | 2025-02-07 21:17:34.903 | 2025-02-08 15:52:34.781 |                                | BWhWyPiEBrWZrQCSqTSc2a/H   |
Query OK, 1 row(s) in set (0.005133s)

     id      |            endpoint            |      role      |   status    |       create_time       |        role_time        |
==================================================================================================================================
           1 | simple-tdengine-0.simple-td... | leader         | ready       | 2025-02-07 21:17:34.906 | 2025-02-08 15:52:34.878 |
Query OK, 1 row(s) in set (0.004299s)
```

To clean up the TDengine cluster, use the following command:

```shell
helm uninstall simple
kubectl delete pvc -l app.kubernetes.io/instance=simple
```

#### Enterprise Case 2: Tiered-Storage Deployment

The following is an example of deploying a TDengine cluster with tiered storage using Helm.

```yaml
# This is an example of a 3-tiered storage deployment with one server replica.
name: "tdengine"

image:
  repository:
  server: tdengine/tdengine-ee

# Set timezone here, not in taoscfg
timezone: "Asia/Shanghai"

labels:
  # Add more labels as needed.

services:
  server:
    type: ClusterIP
    replica: 1
    ports:
      # TCP range required
      tcp: [6041, 6030, 6060]
      # UDP range, optional
      udp:
    volumes:
      - name: tier0
        mountPath: /data/taos0/
        spec:
          storageClassName: "local-path"
          accessModes: [ "ReadWriteOnce" ]
          resources:
            requests:
              storage: "10Gi"
      - name: tier1
        mountPath: /data/taos1/
        spec:
          storageClassName: "local-path"
          accessModes: [ "ReadWriteOnce" ]
          resources:
            requests:
              storage: "10Gi"
      - name: tier2
        mountPath: /data/taos2/
        spec:
          storageClassName: "local-path"
          accessModes: [ "ReadWriteOnce" ]
          resources:
            requests:
              storage: "10Gi"
      - name: log
        mountPath: /var/log/taos/
        spec:
          storageClassName: "local-path"
          accessModes: [ "ReadWriteOnce" ]
          resources:
            requests:
              storage: "10Gi"
    environment:
      TAOS_DEBUG_FLAG: "131"
    files:
      - name: cfg # must be lower case.
        mountPath: /etc/taos/taos.cfg
        content: |
          dataDir /data/taos0/  0 1
          dataDir /data/taos1/  1 0
          dataDir /data/taos2/  2 0
```

You can see that the configuration is similar to the previous one, with the addition of the tiered storage configuration. The dataDir configuration in the taos.cfg file is also modified to support tiered storage.

After configuring the values.yaml file, use the following command to install the TDengine Chart:

```shell
helm install tiered tdengine-3.5.0.tgz -f values.yaml
```

#### Enterprise Case 3: 2-replica Deployment

TDengine support 2-replica deployment with an arbitrator, which can be configured as follows:

```yaml
# This example shows how to deploy a 2-replica TDengine cluster with an arbitrator.
name: "tdengine"

image:
  repository:
  server: tdengine/tdengine-ee

# Set timezone here, not in taoscfg
timezone: "Asia/Shanghai"

labels:
  my-app: "tdengine"
  # Add more labels as needed.

services:
  arbitrator:
    type: ClusterIP
    volumes:
      - name: arb-data
        mountPath: /var/lib/taos
        spec:
          storageClassName: "local-path"
          accessModes: [ "ReadWriteOnce" ]
          resources:
            requests:
              storage: "10Gi"
      - name: arb-log
        mountPath: /var/log/taos/
        spec:
          storageClassName: "local-path"
          accessModes: [ "ReadWriteOnce" ]
          resources:
            requests:
              storage: "10Gi"
  server:
    type: ClusterIP
    replica: 2
    ports:
      # TCP range required
      tcp: [6041, 6030, 6060]
      # UDP range, optional
      udp:
    volumes:
      - name: data
        mountPath: /var/lib/taos
        spec:
          storageClassName: "local-path"
          accessModes: [ "ReadWriteOnce" ]
          resources:
            requests:
              storage: "10Gi"
      - name: log
        mountPath: /var/log/taos/
        spec:
          storageClassName: "local-path"
          accessModes: [ "ReadWriteOnce" ]
          resources:
            requests:
              storage: "10Gi"
```

You can see that the configuration is similar to the first one, with the addition of the arbitrator configuration. The arbitrator service is configured with the same storage as the server service, and the server service is configured with 2 replicas (the arbitrator should be 1 replica and not able to be changed).

#### Enterprise Case 4: 3-replica Deployment with Single taosX

```yaml
# This example shows how to deploy a 3-replica TDengine cluster with separate taosx/explorer service.
# Users should know that the explorer/taosx service is not cluster-ready, so it is recommended to deploy it separately.
name: "tdengine"

image:
  repository:
  server: tdengine/tdengine-ee

# Set timezone here, not in taoscfg
timezone: "Asia/Shanghai"

labels:
  # Add more labels as needed.

services:
  server:
    type: ClusterIP
    replica: 3
    ports:
      # TCP range required
      tcp: [6041, 6030]
      # UDP range, optional
      udp:
    volumes:
      - name: data
        mountPath: /var/lib/taos
        spec:
          storageClassName: "local-path"
          accessModes: ["ReadWriteOnce"]
          resources:
            requests:
              storage: "10Gi"
      - name: log
        mountPath: /var/log/taos/
        spec:
          storageClassName: "local-path"
          accessModes: ["ReadWriteOnce"]
          resources:
            requests:
              storage: "10Gi"
    environment:
      ENABLE_TAOSX: "0" # Disable taosx in server replicas.
  taosx:
    type: ClusterIP
    volumes:
      - name: taosx-data
        mountPath: /var/lib/taos
        spec:
          storageClassName: "local-path"
          accessModes: ["ReadWriteOnce"]
          resources:
            requests:
              storage: "10Gi"
      - name: taosx-log
        mountPath: /var/log/taos/
        spec:
          storageClassName: "local-path"
          accessModes: ["ReadWriteOnce"]
          resources:
            requests:
              storage: "10Gi"
    files:
      - name: taosx
        mountPath: /etc/taos/taosx.toml
        content: |-
          # TAOSX configuration in TOML format.
          [monitor]
          # FQDN of taosKeeper service, no default value
          fqdn = "localhost"
          # How often to send metrics to taosKeeper, default every 10 seconds. Only value from 1 to 10 is valid.
          interval = 10

          # log configuration
          [log]
          # All log files are stored in this directory
          #
          #path = "/var/log/taos" # on linux/macOS

          # log filter level
          #
          #level = "info"

          # Compress archived log files or not
          #
          #compress = false

          # The number of log files retained by the current explorer server instance in the `path` directory
          #
          #rotationCount = 30

          # Rotate when the log file reaches this size
          #
          #rotationSize = "1GB"

          # Log downgrade when the remaining disk space reaches this size, only logging `ERROR` level logs
          #
          #reservedDiskSize = "1GB"

          # The number of days log files are retained
          #
          #keepDays = 30

          # Watching the configuration file for log.loggers changes, default to true.
          #
          #watching = true

          # Customize the log output level of modules, and changes will be applied after modifying the file when log.watching is enabled
          #
          # ## Examples:
          #
          # crate = "error"
          # crate::mod1::mod2 = "info"
          # crate::span[field=value] = "warn"
          #
          [log.loggers]
          #"actix_server::accept" = "warn"
          #"taos::query" = "warn"
```

You can see that the configuration is similar to the first one, with the addition of the taosx configuration. The taosx service is configured with similar storage configuration as the server service, and the server service is configured with 3 replicas. Since the taosx service is not cluster-ready, it is recommended to deploy it separately.

After configuring the values.yaml file, use the following command to install the TDengine Chart:

```shell
helm install replica3 tdengine-3.5.0.tgz -f values.yaml
```

You can use the following command to expose the explorer service to the outside world with ingress:

```shell
tee replica3-ingress.yaml <<EOF
# This is a helm chart example for deploying 3 replicas of TDengine Explorer
apiVersion: networking.k8s.io/v1
kind: Ingress
metadata:
  name: replica3-ingress
  namespace: default
spec:
  rules:
    - host: replica3.local.tdengine.com
      http:
        paths:
          - path: /
            pathType: Prefix
            backend:
              service:
                name:  replica3-tdengine-taosx
                port:
                  number: 6060
EOF

kubectl apply -f replica3-ingress.yaml
```

Use `kubectl get ingress` to view the ingress service.

```shell
root@server:/data1/projects/helm# kubectl get ingress
NAME               CLASS   HOSTS                         ADDRESS        PORTS   AGE
replica3-ingress   nginx   replica3.local.tdengine.com   192.168.1.58   80      48m
```

You can configure the domain name resolution to point to the ingress service's external IP address. For example, add the following line to the hosts file:

```conf
192.168.1.58    replica3.local.tdengine.com
```

Now you can access the explorer service through the domain name `replica3.local.tdengine.com`.

```shell
curl http://replica3.local.tdengine.com
```

</details>
