---
title: Kubernetes 部署
---

作为面向云原生架构设计的时序数据库，TDengine 本身就支持 Kubernetes 部署。这里介绍如何使用 YAML 文件从头一步一步创建一个可用于生产使用的高可用 TDengine 集群，并重点介绍 Kubernetes 环境下 TDengine 的常用操作。本小节要求读者对 Kubernetes 有一定的了解，可以熟练运行常见的 kubectl 命令，了解 statefulset、service、pvc 等概念，对这些概念不熟悉的读者，可以先参考 Kubernetes 的官网进行学习。
为了满足高可用的需求，集群需要满足如下要求：

- 3 个及以上 dnode：TDengine 的同一个 vgroup 中的多个 vnode，不允许同时分布在一个 dnode，所以如果创建 3 副本的数据库，则 dnode 数大于等于 3
- 3 个 mnode：mnode 负责整个集群的管理工作，TDengine 默认是一个 mnode。如果这个 mnode 所在的 dnode 掉线，则整个集群不可用。
- 数据库的 3 副本：TDengine 的副本配置是数据库级别，所以数据库 3 副本可满足在 3 个 dnode 的集群中，任意一个 dnode 下线，都不影响集群的正常使用。如果下线 dnode 个数为 2 时，此时集群不可用，因为 RAFT 无法完成选举。（企业版：在灾难恢复场景，任一节点数据文件损坏，都可以通过重新拉起 dnode 进行恢复）

## 使用 kubectl 部署 TDengine

### 前置条件

要使用 Kubernetes 部署管理 TDengine 集群，需要做好如下准备工作。

- 本文适用 Kubernetes v1.19 以上版本
- 本文使用 kubectl 工具进行安装部署，请提前安装好相应软件
- Kubernetes 已经安装部署并能正常访问使用或更新必要的容器仓库或其他服务

### 配置 Service 服务

创建一个 Service 配置文件：taosd-service.yaml，服务名称 metadata.name（此处为 "taosd"）将在下一步中使用到。首先添加 TDengine 所用到的端口，然后在选择器设置确定的标签 app（此处为“tdengine”）。

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

### 有状态服务 StatefulSet

根据 Kubernetes 对各类部署的说明，我们将使用 StatefulSet 作为 TDengine 的部署资源类型。创建文件 tdengine.yaml，其中 replicas 定义集群节点的数量为 3。节点时区为中国（Asia/Shanghai），每个节点分配 5GB 标准（standard）存储，你也可以根据实际情况进行相应修改。

请特别注意 startupProbe 的配置，在 dnode 的 Pod 掉线一段时间后，再重新启动，这个时候新上线的 dnode 会短暂不可用。如果 startupProbe 配置过小，Kubernetes 会认为该 Pod 处于不正常的状态，并尝试重启该 Pod，该 dnode 的 Pod 会频繁重启，始终无法恢复到正常状态。

下面的示例中，`taos-check startup` 用于 startupProbe，检查 TDengine 是否完成启动；`taos-check service` 用于 readinessProbe 和 livenessProbe，检查 SQL 服务是否已经能够对外提供服务。

- `taos-check startup` 和 `taos-check service` 从 `3.4.1.0` 版本开始支持。
- 这类 SQL 探针会复用 `TAOS_ROOT_PASSWORD_FILE` 或 `TAOS_ROOT_PASSWORD` 作为认证密码，因此如果 root 密码被修改，在重启容器、升级镜像或重建 Pod 之前，必须同步更新对应的环境变量或 Secret。
- Docker 场景下不同版本的密码与升级处理方式，请参考本节前文“Docker 部署”中的说明。

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
                - startup
            failureThreshold: 360
            periodSeconds: 10
          readinessProbe:
            exec:
              command:
                - taos-check
                - service
            initialDelaySeconds: 5
            timeoutSeconds: 5000
          livenessProbe:
            exec:
              command:
                - taos-check
                - service
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

### 使用 kubectl 命令部署 TDengine 集群

首先创建对应的 namespace dengine-test，以及 pvc，并保证 storageClassName 是 standard 的剩余空间足够。然后顺序执行以下命令：

```shell
kubectl apply -f taosd-service.yaml -n tdengine-test
```

上面的配置将生成一个三节点的 TDengine 集群，dnode 为自动配置，可以使用 show dnodes 命令查看当前集群的节点：

```shell
kubectl exec -it tdengine-0 -n tdengine-test -- taos -s "show dnodes"
kubectl exec -it tdengine-1 -n tdengine-test -- taos -s "show dnodes"
kubectl exec -it tdengine-2 -n tdengine-test -- taos -s "show dnodes"
```

输出如下：

```shell
taos show dnodes
     id      | endpoint         | vnodes | support_vnodes |   status   |       create_time       |       reboot_time       |              note              |          active_code           |         c_active_code          |
=============================================================================================================================================================================================================================================
           1 | tdengine-0.ta... |      0 |             16 | ready      | 2023-07-19 17:54:18.552 | 2023-07-19 17:54:18.469 |                                |                                |                                |
           2 | tdengine-1.ta... |      0 |             16 | ready      | 2023-07-19 17:54:37.828 | 2023-07-19 17:54:38.698 |                                |                                |                                |
           3 | tdengine-2.ta... |      0 |             16 | ready      | 2023-07-19 17:55:01.141 | 2023-07-19 17:55:02.039 |                                |                                |                                |
Query OK, 3 row(s) in set (0.001853s)
```

查看当前 mnode

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

创建 mnode

```shell
kubectl exec -it tdengine-0 -n tdengine-test -- taos -s "create mnode on dnode 2"
kubectl exec -it tdengine-0 -n tdengine-test -- taos -s "create mnode on dnode 3"
```

查看 mnode

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

### 端口转发

利用 kubectl 端口转发功能可以使应用可以访问 Kubernetes 环境运行的 TDengine 集群。

```shell
kubectl port-forward -n tdengine-test tdengine-0 6041:6041 &
```

使用 curl 命令验证 TDengine REST API 使用的 6041 接口。

```shell
curl -u root:taosdata -d "show databases" 127.0.0.1:6041/rest/sql
{"code":0,"column_meta":[["name","VARCHAR",64]],"data":[["information_schema"],["performance_schema"],["test"],["test1"]],"rows":4}
```

### 集群扩容

TDengine 支持集群扩容：

```shell
kubectl scale statefulsets tdengine  -n tdengine-test --replicas=4
```

上面命令行中参数 `--replica=4` 表示要将 TDengine 集群扩容到 4 个节点，执行后首先检查 POD 的状态：

```shell
kubectl get pod -l app=tdengine -n tdengine-test  -o wide
```

输出如下：

```text
NAME                       READY   STATUS    RESTARTS        AGE     IP             NODE     NOMINATED NODE   READINESS GATES
tdengine-0   1/1     Running   4 (6h26m ago)   6h53m   10.244.2.75    node86   <none>           <none>
tdengine-1   1/1     Running   1 (6h39m ago)   6h53m   10.244.0.59    node84   <none>           <none>
tdengine-2   1/1     Running   0               5h16m   10.244.1.224   node85   <none>           <none>
tdengine-3   1/1     Running   0               3m24s   10.244.2.76    node86   <none>           <none>
```

此时 Pod 的状态仍然是 Running，TDengine 集群中的 dnode 状态要等 Pod 状态为 ready 之后才能看到：

```shell
kubectl exec -it tdengine-3 -n tdengine-test -- taos -s "show dnodes"
```

扩容后的四节点 TDengine 集群的 dnode 列表：

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

### 清理集群

**Warning**
删除 pvc 时需要注意下 pv persistentVolumeReclaimPolicy 策略，建议改为 Delete，这样在删除 pvc 时才会自动清理 pv，同时会清理底层的 csi 存储资源，如果没有配置删除 pvc 自动清理 pv 的策略，再删除 pvc 后，在手动清理 pv 时，pv 对应的 csi 存储资源可能不会被释放。

完整移除 TDengine 集群，需要分别清理 statefulset、svc、pvc，最后删除命名空间。

```shell
kubectl delete statefulset -l app=tdengine -n tdengine-test
kubectl delete svc -l app=tdengine -n tdengine-test
kubectl delete pvc -l app=tdengine -n tdengine-test
kubectl delete namespace tdengine-test
```

### 集群灾备能力

对于在 Kubernetes 环境下 TDengine 的高可用和高可靠来说，对于硬件损坏、灾难恢复，分为两个层面来讲：

- 底层的分布式块存储具备的灾难恢复能力，块存储的多副本，当下流行的分布式块存储如 Ceph，就具备多副本能力，将存储副本扩展到不同的机架、机柜、机房、数据中心（或者直接使用公有云厂商提供的块存储服务）
- TDengine 的灾难恢复，在 TDengine TSDB Enterprise 中，本身具备了当一个 dnode 永久下线（物理机磁盘损坏，数据分拣丢失）后，重新拉起一个空白的 dnode 来恢复原 dnode 的工作。

## 使用 Helm 部署 TDengine 集群

Helm 是 Kubernetes 的包管理器。
上一节使用 Kubernetes 部署 TDengine 集群的操作已经足够简单，但 Helm 可以提供更强大的能力。

### 安装 Helm

```shell
curl -fsSL -o get_helm.sh \
  https://raw.githubusercontent.com/helm/helm/master/scripts/get-helm-3
chmod +x get_helm.sh
./get_helm.sh
```

Helm 会使用 kubectl 和 kubeconfig 的配置来操作 Kubernetes，可以参考 Rancher 安装 Kubernetes 的配置来进行设置。

### 安装 TDengine Chart

TDengine Chart 尚未发布到 Helm 仓库，当前可以从 GitHub 直接下载：

```shell
wget https://github.com/taosdata/TDengine-Operator/raw/3.0/helm/tdengine-3.0.2.tgz
```

获取当前 Kubernetes 的存储类：

```shell
kubectl get storageclass
```

在 minikube 默认为 standard。之后，使用 helm 命令安装：

```shell
helm install tdengine tdengine-3.0.2.tgz \
  --set storage.className=<your storage class name> \
  --set image.tag=3.2.3.0

```

在 minikube 环境下，可以设置一个较小的容量避免超出磁盘可用空间：

```shell
helm install tdengine tdengine-3.0.2.tgz \
  --set storage.className=standard \
  --set storage.dataSize=2Gi \
  --set storage.logSize=10Mi \
  --set image.tag=3.2.3.0
```

部署成功后，TDengine Chart 将会输出操作 TDengine 的说明：

```shell
export POD_NAME=$(kubectl get pods --namespace default \
  -l "app.kubernetes.io/name=tdengine,app.kubernetes.io/instance=tdengine" \
  -o jsonpath="{.items[0].metadata.name}")
kubectl --namespace default exec $POD_NAME -- taos -s "show dnodes; show mnodes"
kubectl --namespace default exec -it $POD_NAME -- taos
```

可以创建一个表进行测试：

```shell
kubectl --namespace default exec $POD_NAME -- \
  taos -s "create database test;
    use test;
    create table t1 (ts timestamp, n int);
    insert into t1 values(now, 1)(now + 1s, 2);
    select * from t1;"
```

### 配置 values

TDengine 支持 `values.yaml` 自定义。
通过 helm show values 可以获取 TDengine Chart 支持的全部 values 列表：

```shell
helm show values tdengine-3.0.2.tgz
```

你可以将结果保存为 values.yaml，之后可以修改其中的各项参数，如 replica 数量，存储类名称，容量大小，TDengine 配置等，然后使用如下命令安装 TDengine 集群：

```shell
helm install tdengine tdengine-3.0.2.tgz -f values.yaml
```

全部参数如下：

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
# See the variable list at https://www.taosdata.com/cn/documentation/administrator .
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

### 扩容

关于扩容可参考上一节的说明，有一些额外的操作需要从 helm 的部署中获取。
首先，从部署中获取 StatefulSet 的名称。

```shell
export STS_NAME=$(kubectl get statefulset \
  -l "app.kubernetes.io/name=tdengine" \
  -o jsonpath="{.items[0].metadata.name}")
```

扩容操作极其简单，增加 replica 即可。以下命令将 TDengine 扩充到三节点：

```shell
kubectl scale --replicas 3 statefulset/$STS_NAME
```

使用命令 `show dnodes` 和 `show mnodes` 检查是否扩容成功。

### 清理集群

Helm 管理下，清理操作也变得简单：

```shell
helm uninstall tdengine
```

但 Helm 也不会自动移除 PVC，需要手动获取 PVC 然后删除掉。
