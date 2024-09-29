---
sidebar_label: 集群部署
title: 集群部署
toc_max_heading_level: 4
---

由于 TDengine 设计之初就采用了分布式架构，具有强大的水平扩展能力，以满足不断增长的数据处理需求，因此 TDengine 支持集群，并将此核心功能开源。用户可以根据实际环境和需求选择 4 种部署方式—手动部署、Docker 部署、Kubernetes 部署和 Helm 部署。

## 手动部署

### 部署 taosd

taosd 是 TDengine 集群中最主要的服务组件，本节介绍手动部署 taosd 集群的步骤。

#### 1. 清除数据

如果搭建集群的物理节点中存在之前的测试数据或者装过其他版本（如 1.x/2.x）的TDengine，请先将其删除，并清空所有数据。

#### 2. 检查环境

在进行 TDengine 集群部署之前，全面检查所有 dnode 以及应用程序所在物理节点的网络设置至关重要。以下是检查步骤：

- 第 1 步，在每个物理节点上执行 hostname -f 命令，以查看并确认所有节点的hostname 是唯一的。对于应用程序驱动所在的节点，这一步骤可以省略。
- 第 2 步，在每个物理节点上执行 ping host 命令，其中 host 是其他物理节点的 hostname。这一步骤旨在检测当前节点与其他物理节点之间的网络连通性。如果发现无法 ping 通，请立即检查网络和 DNS 设置。对于 Linux 操作系统，请检查 /etc/hosts 文件；对于 Windows 操作系统，请检查C:\Windows\system32\drivers\etc\hosts 文件。网络不通畅将导致无法组建集群，请务必解决此问题。
- 第 3 步，在应用程序运行的物理节点上重复上述网络检测步骤。如果发现网络不通畅，应用程序将无法连接到 taosd 服务。此时，请仔细检查应用程序所在物理节点的DNS 设置或 hosts 文件，确保其配置正确无误。
- 第 4 步，检查端口，确保集群中所有主机在端口 6030 上的 TCP 能够互通。

通过以上步骤，你可以确保所有节点在网络层面顺利通信，从而为成功部署TDengine 集群奠定坚实基础

#### 3. 安装

为了确保集群内各物理节点的一致性和稳定性，请在所有物理节点上安装相同版本的 TDengine。

#### 4. 修改配置

修改 TDengine 的配置文件（所有节点的配置文件都需要修改）。假设准备启动的第 1 个 dnode 的 endpoint 为 h1.taosdata.com:6030，其与集群配置相关参数如下。

```shell
# firstEp 是每个 dnode 首次启动后连接的第 1 个 dnode
firstEp h1.taosdata.com:6030
# 必须配置为本 dnode 的 FQDN，如果本机只有一个 hostname，可注释或删除如下这行代码
fqdn h1.taosdata.com
# 配置本 dnode 的端口，默认是 6030
serverPort 6030
```

一定要修改的参数是 firstEp 和 fqdn。对于每个 dnode，firstEp 配置应该保持一致，但 fqdn 一定要配置成其所在 dnode 的值。其他参数可不做任何修改，除非你很清楚为什么要修改。

对于希望加入集群的 dnode 节点，必须确保下表所列的与 TDengine 集群相关的参数设置完全一致。任何参数的不匹配都可能导致 dnode 节点无法成功加入集群。

| 参数名称         | 含义                                                       |
|:---------------:|:----------------------------------------------------------:|
|statusInterval | dnode 向 mnode 报告状态的间隔 |
|timezone | 时区 |
|locale | 系统区位信息及编码格式 |
|charset | 字符集编码 |
|ttlChangeOnWrite | ttl 到期时间是否伴随表的修改操作而改变 |

#### 5. 启动

按照前述步骤启动第 1 个 dnode，例如 h1.taosdata.com。接着在终端中执行 taos，启动 TDengine 的 CLI 程序 taos，并在其中执行 show dnodes 命令，以查看当前集群中的所有 dnode 信息。

```shell
taos> show dnodes;
 id | endpoint | vnodes|support_vnodes|status| create_time | note |
===================================================================================
 1| h1.taosdata.com:6030 | 0| 1024| ready| 2022-07-16 10:50:42.673 | |
```

可以看到，刚刚启动的 dnode 节点的 endpoint 为 h1.taosdata.com:6030。这个地址就是新建集群的 first Ep。

#### 6. 添加 dnode

按照前述步骤，在每个物理节点启动 taosd。每个 dnode 都需要在 taos.cfg 文件中将 firstEp 参数配置为新建集群首个节点的 endpoint，在本例中是 h1.taosdata.com:6030。在第 1 个 dnode 所在机器，在终端中运行 taos，打开 TDengine 的 CLI 程序 taos，然后登录TDengine 集群，执行如下 SQL。

```shell
create dnode "h2.taosdata.com:6030"
```

将新 dnode 的 endpoint 添加进集群的 endpoint 列表。需要为 `fqdn:port` 加上双引号，否则运行时出错。请注意将示例的 h2.taosdata.com:6030 替换为这个新 dnode 的 endpoint。然后执行如下 SQL 查看新节点是否成功加入。若要加入的 dnode 当前处于离线状态，请参考本节后面的 “常见问题”部分进行解决。

```shell
show dnodes;
```

在日志中，请确认输出的 dnode 的 fqdn 和端口是否与你刚刚尝试添加的 endpoint 一致。如果不一致，请修正为正确的 endpoint。遵循上述步骤，你可以持续地将新的 dnode 逐个加入集群，从而扩展集群规模并提高整体性能。确保在添加新节点时遵循正确的流程，这有助于维持集群的稳定性和可靠性。

**Tips**
- 任何已经加入集群的 dnode 都可以作为后续待加入节点的 firstEp。firstEp 参数仅仅在该 dnode 首次加入集群时起作用，加入集群后，该 dnode 会保存最新的 mnode 的 endpoint 列表，后续不再依赖这个参数。之后配置文件中的 firstEp 参数主要用于客户端连接，如果没有为 TDengine 的 CLI 设置参数，则默认连接由 firstEp 指定的节点。
- 两个没有配置 firstEp 参数的 dnode 在启动后会独立运行。这时无法将其中一个dnode 加入另外一个 dnode，形成集群。
- TDengine 不允许将两个独立的集群合并成新的集群。

#### 7. 添加 mnode

在创建 TDengine 集群时，首个 dnode 将自动成为集群的 mnode，负责集群的管理和协调工作。为了实现 mnode 的高可用性，后续添加的 dnode 需要手动创建 mnode。请注意，一个集群最多允许创建 3 个 mnode，且每个 dnode 上只能创建一个 mnode。当集群中的 dnode 数量达到或超过 3 个时，你可以为现有集群创建 mnode。在第 1个 dnode 中，首先通过 TDengine 的 CLI 程序 taos 登录 TDengine，然后执行如下 SQL。

```shell
create mnode on dnode <dnodeId>
```

请注意将上面示例中的 dnodeId 替换为刚创建 dnode 的序号（可以通过执行 `show dnodes` 命令获得）。最后执行如下 `show mnodes`，查看新创建的 mnode 是否成功加入集群。


**Tips**

在搭建 TDengine 集群的过程中，如果在执行 create dnode 命令以添加新节点后，新节点始终显示为离线状态，请按照以下步骤进行排查。

- 第 1 步，检查新节点上的 taosd 服务是否已经正常启动。你可以通过查看日志文件或使用 ps 命令来确认。
- 第 2 步，如果 taosd 服务已启动，接下来请检查新节点的网络连接是否畅通，并确认防火墙是否已关闭。网络不通或防火墙设置可能会阻止节点与集群的其他节点通信。
- 第 3 步，使用 taos -h fqdn 命令尝试连接到新节点，然后执行 show dnodes 命令。这将显示新节点作为独立集群的运行状态。如果显示的列表与主节点上显示的不一致，说明新节点可能已自行组成一个单节点集群。要解决这个问题，请按照以下步骤操作。首先，停止新节点上的 taosd 服务。其次，清空新节点上 taos.cfg 配置文件中指定的 dataDir 目录下的所有文件。这将删除与该节点相关的所有数据和配置信息。最后，重新启动新节点上的 taosd 服务。这将使新节点恢复到初始状态，并准备好重新加入主集群。

### 部署 taosAdapter

本节讲述如何部署 taosAdapter，taosAdapter 为 TDengine 集群提供 RESTful 和 WebSocket 接入能力，因而在集群中扮演着很重要的角色。

1. 安装

TDengine Enterprise 安装完成后，即可使用 taosAdapter。如果想在不同的服务器上分别部署 taosAdapter，需要在这些服务器上都安装 TDengine Enterprise。

2. 单一实例部署

部署 taosAdapter 的单一实例非常简单，具体命令和配置参数请参考手册中 taosAdapter 部分。

3. 多实例部署

部署 taosAdapter 的多个实例的主要目的如下：
- 提升集群的吞吐量，避免 taosAdapter 成为系统瓶颈。
- 提升集群的健壮性和高可用能力，当有一个实例因某种故障而不再提供服务时，可以将进入业务系统的请求自动路由到其他实例。

在部署 taosAdapter 的多个实例时，需要解决负载均衡问题，以避免某个节点过载而其他节点闲置。在部署过程中，需要分别部署多个单一实例，每个实例的部署步骤与部署单一实例完全相同。接下来关键的部分是配置 Nginx。以下是一个经过验证的较佳实践配置，你只须将其中的 endpoint 替换为实际环境中的正确地址即可。关于各参数的含义，请参考 Nginx 的官方文档。

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

### 部署 taosKeeper

如果要想使用 TDegnine 的监控功能，taosKeeper 是一个必要的组件，关于监控请参考[TDinsight](../../reference/components/tdinsight)，关于部署 taosKeeper 的细节请参考[taosKeeper参考手册](../../reference/components/taoskeeper)。

### 部署 taosX

如果想使用 TDengine 的数据接入能力，需要部署 taosX 服务，关于它的详细说明和部署请参考 TDengine 企业版有关文档。

### 部署 taosX-Agent

有些数据源如 Pi, OPC 等，因为网络条件和数据源访问的限制，taosX 无法直接访问数据源，这种情况下需要部署一个代理服务 taosX-Agent，关于它的详细说明和部署请参考 TDengine 企业版有关文档。

### 部署 taos-Explorer

TDengine 提供了可视化管理 TDengine 集群的能力，要想使用图形化界面需要部署 taos-Explorer 服务，关于它的详细说明和部署请参考[taos-Explorer 参考手册](../../reference/components/explorer)


## Docker 部署

本节将介绍如何在 Docker 容器中启动 TDengine 服务并对其进行访问。你可以在 docker run 命令行或者 docker-compose 文件中使用环境变量来控制容器中服务的行为。

### 启动 TDengine

TDengine 镜像启动时默认激活 HTTP 服务，使用下列命令便可创建一个带有 HTTP 服务的容器化 TDengine 环境。
```shell
docker run -d --name tdengine \
-v ~/data/taos/dnode/data:/var/lib/taos \
-v ~/data/taos/dnode/log:/var/log/taos \
-p 6041:6041 tdengine/tdengine
```

详细的参数说明如下。
- /var/lib/taos：TDengine 默认数据文件目录，可通过配置文件修改位置。
- /var/log/taos：TDengine 默认日志文件目录，可通过配置文件修改位置。

以上命令启动了一个名为 tdengine 的容器，并把其中的 HTTP 服务的端口 6041 映射到主机端口 6041。如下命令可以验证该容器中提供的 HTTP 服务是否可用。

```shell
curl -u root:taosdata -d "show databases" localhost:6041/rest/sql
```

运行如下命令可在容器中访问 TDengine。
```shell
$ docker exec -it tdengine taos

taos> show databases;
              name              |
=================================
 information_schema             |
 performance_schema             |
Query OK, 2 rows in database (0.033802s)
```

在容器中，TDengine CLI 或者各种连接器（例如 JDBC-JNI）与服务器通过容器的 hostname 建立连接。从容器外访问容器内的 TDengine 比较复杂，通过 RESTful/WebSocket 连接方式是最简单的方法。

### 在 host 网络模式下启动 TDengine

运行以下命令可以在 host 网络模式下启动 TDengine，这样可以使用主机的 FQDN 建立连接，而不是使用容器的 hostname。
```shell
docker run -d --name tdengine --network host tdengine/tdengine
```

这种方式与在主机上使用 systemctl 命令启动 TDengine 的效果相同。在主机上已安装 TDengine 客户端的情况下，可以直接使用下面的命令访问 TDengine 服务。
```shell
$ taos

taos> show dnodes;
     id      |            endpoint            | vnodes | support_vnodes |   status   |       create_time       |              note              |
=================================================================================================================================================
           1 | vm98:6030                      |      0 |             32 | ready      | 2022-08-19 14:50:05.337 |                                |
Query OK, 1 rows in database (0.010654s)
```

### 以指定的 hostname 和 port 启动 TDengine

使用如下命令可以利用 TAOS_FQDN 环境变量或者 taos.cfg 中的 fqdn 配置项使TDengine 在指定的 hostname 上建立连接。这种方式为部署 TDengine 提供了更大的灵活性。

```shell
docker run -d \
   --name tdengine \
   -e TAOS_FQDN=tdengine \
   -p 6030:6030 \
   -p 6041-6049:6041-6049 \
   -p 6041-6049:6041-6049/udp \
   tdengine/tdengine
```

首先，上面的命令在容器中启动一个 TDengine 服务，其所监听的 hostname 为tdengine，并将容器的端口 6030 映射到主机的端口 6030，将容器的端口段 [6041, 6049] 映射到主机的端口段 [6041, 6049]。如果主机上该端口段已经被占用，可以修改上述命令以指定一个主机上空闲的端口段。

其次，要确保 tdengine 这个 hostname 在 /etc/hosts 中可解析。通过如下命令可将正确的配置信息保存到 hosts 文件中。
```shell
echo 127.0.0.1 tdengine |sudo tee -a /etc/hosts
```

最后，可以通过 TDengine CLI 以 tdengine 为服务器地址访问 TDengine 服务，命令如下。
```shell
taos -h tdengine -P 6030
```

如果 TAOS_FQDN 被设置为与所在主机名相同，则效果与“在 host 网络模式下启动TDengine”相同。

## Kubernetes 部署

作为面向云原生架构设计的时序数据库，TDengine 本身就支持 Kubernetes 部署。这里介绍如何使用 YAML 文件从头一步一步创建一个可用于生产使用的高可用 TDengine 集群，并重点介绍 Kubernetes 环境下 TDengine 的常用操作。本小节要求读者对 Kubernetes 有一定的了解，可以熟练运行常见的 kubectl 命令，了解 statefulset、service、pvc 等概念，对这些概念不熟悉的读者，可以先参考 Kubernetes 的官网进行学习。
为了满足高可用的需求，集群需要满足如下要求：
- 3 个及以上 dnode ：TDengine 的同一个 vgroup 中的多个 vnode ，不允许同时分布在一个 dnode ，所以如果创建 3 副本的数据库，则 dnode 数大于等于 3
- 3 个 mnode ：mnode 负责整个集群的管理工作，TDengine 默认是一个 mnode。如果这个 mnode 所在的 dnode 掉线，则整个集群不可用。
- 数据库的 3 副本：TDengine 的副本配置是数据库级别，所以数据库 3 副本可满足在 3 个 dnode 的集群中，任意一个 dnode 下线，都不影响集群的正常使用。如果下线 dnode 个数为 2 时，此时集群不可用，因为 RAFT 无法完成选举。（企业版：在灾难恢复场景，任一节点数据文件损坏，都可以通过重新拉起 dnode 进行恢复）

### 前置条件

要使用 Kubernetes 部署管理 TDengine 集群，需要做好如下准备工作。
- 本文适用 Kubernetes v1.19 以上版本
- 本文使用 kubectl 工具进行安装部署，请提前安装好相应软件
- Kubernetes 已经安装部署并能正常访问使用或更新必要的容器仓库或其他服务

### 配置 Service 服务

创建一个 Service 配置文件：taosd-service.yaml，服务名称 metadata.name （此处为 "taosd"） 将在下一步中使用到。首先添加 TDengine 所用到的端口，然后在选择器设置确定的标签 app （此处为 “tdengine”）。

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

根据 Kubernetes 对各类部署的说明，我们将使用 StatefulSet 作为 TDengine 的部署资源类型。 创建文件 tdengine.yaml，其中 replicas 定义集群节点的数量为 3。节点时区为中国（Asia/Shanghai），每个节点分配 5G 标准（standard）存储，你也可以根据实际情况进行相应修改。

请特别注意 startupProbe 的配置，在 dnode 的 Pod 掉线一段时间后，再重新启动，这个时候新上线的 dnode 会短暂不可用。如果 startupProbe 配置过小，Kubernetes 会认为该 Pod 处于不正常的状态，并尝试重启该 Pod，该 dnode 的 Pod 会频繁重启，始终无法恢复到正常状态。

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
- TDengine 的灾难恢复，在 TDengine Enterprise 中，本身具备了当一个 dnode 永久下线（物理机磁盘损坏，数据分拣丢失）后，重新拉起一个空白的 dnode 来恢复原 dnode 的工作。

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
