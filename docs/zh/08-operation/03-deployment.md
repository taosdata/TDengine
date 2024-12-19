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

如果想使用 TDengine 的数据接入能力，需要部署 taosX 服务，关于它的详细说明和部署请参考企业版参考手册。

### 部署 taosX-Agent

有些数据源如 Pi, OPC 等，因为网络条件和数据源访问的限制，taosX 无法直接访问数据源，这种情况下需要部署一个代理服务 taosX-Agent，关于它的详细说明和部署请参考企业版参考手册。

### 部署 taos-Explorer

TDengine 提供了可视化管理 TDengine 集群的能力，要想使用图形化界面需要部署 taos-Explorer 服务，关于它的详细说明和部署请参考[taos-Explorer 参考手册](../../reference/components/explorer)

