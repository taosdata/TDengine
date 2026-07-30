---
title: Docker 部署
---

本节将介绍如何在 Docker 容器中启动 TDengine 服务并对其进行访问。你可以在 docker run 命令行或者 docker-compose 文件中使用环境变量来控制容器中服务的行为。

## 自定义密码、升级与健康检查{#custom-passwords-upgrades-and-health-checks}

如果使用了自定义 root 密码，请注意 Docker 镜像在不同版本阶段的行为差异：

- 对于 `3.3.6.6-3.3.8.4` 版本，如果是在旧版本中修改过 root 密码，需要在 `data` 目录（默认 `/var/lib/taos`）下 `touch` 一个空文件 `.docker-entrypoint-root-password-changed` 后再启动容器。
- 对于 `3.3.8.8` 及以上版本，可以通过 `TAOS_ROOT_PASSWORD` 或 `TAOS_ROOT_PASSWORD_FILE` 提供当前 root 密码，镜像也可以直接升级；但如果此前已经修改过 root 密码，则在升级、重启容器或重建 Pod 之前，仍需确保部署配置中提供的是当前实际密码。
- 对于 `3.4.1.0` 及以上版本，`taos-check startup` 和 `taos-check service` 可用于健康检查，其中 `taos-check service` 会复用上述密码来源。如果密码没有同步更新，健康检查以及其他使用 root 账号鉴权的组件可能会失败。

后文涉及 hostname、docker compose 以及 Kubernetes 探针时，不再重复展开这些版本差异；凡是涉及 root 密码、升级或 `taos-check` 行为的场景，均以上述说明为准。

## 启动 TDengine

TDengine 镜像启动时默认激活 HTTP 服务，使用下列命令便可创建一个带有 HTTP 服务的容器化 TDengine 环境。

```shell
docker run -d --name tdengine \
-v ~/data/taos/dnode/data:/var/lib/taos \
-v ~/data/taos/dnode/log:/var/log/taos \
-p 6041:6041 tdengine/tsdb-ee
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

在容器中，`taos` shell 或者各种连接器（例如 JDBC-JNI）与服务器通过容器的 hostname 建立连接。从容器外访问容器内的 TDengine 比较复杂，通过 RESTful/WebSocket 连接方式是最简单的方法。

## 在 host 网络模式下启动 TDengine

运行以下命令可以在 host 网络模式下启动 TDengine，这样可以使用主机的 FQDN 建立连接，而不是使用容器的 hostname。

```shell
docker run -d --name tdengine --network host tdengine/tsdb-ee
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

## 以指定的 hostname 和 port 启动 TDengine

:::note

- `v3.3.6.0` 版本后，默认的 `fqdn` 从 `buildkitsandbox` 变更为 `localhost`，如果是全新启动不会有任何问题，如果是升级启动，运行容器时需要将 `-e TAOS_FQDN=<old_value>` 和 `-h <old_value>` 指定为之前的 `fqdn`，否则可能会无法启动。
- 如果涉及 root 密码变更、镜像升级或 `taos-check` 的版本差异，请以前文“Docker 镜像在不同版本阶段的行为差异”中的说明为准。
  
:::

使用如下命令可以利用 TAOS_FQDN 环境变量或者 taos.cfg 中的 fqdn 配置项使 TDengine 在指定的 hostname 上建立连接。这种方式为部署 TDengine 提供了更大的灵活性。

```shell
docker run -d \
   --name tdengine \
   -e TAOS_FQDN=tdengine \
   -p 6030:6030 \
   -p 6041-6049:6041-6049 \
   -p 6041-6049:6041-6049/udp \
   tdengine/tsdb-ee
```

首先，上面的命令在容器中启动一个 TDengine 服务，其所监听的 hostname 为 tdengine，并将容器的端口 6030 映射到主机的端口 6030，将容器的端口段 [6041, 6049] 映射到主机的端口段 [6041, 6049]。如果主机上该端口段已经被占用，可以修改上述命令以指定一个主机上空闲的端口段。

其次，要确保 tdengine 这个 hostname 在 /etc/hosts 中可解析。通过如下命令可将正确的配置信息保存到 hosts 文件中。

```shell
echo 127.0.0.1 tdengine |sudo tee -a /etc/hosts
```

最后，可以通过 `taos` shell 以 tdengine 为服务器地址访问 TDengine 服务，命令如下。

```shell
taos -h tdengine -P 6030
```

如果 TAOS_FQDN 被设置为与所在主机名相同，则效果与“在 host 网络模式下启动 TDengine”相同。

## 使用 docker compose 方式启动集群

使用如下 docker compose 配置文件，可以启动一个 3 节点 TDengine 集群。
docker-compose.yaml 内容如下：

```yaml
services:
  td1:
    image: tdengine/tsdb-ee
    environment:
      - TAOS_FQDN=td1
 
  td2:
    image: tdengine/tsdb-ee
    environment:
      - TAOS_FQDN=td2
      - TAOS_FIRST_EP=td1:6030

  td3:
    image: tdengine/tsdb-ee
    environment:
      - TAOS_FQDN=td3
      - TAOS_FIRST_EP=td1:6030
```

配置中的环境变量 TAOS_FIRST_EP 用于主动连接的集群中首个 dnode 的 endpoint，效果与 /etc/taos/taos.cfg 中的 firstEp 参数一致。
如果集群使用了自定义 root 密码，请在每个服务中同步配置对应的密码环境变量，并确保其与数据库实际密码保持一致；版本差异与升级要求仍以前文说明为准。
启动集群：

```shell
docker compose up
```

启动后进入任一节点，比如 td1 节点：

```shell
docker compose exec td1 bash
```

执行如下命令查看集群状态：

```shell
$ taos -s "show dnodes"
Welcome to the TDengine Command Line Interface, Native Client Version:3.3.6.13
Copyright (c) 2025 by TDengine, all rights reserved.

taos> show dnodes
     id      |            endpoint            | vnodes | support_vnodes |    status    |       create_time       |       reboot_time       |              note              |
=============================================================================================================================================================================
           1 | td1:6030                       |      0 |             85 | ready        | 2025-08-21 01:56:41.630 | 2025-08-21 01:56:41.462 |                                |
           2 | td2:6030                       |      1 |             85 | ready        | 2025-08-21 01:56:43.203 | 2025-08-21 01:56:43.453 |                                |
           3 | td3:6030                       |      0 |             85 | ready        | 2025-08-21 01:56:43.296 | 2025-08-21 01:56:43.491 |                                |
Query OK, 3 row(s) in set (0.006355s)
```
