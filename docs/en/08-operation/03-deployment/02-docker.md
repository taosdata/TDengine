---
title: Docker Deployment
slug: /operations-and-maintenance/deploy-your-cluster/docker-deployment
---

You can deploy TDengine services in Docker containers and use environment variables in the docker run command line or docker-compose file to control the behavior of services in the container.

## Starting TDengine

The TDengine image is launched with HTTP service activated by default. Use the following command to create a containerized TDengine environment with HTTP service.

```shell
docker run -d --name tdengine \
-v ~/data/taos/dnode/data:/var/lib/taos \
-v ~/data/taos/dnode/log:/var/log/taos \
-p 6041:6041 tdengine/tsdb-ee
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

## Starting TDengine in host network mode

Run the following command to start TDengine in host network mode, which allows using the host's FQDN to establish connections, rather than using the container's hostname.

```shell
docker run -d --name tdengine --network host tdengine/tsdb-ee
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

## Start TDengine with a specified hostname and port

:::note

- After version `v3.3.6.0`, the default `fqdn` has changed from `buildkitsandbox` to `localhost`. If it is a fresh start, there will be no issues. However, if it is an upgrade start, when running the container, you need to specify the previous `fqdn` with `-e TAOS_FQDN=<old_value>` and `-h <old_value>`, otherwise it may fail to start.

:::

Use the following command to establish a connection on a specified hostname using the TAOS_FQDN environment variable or the fqdn configuration item in taos.cfg. This method provides greater flexibility for deploying TDengine.

```shell
docker run -d \
   --name tdengine \
   -e TAOS_FQDN=tdengine \
   -p 6030:6030 \
   -p 6041-6049:6041-6049 \
   -p 6041-6049:6041-6049/udp \
   tdengine/tsdb-ee
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

## Launching the Cluster with Docker Compose

Use the following Docker Compose configuration file to bring up a 3-node TDengine TSDB cluster.
Contents of docker-compose.yaml:

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

The environment variable TAOS_FIRST_EP specifies the endpoint of the first dnode to connect to in the cluster, equivalent to the firstEp parameter in /etc/taos/taos.cfg.
Start the cluster:

```shell
docker compose up
```

After the cluster is up, enter any node, for example, the td1 node:

```shell
docker compose exec td1 bash
```

And execute the following command to check the cluster status:

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
