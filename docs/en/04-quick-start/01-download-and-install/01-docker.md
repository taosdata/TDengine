---
sidebar_label: Deploy in Docker
title: Get Started with TDengine TSDB Using Docker
description: Quickly experience TDengine's efficient insertion and querying using Docker
---

import Getstarted from "./resource/_get_started.mdx";

This page shows how to start TDengine TSDB Enterprise with Docker and complete a basic walkthrough: start the service, open the CLI, write data, and run queries. If you prefer not to use Docker, see [Get Started with TDengine TSDB Using an Installation Package](./02-package.md). If you want to contribute code or explore internals, see the [TDengine GitHub repository](https://github.com/taosdata/TDengine).

:::note

Starting with version 3.3.7.0, TDengine TSDB image names are as follows:

- Community Edition: `tdengine/tdengine` was renamed to `tdengine/tsdb`
- Enterprise Edition: `tdengine/tdengine-ee` was renamed to `tdengine/tsdb-ee`

:::

## Before You Begin

1. Docker is installed on your machine, and your user can run the `docker` command.
2. Your machine can reach Docker Hub, or you have obtained an offline image from the TDengine product download center.
3. Ensure that the network ports required by TDengine TSDB are not currently in use. For more information, see [Network Port Requirements](../../12-operations-and-tooling/02-operations/01-planning.md#network-port-requirements).

## Start the Service

### Pull the Image

Pull the latest Enterprise Edition image:

```shell
docker pull tdengine/tsdb-ee:latest
```

You can also pull a specific version. For example:

```bash tsdb-ee
docker pull tdengine/tsdb-ee:{{VERSION}}
```

If you cannot access Docker Hub directly, go to the [Docker image download page](https://tdengine.com/downloads/?product=TDengine+TSDB-Enterprise&platform=Docker) in the TDengine product download center, download the offline image, load it as described on that page, and update the image name and tag.

### Start the Container

Run the following command to start the TDengine container:

```shell
docker run -d \
  -v ~/data/taos/dnode/data:/var/lib/taos \
  -v ~/data/taos/dnode/log:/var/log/taos \
  -p 6030:6030 -p 6041:6041 -p 6043:6043 -p 6060:6060 \
  -p 6044-6049:6044-6049 \
  -p 6044-6045:6044-6045/udp \
  -p 6050:6050 -p 6055:6055 \
  --name tdengine-tsdb \
  tdengine/tsdb-ee
```

For port usage of TDengine services, see [Network Port Requirements](../../12-operations-and-tooling/02-operations/01-planning.md#network-port-requirements) in the operations guide.

### Check Container Status

Run the following command to check the container status:

```shell
docker ps -f name=tdengine-tsdb
```

Check the `STATUS` field in the output. If it shows `Up ... (healthy)`, the container has started and is running normally.

### Enter the Container

Run the following command to enter the container:

```shell
docker exec -it tdengine-tsdb bash
```

Inside the container, you can run Linux commands and try TDengine with tools such as `taos` and `taosBenchmark`.

For more details on deploying TDengine with Docker, see [Docker Deployment](../../12-operations-and-tooling/02-operations/03-deployment/02-docker.md).

<Getstarted/>
