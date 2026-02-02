---
sidebar_label: Deploy in Docker
title: Get Started with TDengine TSDB Using Docker
description: Quickly experience TDengine's efficient insertion and querying using Docker
slug: /get-started/deploy-in-docker
---

import Getstarted from "./_get_started.mdx";

You can install TDengine TSDB in a Docker container and perform some basic tests to verify its performance.

To install TDengine TSDB on your local machine instead of in a container, see [Get Started with TDengine TSDB Using an Installation Package](../deploy-from-package/).

## Before You Begin

- Install Docker. For more information, see the [Docker website](https://www.docker.com/).
- Ensure that the network ports required by TDengine TSDB are not currently in use. For more information, see [Network Port Requirements](../../operations-and-maintenance/system-requirements/#network-port-requirements).

## Procedure

1. Pull the latest TDengine TSDB image:

   ```bash
   docker pull tdengine/tsdb-ee:latest
   ```

   :::note
   You can also pull a specific version of the image. For example:

   ```bash tsdb-ee
   docker pull tdengine/tsdb-ee:{{VERSION}}
   ```

   :::

2. Start a container with the following command:

   ```bash
   docker run -d -p 6030:6030 -p 6041:6041 -p 6043-6060:6043-6060 -p 6043-6060:6043-6060/udp tdengine/tsdb-ee
   ```

   To persist data to your local machine, use the following command:

   ```bash
   docker run -d -v <local-data-directory>:/var/lib/taos -v <local-log-directory>:/var/log/taos -p 6030:6030 -p 6041:6041 -p 6043-6060:6043-6060 -p 6043-6060:6043-6060/udp tdengine/tsdb-ee
   ```

3. Verify that the container is running properly:

   ```bash
   docker ps
   ```

4. Enter the container and open a shell:

   ```bash
   docker exec -it <container-name> bash
   ```

You can now work with TDengine TSDB inside your container. For example, you can run the `taos` command to open the TDengine TSDB command-line interface.

<Getstarted/>