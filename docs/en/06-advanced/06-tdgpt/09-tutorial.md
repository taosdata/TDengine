---
title: Quick Start Guide
sidebar_label: Quick Start Guide
---

## Get Started with Docker

This document describes how to get started with TDgpt in Docker.

### Start TDgpt

If you have installed Docker, pull the latest TDengine container:

```shell
docker pull tdengine/tdengine:latest
```

You can specify a version if desired:

```shell
docker pull tdengine/tdengine:3.3.3.0
```

Then run the following command:

```shell
docker run -d -p 6030:6030 -p 6041:6041 -p 6043:6043 -p 6044-6049:6044-6049 -p 6044-6045:6044-6045/udp -p 6060:6060 tdengine/tdengine
```

Note: TDgpt runs on TCP port 6090. TDgpt is a stateless analytics agent and does not persist data. It only saves log files to local disk

Confirm that your Docker container is running:

```shell
docker ps
```

Enter the container and run the bash shell:

```shell
docker exec -it <container name> bash
```

You can now run Linux commands and access TDengine.

## Get Started with an Installation Package

### Obtain the Package

1. Download the tar.gz package from the list:
2. Open the directory containing the downloaded package and decompress it.
3. Open the directory containing the decompressed package and run the `install.sh` script.

Note: Replace `<version>` with the version that you downloaded.

```bash
tar -zxvf TDengine-anode-<version>-Linux-x64.tar.gz
```

Decompress the file, open the directory created, and run the `install.sh` script:

```bash
sudo ./install.sh
```

### Deploy TDgpt

See [Installing TDgpt](../management/) to prepare your environment and deploy TDgpt.

## Get Started in TDengine Cloud

You can use TDgpt with your TDengine Cloud deployment. Register for a TDengine Cloud account, ensure that you have at least one instance, and register TDgpt to your TDengine Cloud instance as described in the documentation. See the TDengine Cloud documentation for more information.

Create a TDgpt instance, and then refer to [Installing TDgpt](../management/) to manage your anode.

