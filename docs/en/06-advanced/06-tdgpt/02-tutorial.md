---
title: Installation
sidebar_label: Installation
---

import PkgListV3 from "/components/PkgListV3";

This section describes how to use TDgpt in Docker

## Get Started with Docker

### Image Versions

| Image                          | Models               |
|-----------------------------------|-----------------------|
| `tdengine/tdgpt`         | TDtsfm v1.0       |
| `tdengine/tdgpt-full`    | TDtsfm v1.0 and Time-MoE   |

### Quick Start Guide

Install Docker on your local machine. Then pull the image and startthe container as described in the following section.

#### Standard Image

Pull the latest TDgpt image:

```shell
docker pull tdengine/tdgpt:latest
```

You can specify a version if desired:

```shell
docker pull tdengine/tdgpt:3.3.7.0
```

Start the container:

```shell
docker run -d \
  -p 6035:6035 \
  -p 6036:6036 \
  tdengine/tdgpt:3.3.7.0
```

:::note

From 3.3.7.5, the port number for TDgpt has changed from 6090 to 6035.

:::

#### Full Image

Pull the latest TDgpt image:

```shell
docker pull tdengine/tdgpt-full:latest
```

You can specify a version if desired:

```shell
docker pull tdengine/tdgpt-full:3.3.7.0
```

Start the container:

```shell
docker run -d -p 6035:6035 -p 6036:6036 -p 6037:6037 tdengine/tdgpt-full:3.3.7.0
```

Note: TDgpt runs on TCP port 6035. The standard image also uses port 6036, and the full image uses port 6037.

TDgpt is a stateless analytics agent and does not persist data. It only saves log files to local disk.

Confirm that your Docker container is running:

```shell
docker ps
```

Enter the container and run the bash shell:

```shell
docker exec -it <container name> bash
```

You can now run Linux commands and access TDengine.

## Use TDgpt in TDengine Cloud

You can try TDgpt with a free TDengine Cloud account. In TDengine Cloud, open **DB Mart** and enable access to the **Time Series Prediction Analysis Dataset** database. You can run TDgpt queries on the data contained in that database, for example: `select forecast(val, 'algo=tdtsfm_1') from forecast.electricity_demand;`

## Install TDgpt Locally

### Preparing Your Environment

To use the analytics capabilities offered by TDgpt, you deploy an AI node (anode) in your TDengine cluster. You must deploy the anode on a Linux machine. The environment must meet the following requirements:

- Python: 3.10 or 3.11. 3.12 is not supported for now due to library conflict.
- TDengine: 3.3.6.0 or later
- C compiler: Because uWSGI is compiled during the TDgpt installation process, your environment must contain a C compiler.

You can run the following commands to install Python 3.10 in Ubuntu.. If you already have a supported version of Python installed, skip this section.

#### Install Python

```shell
sudo apt-get install software-properties-common
sudo add-apt-repository ppa:deadsnakes/ppa
sudo apt update
sudo apt install python3.10
sudo update-alternatives --install /usr/bin/python3 python3 /usr/bin/python3.10 2
sudo update-alternatives --config python3
sudo apt install python3.10-venv
sudo apt install python3.10-dev
```

#### Install PiPy

```shell
curl -sS https://bootstrap.pypa.io/get-pip.py | python3.10
```

Add `~/.local/bin` to the `PATH` environment variable in `~/.bashrc` or `~/.bash_profile`..

```shell
export PATH=$PATH:~/.local/bin
```

The Python environment has been installed. You can now install TDgpt.

#### Install a C Compiler

```shell
sudo apt update
sudo apt install build-essential
```

### Obtain the Package

1. Download the tar.gz package from the list:

   <PkgListV3 type={9}/>

   This package contains the TDtsfm and Time-MoE foundation models for time series. Ensure that you have 16 GB of disk space available to store the models.
  
2. Open the directory containing the downloaded package and decompress it.

Note: Replace `<version>` with the version that you downloaded.

```bash
tar -zxvf TDengine-TDgpt-<version>-Linux-x64.tar.gz
```

### Run the Installation Script

Decompress the file, open the directory created, and run the `install.sh` script:
Note: Replace `<version` with the version that you downloaded.

```bash
cd TDengine-TDgpt-<version>
./install.sh
```

To prevent TDgpt from affecting Python environments that may exist on your machine, anodes are installed in a virtual environment. When you install an anode, a virtual Python environment is deployed in the `/var/lib/taos/taosanode/venv/` directory. All libraries required by the anode are installed in this directory.

Note that this virtual environment is not uninstalled automatically by the `rmtaosanode` command. If you are sure that you do not want to use TDgpt on a machine, you can remove the directory manually.

### Activate the Virtual Environment

The virtual Python environment for TDgpt is located in the `/var/lib/taos/taosanode/venv/` directory. Once the environment is created, PiPy is used to install the Python dependencies for TDgpt.

This environment is not removed y the `rmtaosanode` command. You can remove it manually if desired.

Any algorithms or models that you create for TDgpt must be installed into this virtual environment using Pip.

### Uninstalling TDgpt

You can run the `rmtaosanode` command to uninstall TDgpt.
