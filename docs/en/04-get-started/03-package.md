---
sidebar_label: Deploy from Package
title: Get Started with TDengine TSDB Using an Installation Package
slug: /get-started/deploy-from-package
---

import Tabs from "@theme/Tabs";
import TabItem from "@theme/TabItem";
import PkgListV37 from "/components/PkgListV37";
import Getstarted from './_get_started.mdx';

You can install TDengine TSDB Enterprise on Linux and Windows. To install TDengine TSDB in a Docker container instead of on your machine, see [Get Started with TDengine in Docker](../deploy-in-docker/).

## Before You Begin

- Verify that your machine meets the minimum system requirements for TDengine TSDB. For more information, see [Supported Platforms](../../tdengine-reference/supported-platforms/) and [System Requirements](../../operations-and-maintenance/system-requirements/).
- **(Windows only)** Verify that the latest version of the Microsoft Visual C++ Redistributable is installed on your machine. To download the redistributable package, see [Microsoft Visual C++ Redistributable latest supported downloads](https://learn.microsoft.com/en-us/cpp/windows/latest-supported-vc-redist?view=msvc-170).

## Procedure

<Tabs>
<TabItem label="Linux" value="linux">

1. Download the tar.gz installation package from the list below:  
   <PkgListV37 productName="TDengine TSDB-Enterprise" version="3.3.8.4" platform="Linux-Generic" pkgType="Server" />
2. Navigate to the directory where the package is located and extract it using `tar`. For example, on an x64 architecture:  
   ```bash
   tar -zxvf tdengine-tsdb-enterprise-3.3.8.4-linux-x64.tar.gz
   ```
3. After extracting the files, go into the subdirectory and run the `install.sh` script:  
   ```bash
   sudo ./install.sh
   ```
</TabItem>

<TabItem label="Windows" value="windows">

1. Download the Windows installation package from the list below:  
   <PkgListV37 productName="TDengine TSDB-Enterprise" version="3.3.8.4" platform="Windows" pkgType="Server" />
2. Run the installation package and follow the on-screen instructions to complete the installation of TDengine TSDB.

</TabItem>
</Tabs>

For more package types and versions, visit the [TDengine Download Center](https://tdengine.com/downloads/?product=TDengine+TSDB-Enterprise).

## Start the Service

<Tabs>
<TabItem label="Linux" value="linux">

After installation, execute the following command in your terminal to start all services:

```bash
start-all.sh
```

All TDengine TSDB components are managed by systemd. You can check their service status with the following commands:

```bash
sudo systemctl status taosd
sudo systemctl status taosadapter
sudo systemctl status taoskeeper
sudo systemctl status taos-explorer
```

If the output shows the status as `Active: active (running) since ...`, it means the services have started successfully.

</TabItem>

<TabItem label="Windows System" value="windows">

After installation, open a terminal as administrator and run the following command to start all services:

```cmd
C:\TDengine\start-all.bat
```

You can check the status of each service using:

```cmd
sc query taosd
sc query taosadapter
sc query taoskeeper
sc query taos-explorer
```

If the output shows `RUNNING`, it means the services have started successfully.

</TabItem>
</Tabs>

<Getstarted />