---
sidebar_label: Deploy from Package
title: Get Started with TDengine Using an Installation Package
description: Quick experience with TDengine using the installation package
slug: /get-started/deploy-from-package
---

import Tabs from "@theme/Tabs";
import TabItem from "@theme/TabItem";
import PkgListV3 from "/components/PkgListV3";

You can install TDengine on a local machine and perform some basic tests to verify its performance. The TDengine OSS server can be installed on Linux and macOS, and the TDengine OSS client can be installed on Linux, macOS, and Windows.

To install TDengine in a Docker container instead of on your machine, see [Get Started with TDengine in Docker](../deploy-in-docker/).

## Before You Begin

- Verify that your machine meets the minimum system requirements for TDengine. For more information, see [Supported Platforms](../../tdengine-reference/supported-platforms/) and [System Requirements](../../operations-and-maintenance/system-requirements/).
- **(Windows only)** Verify that the latest version of the Microsoft Visual C++ Redistributable is installed on your machine. To download the redistributable package, see [Microsoft Visual C++ Redistributable latest supported downloads](https://learn.microsoft.com/en-us/cpp/windows/latest-supported-vc-redist?view=msvc-170).

## Procedure

The TDengine OSS installation package is provided for Linux users in .deb, .rpm, and .tar.gz format and can also be installed via APT from our repository. Installation packages are also provided for macOS (client and server) and Windows (client only).

1. Select the appropriate package for your machine and follow the steps to install TDengine.

   <Tabs>
   <TabItem label=".deb" value="debinst">

   1. Download the .deb installation package:
      <PkgListV3 type={6}/>
   2. Run the following command to install TDengine:

      ```bash
      sudo dpkg -i TDengine-server-<version>-Linux-x64.deb
      ```

      Replace `<version>` with the version of the package that you downloaded.

   </TabItem>

   <TabItem label=".rpm" value="rpminst">

   1. Download the .rpm installation package:
      <PkgListV3 type={5}/>
   2. Run the following command to install TDengine:

      ```bash
      sudo rpm -ivh TDengine-server-<version>-Linux-x64.rpm
      ```

      Replace `<version>` with the version of the package that you downloaded.

   </TabItem>

   <TabItem label=".tar.gz" value="tarinst">

   1. Download the desired .tar.gz package from the following list:
      <PkgListV3 type={0}/>
   2. Run the following command to decompress the package:

      ```bash
      tar -zxvf TDengine-server-<version>-Linux-x64.tar.gz
      ```

      Replace `<version>` with the version of the package that you downloaded.
   3. In the directory where you decompressed the package, run the following command to install TDengine:

      ```bash
      sudo ./install.sh
      ```

      :::note

      The `install.sh` script requires you to enter configuration information in the terminal. For a non-interactive installation, run `./install.sh -e no`. You can run `./install.sh -h` for detailed information about all parameters.

      :::

   </TabItem>

   <TabItem label="APT" value="apt-get">

   1. Configure the package repository:

      ```bash
      wget -qO - http://repos.taosdata.com/tdengine.key | sudo apt-key add -
      echo "deb [arch=amd64] http://repos.taosdata.com/tdengine-stable stable main" | sudo tee /etc/apt/sources.list.d/tdengine-stable.list
      ```

   2. Update the list of available packages and install TDengine.

      ```bash
      sudo apt-get update
      apt-cache policy tdengine
      sudo apt-get install tdengine
      ```

   </TabItem>

   <TabItem label="Windows" value="windows">

   :::note

   This procedure installs the TDengine OSS client on Windows. The TDengine OSS server does not support Windows.

   :::

   1. Download the Windows installation package:
      <PkgListV3 type={3}/>
   2. Run the installation package to install TDengine.

   </TabItem>

   <TabItem label="macOS" value="macos">

   1. Download the desired installation package from the following list:
      <PkgListV3 type={7}/>
   2. Run the installation package to install TDengine.

      :::note

      If the installation is blocked, right-click on the package and choose **Open**.

      :::

   </TabItem>
   </Tabs>

2. When installing the first node and prompted with `Enter FQDN:`, you do not need to input anything. Only when installing the second or subsequent nodes do you need to input the FQDN of any available node in the existing cluster to join the new node to the cluster. Alternatively, you can configure it in the new node's configuration file before starting.

3. Select your operating system and follow the steps to start TDengine services.

   <Tabs>
   <TabItem label="Linux" value="linux">

   Run the following command to start all TDengine services:

   ```bash
   sudo start-all.sh 
   ```

   Alternatively, you can manage specific TDengine services through systemd:

   ```bash
   sudo systemctl start taosd
   sudo systemctl start taosadapter
   sudo systemctl start taoskeeper
   sudo systemctl start taos-explorer
   ```

   :::note

   If your machine does not support systemd, you can manually run the TDengine services located in the `/usr/local/taos/bin` directory.

   :::

   </TabItem>

   <TabItem label="macOS" value="macos">

   Run the following command to start all TDengine services:

   ```bash
   sudo start-all.sh
   ```

   Alternatively, you can manage specific TDengine services with the `launchctl` command:

   ```bash
   sudo launchctl start com.tdengine.taosd
   sudo launchctl start com.tdengine.taosadapter
   sudo launchctl start com.tdengine.taoskeeper
   sudo launchctl start com.tdengine.taos-explorer
   ```

   </TabItem>
   </Tabs>

   You can now work with TDengine on your local machine. For example, you can run the `taos` command to open the TDengine command-line interface.

## What to Do Next

### Test Data Ingestion

Your TDengine installation includes taosBenchmark, a tool specifically designed to test TDengine's performance. taosBenchmark can simulate data generated by many devices with a wide range of configuration options so that you can perform tests on sample data similar to your real-world use cases. For more information about taosBenchmark, see [taosBenchmark](../../tdengine-reference/tools/taosbenchmark/).

Perform the following steps to use taosBenchmark to test TDengine's ingestion performance on your machine:

1. Run taosBenchmark with the default settings:

   ```bash
   taosBenchmark -y
   ```

taosBenchmark automatically creates the `test` database and the `meters` supertable inside that database. This supertable contains 10,000 subtables, named `d0` to `d9999`, with each subtable containing 10,000 records. Each record includes the following four metrics:

- `ts` (timestamp), ranging from `2017-07-14 10:40:00 000" to "2017-07-14 10:40:09 999`
- `current`
- `voltage`
- `phase`

Each subtable also has the following two tags:

- `groupId`, ranging from `1` to `10`
- `location`, indicating a city and state such as `California.Campbell` or `California.Cupertino`

When the ingestion process is finished, taosBenchmark outputs the time taken to ingest the specified sample data. From this, you can estimate how TDengine would perform on your system in a production environment.

### Test Data Querying

After inserting data with taosBenchmark as described above, you can use the TDengine CLI to test TDengine's query performance on your machine:

1. Start the TDengine CLI:

   ```bash
   taos
   ```

2. Query the total number of records in the `meters` supertable:

   ```sql
   SELECT COUNT(*) FROM test.meters;
   ```

3. Query the average, maximum, and minimum values of 100 million records:

   ```sql
   SELECT AVG(current), MAX(voltage), MIN(phase) FROM test.meters;
   ```

4. Query the total number of records where the value of the `location` tag is `California.SanFrancisco`:

   ```sql
   SELECT COUNT(*) FROM test.meters WHERE location = "California.SanFrancisco";
   ```

5. Query the average, maximum, and minimum values of all records where the value of the `groupId` tag is `10`:

   ```sql
   SELECT AVG(current), MAX(voltage), MIN(phase) FROM test.meters WHERE groupId = 10;
   ```

6. Calculate the average, maximum, and minimum values for the `d1001` table every 10 seconds:

   ```sql
   SELECT _wstart, AVG(current), MAX(voltage), MIN(phase) FROM test.d1001 INTERVAL(10s);
   ```
