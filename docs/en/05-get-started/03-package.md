---
title: Quick Install from Package
sidebar_label: Package
description: This document describes how to install TDengine on Linux, Windows, and macOS and perform queries and inserts.
---

import Tabs from "@theme/Tabs";
import TabItem from "@theme/TabItem";
import PkgListV3 from "/components/PkgListV3";

This document describes how to install TDengine on Linux/Windows/macOS and perform queries and inserts.

- The easiest way to explore TDengine is through [TDengine Cloud](https://cloud.tdengine.com). 
- To get started with TDengine on Docker, see [Quick Install on Docker](../../get-started/docker).
- If you want to view the source code, build TDengine yourself, or contribute to the project, see the [TDengine GitHub repository](https://github.com/taosdata/TDengine).

The full package of TDengine includes the TDengine Server (`taosd`), TDengine Client (`taosc`), taosAdapter for connecting with third-party systems and providing a RESTful interface, a command-line interface (CLI, taos), and some tools. Note that taosAdapter supports Linux only. In addition to client libraries for multiple languages, TDengine also provides a [REST API](../../reference/rest-api) through [taosAdapter](../../reference/taosadapter).

The standard server installation package includes `taos`, `taosd`, `taosAdapter`, `taosBenchmark`, and sample code. You can also download the Lite package that includes only `taosd` and the C/C++ client library.

TDengine OSS is released as Deb and RPM packages. The Deb package can be installed on Debian, Ubuntu, and derivative systems. The RPM package can be installed on CentOS, RHEL, SUSE, and derivative systems. A .tar.gz package is also provided for enterprise customers, and you can install TDengine over `apt-get` as well. The .tar.tz package includes `taosdump` and the TDinsight installation script. If you want to use these utilities with the Deb or RPM package, download and install taosTools separately. TDengine can also be installed on x64 Windows and x64/m1 macOS.

## Operating environment requirements
In the Linux system, the minimum requirements for the operating environment are as follows:

linux core version - 3.10.0-1160.83.1.el7.x86_64;

glibc version - 2.17;

If compiling and installing through clone source code, it is also necessary to meet the following requirements:

cmake version - 3.26.4 or above;

gcc version - 9.3.1 or above;

## Installation

<Tabs>
<TabItem label=".deb" value="debinst">

1. Download the Deb installation package.
   <PkgListV3 type={6}/>
2. In the directory where the package is located, use `dpkg` to install the package:

> Please replace `<version>` with the corresponding version of the package downloaded

```bash
sudo dpkg -i TDengine-server-<version>-Linux-x64.deb
```

</TabItem>

<TabItem label=".rpm" value="rpminst">

1. Download the .rpm installation package.
   <PkgListV3 type={5}/>
2. In the directory where the package is located, use rpm to install the package:

> Please replace `<version>` with the corresponding version of the package downloaded

```bash
sudo rpm -ivh TDengine-server-<version>-Linux-x64.rpm
```

</TabItem>

<TabItem label=".tar.gz" value="tarinst">

1. Download the .tar.gz installation package.
   <PkgListV3 type={0}/>
2. In the directory where the package is located, use `tar` to decompress the package:

> Please replace `<version>` with the corresponding version of the package downloaded

```bash
tar -zxvf TDengine-server-<version>-Linux-x64.tar.gz
```

In the directory to which the package was decompressed, run `install.sh`:

```bash
sudo ./install.sh
```

:::info
Users will be prompted to enter some configuration information when install.sh is executing. The interactive mode can be disabled by executing `./install.sh -e no`. `./install.sh -h` can show all parameters with detailed explanation.
:::

</TabItem>

<TabItem value="apt-get" label="apt-get">
You can use `apt-get` to install TDengine from the official package repository.

**Configure the package repository**

```bash
wget -qO - http://repos.taosdata.com/tdengine.key | sudo apt-key add -
echo "deb [arch=amd64] http://repos.taosdata.com/tdengine-stable stable main" | sudo tee /etc/apt/sources.list.d/tdengine-stable.list
```

You can install beta versions by configuring the following repository:

```bash
wget -qO - http://repos.taosdata.com/tdengine.key | sudo apt-key add -
echo "deb [arch=amd64] http://repos.taosdata.com/tdengine-beta beta main" | sudo tee /etc/apt/sources.list.d/tdengine-beta.list
```

**Install TDengine with `apt-get`**

```bash
sudo apt-get update
apt-cache policy tdengine
sudo apt-get install tdengine
```

:::tip
This installation method is supported only for Debian and Ubuntu.
:::
</TabItem>
<TabItem label="Windows" value="windows">

Note: TDengine only supports Windows Server 2016/2019 and Windows 10/11 on the Windows platform.

1. Download the Windows installation package.
   <PkgListV3 type={3}/>
2. Run the downloaded package to install TDengine.

</TabItem>
<TabItem label="macOS" value="macos">

1. Download the macOS installation package.
   <PkgListV3 type={7}/>
2. Run the downloaded package to install TDengine. If the installation is blocked, you can right-click or ctrl-click on the installation package and select `Open`.

</TabItem>
</Tabs>

:::info
For information about TDengine other releases, check [Release History](../../releases/tdengine).
:::

:::note
On the first node in your TDengine cluster, leave the `Enter FQDN:` prompt blank and press **Enter**. On subsequent nodes, you can enter the endpoint of the first dnode in the cluster. You can also configure this setting after you have finished installing TDengine.

:::

## Quick Launch

<Tabs>
<TabItem label="Linux" value="linux">

After the installation is complete, run the following command to start the TDengine service:

```bash
systemctl start taosd
```

Run the following command to confirm that TDengine is running normally:

```bash
systemctl status taosd
```

Output similar to the following indicates that TDengine is running normally:

```
Active: active (running)
```

Output similar to the following indicates that TDengine has not started successfully:

```
Active: inactive (dead)
```

After confirming that TDengine is running, run the `taos` command to access the TDengine CLI.

The following `systemctl` commands can help you manage TDengine service:

- Start TDengine Server: `systemctl start taosd`

- Stop TDengine Server: `systemctl stop taosd`

- Restart TDengine Server: `systemctl restart taosd`

- Check TDengine Server status: `systemctl status taosd`

:::info

- The `systemctl` command requires _root_ privileges. If you are not logged in as the _root_ user, use the `sudo` command.
- The `systemctl stop taosd` command does not instantly stop TDengine Server. The server is stopped only after all data in memory is flushed to disk. The time required depends on the cache size.
- If your system does not include `systemd`, you can run `/usr/local/taos/bin/taosd` to start TDengine manually.

:::

## Command Line Interface (CLI)

You can use the TDengine CLI to monitor your TDengine deployment and execute ad hoc queries. To open the CLI, you can execute `taos` in terminal.

</TabItem>

<TabItem label="Windows" value="windows">

After the installation is complete, please run `sc start taosd` or run `C:\TDengine\taosd.exe` with administrator privilege to start TDengine Server. Please run `sc start taosadapter` or run `C:\TDengine\taosadapter.exe` with administrator privilege to start taosAdapter to provide http/REST service.

## Command Line Interface (CLI)

You can use the TDengine CLI to monitor your TDengine deployment and execute ad hoc queries. To open the CLI, you can run `taos.exe` in the `C:\TDengine` directory of the Windows terminal to start the TDengine command line.

</TabItem>

<TabItem label="macOS" value="macos">

After the installation is complete, double-click the /applications/TDengine to start the program, or run `launchctl start com.tdengine.taosd` to start TDengine Server.

The following `launchctl` commands can help you manage TDengine service:

- Start TDengine Server: `sudo launchctl start com.tdengine.taosd`

- Stop TDengine Server: `sudo launchctl stop com.tdengine.taosd`

- Check TDengine Server status: `sudo launchctl list | grep taosd`

- Check TDengine Server status details: `launchctl print system/com.tdengine.taosd`

:::info
- Please use `sudo` to run `launchctl` to manage _com.tdengine.taosd_ with administrator privileges.
- The administrator privilege is required for service management to enhance security.
- Troubleshooting:
- The first column returned by the command `launchctl list | grep taosd` is the PID of the program. If it's `-`, that means the TDengine service is not running.
- If the service is abnormal, please check the `launchd.log` file from the system log or the `taosdlog` from the `/var/log/taos directory` for more information.

:::

## Command Line Interface (CLI)

You can use the TDengine CLI to monitor your TDengine deployment and execute ad hoc queries. To open the CLI, you can execute `taos` in terminal.

</TabItem>
</Tabs>

```bash
taos
```

The TDengine CLI displays a welcome message and version information to indicate that its connection to the TDengine service was successful. If an error message is displayed, see the [FAQ](../../train-faq/faq) for troubleshooting information. At the following prompt, you can execute SQL commands.

```cmd
taos>
```

For example, you can create and delete databases and tables and run all types of queries. Each SQL command must be end with a semicolon (;). For example:

```sql
CREATE DATABASE demo;
USE demo;
CREATE TABLE t (ts TIMESTAMP, speed INT);
INSERT INTO t VALUES ('2019-07-15 00:00:00', 10);
INSERT INTO t VALUES ('2019-07-15 01:00:00', 20);
SELECT * FROM t;

           ts            |    speed    |
========================================
 2019-07-15 00:00:00.000 |          10 |
 2019-07-15 01:00:00.000 |          20 |

Query OK, 2 row(s) in set (0.003128s)
```

You can also can monitor the deployment status, add and remove user accounts, and manage running instances. You can run the TDengine CLI on either machines. For more information, see [TDengine CLI](../../reference/taos-shell/).

## Test data insert performance

After your TDengine Server is running normally, you can run the taosBenchmark utility to test its performance:

Start TDengine service and execute `taosBenchmark` (formerly named `taosdemo`) in a terminal.

```bash
taosBenchmark
```

This command creates the `meters` supertable in the `test` database. In the `meters` supertable, it then creates 10,000 subtables named `d0` to `d9999`. Each table has 10,000 rows and each row has four columns: `ts`, `current`, `voltage`, and `phase`. The timestamps of the data in these columns range from 2017-07-14 10:40:00 000 to 2017-07-14 10:40:09 999. Each table is randomly assigned a `groupId` tag from 1 to 10 and a `location` tag of either `California.Campbell`, `California.Cupertino`, `California.LosAngeles`, `California.MountainView`, `California.PaloAlto`, `California.SanDiego`, `California.SanFrancisco`, `California.SanJose`, `California.SantaClara` or `California.Sunnyvale`.

The `taosBenchmark` command creates a deployment with 100 million data points that you can use for testing purposes. The time required to create the deployment depends on your hardware. On most modern servers, the deployment is created in ten to twenty seconds.

You can customize the test deployment that taosBenchmark creates by specifying command-line parameters. For information about command-line parameters, run the `taosBenchmark --help` command. For more information about taosBenchmark, see [taosBenchmark](../../reference/taosbenchmark).

## Test data query performance

After using `taosBenchmark` to create your test deployment, you can run queries in the TDengine CLI to test its performance:

From the TDengine CLI (taos) query the number of rows in the `meters` supertable:

```sql
SELECT COUNT(*) FROM test.meters;
```

Query the average, maximum, and minimum values of all 100 million rows of data:

```sql
SELECT AVG(current), MAX(voltage), MIN(phase) FROM test.meters;
```

Query the number of rows whose `location` tag is `California.SanFrancisco`:

```sql
SELECT COUNT(*) FROM test.meters WHERE location = "California.SanFrancisco";
```

Query the average, maximum, and minimum values of all rows whose `groupId` tag is `10`:

```sql
SELECT AVG(current), MAX(voltage), MIN(phase) FROM test.meters WHERE groupId = 10;
```

Query the average, maximum, and minimum values for table `d10` in 10 second intervals:

```sql
SELECT FIRST(ts), AVG(current), MAX(voltage), MIN(phase) FROM test.d10 INTERVAL(10s);
```

In the query above you are selecting the first timestamp (ts) in the interval, another way of selecting this would be `\_wstart` which will give the start of the time window. For more information about windowed queries, see [Time-Series Extensions](../../taos-sql/distinguished/).
