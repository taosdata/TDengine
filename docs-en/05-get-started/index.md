---
title: Get Started
description: 'Install TDengine from Docker image, apt-get or package, and run TAOS CLI and taosBenchmark to experience the features'
---

import Tabs from "@theme/Tabs";
import TabItem from "@theme/TabItem";
import PkgInstall from "./\_pkg_install.mdx";
import AptGetInstall from "./\_apt_get_install.mdx";

## Quick Install

The full package of TDengine includes the server(taosd), taosAdapter for connecting with third-party systems and providing a RESTful interface, client driver(taosc), command-line program(CLI, taos) and some tools. For the current version, the server taosd and taosAdapter can only be installed and run on Linux systems. In the future taosd and taosAdapter will also be supported on Windows, macOS and other systems. The client driver taosc and TDengine CLI can be installed and run on Windows or Linux. In addition to connectors for multiple languages, TDengine also provides a [RESTful interface](/reference/rest-api) through [taosAdapter](/reference/taosadapter). Prior to version 2.4.0.0, taosAdapter did not exist and the RESTful interface was provided by the built-in HTTP service of taosd.

TDengine supports X64/ARM64/MIPS64/Alpha64 hardware platforms, and will support ARM32, RISC-V and other CPU architectures in the future.

<Tabs defaultValue="apt-get">
<TabItem value="docker" label="Docker">
If docker is already installed on your computer, execute the following command:

```shell
docker run -d -p 6030-6049:6030-6049 -p 6030-6049:6030-6049/udp tdengine/tdengine
```

Make sure the container is running

```shell
docker ps
```

Enter into container and execute bash

```shell
docker exec -it <container name> bash
```

Then you can execute the Linux commands and access TDengine.

For detailed steps, please visit [Experience TDengine via Docker](/train-faq/docker)。

:::info
Starting from 2.4.0.10，besides taosd，TDengine docker image includes: taos，taosAdapter，taosdump，taosBenchmark，TDinsight, scripts and sample code. Once the TDengine container is started，it will start both taosAdapter and taosd automatically to support RESTful interface.

:::

</TabItem>
<TabItem value="apt-get" label="apt-get">
<AptGetInstall />
</TabItem>
<TabItem value="pkg" label="Package">
<PkgInstall />
</TabItem>
<TabItem value="src" label="Source Code">

If you like to check the source code, build the package by yourself or contribute to the project, please check [TDengine GitHub Repository](https://github.com/taosdata/TDengine)

</TabItem>
</Tabs>

## Quick Launch

After installation, you can launch the TDengine service by the 'systemctl' command to start 'taosd'.

```bash
systemctl start taosd
```

Check if taosd is running：

```bash
systemctl status taosd
```

If everything is fine, you can run TDengine command-line interface `taos` to access TDengine and test it out yourself.

:::info

- systemctl requires _root_ privileges，if you are not _root_ ，please add sudo before the command.
- To get feedback and keep improving the product, TDengine is collecting some basic usage information, but you can turn it off by setting telemetryReporting to 0 in configuration file taos.cfg. 
- TDengine uses FQDN (usually hostname）as the ID for a node. To make the system work, you need to configure the FQDN for the server running taosd, and configure the DNS service or hosts file on the the machine where the application or TDengine CLI runs to ensure that the FQDN can be resolved.     
- `systemctl stop taosd` won't stop the server right away, it will wait until all the data in memory are flushed to disk. It may takes time depending on the cache size.

TDengine supports the installation on system which runs [`systemd`](https://en.wikipedia.org/wiki/Systemd) for process management，use `which systemctl` to check if the system has `systemd` installed:

```bash
which systemctl
```

If the system does not have `systemd`，you can start TDengine manually by executing `/usr/local/taos/bin/taosd`

:::note

## Command Line Interface

To manage the TDengine running instance，or execute ad-hoc queries, TDengine provides a Command Line Interface (hereinafter referred to as TDengine CLI) taos. To enter into the interactive CLI，execute `taos` on a Linux terminal where TDengine is installed.

```bash
taos
```

If it connects to the TDengine server successfully, it will print out the version and welcome message. If it fails, it will print out the error message, please check [FAQ](/train-faq/faq) for trouble shooting connection issue. TDengine CLI's prompt is：

```cmd
taos>
```

Inside TDengine CLI，you can execute SQL commands to create/drop database/table, and run queries. The SQL command must be ended with a semicolon. For example:

```sql
create database demo;
use demo;
create table t (ts timestamp, speed int);
insert into t values ('2019-07-15 00:00:00', 10);
insert into t values ('2019-07-15 01:00:00', 20);
select * from t;
           ts            |    speed    |
========================================
 2019-07-15 00:00:00.000 |          10 |
 2019-07-15 01:00:00.000 |          20 |
Query OK, 2 row(s) in set (0.003128s)
```

Besides executing SQL commands, system administrators can check running status, add/drop user accounts and manage the running instances. TAOS CLI with client driver can be installed and run on either Linux or Windows machines. For more details on CLI, please [check here](../reference/taos-shell/).

## Experience the blazing fast speed

After TDengine server is running，execute `taosBenchmark` (previously named taosdemo) from a Linux terminal：

```bash
taosBenchmark
```

This command will create a super table "meters" under database "test". Under "meters", 10000 tables are created with names from "d0" to "d9999". Each table has 10000 rows and each row has four columns (ts, current, voltage, phase). Time stamp is starting from "2017-07-14 10:40:00 000" to "2017-07-14 10:40:09 999". Each table has tags "location" and "groupId". groupId is set 1 to 10 randomly, and location is set to "California.SanFrancisco" or "California.SanDiego".

This command will insert 100 million rows into the database quickly. Time to insert depends on the hardware configuration, it only takes a dozen seconds for a regular PC server.            

taosBenchmark provides command-line options and a configuration file to customize the scenarios, like number of tables, number of rows per table, number of columns and more. Please execute `taosBenchmark --help` to list them. For details on running taosBenchmark, please check [reference for taosBenchmark](/reference/taosbenchmark)

## Experience query speed
           
After using taosBenchmark to insert a number of rows data, you can execute queries from TDengine CLI to experience the lightning fast query speed.

query the total number of rows under super table "meters"：

```sql
taos> select count(*) from test.meters;
```

query the average, maximum, minimum of 100 million rows:

```sql
taos> select avg(current), max(voltage), min(phase) from test.meters;
```

query the total number of rows with location="California.SanFrancisco":

```sql
taos> select count(*) from test.meters where location="California.SanFrancisco";
```

query the average, maximum, minimum of all rows with groupId=10:

```sql
taos> select avg(current), max(voltage), min(phase) from test.meters where groupId=10;
```

query the average, maximum, minimum for table d10 in 10 seconds time interval：

```sql
taos> select avg(current), max(voltage), min(phase) from test.d10 interval(10s);
```
