# Quick Start

## <a class="anchor" id="install"></a>Quick Install

TDengine software consists of 3 parts: server, client, and alarm module. At the moment, TDengine server only runs on Linux (Windows, mac OS and more OS supports will come soon), but client can run on either Windows or Linux. TDengine client can be installed and run on Windows or Linux. Applications based-on any OSes can all connect to server taosd via a RESTful interface. About CPU, TDengine supports X64/ARM64/MIPS64/Alpha64, and ARM32、RISC-V, other more CPU architectures will be supported soon. You can set up and install TDengine server either from the [source code](https://www.taosdata.com/en/getting-started/#Install-from-Source) or the [packages](https://www.taosdata.com/en/getting-started/#Install-from-Package).

### <a class="anchor" id="source-install"></a>Install from Source

Please visit our [TDengine github page](https://github.com/taosdata/TDengine) for instructions on installation from the source code.

### Install from Docker Container

Please visit our [TDengine Official Docker Image: Distribution, Downloading, and Usage](https://www.taosdata.com/blog/2020/05/13/1509.html).

### <a class="anchor" id="package-install"></a>Install from Package

It’s extremely easy to install for TDengine, which takes only a few seconds from downloaded to successful installed. The server installation package includes clients and connectors. We provide 3 installation packages, which you can choose according to actual needs:

Click [here](https://www.taosdata.com/cn/getting-started/#%E9%80%9A%E8%BF%87%E5%AE%89%E8%A3%85%E5%8C%85%E5%AE%89%E8%A3%85) to download the install package.

For more about installation process, please refer [TDengine Installation Packages: Install and Uninstall](https://www.taosdata.com/blog/2019/08/09/566.html), and [Video Tutorials](https://www.taosdata.com/blog/2020/11/11/1941.html).

## <a class="anchor" id="start"></a>Quick Launch

After installation, you can start the TDengine service by the `systemctl` command.

```bash
$ systemctl start taosd
```

Then check if the service is working now.

```bash
$ systemctl status taosd
```

If the service is running successfully, you can play around through TDengine shell `taos`.

**Note:**

- The `systemctl` command needs the **root** privilege. Use **sudo** if you are not the **root** user.
- To get better product feedback and improve our solution, TDengine will collect basic usage information, but you can modify the configuration parameter **telemetryReporting** in the system configuration file taos.cfg, and set it to 0 to turn it off.
- TDengine uses FQDN (usually hostname) as the node ID. In order to ensure normal operation, you need to set hostname for the server running taosd, and configure DNS service or hosts file for the machine running client application, to ensure the FQDN can be resolved.
- TDengine supports installation on Linux systems with[ systemd ](https://en.wikipedia.org/wiki/Systemd)as the process service management, and uses `which systemctl` command to detect whether `systemd` packages exist in the system:
  
  ```bash
  $ which systemctl
  ```

If `systemd` is not supported in the system, TDengine service can also be launched via `/usr/local/taos/bin/taosd` manually.

## <a class="anchor" id="console"></a>TDengine Shell Command Line

To launch TDengine shell, the command line interface, in a Linux terminal, type:

```bash
$ taos
```

The welcome message is printed if the shell connects to TDengine server successfully, otherwise, an error message will be printed (refer to our [FAQ](https://www.taosdata.com/en/faq) page for troubleshooting the connection error). The TDengine shell prompt is:

```cmd
taos>
```

In the TDengine shell, you can create databases, create tables and insert/query data with SQL. Each query command ends with a semicolon. It works like MySQL, for example:

```mysql
create database demo;

use demo;

create table t (ts timestamp, speed int);

insert into t values ('2019-07-15 00:00:00', 10);

insert into t values ('2019-07-15 01:00:00', 20);

select * from t;

ts     |  speed  |

===================================

19-07-15 00:00:00.000|     10|

19-07-15 01:00:00.000|     20|

Query OK, 2 row(s) in set (0.001700s)
```

Besides the SQL commands, the system administrator can check system status, add or delete accounts, and manage the servers.

### Shell Command Line Parameters

You can configure command parameters to change how TDengine shell executes. Some frequently used options are listed below:

- -c, --config-dir: set the configuration directory. It is */etc/taos* by default.
- -h, --host: set the IP address of the server it will connect to. Default is localhost.
- -s, --commands: set the command to run without entering the shell.
- -u, -- user: user name to connect to server. Default is root.
- -p, --password: password. Default is 'taosdata'.
- -?, --help: get a full list of supported options.

Examples:

```bash
$ taos -h 192.168.0.1 -s "use db; show tables;"
```

### Run SQL Command Scripts

Inside TDengine shell, you can run SQL scripts in a file with source command.

```mysql
taos> source <filename>;
```

### Shell Tips

- Use up/down arrow key to check the command history
- To change the default password, use "alter user" command
- Use ctrl+c to interrupt any queries
- To clean the schema of local cached tables, execute command `RESET QUERY CACHE`

## <a class="anchor" id="demo"></a>Experience TDengine’s Lightning Speed

After starting the TDengine server, you can execute the command `taosdemo` in the Linux terminal.

```bash 
$ taosdemo
```

Using this command, a STable named `meters` will be created in the database `test` There are 10k tables under this stable, named from `t0` to `t9999`. In each table there are 100k rows of records, each row with columns （`f1`, `f2` and `f3`. The timestamp is from "2017-07-14 10:40:00 000" to "2017-07-14 10:41:39 999". Each table also has tags `areaid` and `loc`: `areaid` is set from 1 to 10, `loc` is set to "beijing" or "shanghai".

It takes about 10 minutes to execute this command. Once finished, 1 billion rows of records will be inserted.

In the TDengine client, enter sql query commands and then experience our lightning query speed.

- query total rows of records：

```mysql
taos> select count(*) from test.meters;
```

- query average, max and min of the total 1 billion records：

```mysql
taos> select avg(f1), max(f2), min(f3) from test.meters;
```

- query the number of records where loc="beijing":

```mysql
taos> select count(*) from test.meters where loc="beijing";
```

- query the average, max and min of total records where areaid=10：

```mysql
taos> select avg(f1), max(f2), min(f3) from test.meters where areaid=10;
```

- query the average, max, min from table t10 when aggregating over every 10s:

```mysql
taos> select avg(f1), max(f2), min(f3) from test.t10 interval(10s);
```

**Note**: you can run command `taosdemo` with many options, like number of tables, rows of records and so on. To know more about these options, you can execute `taosdemo --help` and then take a try using different options.

## Client and Alarm Module

If your client and server running on different machines, please install the client separately. Linux and Windows packages are provided:

- TDengine-client-2.0.10.0-Linux-x64.tar.gz(3.0M)
- TDengine-client-2.0.10.0-Windows-x64.exe(2.8M)
- TDengine-client-2.0.10.0-Windows-x86.exe(2.8M)

Linux package of Alarm Module is as following (please refer [How to Use Alarm Module](https://github.com/taosdata/TDengine/blob/master/alert/README_cn.md)):

- TDengine-alert-2.0.10.0-Linux-x64.tar.gz (8.1M)

## <a class="anchor" id="platforms"></a>List of Supported Platforms

List of platforms supported by TDengine server

|                    | **CentOS 6/7/8** | **Ubuntu 16/18/20** | **Other Linux** | UnionTech UOS | NeoKylin | LINX V60/V80 |
| ------------------ | ---------------- | ------------------- | --------------- | ------------- | -------- | ------------ |
| X64                | ●                | ●                   |                 | ○             | ●        | ●            |
| Loongson MIPS64    |                  |                     | ●               |               |          |              |
| Kunpeng ARM64      |                  | ○                   | ○               |               | ●        |              |
| SWCPU Alpha64      |                  |                     | ○               | ●             |          |              |
| FT ARM64           |                  | ○Ubuntu Kylin       |                 |               |          |              |
| Hygon X64          | ●                | ●                   | ●               | ○             | ●        | ●            |
| Rockchip ARM64     |                  |                     | ○               |               |          |              |
| Allwinner ARM64    |                  |                     | ○               |               |          |              |
| Actions ARM64      |                  |                     | ○               |               |          |              |

Note: ● has been verified by official tests; ○ has been verified by unofficial tests. 

List of platforms supported by TDengine client and connectors

At the moment, TDengine connectors can support a wide range of platforms, including hardware platforms such as X64/X86/ARM64/ARM32/MIPS/Alpha, and development environments such as Linux/Win64/Win32.

Comparison matrix as following:

| **CPU**     | **X64 64bit** |           |           | **X86 32bit** | **ARM64** | **ARM32** | **MIPS Godson** | **Alpha Shenwei** | **X64 TimecomTech** |
| ----------- | ------------- | --------- | --------- | ------------- | --------- | --------- | --------------- | ----------------- | ------------------- |
| **OS**      | **Linux**     | **Win64** | **Win32** | **Win32**     | **Linux** | **Linux** | **Linux**       | **Linux**         | **Linux**           |
| **C/C++**   | ●             | ●         | ●         | ○             | ●         | ●         | ●               | ●                 | ●                   |
| **JDBC**    | ●             | ●         | ●         | ○             | ●         | ●         | ●               | ●                 | ●                   |
| **Python**  | ●             | ●         | ●         | ○             | ●         | ●         | ●               | --                | ●                   |
| **Go**      | ●             | ●         | ●         | ○             | ●         | ●         | ○               | --                | --                  |
| **NodeJs**  | ●             | ●         | ○         | ○             | ●         | ●         | ○               | --                | --                  |
| **C#**      | ○             | ●         | ●         | ○             | ○         | ○         | ○               | --                | --                  |
| **RESTful** | ●             | ●         | ●         | ●             | ●         | ●         | ●               | ●                 | ●                   |

Note: ● has been verified by official tests; ○ has been verified by unofficial tests.

Please visit [Connectors](https://www.taosdata.com/en/documentation/connector) section for more detailed information.
