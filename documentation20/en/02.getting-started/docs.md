# Getting Started

## <a class="anchor" id="install"></a>Quick Install

TDengine includes server, client, and ecosystem software and peripheral tools. Currently, version 2.0 of the server can only be installed and run on Linux and will support Windows, macOS, and other OSes in the future. The client can be installed and run on Windows or Linux. Applications on any operating system can use the RESTful interface to connect to the taosd server. Starting with 2.4, TDengine includes taosAdapter to provide an easy-to-use and efficient way to ingest data and includes a RESTful service. taosAdapter needs to be started manually as a stand-alone component. The earlier version uses an embedded HTTP component to provide the RESTful interface.

TDengine supports X64/ARM64/MIPS64/Alpha64 hardware platforms and will support ARM32, RISC-V, and other CPU architectures in the future.

### Install with Docker Container

```
docker run -d -p 6030-6049:6030-6049 -p 6030-6049:6030-6049/udp tdengine/tdengine
```

Please refer to [Quickly Taste TDengine with Docker](https://www.taosdata.com/en/documentation/getting-started/docker) for the details.

For the time being, we do not recommend using Docker to deploy the TDengine server or client in production environments. However it is a convenient way to deploy TDengine for development purposes. In particular, it is easy to try TDengine in macOS and Windows environments with Docker.

### <a class="anchor" id="package-install"></a>Install from Package

TDengine is very easy to run; download to successful installation takes just a few seconds. For ease of use, the standard server installation package includes the client application and sample code. But if you only need the server application and C/C++ language support for the client connection, you can also download only the lite version of the installation package. The installation packages are available in `rpm` and `deb` formats, as well as `tar.gz` format for enterprise customers who need to facilitate use on specific operating systems. Releases include both stable and beta releases. We recommend the stable release for production use or testing. The beta release may contain more new features. You can choose to download from the following as needed:

<ul id="server-packageList" class="package-list"></ul>

For detailed installation steps, please refer to [How to install/uninstall TDengine with installation package](https://www.taosdata.com/getting-started/install).

**Click [here](https://github.com/taosdata/TDengine/releases) for release notes.**

### Install TDengine by apt-get

If you use Debian or Ubuntu system you can use the `apt-get` command to install TDengine from the official repository. Please use the following commands to setup:

```
wget -qO - http://repos.taosdata.com/tdengine.key | sudo apt-key add -
echo "deb [arch=amd64] http://repos.taosdata.com/tdengine-stable stable main" | sudo tee /etc/apt/sources.list.d/tdengine-stable.list
[Optional] echo "deb [arch=amd64] http://repos.taosdata.com/tdengine-beta beta main" | sudo tee /etc/apt/sources.list.d/tdengine-beta.list
sudo apt-get update
apt-cache policy tdengine
sudo apt-get install tdengine
```

### Install client only

If the client and server are running on different computers, you can install the client separately. When downloading, please note that the selected client version number should strictly match the server version number downloaded above. Linux and Windows installation packages are as follows (the lite version of the installer comes with connection support for the C/C++ language only, while the standard version of the installer also contains sample code):

<ul id="client-packagelist" class="package-list"></ul>

### <a class="anchor" id="source-install"></a>Install from Source

If you want to contribute to TDengine, please visit [TDengine GitHub page](https://github.com/taosdata/TDengine) for detailed instructions on build and installation from the source code.

**To download other components, beta version, or early releases, please click [here](https://www.taosdata.com/en/all-downloads/).**

## <a class="anchor" id="start"></a>Quick Launch

After installation, you can start the TDengine service by the `systemctl` command.

```bash
systemctl start taosd
```

Then check if the service is working.

```bash
systemctl status taosd
```

If the service is running successfully, you can play around through the TDengine shell, `taos`.

**Note:**

- The `systemctl` command needs the **root** privilege. Use **sudo** if you are not the **root** user.
- To get better product feedback and improve our solution, TDengine will collect basic usage information, but you can modify the configuration parameter **telemetryReporting** in the system configuration file `taos.cfg`, and set it to 0 to turn it off.
- TDengine uses FQDN (usually hostname) as the node ID. To ensure normal operation, you need to set the host's name for the server running `taosd`, and configure DNS service or hosts file for the machine running the client application, to ensure the FQDN can be resolved.
- TDengine supports installation on Linux systems with [systemd](https://en.wikipedia.org/wiki/Systemd) as the process service management and uses `which systemctl` command to detect whether `systemd` packages exist in the system:

  ```bash
  which systemctl
  ```

If `systemd` is not supported in the system, TDengine service can also be launched via `/usr/local/taos/bin/taosd` manually.

## <a class="anchor" id="console"></a>TDengine Shell Command Line

To launch TDengine shell, the command-line interface, in a Linux terminal, type:

```bash
taos
```

The welcome message is printed if the shell connects to the TDengine server successfully, otherwise, an error message will be printed (refer to our [FAQ](https://www.taosdata.com/en/faq) page for troubleshooting the connection error). The TDengine shell prompt is:

```cmd
taos>
```

In the TDengine shell, you can create databases, create tables and insert/query data with SQL. Each query command ends with a semicolon. It works like MySQL, for example:

```mysql
CREATE DATABASE demo;
USE demo;
CREATE TABLE t (ts TIMESTAMP, speed INT);
INSERT INTO t VALUES ('2019-07-15 00:00:00', 10);
INSERT INTO t VALUES ('2019-07-15 01:00:00', 20);
SELECT * FROM t;

ts     |  speed  |

===================================

19-07-15 00:00:00.000|     10|

19-07-15 01:00:00.000|     20|

Query OK, 2 row(s) in set (0.001700s)
```

Besides executing SQL commands, the system administrator can check system status, add or delete accounts, and manage the servers.

### Shell Command Line Parameters

You can configure command parameters to change how the TDengine shell executes. Some frequently used options are listed below:

- -c, --config-dir: set the configuration directory. It is */etc/taos* by default.
- -h, --host: set the IP address of the server it will connect to. Default is localhost.
- -s, --commands: set the command to run without entering the shell.
- -u, -- user: user name to connect to the server/cluster. Default is root.
- -p, --password: password. Default is 'taosdata'.
- -?, --help: get a full list of supported options.

Examples:

```bash
taos -h h1.taos.com -s "USE db; SHOW TABLES;"
```

### Run SQL Command Scripts

Inside TDengine shell, you can run SQL scripts in a file with the `SOURCE` command.

```mysql
taos> SOURCE <filename>;
```

### taos shell tips

- Use the up/down arrow key to check the command history
- To change the default password, use `ALTER USER` command
- Use Ctrl+c to interrupt any queries
- To clean the schema of locally cached tables, execute the command `RESET QUERY CACHE`

## <a class="anchor" id="demo"></a>Taste TDengine’s Lightning Speed

### <a class="anchor" id="taosBenchmark"></a> Taste insertion speed with taosBenchmark

Once the TDengine server started, you can execute the command `taosBenchmark` (which was named `taosdemo`) in the Linux terminal. In 2.4.0.7 and early release, taosBenchmark is distributed within taosTools package. In later release, taosBenchmark will be included within TDengine again. 

```bash
taosBenchmark
```

Using this command, a STable named `meters` will be created in the database `test`. There are 10k tables under this STable, named from `d0` to `d9999`. In each table, there are 100k rows of records, each row with columns （`ts`, `current`, `voltage`, and `phase`. The timestamp is from "2017-07-14 10:40:00 000" to "2017-07-14 10:41:39 999". Each table also has tags `location` and `groupId`: `groupId` is set from 1 to 10, `location` is set to "beijing" or "shanghai".

Once execution is finished, 1 billion rows of records will be inserted. It usually takes about a dozen seconds to execute this command on a normal PC server but it may be different depending on the particular hardware platform performance.

### <a class="anchor" id="taosBenchmark"></a> Using taosBenchmark in detail

you can run the command `taosBenchmark` with many options, like the number of tables, rows of records, and so on. To know more about these options, you can execute `taosBenchmark --help` and then take a try using different options.

For more details on how to use taosBenchmark, please refer to [How to use taosBenchmark to test the performance of TDengine](https://tdengine.com/docs/en/v2.0/getting-started/taosdemo).

### <a class="anchor" id="taosshell"></a> Taste query speed with taos shell

In the TDengine client, enter sql query commands and then taste our lightning query speed.

- query total rows of records：

```mysql
taos> SELECT COUNT(*) FROM test.meters;
```

- query average, max, and min of the total 1 billion records：

```mysql
taos> SELECT AVG(current), MAX(voltage), MIN(phase) FROM test.meters;
```

- query the number of records where location="beijing":

```mysql
taos> SELECT COUNT(*) FROM test.meters WHERE location="beijing";
```

- query the average, max and min of total records where areaid=10：

```mysql
taos> SELECT AVG(current), MAX(voltage), MIN(phase) FROM test.meters WHERE groupId=10;
```

- query the average, max, min from table t10 when aggregating over every 10s:

```mysql
taos> SELECT AVG(current), MAX(voltage), MIN(phase) FROM test.d10 INTERVAL(10s);
```

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

At the moment, TDengine connectors can support a wide range of platforms, including hardware platforms such as X64/X86/ARM64/ARM32/MIPS/Alpha, and operating systems such as Linux/Win64/Win32.

Comparison matrix as following:

| **CPU**     | **X64 64bit** |           |           | **X86 32bit** | **ARM64** | **ARM32** | **MIPS Godson** | **Alpha Shenwei** | **X64 TimecomTech** |
| ----------- | ------------- | --------- | --------- | ------------- | --------- | --------- | --------------- | ----------------- | ------------------- |
| **OS**      | **Linux**     | **Win64** | **Win32** | **Win32**     | **Linux** | **Linux** | **Linux**       | **Linux**         | **Linux**           |
| **C/C++**   | ●             | ●         | ●         | ○             | ●         | ●         | ●               | ●                 | ●                   |
| **JDBC**    | ●             | ●         | ●         | ○             | ●         | ●         | ●               | ●                 | ●                   |
| **Python**  | ●             | ●         | ●         | ○             | ●         | ●         | ●               | --                | ●                   |
| **Go**      | ●             | ●         | ●         | ○             | ●         | ●         | ○               | --                | --                  |
| **Node.js** | ●             | ●         | ○         | ○             | ●         | ●         | ○               | --                | --                  |
| **C#**      | ○             | ●         | ●         | ○             | ○         | ○         | ○               | --                | --                  |
| **RESTful** | ●             | ●         | ●         | ●             | ●         | ●         | ●               | ●                 | ●                   |

Note: ● has been verified by official tests; ○ has been verified by unofficial tests.

Please visit Connectors section for more detailed information.

<script src="/wp-includes/js/quick-start.js?v=1"></script>
