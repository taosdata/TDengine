#Getting Started

## Quick Start

At the moment, TDengine only runs on Linux. You can set up and install it either from the  <a href='#Install-from-Source'>source code</a> or the <a href='#Install-from-Package'>packages</a>. It takes only a few seconds from download to run it successfully. 

### Install from Source

Please visit our [github page](https://github.com/taosdata/TDengine) for instructions on installation from the source code.

### Install from Package

Three different packages are provided, please pick up the one you like. 
<ul id='packageList'>
<li><a id='tdengine-rpm' style='color:var(--b2)'>TDengine RPM package (1.5M)</a></li>
<li><a id='tdengine-deb' style='color:var(--b2)'>TDengine DEB package (1.7M)</a></li>
<li><a id='tdengine-tar' style='color:var(--b2)'>TDengine Tarball (3.0M)</a></li>
</ul>
For the time being, TDengine only supports installation on Linux systems using [`systemd`](https://en.wikipedia.org/wiki/Systemd) as the service manager. To check if your system has *systemd* package, use the _which systemctl_ command.

```cmd
which systemctl
```

If the `systemd` package is not found, please [install from source code](#Install-from-Source). 

### Running TDengine

After installation, start the TDengine service by the `systemctl` command.

```cmd
systemctl start taosd
```

Then check if the server is working now.
```cmd
systemctl status taosd
```

If the service is running successfully, you can play around through TDengine shell `taos`, the command line interface tool located in directory /usr/local/bin/taos 

**Note: The _systemctl_ command needs the root privilege. Use _sudo_ if you are not the _root_ user.**

##TDengine Shell
To launch TDengine shell, the command line interface, in a Linux terminal, type:

```cmd
taos
```

The welcome message is printed if the shell connects to TDengine server successfully, otherwise, an error message will be printed (refer to our [FAQ](../faq) page for troubleshooting the connection error). The TDengine shell prompt is: 

```cmd
taos>
```

In the TDengine shell, you can create databases, create tables and insert/query data with SQL. Each query command ends with a semicolon. It works like MySQL, for example:

```mysql
create database db;
use db;
create table t (ts timestamp, cdata int);
insert into t values ('2019-07-15 10:00:00', 10);
insert into t values ('2019-07-15 10:01:05', 20);
select * from t;
          ts          |   speed   |
===================================
 19-07-15 10:00:00.000|         10|
 19-07-15 10:01:05.000|         20|
Query OK, 2 row(s) in set (0.001700s)
```

Besides the SQL commands, the system administrator can check system status, add or delete accounts, and manage the servers.

###Shell Command Line Parameters

You can run `taos` command with command line options to fit your needs. Some frequently used options are listed below:

- -c, --config-dir: set the configuration directory. It is _/etc/taos_ by default
- -h, --host: set the IP address of the server it will connect to, Default is localhost
- -s, --commands: set the command to run without entering the shell
- -u, -- user:  user name to connect to server. Default is root
- -p, --password: password. Default is 'taosdata'
- -?, --help: get a full list of supported options 

Examples:

```cmd
taos -h 192.168.0.1 -s "use db; show tables;"
```

###Run Batch Commands

Inside TDengine shell, you can run batch commands in a file with *source* command.

```
taos> source <filename>;
```

### Tips

- Use up/down arrow key to check the command history
- To change the default password, use "`alter user`" command 
- ctrl+c to interrupt any queries 
- To clean the cached schema of tables or STables, execute command `RESET QUERY CACHE` 

## Major Features

The core functionality of TDengine is the time-series database. To reduce the development and management complexity, and to improve the system efficiency further, TDengine also provides caching, pub/sub messaging system, and stream computing functionalities. It provides a full stack for IoT big data platform. The detailed features are listed below:

- SQL like query language used to insert or explore data

- C/C++, Java(JDBC), Python, Go, RESTful, and Node.JS interfaces for development

- Ad hoc queries/analysis via Python/R/Matlab or TDengine shell

- Continuous queries to support sliding-window based stream computing

- Super table to aggregate multiple time-streams efficiently with flexibility  

- Aggregation over a time window on one or multiple time-streams

- Built-in messaging system to support publisher/subscriber model

- Built-in cache for each time stream to make latest data available as fast as light speed

- Transparent handling of historical data and real-time data 

- Integrating with Telegraf, Grafana and other tools seamlessly  

- A set of tools or configuration to manage TDengine 


For enterprise edition, TDengine provides more advanced features below:

- Linear scalability to deliver higher capacity/throughput 

- High availability to guarantee the carrier-grade service  

- Built-in replication between nodes which may span multiple geographical sites 

- Multi-tier storage to make historical data management simpler and cost-effective

- Web-based management tools and other tools to make maintenance simpler

TDengine is specially designed and optimized for time-series data processing in IoT, connected cars, Industrial IoT, IT infrastructure and application monitoring, and other scenarios. Compared with other solutions, it is 10x faster on insert/query speed. With a single-core machine, over 20K requestes can be processed, millions data points can be ingested, and over 10 million data points can be retrieved in a second. Via column-based storage and tuned compression algorithm for different data types, less than 1/10 storage space is required. 

## Explore More on TDengine

Please read through the whole <a href='../documentation'>documentation</a> to learn more about TDengine.

