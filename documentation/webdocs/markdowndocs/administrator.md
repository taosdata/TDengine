#Administrator

## Directory and Files

After TDengine is installed, by default, the following directories will be created:

| Directory/File         | Description                     |
| ---------------------- | :------------------------------ |
| /etc/taos/taos.cfg     | TDengine configuration file     |
| /usr/local/taos/driver | TDengine dynamic link library   |
| /var/lib/taos          | TDengine default data directory |
| /var/log/taos          | TDengine default log directory  |
| /usr/local/taos/bin.   | TDengine executables            |

### Executables

All TDengine executables are located at _/usr/local/taos/bin_ , including:

- `taosd`：TDengine server 
- `taos`： TDengine Shell, the command line interface.
- `taosdump`：TDengine data export tool
- `rmtaos`： a script to uninstall TDengine

You can change the data directory and log directory setting through the system configuration file

## Configuration on Server

`taosd` is running on the server side, you can change the system configuration file taos.cfg to customize its behavior. By default, taos.cfg is located at /etc/taos, but you can specify the path to configuration file via the command line parameter -c. For example: `taosd -c /home/user` means the configuration file will be read from directory /home/user.

This section lists only the most important configuration parameters. Please check taos.cfg to find all the configurable parameters. **Note: to make your new configurations work, you have to restart taosd after you change taos.cfg**.

- mgmtShellPort: TCP and UDP port between client and TDengine mgmt (default: 6030). Note: 5 successive UDP ports (6030-6034) starting from this number will be used.
- vnodeShellPort: TCP and UDP port between client and TDengine vnode (default: 6035). Note: 5 successive UDP ports (6035-6039) starting from this number will be used.
- httpPort: TCP port for RESTful service (default: 6020)
- dataDir: data directory, default is /var/lib/taos
- maxUsers: maximum number of users allowed
- maxDbs: maximum number of databases allowed
- maxTables: maximum number of tables allowed
- enableMonitor: turn on/off system monitoring, 0: off, 1: on
- logDir: log directory, default is /var/log/taos
- numOfLogLines: maximum number of lines in the log file
- debugFlag: log level, 131: only error and warnings, 135: all

In different scenarios, data characteristics are different. For example, the retention policy, data sampling period, record size, the number of devices, and data compression may be different. To gain the best performance, you can change the following configurations related to storage:  

- days: number of days to cover for a data file
- keep: number of days to keep the data
- rows: number of rows of records in a block in data file.
- comp: compression algorithm, 0: off, 1: standard; 2: maximum compression
- ctime: period (seconds) to flush data to disk
- clog: flag to turn on/off Write Ahead Log, 0: off, 1: on 
- tables: maximum number of tables allowed in a vnode
- cache: cache block size (bytes)
- tblocks: maximum number of cache blocks for a table
- abloks: average number of cache blocks for a table 
- precision: timestamp precision, us: microsecond ms: millisecond, default is ms

For an application, there may be multiple data scenarios. The best design is to put all data with the same characteristics into one database. One application may have multiple databases, and every database has its own configuration to maximize the system performance. You can specify the above configurations related to storage when you create a database. For example:  

```mysql
CREATE DATABASE demo DAYS 10 CACHE 16000 ROWS 2000 
```

The above SQL statement will create a database demo, with 10 days for each data file, 16000 bytes for a cache block, and 2000 rows in a file block.

The configuration provided when creating a database will overwrite the configuration in taos.cfg. 

## Configuration on Client 

*taos* is the TDengine shell and is a client that connects to taosd. TDengine uses the same configuration file taos.cfg for the client, with default location at /etc/taos. You can change it by specifying command line parameter -c when you run taos. For example, *taos -c /home/user*, it will read the configuration file taos.cfg from directory /home/user.

The parameters related to client configuration are listed below: 

- masterIP: IP address of TDengine server
- charset: character set, default is the system . For data type nchar, TDengine uses unicode to store the data. Thus, the client needs to tell its character set.
- locale: system language setting
- defaultUser: default login user, default is root
- defaultPass: default password, default is taosdata

For TCP/UDP port, and system debug/log configuration, it is the same as the server side.

For server IP, user name, password, you can always specify them in the command line when you run taos. If they are not specified, they will be read from the taos.cfg

## User Management

System administrator (user root) can add, remove a user, or change the password from the TDengine shell. Commands are listed below:

Create a user, password shall be quoted with the single quote.

```mysql
CREATE USER user_name PASS ‘password’
```

Remove a user

```mysql
DROP USER user_name
```

Change the password for a user

```mysql
ALTER USER user_name PASS ‘password’  
```

List all users

```mysql
SHOW USERS
```

## Import Data

Inside the TDengine shell, you can import data into TDengine from either a script or CSV file

**Import from Script**

```
source <filename>
```

Inside the file, you can put all SQL statements there. Each SQL statement has a line. If a line starts with "#", it means comments, it will be skipped. The system will execute the SQL statements line by line automatically until the ends 

**Import from CVS**

```mysql
insert into tb1 file a.csv b.csv tb2 c.csv …
import into tb1 file a.csv b.csv tb2 c.csv …
```

Each csv file contains records for only one table, and the data structure shall be the same as the defined schema for the table. 

## Export Data

You can export data either from TDengine shell or from tool taosdump.

**Export from TDengine Shell**

```mysql
select * from <tb_name> >> a.csv
```

The above SQL statement will dump the query result set into a csv file. 

**Export Using taosdump**

TDengine provides a data dumping tool taosdump. You can choose to dump a database, a table, all data or data only a time range, even only the metadata. For example:

- Export one or more tables in a DB: taosdump [OPTION…] dbname tbname …
- Export one or more DBs: taosdump [OPTION…] --databases dbname…
- Export all DBs (excluding system DB): taosdump [OPTION…] --all-databases

run *taosdump —help* to get a full list of the options

## Management of Connections, Streams, Queries 

The system administrator can check, kill the ongoing connections, streams, or queries. 

```
SHOW CONNECTIONS
```

It lists all connections, one column shows ip:port from the client. 

```
KILL CONNECTION <connection-id>
```

It kills the connection, where connection-id is the ip:port showed by "SHOW CONNECTIONS". You can copy and paste it.

```
SHOW QUERIES
```

It shows the ongoing queries, one column ip:port:id shows the ip:port from the client, and id assigned by the system

```
KILL QUERY <query-id>
```

It kills the query, where query-id is the ip:port:id showed by "SHOW QUERIES". You can copy and paste it.

```
SHOW STREAMS
```

It shows the continuous queries, one column shows the ip:port:id, where ip:port is the connection from the client, and id assigned by the system.

```
KILL STREAM <stream-id>
```

It kills the continuous query, where stream-id is the ip:port:id showed by "SHOW STREAMS". You can copy and paste it.

## System Monitor

TDengine runs a system monitor in the background. Once it is started, it will create a database sys automatically. System monitor collects the metric like CPU, memory, network, disk, number of requests periodically, and writes them into database sys. Also, TDengine will log all important actions, like login, logout, create database, drop database and so on, and write them into database sys. 

You can check all the saved monitor information from database sys. By default, system monitor is turned on. But you can turn it off by changing the parameter in the configuration file.

