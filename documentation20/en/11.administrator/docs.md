# TDengine Operation and Maintenance

## <a class="anchor" id="planning"></a> Capacity Planing

Using TDengine to build an IoT big data platform, computing resource and storage resource need to be planned according to business scenarios. The following is a discussion of the memory, CPU and hard disk space required for the system to run.

### Memory requirements

Each DB can create a fixed number of vgroups, which is the same as the CPU cores by default and can be configured by maxVgroupsPerDb; each replica in the vgroup would be a vnode; each vnode takes up a fixed amount of memory (the size is related to the database's configuration parameters blocks and cache); each table takes up memory related to the total length of the tag; in addition, the system will have some fixed memory overhead. Therefore, the system memory required for each DB can be calculated by the following formula:

```
Database Memory Size = maxVgroupsPerDb * (blocks * cache + 10MB) + numOfTables * (tagSizePerTable + 0.5KB)
```

Example: Assuming a 4-core machine, cache is the default size of 16M, blocks is the default value of 6, assuming there are 100,000 tables, and the total tag length is 256 bytes, the total memory requirement is: 4 * (16 * 6 + 10) + 100,000 * (0.25 + 0.5)/1000 = 499M.

The actual running system often stores the data in different DBs according to different characteristics of the data. All these shall be considered when planning.

If there is plenty of memory, the configuration of Blocks can be increased so that more data will be stored in memory and the query speed will be improved.

### CPU requirements

CPU requirements depend on the following two aspects:

- **Data insertion** TDengine single core can handle at least 10,000 insertion requests per second. Each insertion request can take multiple records, and inserting one record at a time is almost the same as inserting 10 records in computing resources consuming. Therefore, the larger the number of inserts, the higher the insertion efficiency. If an insert request has more than 200 records, a single core can insert 1 million records per second. However, the faster the insertion speed, the higher the requirement for front-end data collection, because records need to be cached and then inserted in batches.
- **Query requirements** TDengine to provide efficient queries, but the queries in each scenario vary greatly and the query frequency too, making it difficult to give objective figures. Users need to write some query statements for their own scenes to determine.

Therefore, only for data insertion, CPU can be estimated, but the computing resources consumed by query cannot be that clear. In the actual operation, it is not recommended to make CPU utilization rate over 50%. After that, new nodes need to be added to bring more computing resources.

### Storage requirements

Compared with general databases, TDengine has an ultra-high compression ratio. In most scenarios, the compression ratio of TDengine will not be less than 5:1, and in some scenarios, maybe over 10:1, depending on the actual data characteristics. The raw data size before compressed can be calculated as follows:

```
Raw DataSize = numOfTables * rowSizePerTable * rowsPerTable
```

Example: 10 million smart meters, each meter collects data every 15 minutes, and the data collected each time is 128 bytes, so the original data amount in one year is: 10000000 * 128 * 24 * 60/15 * 365 = 44.8512 T. The TDengine consumes approximately 44.851/5 = 8.97024 T.

User can set the maximum retention time of data on disk through parameter `keep`. In order to further reduce the storage cost, TDengine also provides tiered storage. The coldest data can be stored on the cheapest storage media. Application access does not need to be adjusted, but lower reading speed.

To improve speed, multiple hard disks can be configured so that data can be written or read concurrently. It should be reminded that TDengine provides high reliability of data in the form of multiple replicas, so it is no longer necessary to use expensive disk arrays.

### Number of physical or virtual machines

According to the above estimation of memory, CPU and storage, we can know how many cores, how much memory and storage space the whole system needs. If the number of data replicas is not 1, the total demand needs to be multiplied by the number of replicas.

Because TDengine provides great scale-out feature, it is easy to decide how many physical or virtual machines need to be purchased according to the total amount and the resources of a single physical/ virtual machine.

**Calculate CPU, memory and storage immediately, see:** [**Resource Estimation**](https://www.taosdata.com/config/config.html)

### Fault Tolerance and Disaster Recovery

### Fault tolerance

TDengine supports WAL (Write Ahead Log) mechanism to realize fault tolerance of data and ensure high-availability of data.

When TDengine receives the application's request packet, it first writes the requested original packet into the database log file, and then deletes the corresponding WAL after the data is successfully written. This ensures that TDengine can recover data from the database log file when the service is restarted due to power failure or other reasons, thus avoiding data loss.

There are two system configuration parameters involved:

- walLevel: WAL level, 0: do not write wal; 1: write wal, but do not execute fsync; 2: write wal and execute fsync.
- fsync: the cycle in which fsync is executed when walLevel is set to 2. Setting to 0 means that fsync is executed immediately whenever there is a write.

To guarantee 100% data safe, you need to set walLevel to 2 and fsync to 0. In that way, the write speed will decrease. However, if the number of threads starting to write data on the application side reaches a certain number (more than 50), the performance of writing data will also be good, only about 30% lower than that of fsync set to 3000 milliseconds.

### Disaster recovery

The cluster of TDengine provides high-availability of the system and implements disaster recovery through the multipl-replica mechanism.

TDengine cluster is managed by mnode. In order to ensure the high reliability of the mnode, multiple mnode replicas can be configured. The number of replicas is determined by system configuration parameter numOfMnodes. In order to support high reliability, it needs to be set to be greater than 1. In order to ensure the strong consistency of metadata, mnode replicas duplicate data synchronously to ensure the strong consistency of metadata.

The number of replicas of time-series data in TDengine cluster is associated with databases. There can be multiple databases in a cluster, and each database can be configured with different replicas. When creating a database, specify the number of replicas through parameter replica. In order to support high reliability, it is necessary to set the number of replicas greater than 1.

The number of nodes in TDengine cluster must be greater than or equal to the number of replicas, otherwise an error will be reported in table creation.

When the nodes in TDengine cluster are deployed on different physical machines and multiple replicas are set, the high reliability of the system is implemented without using other software or tools. TDengine Enterprise Edition can also deploy replicas in different server rooms, thus realizing remote disaster recovery.

## <a class="anchor" id="config"></a> Server-side Configuration

The background service of TDengine system is provided by taosd, and the configuration parameters can be modified in the configuration file taos.cfg to meet the requirements of different scenarios. The default location of the configuration file is the /etc/taos directory, which can be specified by executing the parameter -c from the taosd command line. Such as taosd-c/home/user, to specify that the configuration file is located in the /home/user directory.

You can also use “-C” to show the current server configuration parameters:

```
taosd -C
```

Only some important configuration parameters are listed below. For more parameters, please refer to the instructions in the configuration file. Please refer to the previous chapters for detailed introduction and function of each parameter, and the default of these parameters is working and generally does not need to be set. **Note: After the configuration is modified, \*taosd service\* needs to be restarted to take effect.**

- firstEp: end point of the first dnode in the actively connected cluster when taosd starts, the default value is localhost: 6030.
- fqdn: FQDN of the data node, which defaults to the first hostname configured by the operating system. If you are accustomed to IP address access, you can set it to the IP address of the node.
- serverPort: the port number of the external service after taosd started, the default value is 6030.
- httpPort: the port number used by the RESTful service to which all HTTP requests (TCP) require a query/write request. The default value is 6041.
- dataDir: the data file directory to which all data files will be written. [Default:/var/lib/taos](http://default/var/lib/taos).
- logDir: the log file directory to which the running log files of the client and server will be written. [Default:/var/log/taos](http://default/var/log/taos).
- arbitrator: the end point of the arbiter in the system; the default value is null.
- role: optional role for dnode. 0-any; it can be used as an mnode and to allocate vnodes; 1-mgmt; It can only be an mnode, but not to allocate vnodes; 2-dnode; caannot be an mnode, only vnode can be allocated
- debugFlage: run the log switch. 131 (output error and warning logs), 135 (output error, warning, and debug logs), 143 (output error, warning, debug, and trace logs). Default value: 131 or 135 (different modules have different default values).
- numOfLogLines: the maximum number of lines allowed for a single log file. Default: 10,000,000 lines.
- logKeepDays: the maximum retention time of the log file. When it is greater than 0, the log file will be renamed to taosdlog.xxx, where xxx is the timestamp of the last modification of the log file in seconds. Default: 0 days.
- maxSQLLength: the maximum length allowed for a single SQL statement. Default: 65380 bytes.
- telemetryReporting: whether TDengine is allowed to collect and report basic usage information. 0 means not allowed, and 1 means allowed. Default: 1.
- stream: whether continuous query (a stream computing function) is enabled, 0 means not allowed, 1 means allowed. Default: 1.
- queryBufferSize: the amount of memory reserved for all concurrent queries. The calculation rule can be multiplied by the number of the table according to the maximum possible concurrent number in practical application, and then multiplied by 170. The unit is MB (in versions before 2.0. 15, the unit of this parameter is byte).
- ratioOfQueryCores: set the maximum number of query threads. The minimum value of 0 means that there is only one query thread; the maximum value of 2 indicates that the maximum number of query threads established is 2 times the number of CPU cores. The default is 1, which indicates the maximum number of query threads equals to the number of CPU cores. This value can be a decimal, that is, 0.5 indicates that the query thread with half of the maximum CPU cores is established.

**Note:** for ports, TDengine will use 13 continuous TCP and UDP port numbers from serverPort, so be sure to open them in the firewall. Therefore, if it is the default configuration, a total of 13 ports from 6030 to 6042 need to be opened, and the same for both TCP and UDP.

Data in different application scenarios often have different data characteristics, such as retention days, number of replicas, collection frequency, record size, number of collection points, compression, etc. In order to obtain the best efficiency in storage, TDengine provides the following storage-related system configuration parameters:

- days: the time span for a data file to store data, in days, the default value is 10.
- keep: the number of days to keep data in the database, in days, default value: 3650.
- minRows: the minimum number of records in a file block, in pieces, default: 100.
- maxRows: the maximum number of records in a file block, in pieces, default: 4096.
- comp: file compression flag bit, 0: off; 1: one-stage compression; 2: two-stage compression. Default: 2.
- walLevel: WAL level. 1: write wal, but do not execute fsync; 2: write wal and execute fsync. Default: 1.
- fsync: the period during which fsync is executed when wal is set to 2. Setting to 0 means that fsync is executed immediately whenever a write happens, in milliseconds, and the default value is 3000.
- cache: the size of the memory block in megabytes (MB), default: 16.
- blocks: how many cache-sized memory blocks are in each VNODE (TSDB). Therefore, the memory size used by a VNODE is roughly (cache * blocks), in blocks, and the default value is 4.
- replica: number of replicas; value range: 1-3, in items, default value: 1
- precision: timestamp precision identification, ms for milliseconds and us for microseconds. Default: ms
- cacheLast: whether the sub-table last_row is cached in memory, 0: off; 1: on. Default: 0. (This parameter is supported as of version 2.0. 11)

For an application scenario, there may be data with multiple characteristics coexisting. The best design is to put tables with the same data characteristics in one database. Such an application has multiple databases, and each one can be configured with different storage parameters, thus ensuring the optimal performance of the system. TDengine allows the application to specify the above storage parameter in database creation. If specified, the parameters will override the corresponding system configuration parameters. For example, there is the following SQL:

```
 create database demo days 10 cache 32 blocks 8 replica 3 update 1;
```

The SQL creates a database demo, each data file stores 10 days of data, the memory block is 32 megabytes, each VNODE occupies 8 memory blocks, the number of replicas is 3, updates are allowed, and other parameters are completely consistent with the system configuration.

When adding a new dnode to the TDengine cluster, some parameters related to the cluster must be the same as the configuration of the existing cluster, otherwise it cannot be successfully added to the cluster. The parameters that will be verified are as follows:

- numOfMnodes: the number of management nodes in the system. Default: 3. (Since version 2.0.20.11 and version 2.1.6.0, the default value of "numOfMnodes" has been changed to 1.)
- balance: whether to enable load balancing. 0: No, 1: Yes. Default: 1.
- mnodeEqualVnodeNum: an mnode is equal to the number of vnodes consumed. Default: 4.
- offlineThreshold: the threshold for a dnode to be offline, exceed which the dnode will be removed from the cluster. The unit is seconds, and the default value is 86400*10 (that is, 10 days).
- statusInterval: the length of time dnode reports status to mnode. The unit is seconds, and the default value is 1.
- maxTablesPerVnode: the maximum number of tables that can be created in each vnode. Default: 1000000.
- maxVgroupsPerDb: the maximum number of vgroups that can be used in each database.
- arbitrator: the end point of the arbiter in system, which is empty by default.
- See Client Configuration for the configuration of timezone, locale and charset.

For the convenience of debugging, the log configuration of each dnode can be temporarily adjusted through SQL statements, and all will be invalid after system restarting:

```mysql
ALTER DNODE <dnode_id> <config>
```

- dnode_id: available from the SQL statement "SHOW DNODES" command
- config: the log parameter to be adjusted, and the value is taken in the following list

resetlog truncates the old log file and creates a new log file debugFlag < 131 135 143 > Set debugFlag to 131, 135 or 143.

For example:

```
    alter dnode 1 debugFlag 135;
```

## <a class="anchor" id="client"></a> Client Configuration

The foreground interactive client application of TDengine system is taos and application driver, which shares the same configuration file taos.cfg with taosd. When running taos, use the parameter -c to specify the configuration file directory, such as taos-c/home/cfg, which means using the parameters in the taos.cfg configuration file under the /home/cfg/ directory. The default directory is /etc/taos. For more information on how to use taos, see the help information taos --help. This section mainly describes the parameters used by the taos client application in the configuration file taos.cfg.

**Versions after 2.0. 10.0 support the following parameters on command line to display the current client configuration parameters**

```bash
taos -C  或  taos --dump-config
```

Client configuration parameters:

- firstEp: end point of the first taosd instance in the actively connected cluster when taos is started, the default value is localhost: 6030.
- secondEp: when taos starts, if not impossible to connect to firstEp, it will try to connect to secondEp.
- locale
    Default value: obtained dynamically from the system. If the automatic acquisition fails, user needs to set it in the configuration file or through API
    
    TDengine provides a special field type nchar for storing non-ASCII encoded wide characters such as Chinese, Japanese and Korean. The data written to the nchar field will be uniformly encoded in UCS4-LE format and sent to the server. It should be noted that the correctness of coding is guaranteed by the client. Therefore, if users want to normally use nchar fields to store non-ASCII characters such as Chinese, Japanese, Korean, etc., it’s needed to set the encoding format of the client correctly.
    
    The characters inputted by the client are all in the current default coding format of the operating system, mostly UTF-8 on Linux systems, and some Chinese system codes may be GB18030 or GBK, etc. The default encoding in the docker environment is POSIX. In the Chinese versions of Windows system, the code is CP936. The client needs to ensure that the character set it uses is correctly set, that is, the current encoded character set of the operating system running by the client, in order to ensure that the data in nchar is correctly converted into UCS4-LE encoding format.
    
    The naming rules of locale in Linux are: < language > _ < region >. < character set coding >, such as: zh_CN.UTF-8, zh stands for Chinese, CN stands for mainland region, and UTF-8 stands for character set. Character set encoding provides a description of encoding transformations for clients to correctly parse local strings. Linux system and Mac OSX system can determine the character encoding of the system by setting locale. Because the locale used by Windows is not the POSIX standard locale format, another configuration parameter charset is needed to specify the character encoding under Windows. You can also use charset to specify character encoding in Linux systems.

- charset

    Default value: obtained dynamically from the system. If the automatic acquisition fails, user needs to set it in the configuration file or through API
    
    If charset is not set in the configuration file, in Linux system, when taos starts up, it automatically reads the current locale information of the system, and parses and extracts the charset encoding format from the locale information. If the automatic reading of locale information fails, an attempt is made to read the charset configuration, and if the reading of the charset configuration also fails, the startup process is interrupted.
    
    In Linux system, locale information contains character encoding information, so it is unnecessary to set charset separately after setting locale of Linux system correctly. For example:
    
    ```
    locale zh_CN.UTF-8
    ```
    On Windows systems, the current system encoding cannot be obtained from locale. If string encoding information cannot be read from the configuration file, taos defaults to CP936. It is equivalent to adding the following to the configuration file:
    ```
    charset CP936
    ```
    If you need to adjust the character encoding, check the encoding used by the current operating system and set it correctly in the configuration file.
    
    In Linux systems, if user sets both locale and charset encoding charset, and the locale and charset are inconsistent, the value set later will override the value set earlier.
    ```
    locale zh_CN.UTF-8
    charset GBK
    ```
    The valid value for charset is GBK.
    
    And the valid value for charset is UTF-8.
    
    The configuration parameters of log are exactly the same as those of server.

- timezone

    Default value: get the current time zone option dynamically from the system

    The time zone in which the client runs the system. In order to deal with the problem of data writing and query in multiple time zones, TDengine uses Unix Timestamp to record and store timestamps. The characteristics of UNIX timestamps determine that the generated timestamps are consistent at any time regardless of any time zone. It should be noted that UNIX timestamps are converted and recorded on the client side. In order to ensure that other forms of time on the client are converted into the correct Unix timestamp, the correct time zone needs to be set.

    In Linux system, the client will automatically read the time zone information set by the system. Users can also set time zones in profiles in a number of ways. For example:
    ```
    timezone UTC-8
    timezone GMT-8
    timezone Asia/Shanghai
    ```
    
    All above are legal to set the format of the East Eight Zone.
    
    The setting of time zone affects the content of non-Unix timestamp (timestamp string, parsing of keyword now) in query and writing SQL statements. For example:

    ```sql
    SELECT count(*) FROM table_name WHERE TS<'2019-04-11 12:01:08';
    ```
    
    In East Eight Zone, the SQL statement is equivalent to
    ```sql
    SELECT count(*) FROM table_name WHERE TS<1554955268000;
    ```
    
    In the UTC time zone, the SQL statement is equivalent to
    ```sql
    SELECT count(*) FROM table_name WHERE TS<1554984068000;
    ```
    In order to avoid the uncertainty caused by using string time format, Unix timestamp can also be used directly. In addition, timestamp strings with time zones can also be used in SQL statements, such as: timestamp strings in RFC3339 format, 2013-04-12T15:52:01.123+08:00, or ISO-8601 format timestamp strings 2013-04-12T15:52:01.123+0800. The conversion of the above two strings into Unix timestamps is not affected by the time zone in which the system is located.
    
    When starting taos, you can also specify an end point for an instance of taosd from the command line, otherwise read from taos.cfg.

- maxBinaryDisplayWidth
    The upper limit of the display width of binary and nchar fields in a shell, beyond which parts will be hidden. Default: 30. You can modify this option dynamically in the shell with the command set max_binary_display_width nn.

## <a class="anchor" id="user"></a>User Management

System administrators can add and delete users in CLI, and also modify passwords. The SQL syntax in the CLI is as follows:

```sql
CREATE USER <user_name> PASS <'password'>;
```

Create a user, and specify the user name and password. The password needs to be enclosed in single quotation marks. The single quotation marks are in English half-width.

```sql
DROP USER <user_name>;
```

Delete a user, root only.

```sql
ALTER USER <user_name> PASS <'password'>;
```

Modify the user password. In order to avoid being converted to lowercase, the password needs to be quoted in single quotation marks. The single quotation marks are in English half-width

```sql
ALTER USER <user_name> PRIVILEGE <write|read>;
```

Modify the user privilege to: write or read, without adding single quotation marks.

Note: There are three privilege levels: super/write/read in the system, but it is not allowed to give super privilege to users through alter instruction at present.

```mysql
SHOW USERS;
```

Show all users

**Note:** In SQL syntax, < > indicates the part that requires user to input, but do not enter < > itself

## <a class="anchor" id="import"></a> Import Data

TDengine provides a variety of convenient data import functions, including imported by script file, by data file, and by taosdump tool.

**Import by script file**

TDengine shell supports source filename command, which is used to run SQL statements from a file in batch. Users can write SQL commands such as database building, table building and data writing in the same file. Each command has a separate line. By running source command in the shell, SQL statements in the file can be run in batches in sequence. SQL statements beginning with '#' are considered comments and are automatically ignored by the shell.

**Import by data file**

TDengine also supports data import from CSV files on existing tables in the shell. The CSV file belongs to only one table, and the data format in the CSV file should be the same as the structure of the table to be imported. When importing, its syntax is as follows:

```mysql
insert into tb1 file 'path/data.csv';
```

Note: if there is descriptive information in the first line of the CSV file, please delete it manually before importing

For example, there is now a sub-table d1001 whose table structure is as follows:

```mysql
taos> DESCRIBE d1001
             Field              |        Type        |   Length    |    Note    |
=================================================================================
 ts                             | TIMESTAMP          |           8 |            |
 current                        | FLOAT              |           4 |            |
 voltage                        | INT                |           4 |            |
 phase                          | FLOAT              |           4 |            |
 location                       | BINARY             |          64 | TAG        |
 groupid                        | INT                |           4 | TAG        |
```

And the format of the data.csv to import is as follows:

```csv
'2018-10-04 06:38:05.000',10.30000,219,0.31000
'2018-10-05 06:38:15.000',12.60000,218,0.33000
'2018-10-06 06:38:16.800',13.30000,221,0.32000
'2018-10-07 06:38:05.000',13.30000,219,0.33000
'2018-10-08 06:38:05.000',14.30000,219,0.34000
'2018-10-09 06:38:05.000',15.30000,219,0.35000
'2018-10-10 06:38:05.000',16.30000,219,0.31000
'2018-10-11 06:38:05.000',17.30000,219,0.32000
'2018-10-12 06:38:05.000',18.30000,219,0.31000
```

Then we can use the following command to import:

```mysql
taos> insert into d1001 file '~/data.csv';
Query OK, 9 row(s) affected (0.004763s)
```

**Import via taosdump tool**

TDengine provides a convenient database import and export tool, taosdump. Users can import data exported by taosdump from one system into other systems. Please refer to the blog: [User Guide of TDengine DUMP Tool](https://www.taosdata.com/blog/2020/03/09/1334.html).

## <a class="anchor" id="export"></a> Export Data

To facilitate data export, TDengine provides two export methods, namely, export by table and export by taosdump.

**Export CSV file by table**

If user needs to export data from a table or a STable, it can run in a shell

```mysql
select * from <tb_name> >> data.csv;
```

In this way, the data in table tb_name will be exported to the file data.csv in CSV format.

**Export data by taosdump**

TDengine provides a convenient database export tool, taosdump. Users can choose to export all databases, a database or a table in a database, all data or data for a time period, or even just the definition of a table as needed. Please refer to the blog: [User Guide of TDengine DUMP Tool](https://www.taosdata.com/blog/2020/03/09/1334.html)

## <a class="anchor" id="status"></a> System Connection and Task Query Management

The system administrator can query the connection, ongoing query and stream computing of the system from CLI, and can close the connection and stop the ongoing query and stream computing. The SQL syntax in the CLI is as follows:

```mysql
SHOW CONNECTIONS;
```

Show the connection of the database, and one column shows ip: port, which is the IP address and port number of the connection.

```mysql
KILL CONNECTION <connection-id>;
```

Force the database connection to close, where connection-id is the number in the first column displayed in SHOW CONNECTIONS.

```mysql
SHOW QUERIES;
```

Show the data query, where the two numbers separated by colons displayed in the first column are query-id and the connection-id that initiated the query application connection and the number of queries.

```mysql
KILL QUERY <query-id>;
```

Force to close the data query, where query-id is the connection-id: query-no string displayed in SHOW QUERIES, such as "105: 2", copy and paste it.

```mysql
SHOW STREAMS;
```

Show the stream computing, where the first column shows the two numbers separated by colons as stream-id and the connection-id to start the stream application connection and the number of times the stream was initiated.

```mysql
KILL STREAM <stream-id>;
```

Force to turn off the stream computing, in which stream-id is the connection-id: stream-no string displayed in SHOW STREAMS, such as 103: 2, copy and paste it.

## System Monitoring

After TDengine is started, it will automatically create a monitoring database log and write the server's CPU, memory, hard disk space, bandwidth, number of requests, disk read-write speed, slow query and other information into the database regularly. TDengine also records important system operations (such as logging in, creating, deleting databases, etc.) logs and various error alarm information and stores them in the log database. The system administrator can view the database directly from CLI or view the monitoring information through GUI on WEB.

The collection of these monitoring metrics is turned on by default, but you can modify option enableMonitor in the configuration file to turn it off or on.

## <a class="anchor" id="directories"></a> File Directory Structure

After installing TDengine, the following directories or files are generated in the operating system by default:



| **Directory/File**        | **Description**                                              |
| ------------------------- | ------------------------------------------------------------ |
| /usr/local/taos/bin       | TEngine’s executable directory. The executables are connected to the/usr/bin directory via softly links. |
| /usr/local/taos/connector | TDengine’s various connector directories.                    |
| /usr/local/taos/driver    | TDengine’s dynamic link library directory. Connect to /usr/lib directory via soft links. |
| /usr/local/taos/examples  | TDengine’s application example directory for various languages. |
| /usr/local/taos/include   | TDengine’s header files of C interface for externally serving. |
| /etc/taos/taos.cfg        | TDengine’s default [configuration files].                    |
| /var/lib/taos             | TDengine’s default data file directory, where the local can be modified via [configuration files]. |
| /var/log/taos             | TDengine’s default log file directory, where the local can be modified via [configuration files]. |

**Executables**

All executables of TDengine are stored in the directory /usr/local/taos/bin by default. Including:

- *taosd*: TDengine server-side executable
- *taos*: TDengine Shell executable
- *taosdump*: A data import/export tool
- remove.sh: uninstall the TDengine script, please execute carefully, and link to rmtaos command in the/usr/bin directory. The TDengine installation directory /usr/local/taos will be removed, but/etc/taos,/var/lib/taos,/var/log/taos will remain.

You can configure different data directories and log directories by modifying system configuration file taos.cfg.

## <a class="anchor" id="keywords"></a> TDengine Parameter Limits and Reserved Keywords

- Database name: cannot contain "." and other special characters, and cannot exceed 32 characters
- Table name: cannot contain "." and other special characters, and cannot exceed 192 characters together with the database name to which it belongs
- Table column name: cannot contain special characters, and cannot exceed 64 characters
- Database name, table name, column name cannot begin with a number
- Number of columns in table: cannot exceed 1024 columns
- Maximum length of record: including 8 bytes as timestamp, no more than 16KB (each column of BINARY/NCHAR type will occupy an additional 2 bytes of storage location)
- Default maximum string length for a single SQL statement: 65480 bytes
- Number of database replicas: no more than 3
- User name: no more than 23 bytes
- User password: no more than 15 bytes
- Number of Tags: no more than 128
- Total length of label: cannot exceed 16K bytes
- Number of records: limited by storage space only
- Number of tables: limited only by the number of nodes
- Number of databases: limited only by the number of nodes
- Number of virtual nodes on a single database: cannot exceed 64

At the moment, TDengine has nearly 200 internal reserved keywords, which cannot be used as database name, table name, STable name, data column name or tag column name regardless of case. The list of these keywords is as follows:

| **List of Keywords** |             |              |            |           |
| -------------------- | ----------- | ------------ | ---------- | --------- |
| ABLOCKS              | CONNECTIONS | GT           | MNODES     | SLIDING   |
| ABORT                | COPY        | ID           | MODULES    | SLIMIT    |
| ACCOUNT              | COUNT       | IF           | NCHAR      | SMALLINT  |
| ACCOUNTS             | CREATE      | IGNORE       | NE         | SPREAD    |
| ADD                  | CTIME       | IMMEDIATE    | NONE       | STABLE    |
| AFTER                | DATABASE    | IMPORT       | NOT        | STABLES   |
| ALL                  | DATABASES   | IN           | NOTNULL    | STAR      |
| ALTER                | DAYS        | INITIALLY    | NOW        | STATEMENT |
| AND                  | DEFERRED    | INSERT       | OF         | STDDEV    |
| AS                   | DELIMITERS  | INSTEAD      | OFFSET     | STREAM    |
| ASC                  | DESC        | INTEGER      | OR         | STREAMS   |
| ATTACH               | DESCRIBE    | INTERVAL     | ORDER      | STRING    |
| AVG                  | DETACH      | INTO         | PASS       | SUM       |
| BEFORE               | DIFF        | IP           | PERCENTILE | TABLE     |
| BEGIN                | DISTINCT    | IS           | PLUS       | TABLES    |
| BETWEEN              | DIVIDE      | ISNULL       | PRAGMA     | TAG       |
| BIGINT               | DNODE       | JOIN         | PREV       | TAGS      |
| BINARY               | DNODES      | KEEP         | PRIVILEGE  | TBLOCKS   |
| BITAND               | DOT         | KEY          | QUERIES    | TBNAME    |
| BITNOT               | DOUBLE      | KILL         | QUERY      | TIMES     |
| BITOR                | DROP        | LAST         | RAISE      | TIMESTAMP |
| BOOL                 | EACH        | LE           | REM        | TINYINT   |
| BOTTOM               | END         | LEASTSQUARES | REPLACE    | TOP       |
| BY                   | EQ          | LIKE         | REPLICA    | TRIGGER   |
| CACHE                | EXISTS      | LIMIT        | RESET      | UMINUS    |
| CASCADE              | EXPLAIN     | LINEAR       | RESTRICT   | UPLUS     |
| CHANGE               | FAIL        | LOCAL        | ROW        | USE       |
| CLOG                 | FILL        | LP           | ROWS       | USER      |
| CLUSTER              | FIRST       | LSHIFT       | RP         | USERS     |
| COLON                | FLOAT       | LT           | RSHIFT     | USING     |
| COLUMN               | FOR         | MATCH        | SCORES     | VALUES    |
| COMMA                | FROM        | MAX          | SELECT     | VARIABLE  |
| COMP                 | GE          | METRIC       | SEMI       | VGROUPS   |
| CONCAT               | GLOB        | METRICS      | SET        | VIEW      |
| CONFIGS              | GRANTS      | MIN          | SHOW       | WAVG      |
| CONFLICT             | GROUP       | MINUS        | SLASH      | WHERE     |
| CONNECTION           |             |              |            |           |
