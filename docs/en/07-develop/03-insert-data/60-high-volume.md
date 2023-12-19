---
title: High Performance Writing
sidebar_label: High Performance Writing
description: This document describes how to achieve high performance when writing data into TDengine.
---

import Tabs from "@theme/Tabs";
import TabItem from "@theme/TabItem";

This chapter introduces how to write data into TDengine with high throughput.

## How to achieve high performance data writing

To achieve high performance writing, there are a few aspects to consider. In the following sections we will describe these important factors in achieving high performance writing.

### Application Program

From the perspective of application program, you need to consider:

1. The data size of each single write, also known as batch size. Generally speaking, higher batch size generates better writing performance. However, once the batch size is over a specific value, you will not get any additional benefit anymore. When using SQL to write into TDengine, it's better to put as much as possible data in single SQL. The maximum SQL length supported by TDengine is 1,048,576 bytes, i.e. 1 MB.

2. The number of concurrent connections. Normally more connections can get better result. However, once the number of connections exceeds the processing ability of the server side, the performance may downgrade.

3. The distribution of data to be written across tables or sub-tables. Writing to single table in one batch is more efficient than writing to multiple tables in one batch.

4. Data Writing Protocol.
   - Parameter binding mode is more efficient than SQL because it doesn't have the cost of parsing SQL.
   - Writing to known existing tables is more efficient than writing to uncertain tables in automatic creating mode because the later needs to check whether the table exists or not before actually writing data into it.
   - Writing in SQL is more efficient than writing in schemaless mode because schemaless writing creates table automatically and may alter table schema.

Application programs need to take care of the above factors and try to take advantage of them. The application program should write to single table in each write batch. The batch size needs to be tuned to a proper value on a specific system. The number of concurrent connections needs to be tuned to a proper value too to achieve the best writing throughput.

### Data Source

Application programs need to read data from data source then write into TDengine. If you meet one or more of below situations, you need to setup message queues between the threads for reading from data source and the threads for writing into TDengine.

1. There are multiple data sources, the data generation speed of each data source is much slower than the speed of single writing thread. In this case, the purpose of message queues is to consolidate the data from multiple data sources together to increase the batch size of single write.
2. The speed of data generation from single data source is much higher than the speed of single writing thread. The purpose of message queue in this case is to provide buffer so that data is not lost and multiple writing threads can get data from the buffer.
3. The data for single table are from multiple data source. In this case the purpose of message queues is to combine the data for single table together to improve the write efficiency.

If the data source is Kafka, then the application program is a consumer of Kafka, you can benefit from some kafka features to achieve high performance writing:

1. Put the data for a table in single partition of single topic so that it's easier to put the data for each table together and write in batch
2. Subscribe multiple topics to accumulate data together.
3. Add more consumers to gain more concurrency and throughput.
4. Incrase the size of single fetch to increase the size of write batch.

### Tune TDengine

On the server side, database configuration parameter `vgroups` needs to be set carefully to maximize the system performance. If it's set too low, the system capability can't be utilized fully; if it's set too big, unnecessary resource competition may be produced. A normal recommendation for `vgroups` parameter is 2 times of the number of CPU cores. However, depending on the actual system resources, it may still need to tuned.

For more configuration parameters, please refer to [Database Configuration](../../../taos-sql/database) and [Server Configuration](../../../reference/config).

## Sample Programs

This section will introduce the sample programs to demonstrate how to write into TDengine with high performance.

### Scenario

Below are the scenario for the sample programs of high performance writing.

- Application program reads data from data source, the sample program simulates a data source by generating data
- The speed of single writing thread is much slower than the speed of generating data, so the program starts multiple writing threads while each thread establish a connection to TDengine and each thread has a message queue of fixed size.
- Application program maps the received data to different writing threads based on table name to make sure all the data for each table is always processed by a specific writing thread.
- Each writing thread writes the received data into TDengine once the message queue becomes empty or the read data meets a threshold.

![Thread Model of High Performance Writing into TDengine](highvolume.webp)

### Sample Programs

The sample programs listed in this section are based on the scenario described previously. If your scenarios is different, please try to adjust the code based on the principles described in this chapter.

The sample programs assume the source data is for all the different sub tables in same super table (meters). The super table has been created before the sample program starts to writing data. Sub tables are created automatically according to received data. If there are multiple super tables in your case, please try to adjust the part of creating table automatically.

<Tabs defaultValue="java" groupId="lang">
<TabItem label="Java" value="java">

**Program Inventory**

| Class            | Description                                                                                           |
| ---------------- | ----------------------------------------------------------------------------------------------------- |
| FastWriteExample | Main Program                                                                                          |
| ReadTask         | Read data from simulated data source and put into a queue according to the hash value of table name   |
| WriteTask        | Read data from Queue, compose a write batch and write into TDengine                                   |
| MockDataSource   | Generate data for some sub tables of super table meters                                               |
| SQLWriter        | WriteTask uses this class to compose SQL, create table automatically, check SQL length and write data |
| StmtWriter       | Write in Parameter binding mode (Not finished yet)                                                    |
| DataBaseMonitor  | Calculate the writing speed and output on console every 10 seconds                                    |

Below is the list of complete code of the classes in above table and more detailed description.

<details>
<summary>FastWriteExample</summary>
The main Program is responsible for:

1. Create message queues
2. Start writing threads
3. Start reading threads
4. Output writing speed every 10 seconds

The main program provides 4 parameters for tuning:

1. The number of reading threads, default value is 1
2. The number of writing threads, default value is 2
3. The total number of tables in the generated data, default value is 1000. These tables are distributed evenly across all writing threads. If the number of tables is very big, it will cost much time to firstly create these tables.
4. The batch size of single write, default value is 3,000

The capacity of message queue also impacts performance and can be tuned by modifying program. Normally it's always better to have a larger message queue. A larger message queue means lower possibility of being blocked when enqueueing and higher throughput. But a larger message queue consumes more memory space. The default value used in the sample programs is already big enough.

```java
{{#include docs/examples/java/src/main/java/com/taos/example/highvolume/FastWriteExample.java}}
```

</details>

<details>
<summary>ReadTask</summary>

ReadTask reads data from data source. Each ReadTask is associated with a simulated data source, each data source generates data for a group of specific tables, and the data of any table is only generated from a single specific data source.

ReadTask puts data in message queue in blocking mode. That means, the putting operation is blocked if the message queue is full.

```java
{{#include docs/examples/java/src/main/java/com/taos/example/highvolume/ReadTask.java}}
```

</details>

<details>
<summary>WriteTask</summary>

```java
{{#include docs/examples/java/src/main/java/com/taos/example/highvolume/WriteTask.java}}
```

</details>

<details>

<summary>MockDataSource</summary>

```java
{{#include docs/examples/java/src/main/java/com/taos/example/highvolume/MockDataSource.java}}
```

</details>

<details>

<summary>SQLWriter</summary>

SQLWriter class encapsulates the logic of composing SQL and writing data. Please be noted that the tables have not been created before writing, but are created automatically when catching the exception of table doesn't exist. For other exceptions caught, the SQL which caused the exception are logged for you to debug.

```java
{{#include docs/examples/java/src/main/java/com/taos/example/highvolume/SQLWriter.java}}
```

</details>

<details>

<summary>DataBaseMonitor</summary>

```java
{{#include docs/examples/java/src/main/java/com/taos/example/highvolume/DataBaseMonitor.java}}
```

</details>

**Steps to Launch**

<details>
<summary>Launch Java Sample Program</summary>

You need to set environment variable `TDENGINE_JDBC_URL` before launching the program. If TDengine Server is setup on localhost, then the default value for user name, password and port can be used, like below:

```
TDENGINE_JDBC_URL="jdbc:TAOS://localhost:6030?user=root&password=taosdata"
```

**Launch in IDE**

1. Clone TDengine repository
   ```
   git clone git@github.com:taosdata/TDengine.git --depth 1
   ```
2. Use IDE to open `docs/examples/java` directory
3. Configure environment variable `TDENGINE_JDBC_URL`, you can also configure it before launching the IDE, if so you can skip this step.
4. Run class `com.taos.example.highvolume.FastWriteExample`

**Launch on server**

If you want to launch the sample program on a remote server, please follow below steps:

1. Package the sample programs. Execute below command under directory `TDengine/docs/examples/java`:
   ```
   mvn package
   ```
2. Create `examples/java` directory on the server
   ```
   mkdir -p examples/java
   ```
3. Copy dependencies (below commands assume you are working on a local Windows host and try to launch on a remote Linux host)
   - Copy dependent packages
     ```
     scp -r .\target\lib <user>@<host>:~/examples/java
     ```
   - Copy the jar of sample programs
     ```
     scp -r .\target\javaexample-1.0.jar <user>@<host>:~/examples/java
     ```
4. Configure environment variable
   Edit `~/.bash_profile` or `~/.bashrc` and add below:

   ```
   export TDENGINE_JDBC_URL="jdbc:TAOS://localhost:6030?user=root&password=taosdata"
   ```

   If your TDengine server is not deployed on localhost or doesn't use default port, you need to change the above URL to correct value in your environment.

5. Launch the sample program

   ```
   java -classpath lib/*:javaexample-1.0.jar  com.taos.example.highvolume.FastWriteExample <read_thread_count>  <white_thread_count> <total_table_count> <max_batch_size>
   ```

6. The sample program doesn't exit unless you press <kbd>CTRL</kbd> + <kbd>C</kbd> to terminate it.
   Below is the output of running on a server of 16 cores, 64GB memory and SSD hard disk.

   ```
   root@vm85$ java -classpath lib/*:javaexample-1.0.jar  com.taos.example.highvolume.FastWriteExample 2 12
   18:56:35.896 [main] INFO  c.t.e.highvolume.FastWriteExample - readTaskCount=2, writeTaskCount=12 tableCount=1000 maxBatchSize=3000
   18:56:36.011 [WriteThread-0] INFO  c.taos.example.highvolume.WriteTask - started
   18:56:36.015 [WriteThread-0] INFO  c.taos.example.highvolume.SQLWriter - maxSQLLength=1048576
   18:56:36.021 [WriteThread-1] INFO  c.taos.example.highvolume.WriteTask - started
   18:56:36.022 [WriteThread-1] INFO  c.taos.example.highvolume.SQLWriter - maxSQLLength=1048576
   18:56:36.031 [WriteThread-2] INFO  c.taos.example.highvolume.WriteTask - started
   18:56:36.032 [WriteThread-2] INFO  c.taos.example.highvolume.SQLWriter - maxSQLLength=1048576
   18:56:36.041 [WriteThread-3] INFO  c.taos.example.highvolume.WriteTask - started
   18:56:36.042 [WriteThread-3] INFO  c.taos.example.highvolume.SQLWriter - maxSQLLength=1048576
   18:56:36.093 [WriteThread-4] INFO  c.taos.example.highvolume.WriteTask - started
   18:56:36.094 [WriteThread-4] INFO  c.taos.example.highvolume.SQLWriter - maxSQLLength=1048576
   18:56:36.099 [WriteThread-5] INFO  c.taos.example.highvolume.WriteTask - started
   18:56:36.100 [WriteThread-5] INFO  c.taos.example.highvolume.SQLWriter - maxSQLLength=1048576
   18:56:36.100 [WriteThread-6] INFO  c.taos.example.highvolume.WriteTask - started
   18:56:36.101 [WriteThread-6] INFO  c.taos.example.highvolume.SQLWriter - maxSQLLength=1048576
   18:56:36.103 [WriteThread-7] INFO  c.taos.example.highvolume.WriteTask - started
   18:56:36.104 [WriteThread-7] INFO  c.taos.example.highvolume.SQLWriter - maxSQLLength=1048576
   18:56:36.105 [WriteThread-8] INFO  c.taos.example.highvolume.WriteTask - started
   18:56:36.107 [WriteThread-8] INFO  c.taos.example.highvolume.SQLWriter - maxSQLLength=1048576
   18:56:36.108 [WriteThread-9] INFO  c.taos.example.highvolume.WriteTask - started
   18:56:36.109 [WriteThread-9] INFO  c.taos.example.highvolume.SQLWriter - maxSQLLength=1048576
   18:56:36.156 [WriteThread-10] INFO  c.taos.example.highvolume.WriteTask - started
   18:56:36.157 [WriteThread-11] INFO  c.taos.example.highvolume.WriteTask - started
   18:56:36.158 [WriteThread-10] INFO  c.taos.example.highvolume.SQLWriter - maxSQLLength=1048576
   18:56:36.158 [ReadThread-0] INFO  com.taos.example.highvolume.ReadTask - started
   18:56:36.158 [ReadThread-1] INFO  com.taos.example.highvolume.ReadTask - started
   18:56:36.158 [WriteThread-11] INFO  c.taos.example.highvolume.SQLWriter - maxSQLLength=1048576
   18:56:46.369 [main] INFO  c.t.e.highvolume.FastWriteExample - count=18554448 speed=1855444
   18:56:56.946 [main] INFO  c.t.e.highvolume.FastWriteExample - count=39059660 speed=2050521
   18:57:07.322 [main] INFO  c.t.e.highvolume.FastWriteExample - count=59403604 speed=2034394
   18:57:18.032 [main] INFO  c.t.e.highvolume.FastWriteExample - count=80262938 speed=2085933
   18:57:28.432 [main] INFO  c.t.e.highvolume.FastWriteExample - count=101139906 speed=2087696
   18:57:38.921 [main] INFO  c.t.e.highvolume.FastWriteExample - count=121807202 speed=2066729
   18:57:49.375 [main] INFO  c.t.e.highvolume.FastWriteExample - count=142952417 speed=2114521
   18:58:00.689 [main] INFO  c.t.e.highvolume.FastWriteExample - count=163650306 speed=2069788
   18:58:11.646 [main] INFO  c.t.e.highvolume.FastWriteExample - count=185019808 speed=2136950
   ```

</details>

</TabItem>
<TabItem label="Python" value="python">

**Program Inventory**

Sample programs in Python uses multi-process and cross-process message queues.

| Function/CLass               | Description                                                                 |
| ---------------------------- | --------------------------------------------------------------------------- |
| main Function                | Program entry point, create child processes and message queues              |
| run_monitor_process Function | Create database, super table, calculate writing speed and output to console |
| run_read_task Function       | Read data and distribute to message queues                                  |
| MockDataSource Class         | Simulate data source, return next 1,000 rows of each table                  |
| run_write_task Function      | Read as much as possible data from message queue and write in batch         |
| SQLWriter Class              | Write in SQL and create table automatically                                 |
| StmtWriter Class             | Write in parameter binding mode (not finished yet)                          |

<details>
<summary>main function</summary>

`main` function is responsible for creating message queues and fork child processes, there are 3 kinds of child processes:

1. Monitoring process, initializes database and calculating writing speed
2. Reading process (n), reads data from data source
3. Writing process (m), writes data into TDengine

`main` function provides 5 parameters:

1. The number of reading tasks, default value is 1
2. The number of writing tasks, default value is 1
3. The number of tables, default value is 1,000
4. The capacity of message queue, default value is 1,000,000 bytes
5. The batch size in single write, default value is 3000

```python
{{#include docs/examples/python/fast_write_example.py:main}}
```

</details>

<details>
<summary>run_monitor_process</summary>

Monitoring process initializes database and monitoring writing speed.

```python
{{#include docs/examples/python/fast_write_example.py:monitor}}
```

</details>

<details>

<summary>run_read_task function</summary>

Reading process reads data from other data system and distributes to the message queue allocated for it.

```python
{{#include docs/examples/python/fast_write_example.py:read}}
```

</details>

<details>

<summary>MockDataSource</summary>

Below is the simulated data source, we assume table name exists in each generated data.

```python
{{#include docs/examples/python/mockdatasource.py}}
```

</details>

<details>
<summary>run_write_task function</summary>

Writing process tries to read as much as possible data from message queue and writes in batch.

```python
{{#include docs/examples/python/fast_write_example.py:write}}
```

</details>

<details>

SQLWriter class encapsulates the logic of composing SQL and writing data. Please be noted that the tables have not been created before writing, but are created automatically when catching the exception of table doesn't exist. For other exceptions caught, the SQL which caused the exception are logged for you to debug. This class also checks the SQL length, and passes the maximum SQL length by parameter maxSQLLength according to actual TDengine limit.

<summary>SQLWriter</summary>

```python
{{#include docs/examples/python/sql_writer.py}}
```

</details>

**Steps to Launch**

<details>

<summary>Launch Sample Program in Python</summary>

1. Prerequisites

   - TDengine client driver has been installed
   - Python3 has been installed, the the version >= 3.8
   - TDengine Python client library `taospy` has been installed

2. Install faster-fifo to replace python builtin multiprocessing.Queue

   ```
   pip3 install faster-fifo
   ```

3. Click the "Copy" in the above sample programs to copy `fast_write_example.py`, `sql_writer.py`, and `mockdatasource.py`.

4. Execute the program

   ```
   python3  fast_write_example.py <READ_TASK_COUNT> <WRITE_TASK_COUNT> <TABLE_COUNT> <QUEUE_SIZE> <MAX_BATCH_SIZE>
   ```

   Below is the output of running on a server of 16 cores, 64GB memory and SSD hard disk.

   ```
   root@vm85$ python3 fast_write_example.py  8 8
   2022-07-14 19:13:45,869 [root] - READ_TASK_COUNT=8, WRITE_TASK_COUNT=8, TABLE_COUNT=1000, QUEUE_SIZE=1000000, MAX_BATCH_SIZE=3000
   2022-07-14 19:13:48,882 [root] - WriteTask-0 started with pid 718347
   2022-07-14 19:13:48,883 [root] - WriteTask-1 started with pid 718348
   2022-07-14 19:13:48,884 [root] - WriteTask-2 started with pid 718349
   2022-07-14 19:13:48,884 [root] - WriteTask-3 started with pid 718350
   2022-07-14 19:13:48,885 [root] - WriteTask-4 started with pid 718351
   2022-07-14 19:13:48,885 [root] - WriteTask-5 started with pid 718352
   2022-07-14 19:13:48,886 [root] - WriteTask-6 started with pid 718353
   2022-07-14 19:13:48,886 [root] - WriteTask-7 started with pid 718354
   2022-07-14 19:13:48,887 [root] - ReadTask-0 started with pid 718355
   2022-07-14 19:13:48,888 [root] - ReadTask-1 started with pid 718356
   2022-07-14 19:13:48,889 [root] - ReadTask-2 started with pid 718357
   2022-07-14 19:13:48,889 [root] - ReadTask-3 started with pid 718358
   2022-07-14 19:13:48,890 [root] - ReadTask-4 started with pid 718359
   2022-07-14 19:13:48,891 [root] - ReadTask-5 started with pid 718361
   2022-07-14 19:13:48,892 [root] - ReadTask-6 started with pid 718364
   2022-07-14 19:13:48,893 [root] - ReadTask-7 started with pid 718365
   2022-07-14 19:13:56,042 [DataBaseMonitor] - count=6676310 speed=667631.0
   2022-07-14 19:14:06,196 [DataBaseMonitor] - count=20004310 speed=1332800.0
   2022-07-14 19:14:16,366 [DataBaseMonitor] - count=32290310 speed=1228600.0
   2022-07-14 19:14:26,527 [DataBaseMonitor] - count=44438310 speed=1214800.0
   2022-07-14 19:14:36,673 [DataBaseMonitor] - count=56608310 speed=1217000.0
   2022-07-14 19:14:46,834 [DataBaseMonitor] - count=68757310 speed=1214900.0
   2022-07-14 19:14:57,280 [DataBaseMonitor] - count=80992310 speed=1223500.0
   2022-07-14 19:15:07,689 [DataBaseMonitor] - count=93805310 speed=1281300.0
   2022-07-14 19:15:18,020 [DataBaseMonitor] - count=106111310 speed=1230600.0
   2022-07-14 19:15:28,356 [DataBaseMonitor] - count=118394310 speed=1228300.0
   2022-07-14 19:15:38,690 [DataBaseMonitor] - count=130742310 speed=1234800.0
   2022-07-14 19:15:49,000 [DataBaseMonitor] - count=143051310 speed=1230900.0
   2022-07-14 19:15:59,323 [DataBaseMonitor] - count=155276310 speed=1222500.0
   2022-07-14 19:16:09,649 [DataBaseMonitor] - count=167603310 speed=1232700.0
   2022-07-14 19:16:19,995 [DataBaseMonitor] - count=179976310 speed=1237300.0
   ```

</details>

:::note
Don't establish connection to TDengine in the parent process if using Python client library in multi-process way, otherwise all the connections in child processes are blocked always. This is a known issue.

:::

</TabItem>
</Tabs>
