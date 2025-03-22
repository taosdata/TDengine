---
title: Ingesting Data Efficiently
slug: /developer-guide/ingesting-data-efficiently
---

import Tabs from "@theme/Tabs";
import TabItem from "@theme/TabItem";
import Image from '@theme/IdealImage';
import imgThread from '../assets/ingesting-data-efficiently-01.png';

This section describes how to write data to TDengine efficiently.

## Principles of Efficient Writing {#principle}

### From the Client Application's Perspective {#application-view}

From the perspective of the client application, efficient data writing should consider the following factors:

1. The amount of data written at once. Generally, the larger the batch of data written at once, the more efficient it is (but the advantage disappears beyond a certain threshold). When writing to TDengine using SQL, try to concatenate more data in one SQL statement. Currently, the maximum length of a single SQL statement supported by TDengine is 1,048,576 (1MB) characters.
2. Number of concurrent connections. Generally, the more concurrent connections writing data at the same time, the more efficient it is (but efficiency may decrease beyond a certain threshold, depending on the server's processing capacity).
3. Distribution of data across different tables (or subtables), i.e., the adjacency of the data being written. Generally, writing data to the same table (or subtable) in each batch is more efficient than writing to multiple tables (or subtables).
4. Method of writing. Generally:
   - Binding parameters is more efficient than writing SQL. Parameter binding avoids SQL parsing (but increases the number of calls to the C interface, which also has a performance cost).
   - Writing SQL without automatic table creation is more efficient than with automatic table creation because it frequently checks whether the table exists.
   - Writing SQL is more efficient than schema-less writing because schema-less writing automatically creates tables and supports dynamic changes to the table structure.

Client applications should fully and appropriately utilize these factors. In a single write operation, try to write data only to the same table (or subtable), set the batch size after testing and tuning to a value that best suits the current system's processing capacity, and similarly set the number of concurrent writing connections after testing and tuning to achieve the best writing speed in the current system.

### From the Data Source's Perspective {#datasource-view}

Client applications usually need to read data from a data source before writing it to TDengine. From the data source's perspective, the following situations require adding a queue between the reading and writing threads:

1. There are multiple data sources, and the data generation speed of a single data source is much lower than the writing speed of a single thread, but the overall data volume is relatively large. In this case, the role of the queue is to aggregate data from multiple sources to increase the amount of data written at once.
2. The data generation speed of a single data source is much greater than the writing speed of a single thread. In this case, the role of the queue is to increase the concurrency of writing.
3. Data for a single table is scattered across multiple data sources. In this case, the role of the queue is to aggregate the data for the same table in advance, improving the adjacency of the data during writing.

If the data source for the writing application is Kafka, and the writing application itself is a Kafka consumer, then Kafka's features can be utilized for efficient writing. For example:

1. Write data from the same table to the same Topic and the same Partition to increase data adjacency.
2. Aggregate data by subscribing to multiple Topics.
3. Increase the concurrency of writing by increasing the number of Consumer threads.
4. Increase the maximum amount of data fetched each time to increase the maximum amount of data written at once.

### From the Server Configuration's Perspective {#setting-view}

From the server configuration's perspective, the number of vgroups should be set appropriately when creating the database based on the number of disks in the system, the I/O capability of the disks, and the processor's capacity to fully utilize system performance. If there are too few vgroups, the system's performance cannot be maximized; if there are too many vgroups, it will cause unnecessary resource competition. The recommended number of vgroups is typically twice the number of CPU cores, but this should still be adjusted based on the specific system resource configuration.

For more tuning parameters, please refer to [Database Management](../../tdengine-reference/sql-manual/manage-databases/) and [Server Configuration](../../tdengine-reference/components/taosd/).

## Efficient Writing Example {#sample-code}

### Scenario Design {#scenario}

The following example program demonstrates how to write data efficiently, with the scenario designed as follows:

- The TDengine client application continuously reads data from other data sources. In the example program, simulated data generation is used to mimic reading from data sources.
- The speed of a single connection writing to TDengine cannot match the speed of reading data, so the client application starts multiple threads, each establishing a connection with TDengine, and each thread has a dedicated fixed-size message queue.
- The client application hashes the received data according to the table name (or subtable name) to different threads, i.e., writing to the message queue corresponding to that thread, ensuring that data belonging to a certain table (or subtable) will always be processed by a fixed thread.
- Each sub-thread empties the data in its associated message queue or reaches a predetermined threshold of data volume, writes that batch of data to TDengine, and continues to process the data received afterwards.

<figure>
<Image img={imgThread} alt="Thread model for efficient writing example"/>
<figcaption>Figure 1. Thread model for efficient writing example</figcaption>
</figure>

### Sample Code {#code}

This section provides sample code for the above scenario. The principle of efficient writing is the same for other scenarios, but the code needs to be modified accordingly.

This sample code assumes that the source data belongs to different subtables of the same supertable (meters). The program has already created this supertable in the test database before starting to write data. For subtables, they will be automatically created by the application according to the received data. If the actual scenario involves multiple supertables, only the code for automatic table creation in the write task needs to be modified.

<Tabs defaultValue="java" groupId="lang">
<TabItem label="Java" value="java">

**Program Listing**

| Class Name        | Function Description                                                             |
| ----------------- | -------------------------------------------------------------------------------- |
| FastWriteExample  | Main program                                                                     |
| ReadTask          | Reads data from a simulated source, hashes the table name to get the Queue Index, writes to the corresponding Queue |
| WriteTask         | Retrieves data from the Queue, forms a Batch, writes to TDengine                 |
| MockDataSource    | Simulates generating data for a certain number of meters subtables              |
| SQLWriter         | WriteTask relies on this class to complete SQL stitching, automatic table creation, SQL writing, and SQL length checking |
| StmtWriter        | Implements parameter binding for batch writing (not yet completed)               |
| DataBaseMonitor   | Counts the writing speed and prints the current writing speed to the console every 10 seconds |

Below are the complete codes and more detailed function descriptions for each class.

<details>
<summary>FastWriteExample</summary>
The main program is responsible for:

1. Creating message queues
2. Starting write threads
3. Starting read threads
4. Counting the writing speed every 10 seconds

The main program exposes 4 parameters by default, which can be adjusted each time the program is started, for testing and tuning:

1. Number of read threads. Default is 1.
2. Number of write threads. Default is 3.
3. Total number of simulated tables. Default is 1,000. This will be evenly divided among the read threads. If the total number of tables is large, table creation will take longer, and the initial writing speed statistics may be slow.
4. Maximum number of records written per batch. Default is 3,000.

Queue capacity (taskQueueCapacity) is also a performance-related parameter, which can be adjusted by modifying the program. Generally speaking, the larger the queue capacity, the less likely it is to be blocked when enqueuing, the greater the throughput of the queue, but the larger the memory usage. The default value of the sample program is already set large enough.

```java
{{#include docs/examples/java/src/main/java/com/taos/example/highvolume/FastWriteExample.java}}
```

</details>

<details>
<summary>ReadTask</summary>

The read task is responsible for reading data from the data source. Each read task is associated with a simulated data source. Each simulated data source can generate data for a certain number of tables. Different simulated data sources generate data for different tables.

The read task writes to the message queue in a blocking manner. That is, once the queue is full, the write operation will be blocked.

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

The SQLWriter class encapsulates the logic of SQL stitching and data writing. Note that none of the tables are created in advance; instead, they are created in batches using the supertable as a template when a table not found exception is caught, and then the INSERT statement is re-executed. For other exceptions, this simply logs the SQL statement being executed at the time; you can also log more clues to facilitate error troubleshooting and fault recovery.

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

**Execution Steps**

<details>
<summary>Execute the Java Example Program</summary>

Before running the program, configure the environment variable `TDENGINE_JDBC_URL`. If the TDengine Server is deployed on the local machine, and the username, password, and port are all default values, then you can configure:

```shell
TDENGINE_JDBC_URL="jdbc:TAOS://localhost:6030?user=root&password=taosdata"
```

**Execute the example program in a local integrated development environment**

1. Clone the TDengine repository

   ```shell
   git clone git@github.com:taosdata/TDengine.git --depth 1
   ```

2. Open the `docs/examples/java` directory with the integrated development environment.
3. Configure the environment variable `TDENGINE_JDBC_URL` in the development environment. If the global environment variable `TDENGINE_JDBC_URL` has already been configured, you can skip this step.
4. Run the class `com.taos.example.highvolume.FastWriteExample`.

**Execute the example program on a remote server**

To execute the example program on a server, follow these steps:

1. Package the example code. Execute in the directory TDengine/docs/examples/java:

   ```shell
   mvn package
   ```

2. Create an examples directory on the remote server:

   ```shell
   mkdir -p examples/java
   ```

3. Copy dependencies to the specified directory on the server:
   - Copy dependency packages, only once

     ```shell
     scp -r .\target\lib <user>@<host>:~/examples/java
     ```

   - Copy the jar package of this program, copy every time the code is updated

     ```shell
     scp -r .\target\javaexample-1.0.jar <user>@<host>:~/examples/java
     ```

4. Configure the environment variable.
   Edit `~/.bash_profile` or `~/.bashrc` and add the following content for example:

   ```shell
   export TDENGINE_JDBC_URL="jdbc:TAOS://localhost:6030?user=root&password=taosdata"
   ```

   The above uses the default JDBC URL when TDengine Server is deployed locally. You need to modify it according to your actual situation.

5. Start the example program with the Java command, command template:

   ```shell
   java -classpath lib/*:javaexample-1.0.jar  com.taos.example.highvolume.FastWriteExample <read_thread_count>  <white_thread_count> <total_table_count> <max_batch_size>
   ```

6. End the test program. The test program will not end automatically; after obtaining a stable writing speed under the current configuration, press <kbd>CTRL</kbd> + <kbd>C</kbd> to end the program.
   Below is a log output from an actual run, with machine configuration 16 cores + 64G + SSD.

```text
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

**Program Listing**

The Python example program uses a multi-process architecture and employs a cross-process message queue.

| Function or Class        | Description                                                         |
| ------------------------ | ------------------------------------------------------------------- |
| main function            | Entry point of the program, creates various subprocesses and message queues |
| run_monitor_process function | Creates database, supertables, tracks write speed and periodically prints to console |
| run_read_task function   | Main logic for read processes, responsible for reading data from other data systems and distributing it to assigned queues |
| MockDataSource class     | Simulates a data source, implements iterator interface, returns the next 1,000 records for each table in batches |
| run_write_task function  | Main logic for write processes. Retrieves as much data as possible from the queue and writes in batches |
| SQLWriter class          | Handles SQL writing and automatic table creation                   |
| StmtWriter class         | Implements batch writing with parameter binding (not yet completed) |

<details>
<summary>main function</summary>

The main function is responsible for creating message queues and launching subprocesses, which are of 3 types:

1. 1 monitoring process, responsible for database initialization and tracking write speed
2. n read processes, responsible for reading data from other data systems
3. m write processes, responsible for writing to the database

The main function can accept 5 startup parameters, in order:

1. Number of read tasks (processes), default is 1
2. Number of write tasks (processes), default is 1
3. Total number of simulated tables, default is 1,000
4. Queue size (in bytes), default is 1,000,000
5. Maximum number of records written per batch, default is 3,000

```python
{{#include docs/examples/python/fast_write_example.py:main}}
```

</details>

<details>
<summary>run_monitor_process</summary>

The monitoring process is responsible for initializing the database and monitoring the current write speed.

```python
{{#include docs/examples/python/fast_write_example.py:monitor}}
```

</details>

<details>

<summary>run_read_task function</summary>

The read process, responsible for reading data from other data systems and distributing it to assigned queues.

```python
{{#include docs/examples/python/fast_write_example.py:read}}
```

</details>

<details>

<summary>MockDataSource</summary>

Below is the implementation of the mock data source. We assume that each piece of data generated by the data source includes the target table name information. In practice, you might need certain rules to determine the target table name.

```python
{{#include docs/examples/python/mockdatasource.py}}
```

</details>

<details>
<summary>run_write_task function</summary>

The write process retrieves as much data as possible from the queue and writes in batches.

```python
{{#include docs/examples/python/fast_write_example.py:write}}
```

</details>

<details>

The SQLWriter class encapsulates the logic of SQL stitching and data writing. None of the tables are pre-created; instead, they are batch-created using the supertable as a template when a table does not exist error occurs, and then the INSERT statement is re-executed. For other errors, the SQL executed at the time is recorded for error troubleshooting and fault recovery. This class also checks whether the SQL exceeds the maximum length limit, based on the TDengine 3.0 limit, the supported maximum SQL length of 1,048,576 is passed in by the input parameter maxSQLLength.

<summary>SQLWriter</summary>

```python
{{#include docs/examples/python/sql_writer.py}}
```

</details>

**Execution Steps**

<details>

<summary>Execute the Python Example Program</summary>

1. Prerequisites

   - TDengine client driver installed
   - Python3 installed, recommended version >= 3.8
   - taospy installed

2. Install faster-fifo to replace the built-in multiprocessing.Queue in python

   ```shell
   pip3 install faster-fifo
   ```

3. Click the "View Source" link above to copy the `fast_write_example.py`, `sql_writer.py`, and `mockdatasource.py` files.

4. Execute the example program

   ```shell
   python3 fast_write_example.py <READ_TASK_COUNT> <WRITE_TASK_COUNT> <TABLE_COUNT> <QUEUE_SIZE> <MAX_BATCH_SIZE>
   ```

   Below is an actual output from a run, on a machine configured with 16 cores + 64G + SSD.

   ```text
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
When using the Python connector to connect to TDengine with multiple processes, there is a limitation: connections cannot be established in the parent process; all connections must be created in the child processes.
If a connection is created in the parent process, any connection attempts in the child processes will be perpetually blocked. This is a known issue.

:::

</TabItem>
</Tabs>
