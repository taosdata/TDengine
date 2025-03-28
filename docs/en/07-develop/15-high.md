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

The following sample program demonstrates how to efficiently write data, with the scenario designed as follows:  
- The TDengine client program continuously reads data from other data sources. In the sample program, simulated data generation is used to mimic data source reading, while also providing an example of pulling data from Kafka and writing it to TDengine.
- To improve the data reading speed of the TDengine client program, multi-threading is used for reading. To avoid out-of-order issues, the sets of tables read by multiple reading threads should be non-overlapping.
- To match the data reading speed of each data reading thread, a set of write threads is launched in the background. Each write thread has an exclusive fixed-size message queue.

<figure>
<Image img={imgThread} alt="Thread model for efficient writing example"/>
<figcaption>Figure 1. Thread model for efficient writing example</figcaption>
</figure>

### Sample Code {#code}

This section provides sample code for the above scenario. The principle of efficient writing is the same for other scenarios, but the code needs to be modified accordingly.

This sample code assumes that the source data belongs to different subtables of the same supertable (meters). The program has already created this supertable in the test database before starting to write data. For subtables, they will be automatically created by the application according to the received data. If the actual scenario involves multiple supertables, only the code for automatic table creation in the write task needs to be modified.

<Tabs defaultValue="java" groupId="lang">
<TabItem label="Java" value="java">

**Introduction to JDBC Efficient Writing Features**

Starting from version 3.6.0, the JDBC driver provides efficient writing features over WebSocket connections. For configuration parameters, refer to efficient Writing Configuration. The JDBC driver's efficient writing features include the following characteristics:

- Supports the JDBC standard parameter binding interface.
- When resources are sufficient, writing capacity is linearly correlated with the configuration of write threads.
- Supports configuration of write timeout, number of retries, and retry interval after connection disconnection and reconnection.
- Supports invoking the `executeUpdate` interface to obtain the number of written data records, and exceptions during writing can be caught.

**JDBC Efficient Writing User Guide**

The following is a simple example of efficient writing using JDBC, which illustrates the relevant configurations and interfaces for efficient writing.

```java
{{#include docs/examples/java/src/main/java/com/taos/example/WSHighVolumeDemo.java:efficient_writing}}
```

**Program Listing**

| Class Name         | Functional Description                                                                                                                                       |
| ------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| FastWriteExample   | The main program responsible for command-line argument parsing, thread pool creation, and waiting for task completion.                                       |
| WorkTask           | Reads data from a simulated source and writes it using the JDBC standard interface.                                                                          |
| MockDataSource     | Simulates and generates data for a certain number of `meters` child tables.                                                                                  |
| DataBaseMonitor    | Tracks write speed and prints the current write speed to the console every 10 seconds.                                                                       |
| CreateSubTableTask | Creates child tables within a specified range for invocation by the main program.                                                                            |
| Meters             | Provides serialization and deserialization of single records in the `meters` table, used for sending messages to Kafka and receiving messages from Kafka.    |
| ProducerTask       | A producer that sends messages to Kafka.                                                                                                                     |
| ConsumerTask       | A consumer that receives messages from Kafka, writes data to TDengine using the JDBC efficient writing interface, and commits offsets according to progress. |
| Util               | Provides basic functionalities, including creating connections, creating Kafka topics, and counting write operations.                                        |


Below are the complete codes and more detailed function descriptions for each class.

<details>
<summary>FastWriteExample</summary>

**Introduction to Main Program Command-Line Arguments:**  

```shell
   -b,--batchSizeByRow <arg>             Specifies the `batchSizeByRow` parameter for Efficient Writing, default is 1000  
   -c,--cacheSizeByRow <arg>             Specifies the `cacheSizeByRow` parameter for Efficient Writing, default is 10000  
   -d,--dbName <arg>                     Specifies the database name, default is `test`  
      --help                             Prints help information  
   -K,--useKafka                         Enables Kafka mode, creating a producer to send messages and a consumer to receive messages for writing to TDengine. Otherwise, uses worker threads to subscribe to simulated data for writing.  
   -r,--readThreadCount <arg>            Specifies the number of worker threads, default is 5. In Kafka mode, this parameter also determines the number of producer and consumer threads.  
   -R,--rowsPerSubTable <arg>            Specifies the number of rows to write per child table, default is 100  
   -s,--subTableNum <arg>                Specifies the total number of child tables, default is 1000000  
   -w,--writeThreadPerReadThread <arg>   Specifies the number of write threads per worker thread, default is 5  
```  

**JDBC URL and Kafka Cluster Address Configuration:**  

1. The JDBC URL is configured via an environment variable, for example:  
   ```shell
   export TDENGINE_JDBC_URL="jdbc:TAOS-WS://localhost:6041?user=root&password=taosdata"
   ```  
2. The Kafka cluster address is configured via an environment variable, for example:  
   ```shell
   export KAFKA_BOOTSTRAP_SERVERS=localhost:9092
   ```  

**Usage:**  
```shell
1. Simulated data writing mode:  
   java -jar highVolume.jar -r 5 -w 5 -b 10000 -c 100000 -s 1000000 -R 1000  
2. Kafka subscription writing mode:  
   java -jar highVolume.jar -r 5 -w 5 -b 10000 -c 100000 -s 1000000 -R 100 -K  
```  

**Responsibilities of the Main Program:**  
1. Parses command-line arguments.  
2. Creates child tables.  
3. Creates worker threads or Kafka producers and consumers.  
4. Tracks write speed.  
5. Waits for writing to complete and releases resources.


```java
{{#include docs/examples/JDBC/highvolume/src/main/java/com/taos/example/highvolume/FastWriteExample.java}}
```

</details>



<details>
<summary>WorkTask</summary>

The worker thread is responsible for reading data from the simulated data source. Each read task is associated with a simulated data source, which can generate data for a specific range of sub-tables. Different simulated data sources generate data for different tables.  
The worker thread uses a blocking approach to invoke the JDBC standard interface `addBatch`. This means that if the corresponding efficient writing backend queue is full, the write operation will block.

```java
{{#include docs/examples/JDBC/highvolume/src/main/java/com/taos/example/highvolume/WorkTask.java}}
```

</details>

<details>
<summary>MockDataSource</summary>

A simulated data generator that produces data for a certain range of sub-tables. To mimic real-world scenarios, it generates data in a round-robin fashion, one row per subtable.

```java
{{#include docs/examples/JDBC/highvolume/src/main/java/com/taos/example/highvolume/MockDataSource.java}}
```

</details>

<details>
<summary>CreateSubTableTask</summary>

Creates sub-tables within a specified range using a batch SQL creation approach.

```java
{{#include docs/examples/JDBC/highvolume/src/main/java/com/taos/example/highvolume/CreateSubTableTask.java}}
```

</details>

<details>
<summary>Meters</summary>

A data model class that provides serialization and deserialization methods for sending data to Kafka and receiving data from Kafka.

```java
{{#include docs/examples/JDBC/highvolume/src/main/java/com/taos/example/highvolume/Meters.java}}
```

</details>

<details>
<summary>ProducerTask</summary>

A message producer that writes data generated by the simulated data generator to all partitions using a hash method different from JDBC efficient writing.

```java
{{#include docs/examples/JDBC/highvolume/src/main/java/com/taos/example/highvolume/ProducerTask.java}}
```

</details>

<details>
<summary>ConsumerTask</summary>

A message consumer that receives messages from Kafka and writes them to TDengine.

```java
{{#include docs/examples/JDBC/highvolume/src/main/java/com/taos/example/highvolume/ConsumerTask.java}}
```

</details>

<details>
<summary>StatTask</summary>

Provides a periodic function to count the number of written records.

```java
{{#include docs/examples/JDBC/highvolume/src/main/java/com/taos/example/highvolume/StatTask.java}}
```

</details>

<details>
<summary>Util</summary>

A utility class that provides functions such as creating connections, creating databases, and creating topics.

```java
{{#include docs/examples/JDBC/highvolume/src/main/java/com/taos/example/highvolume/Util.java}}
```

</details>

**Execution Steps**

<details>
<summary>Execute the Java Example Program</summary>

**Execute the example program in a local integrated development environment**

1. Clone the TDengine repository

   ```shell
   git clone git@github.com:taosdata/TDengine.git --depth 1
   ```

2. Open the `TDengine/docs/examples/JDBC/highvolume` directory with the integrated development environment.
3. Configure the environment variable `TDENGINE_JDBC_URL` in the development environment. If the global environment variable `TDENGINE_JDBC_URL` has already been configured, you can skip this step.
4. If you want to run the Kafka example, you need to set the environment variable `KAFKA_BOOTSTRAP_SERVERS` for the Kafka cluster address.
5. Specify command-line arguments, such as `-r 3 -w 3 -b 100 -c 1000 -s 1000 -R 100`.
6. Run the class `com.taos.example.highvolume.FastWriteExample`.

**Execute the example program on a remote server**

To execute the example program on a server, follow these steps:

1. Package the sample code. Navigate to the directory `TDengine/docs/examples/JDBC/highvolume` and run the following command to generate `highVolume.jar`:

   ```shell
   mvn package
   ```

2. Copy the program to the specified directory on the server:
  
   ```shell
   scp -r .\target\highVolume.jar <user>@<host>:~/dest-path
   ```

3. Configure the environment variable.
   Edit `~/.bash_profile` or `~/.bashrc` and add the following content for example:

   ```shell
   export TDENGINE_JDBC_URL="jdbc:TAOS://localhost:6030?user=root&password=taosdata"
   ```

   The above uses the default JDBC URL for a locally deployed TDengine Server. Modify it according to your actual environment.
   If you want to use Kafka subscription mode, additionally configure the Kafka cluster environment variable:

   ```shell
   export KAFKA_BOOTSTRAP_SERVERS=localhost:9092
   ```

4. Start the sample program with the Java command. Use the following template (append `-K` for Kafka subscription mode):

   ```shell
   java -jar highVolume.jar -r 5 -w 5 -b 10000 -c 100000 -s 1000000 -R 1000
   ```

5. Terminate the test program. The program does not exit automatically. Once a stable write speed is achieved under the current configuration, press <kbd>CTRL</kbd> + <kbd>C</kbd> to terminate it.
Below is a sample log output from an actual run on a machine with a 40-core CPU, 256GB RAM, and SSD storage.

```text
   ---------------$ java -jar highVolume.jar -r 2 -w 10 -b 10000 -c 100000 -s 1000000 -R 100
   [INFO ] 2025-03-24 18:03:17.980 com.taos.example.highvolume.FastWriteExample main 309 main readThreadCount=2, writeThreadPerReadThread=10 batchSizeByRow=10000 cacheSizeByRow=100000, subTableNum=1000000, rowsPerSubTable=100
   [INFO ] 2025-03-24 18:03:17.983 com.taos.example.highvolume.FastWriteExample main 312 main create database begin.
   [INFO ] 2025-03-24 18:03:34.499 com.taos.example.highvolume.FastWriteExample main 315 main create database end.
   [INFO ] 2025-03-24 18:03:34.500 com.taos.example.highvolume.FastWriteExample main 317 main create sub tables start.
   [INFO ] 2025-03-24 18:03:34.502 com.taos.example.highvolume.FastWriteExample createSubTables 73 main create sub table task started.
   [INFO ] 2025-03-24 18:03:55.777 com.taos.example.highvolume.FastWriteExample createSubTables 82 main create sub table task finished.
   [INFO ] 2025-03-24 18:03:55.778 com.taos.example.highvolume.FastWriteExample main 319 main create sub tables end.
   [INFO ] 2025-03-24 18:03:55.781 com.taos.example.highvolume.WorkTask run 41 FW-work-thread-2 started
   [INFO ] 2025-03-24 18:03:55.781 com.taos.example.highvolume.WorkTask run 41 FW-work-thread-1 started
   [INFO ] 2025-03-24 18:04:06.580 com.taos.example.highvolume.StatTask run 36 pool-1-thread-1 numberOfTable=1000000 count=12235906 speed=1223590
   [INFO ] 2025-03-24 18:04:17.531 com.taos.example.highvolume.StatTask run 36 pool-1-thread-1 numberOfTable=1000000 count=31185614 speed=1894970
   [INFO ] 2025-03-24 18:04:28.490 com.taos.example.highvolume.StatTask run 36 pool-1-thread-1 numberOfTable=1000000 count=51464904 speed=2027929
   [INFO ] 2025-03-24 18:04:40.851 com.taos.example.highvolume.StatTask run 36 pool-1-thread-1 numberOfTable=1000000 count=71498113 speed=2003320
   [INFO ] 2025-03-24 18:04:51.948 com.taos.example.highvolume.StatTask run 36 pool-1-thread-1 numberOfTable=1000000 count=91242103 speed=1974399

```

</details>

</TabItem>

</Tabs>
