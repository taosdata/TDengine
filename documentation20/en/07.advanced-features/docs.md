# Advanced Features

## <a class="anchor" id="continuous-query"></a> Continuous Query

Continuous Query is a query executed by TDengine periodically with a sliding window, it is a simplified stream computing driven by timers. Continuous query can be applied to a table or a STable automatically and periodically, and the result set can be passed to the application directly via call back function, or written into a new table in TDengine. The query is always executed on a specified time window (window size is specified by parameter interval), and this window slides forward while time flows (the sliding period is specified by parameter sliding).

Continuous query of TDengine adopts time-driven mode, which can be defined directly by TAOS SQL without additional operation. Using continuous query, results can be generated conveniently and quickly according to the time window, thus down sampling the original collected data. After the user defines a continuous query through TAOS SQL, TDengine automatically pulls up the query at the end of the last complete time period and pushes the calculated results to the user or writes them back to TDengine.

The continuous query provided by TDengine differs from the time window calculation in ordinary stream computing in the following ways:

- Unlike the real-time feedback calculated results of stream computing, continuous query only starts calculation after the time window is closed. For example, if the time period is 1 day, the results of that day will only be generated after 23:59:59.
- If a history record is written to the time interval that has been calculated, the continuous query will not recalculate and will not push the results to the user again. For the mode of writing back to TDengine, the existing calculated results will not be updated.
- Using the mode of continuous query pushing results, the server does not cache the client's calculation status, nor does it provide Exactly-Once semantic guarantee. If the user's application side crashed, the continuous query pulled up again would only recalculate the latest complete time window from the time pulled up again. If writeback mode is used, TDengine can ensure the validity and continuity of data writeback.

### How to use continuous query

The following is an example of the smart meter scenario to introduce the specific use of continuous query. Suppose we create a STables and sub-tables through the following SQL statement:

```sql
create table meters (ts timestamp, current float, voltage int, phase float) tags (location binary(64), groupdId int);
create table D1001 using meters tags ("Beijing.Chaoyang", 2);
create table D1002 using meters tags ("Beijing.Haidian", 2);
...
```

We already know that the average voltage of these meters can be counted with one minute as the time window and 30 seconds as the forward increment through the following SQL statement.

```sql
select avg(voltage) from meters interval(1m) sliding(30s);
```

Every time this statement is executed, all data will be recalculated. If you need to execute every 30 seconds to incrementally calculate the data of the latest minute, you can improve the above statement as following, using a different `startTime` each time and executing it regularly:

```sql
select avg(voltage) from meters where ts > {startTime} interval(1m) sliding(30s);
```

There is no problem with this, but TDengine provides a simpler method, just add `create table {tableName} as` before the initial query statement, for example:

```sql
create table avg_vol as select avg(voltage) from meters interval(1m) sliding(30s);
```

A new table named `avg_vol` will be automatically created, and then every 30 seconds, TDengine will incrementally execute the SQL statement after `as` and write the query result into this table. The user program only needs to query the data from `avg_vol`. For example:

```mysql
taos> select * from avg_vol;
            ts           |        avg_voltage_    |
===================================================
 2020-07-29 13:37:30.000 |            222.0000000 |
 2020-07-29 13:38:00.000 |            221.3500000 |
 2020-07-29 13:38:30.000 |            220.1700000 |
 2020-07-29 13:39:00.000 |            223.0800000 |
```

It should be noted that the minimum value of the query time window is 10 milliseconds, and there is no upper limit of the time window range.

In addition, TDengine also supports users to specify the starting and ending times of a continuous query. If the start time is not entered, the continuous query will start from the time window where the first original data is located; If no end time is entered, the continuous query will run permanently; If the user specifies an end time, the continuous query stops running after the system time reaches the specified time. For example, a continuous query created with the following SQL will run for one hour and then automatically stop.

```mysql
create table avg_vol as select avg(voltage) from meters where ts > now and ts <= now + 1h interval(1m) sliding(30s);
```

It should be noted that now in the above example refers to the time when continuous queries are created, not the time when queries are executed, otherwise, queries cannot be stopped automatically. In addition, in order to avoid the problems caused by delayed writing of original data as much as possible, there is a certain delay in the calculation of continuous queries in TDengine. In other words, after a time window has passed, TDengine will not immediately calculate the data of this window, so it will take a while (usually not more than 1 minute) to find the calculation result.

### Manage the Continuous Query

Users can view all continuous queries running in the system through the show streams command in the console, and can kill the corresponding continuous queries through the kill stream command. Subsequent versions will provide more finer-grained and convenient continuous query management commands.

## <a class="anchor" id="subscribe"></a> Publisher/Subscriber

Based on the natural time-series characteristics of data, the data insert of TDengine is logically consistent with the data publish (pub) of messaging system, which can be regarded as a new record inserted with timestamp in the system. At the same time, TDengine stores data in strict accordance with the monotonous increment of time-series. Essentially, every table in TDengine can be regarded as a standard messaging queue.

TDengine supports embedded lightweight message subscription and publishment services. Using the API provided by the system, users can subscribe to one or more tables in the database using common query statements. The maintenance of subscription logic and operation status is completed by the client. The client regularly polls the server for whether new records arrive, and the results will be fed back to the client when new records arrive.

The status of the subscription and publishment services of TDengine is maintained by the client, but not by the TDengine server. Therefore, if the application restarts, it is up to the application to decide from which point of time to obtain the latest data.

In TDengine, there are three main APIs relevant to subscription:

```c
taos_subscribe
taos_consume
taos_unsubscribe
```

Please refer to the [C/C++ Connector](https://www.taosdata.com/cn/documentation/connector/) for the documentation of these APIs. The following is still a smart meter scenario as an example to introduce their specific usage (please refer to the previous section "Continuous Query" for the structure of STables and sub-tables). The complete sample code can be found [here](https://github.com/taosdata/TDengine/blob/master/tests/examples/c/subscribe.c).

If we want to be notified and do some process when the current of a smart meter exceeds a certain limit (e.g. 10A), there are two methods: one is to query each sub-table separately, record the timestamp of the last piece of data after each query, and then only query all data after this timestamp:

```sql
select * from D1001 where ts > {last_timestamp1} and current > 10;
select * from D1002 where ts > {last_timestamp2} and current > 10;
...
```

This is indeed feasible, but as the number of meters increases, the number of queries will also increase, and the performance of both the client and the server will be affected, until the system cannot afford it.

Another method is to query the STable. In this way, no matter how many meters there are, only one query is required:

```sql
select * from meters where ts > {last_timestamp} and current > 10;
```

However, how to choose `last_timestamp` has become a new problem. Because, on the one hand, the time of data generation (the data timestamp) and the time of data storage are generally not the same, and sometimes the deviation is still very large; On the other hand, the time when the data of different meters arrive at TDengine will also vary. Therefore, if we use the timestamp of the data from the slowest meter as `last_timestamp` in the query, we may repeatedly read the data of other meters; If the timestamp of the fastest meter is used, the data of other meters may be missed.

The subscription function of TDengine provides a thorough solution to the above problem.

First, use `taos_subscribe` to create a subscription:

```c
TAOS_SUB* tsub = NULL;
if (async) {
　　// create an asynchronized subscription, the callback function will be called every 1s
　　tsub = taos_subscribe(taos, restart, topic, sql, subscribe_callback, &blockFetch, 1000);
} else {
　　// create an synchronized subscription, need to call 'taos_consume' manually
　　tsub = taos_subscribe(taos, restart, topic, sql, NULL, NULL, 0);
}
```

Subscriptions in TDengine can be either synchronous or asynchronous, and the above code will decide which method to use based on the value of parameter `async` obtained from the command line. Here, synchronous means that the user program calls `taos_consume` directly to pull data, while asynchronous means that the API calls `taos_consume` in another internal thread, and then gives the pulled data to the callback function `subscribe_callback` for processing.

Parameter `taos` is an established database connection and has no special requirements in synchronous mode. However, in asynchronous mode, it should be noted that it will not be used by other threads, otherwise it may lead to unpredictable errors, because the callback function is called in the internal thread of the API, while some APIs of TDengine are not thread-safe.

Parameter `sql` is a query statement in which you can specify filters using where clause. In our example, if you only want to subscribe to data when the current exceeds 10A, you can write as follows:

```sql
select * from meters where current > 10;
```

Note that the starting time is not specified here, so the data of all timers will be read. If you only want to start subscribing from the data one day ago and do not need earlier historical data, you can add a time condition:

```sql
select * from meters where ts > now - 1d and current > 10;
```

The `topic` of the subscription is actually its name, because the subscription function is implemented in the client API, so it is not necessary to ensure that it is globally unique, but it needs to be unique on a client machine.

If the subscription of name `topic` does not exist, the parameter restart is meaningless; However, if the user program exits after creating this subscription, when it starts again and reuses this `topic`, `restart` will be used to decide whether to read data from scratch or from the previous location. In this example, if `restart` is **true** (non-zero value), the user program will definitely read all the data. However, if this subscription exists before, and some data has been read, and `restart` is **false** (zero), the user program will not read the previously read data.

The last parameter of `taos_subscribe` is the polling period in milliseconds. In synchronous mode, if the interval between the two calls to `taos_consume` is less than this time, `taos_consume` will block until the interval exceeds this time. In asynchronous mode, this time is the minimum time interval between two calls to the callback function.

The penultimate parameter of `taos_subscribe` is used by the user program to pass additional parameters to the callback function, which is passed to the callback function as it is without any processing by the subscription API. This parameter is meaningless in sync mode.

After created, the subscription can consume data. In synchronous mode, the sample code is the following as the `else` section:

```c
if (async) {
　　getchar();
} else while(1) {
　　TAOS_RES* res = taos_consume(tsub);
　　if (res == NULL) {
　　　　printf("failed to consume data.");
　　　　break;
　　} else {
　　　　print_result(res, blockFetch);
　　　　getchar();
　　}
}
```

Here is a **while** loop. Every time the user presses the Enter key, `taos_consume` is called, and the return value of `taos_consume` is the query result set, which is exactly the same as `taos_use_result`. In the example, the code using this result set is the function `print_result`:

```c
void print_result(TAOS_RES* res, int blockFetch) {
　　TAOS_ROW row = NULL;
　　int num_fields = taos_num_fields(res);
　　TAOS_FIELD* fields = taos_fetch_fields(res);
　　int nRows = 0;
　　if (blockFetch) {
　　　　nRows = taos_fetch_block(res, &row);
　　　　for (int i = 0; i < nRows; i++) {
　　　　　　char temp[256];
　　　　　　taos_print_row(temp, row + i, fields, num_fields);
　　　　　　puts(temp);
　　　　}
　　} else {
　　　　while ((row = taos_fetch_row(res))) {
　　　　　　char temp[256];
　　　　　　taos_print_row(temp, row, fields, num_fields);puts(temp);
　　　　　　nRows++;
　　　　}
　　}
　　printf("%d rows consumed.\n", nRows);
}
```

Among them, `taos_print_row` is used to process subscription to data. In our example, it will print out all eligible records. In asynchronous mode, it is simpler to consume subscribed data:

```c
void subscribe_callback(TAOS_SUB* tsub, TAOS_RES *res, void* param, int code) {
　　print_result(res, *(int*)param);
}
```

To end a data subscription, you need to call `taos_unsubscribe`:

```c
taos_unsubscribe(tsub, keep);
```

Its second parameter is used to decide whether to keep the progress information of subscription on the client. If this parameter is **false** (zero), the subscription can only be restarted no matter what the `restart` parameter is when `taos_subscribe` is called next time. In addition, progress information is saved in the directory {DataDir}/subscribe/. Each subscription has a file with the same name as its `topic`. Deleting a file will also lead to a new start when the corresponding subscription is created next time.

After introducing the code, let's take a look at the actual running effect. For exmaple:

- Sample code has been downloaded locally
- TDengine has been installed on the same machine
- All the databases, STables and sub-tables required by the example have been created

You can compile and start the sample program by executing the following command in the directory where the sample code is located:

```shell
$ make
$ ./subscribe -sql='select * from meters where current > 10;'
```

After the sample program starts, open another terminal window, and the shell that starts TDengine inserts a data with a current of 12A into **D1001**:

```shell
$ taos
> use test;
> insert into D1001 values(now, 12, 220, 1);
```

At this time, because the current exceeds 10A, you should see that the sample program outputs it to the screen. You can continue to insert some data to observe the output of the sample program.

### Use data subscription in Java

The subscription function also provides a Java development interface, as described in [Java Connector](https://www.taosdata.com/cn/documentation/connector/). It should be noted that the Java interface does not provide asynchronous subscription mode at present, but user programs can achieve the same feature by creating  TimerTask.

The following is an example to introduce its specific use. The function it completes is basically the same as the C language example described earlier, and it is also to subscribe to all records with current exceeding 10A in the database.

#### Prepare data

```sql
# Create power Database
taos> create database power;
# Switch to the database
taos> use power;
# Create a STable
taos> create table meters(ts timestamp, current float, voltage int, phase int) tags(location binary(64), groupId int);
# Create tables
taos> create table d1001 using meters tags ("Beijing.Chaoyang", 2);
taos> create table d1002 using meters tags ("Beijing.Haidian", 2);
# Insert test data
taos> insert into d1001 values("2020-08-15 12:00:00.000", 12, 220, 1),("2020-08-15 12:10:00.000", 12.3, 220, 2),("2020-08-15 12:20:00.000", 12.2, 220, 1);
taos> insert into d1002 values("2020-08-15 12:00:00.000", 9.9, 220, 1),("2020-08-15 12:10:00.000", 10.3, 220, 1),("2020-08-15 12:20:00.000", 11.2, 220, 1);
# Query all records with current over 10A from STable meters
taos> select * from meters where current > 10;
           ts            |    current   |    voltage   |  phase |         location          |   groupid   |
===========================================================================================================
 2020-08-15 12:10:00.000 |    10.30000  |     220      |      1 |      Beijing.Haidian      |           2 |
 2020-08-15 12:20:00.000 |    11.20000  |     220      |      1 |      Beijing.Haidian      |           2 |
 2020-08-15 12:00:00.000 |    12.00000  |     220      |      1 |      Beijing.Chaoyang     |           2 |
 2020-08-15 12:10:00.000 |    12.30000  |     220      |      2 |      Beijing.Chaoyang     |           2 |
 2020-08-15 12:20:00.000 |    12.20000  |     220      |      1 |      Beijing.Chaoyang     |           2 |
Query OK, 5 row(s) in set (0.004896s)
```

#### Example

```java
public class SubscribeDemo {
    private static final String topic = "topic-meter-current-bg-10";
    private static final String sql = "select * from meters where current > 10";

    public static void main(String[] args) {
        Connection connection = null;
        TSDBSubscribe subscribe = null;

        try {
            Class.forName("com.taosdata.jdbc.TSDBDriver");
            Properties properties = new Properties();
            properties.setProperty(TSDBDriver.PROPERTY_KEY_CHARSET, "UTF-8");
            properties.setProperty(TSDBDriver.PROPERTY_KEY_TIME_ZONE, "UTC-8");
            String jdbcUrl = "jdbc:TAOS://127.0.0.1:6030/power?user=root&password=taosdata";
            connection = DriverManager.getConnection(jdbcUrl, properties);
            subscribe = ((TSDBConnection) connection).subscribe(topic, sql, true); // Create a subscription
            int count = 0;
            while (count < 10) {
                TimeUnit.SECONDS.sleep(1); / Wait 1 second to avoid calling consume too frequently and causing pressure on server
                TSDBResultSet resultSet = subscribe.consume(); // 消费数据
                if (resultSet == null) {
                    continue;
                }
                ResultSetMetaData metaData = resultSet.getMetaData();
                while (resultSet.next()) {
                    int columnCount = metaData.getColumnCount();
                    for (int i = 1; i <= columnCount; i++) {
                        System.out.print(metaData.getColumnLabel(i) + ": " + resultSet.getString(i) + "\t");
                    }
                    System.out.println();
                    count++;
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if (null != subscribe)
                    subscribe.close(true); // Close the subscription
                if (connection != null) 
                    connection.close(); 
            } catch (SQLException throwables) {
                throwables.printStackTrace();
            }
        }
    }
}
```

Run the sample program. First, it consumes all the historical data that meets the query conditions:

```shell
# java -jar subscribe.jar 

ts: 1597464000000	current: 12.0	voltage: 220	phase: 1	location: Beijing.Chaoyang	groupid : 2
ts: 1597464600000	current: 12.3	voltage: 220	phase: 2	location: Beijing.Chaoyang	groupid : 2
ts: 1597465200000	current: 12.2	voltage: 220	phase: 1	location: Beijing.Chaoyang	groupid : 2
ts: 1597464600000	current: 10.3	voltage: 220	phase: 1	location: Beijing.Haidian	groupid : 2
ts: 1597465200000	current: 11.2	voltage: 220	phase: 1	location: Beijing.Haidian	groupid : 2
```

Then, add a piece of data to the table via taos client:

```sql
# taos
taos> use power;
taos> insert into d1001 values("2020-08-15 12:40:00.000", 12.4, 220, 1);
```

Because the current of this data is greater than 10A, the sample program will consume it:

```shell
ts: 1597466400000	current: 12.4	voltage: 220	phase: 1	location: Beijing.Chaoyang	groupid: 2
```

## <a class="anchor" id="cache"></a> Cache

TDengine adopts a time-driven cache management strategy (First-In-First-Out, FIFO), also known as a write-driven cache management mechanism. This strategy is different from the read-driven data cache mode (Least-Recent-Use, LRU), which directly saves the most recently written data in the system buffer. When the buffer reaches a threshold, the oldest data is written to disk in batches. Generally speaking, for the use of IoT data, users are most concerned about the recently generated data, that is, the current status. TDengine takes full advantage of this feature by storing the most recently arrived (current status) data in the buffer.

TDengine provides data collection in milliseconds to users through query functions. Saving the recently arrived data directly in buffer can respond to the user's query analysis for the latest piece or batch of data more quickly, and provide faster database query response as a whole. In this way, TDengine can be used as a data buffer by setting appropriate configuration parameters without deploying additional caching systems, which can effectively simplify the system architecture and reduce the operation costs. It should be noted that after the TDengine is restarted, the buffer of the system will be emptied, the previously cached data will be written to disk in batches, and the cached data will not reload the previously cached data into the buffer like some proprietary Key-value cache system.

TDengine allocates a fixed size of memory space as a buffer, which can be configured according to application requirements and hardware resources. By properly setting the buffer space, TDengine can provide extremely high-performance write and query support. Each virtual node in TDengine is allocated a separate cache pool when it is created. Each virtual node manages its own cache pool, and different virtual nodes do not share the pool. All tables belonging to each virtual node share the cache pool owned by itself.

TDengine manages the memory pool by blocks, and the data is stored in the form of rows within. The memory pool of a vnode is allocated by blocks when the vnode is created, and each memory block is managed according to the First-In-First-Out strategy. When creating a memory pool, the size of the blocks is determined by the system configuration parameter cache; The number of memory blocks in each vnode is determined by the configuration parameter blocks. So for a vnode, the total memory size is: cache * blocks. A cache block needs to ensure that each table can store at least dozens of records in order to be efficient.

You can quickly obtain the last record of a table or a STable through the function last_row, which is very convenient to show the real-time status or collected values of each device on a large screen. For example:

```mysql
select last_row(voltage) from meters where location='Beijing.Chaoyang';
```

This SQL statement will obtain the last recorded voltage value of all smart meters located in Chaoyang District, Beijing.

## <a class="anchor" id="alert"></a> Alert

In scenarios of TDengine, alarm monitoring is a common requirement. Conceptually, it requires the program to filter out data that meet certain conditions from the data of the latest period of time, and calculate a result according to a defined formula based on these data. When the result meets certain conditions and lasts for a certain period of time, it will notify the user in some form.

In order to meet the needs of users for alarm monitoring, TDengine provides this function in the form of an independent module. For its installation and use, please refer to the blog [How to Use TDengine for Alarm Monitoring](https://www.taosdata.com/blog/2020/04/14/1438.html).