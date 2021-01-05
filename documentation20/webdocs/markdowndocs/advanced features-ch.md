# 高级功能

## 连续查询(Continuous Query)

连续查询是TDengine定期自动执行的查询，采用滑动窗口的方式进行计算，是一种简化的时间驱动的流式计算。
针对库中的表或超级表，TDengine可提供定期自动执行的连续查询，
用户可让TDengine推送查询的结果，也可以将结果再写回到TDengine中。
每次执行的查询是一个时间窗口，时间窗口随着时间流动向前滑动。
在定义连续查询的时候需要指定时间窗口（time window, 参数interval）大小和每次前向增量时间（forward sliding times, 参数sliding）。

TDengine的连续查询采用时间驱动模式，可以直接使用TAOS SQL进行定义，不需要额外的操作。
使用连续查询，可以方便快捷地按照时间窗口生成结果，从而对原始采集数据进行降采样（down sampling）。
用户通过TAOS SQL定义连续查询以后，TDengine自动在最后的一个完整的时间周期末端拉起查询，
并将计算获得的结果推送给用户或者写回TDengine。

TDengine提供的连续查询与普通流计算中的时间窗口计算具有以下区别：

- 不同于流计算的实时反馈计算结果，连续查询只在时间窗口关闭以后才开始计算。
例如时间周期是1天，那么当天的结果只会在23:59:59以后才会生成。

- 如果有历史记录写入到已经计算完成的时间区间，连续查询并不会重新进行计算，
也不会重新将结果推送给用户。对于写回TDengine的模式，也不会更新已经存在的计算结果。

- 使用连续查询推送结果的模式，服务端并不缓存客户端计算状态，也不提供Exactly-Once的语意保证。
如果用户的应用端崩溃，再次拉起的连续查询将只会从再次拉起的时间开始重新计算最近的一个完整的时间窗口。
如果使用写回模式，TDengine可确保数据写回的有效性和连续性。

### 使用连续查询

下面以智能电表场景为例介绍连续查询的具体使用方法。假设我们通过下列SQL语句创建了超级表和子表：

```sql
create table meters (ts timestamp, current float, voltage int, phase float) tags (location binary(64), groupdId int);
create table D1001 using meters tags ("Beijing.Chaoyang", 2);
create table D1002 using meters tags ("Beijing.Haidian", 2);
...
```

我们已经知道，可以通过下面这条SQL语句以一分钟为时间窗口、30秒为前向增量统计这些电表的平均电压。

```sql
select avg(voltage) from meters interval(1m) sliding(30s);
```

每次执行这条语句，都会重新计算所有数据。 
如果需要每隔30秒执行一次来增量计算最近一分钟的数据，
可以把上面的语句改进成下面的样子，每次使用不同的 `startTime` 并定期执行：

```sql
select avg(voltage) from meters where ts > {startTime} interval(1m) sliding(30s);
```

这样做没有问题，但TDengine提供了更简单的方法，
只要在最初的查询语句前面加上 `create table {tableName} as ` 就可以了, 例如：

```sql
create table avg_vol as select avg(voltage) from meters interval(1m) sliding(30s);
```

会自动创建一个名为 `avg_vol` 的新表，然后每隔30秒，TDengine会增量执行 `as` 后面的 SQL 语句，
并将查询结果写入这个表中，用户程序后续只要从 `avg_vol` 中查询数据即可。 例如：

```mysql
taos> select * from avg_vol;
            ts           |        avg_voltage_    |
===================================================
 2020-07-29 13:37:30.000 |            222.0000000 |
 2020-07-29 13:38:00.000 |            221.3500000 |
 2020-07-29 13:38:30.000 |            220.1700000 |
 2020-07-29 13:39:00.000 |            223.0800000 |
```

需要注意，查询时间窗口的最小值是10毫秒，没有时间窗口范围的上限。

此外，TDengine还支持用户指定连续查询的起止时间。
如果不输入开始时间，连续查询将从第一条原始数据所在的时间窗口开始；
如果没有输入结束时间，连续查询将永久运行；
如果用户指定了结束时间，连续查询在系统时间达到指定的时间以后停止运行。
比如使用下面的SQL创建的连续查询将运行一小时，之后会自动停止。

```mysql
create table avg_vol as select avg(voltage) from meters where ts > now and ts <= now + 1h interval(1m) sliding(30s);
```

需要说明的是，上面例子中的 `now` 是指创建连续查询的时间，而不是查询执行的时间，否则，查询就无法自动停止了。 
另外，为了尽量避免原始数据延迟写入导致的问题，TDengine中连续查询的计算有一定的延迟。
也就是说，一个时间窗口过去后，TDengine并不会立即计算这个窗口的数据，
所以要稍等一会（一般不会超过1分钟）才能查到计算结果。


### 管理连续查询

用户可在控制台中通过 `show streams` 命令来查看系统中全部运行的连续查询，
并可以通过 `kill stream` 命令杀掉对应的连续查询。
后续版本会提供更细粒度和便捷的连续查询管理命令。


## 数据订阅(Publisher/Subscriber)

基于数据天然的时间序列特性，TDengine的数据写入（insert）与消息系统的数据发布（pub）逻辑上一致，
均可视为系统中插入一条带时间戳的新记录。
同时，TDengine在内部严格按照数据时间序列单调递增的方式保存数据。
本质上来说，TDengine中里每一张表均可视为一个标准的消息队列。

TDengine内嵌支持轻量级的消息订阅与推送服务。
使用系统提供的API，用户可使用普通查询语句订阅数据库中的一张或多张表。
订阅的逻辑和操作状态的维护均是由客户端完成，客户端定时轮询服务器是否有新的记录到达，
有新的记录到达就会将结果反馈到客户。

TDengine的订阅与推送服务的状态是客户端维持，TDengine服务器并不维持。
因此如果应用重启，从哪个时间点开始获取最新数据，由应用决定。

TDengine的API中，与订阅相关的主要有以下三个：

```c
taos_subscribe
taos_consume
taos_unsubscribe
```

这些API的文档请见 [C/C++ Connector](https://www.taosdata.com/cn/documentation20/connector/)，
下面仍以智能电表场景为例介绍一下它们的具体用法（超级表和子表结构请参考上一节“连续查询”），
完整的示例代码可以在 [这里](https://github.com/taosdata/TDengine/blob/master/tests/examples/c/subscribe.c) 找到。

如果我们希望当某个电表的电流超过一定限制（比如10A）后能得到通知并进行一些处理， 有两种方法：
一是分别对每张子表进行查询，每次查询后记录最后一条数据的时间戳，后续只查询这个时间戳之后的数据：

```sql
select * from D1001 where ts > {last_timestamp1} and current > 10;
select * from D1002 where ts > {last_timestamp2} and current > 10;
...
```

这确实可行，但随着电表数量的增加，查询数量也会增加，客户端和服务端的性能都会受到影响，
当电表数增长到一定的程度，系统就无法承受了。

另一种方法是对超级表进行查询。这样，无论有多少电表，都只需一次查询：

```sql
select * from meters where ts > {last_timestamp} and current > 10;
```

但是，如何选择 `last_timestamp` 就成了一个新的问题。
因为，一方面数据的产生时间（也就是数据时间戳）和数据入库的时间一般并不相同，有时偏差还很大；
另一方面，不同电表的数据到达TDengine的时间也会有差异。
所以，如果我们在查询中使用最慢的那台电表的数据的时间戳作为 `last_timestamp`，
就可能重复读入其它电表的数据；
如果使用最快的电表的时间戳，其它电表的数据就可能被漏掉。

TDengine的订阅功能为上面这个问题提供了一个彻底的解决方案。

首先是使用`taos_subscribe`创建订阅：

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

TDengine中的订阅既可以是同步的，也可以是异步的，
上面的代码会根据从命令行获取的参数`async`的值来决定使用哪种方式。
这里，同步的意思是用户程序要直接调用`taos_consume`来拉取数据，
而异步则由API在内部的另一个线程中调用`taos_consume`，
然后把拉取到的数据交给回调函数`subscribe_callback`去处理。

参数`taos`是一个已经建立好的数据库连接，在同步模式下无特殊要求。
但在异步模式下，需要注意它不会被其它线程使用，否则可能导致不可预计的错误，
因为回调函数在API的内部线程中被调用，而TDengine的部分API不是线程安全的。

参数`sql`是查询语句，可以在其中使用where子句指定过滤条件。
在我们的例子中，如果只想订阅电流超过10A时的数据，可以这样写：

```sql
select * from meters where current > 10;
```

注意，这里没有指定起始时间，所以会读到所有时间的数据。
如果只想从一天前的数据开始订阅，而不需要更早的历史数据，可以再加上一个时间条件：

```sql
select * from meters where ts > now - 1d and current > 10;
```

订阅的`topic`实际上是它的名字，因为订阅功能是在客户端API中实现的，
所以没必要保证它全局唯一，但需要它在一台客户端机器上唯一。

如果名`topic`的订阅不存在，参数`restart`没有意义；
但如果用户程序创建这个订阅后退出，当它再次启动并重新使用这个`topic`时，
`restart`就会被用于决定是从头开始读取数据，还是接续上次的位置进行读取。
本例中，如果`restart`是 **true**（非零值），用户程序肯定会读到所有数据。
但如果这个订阅之前就存在了，并且已经读取了一部分数据，
且`restart`是 **false**（**0**），用户程序就不会读到之前已经读取的数据了。

`taos_subscribe`的最后一个参数是以毫秒为单位的轮询周期。
在同步模式下，如过前后两次调用`taos_consume`的时间间隔小于此时间，
`taos_consume`会阻塞，直到间隔超过此时间。
异步模式下，这个时间是两次调用回调函数的最小时间间隔。

`taos_subscribe`的倒数第二个参数用于用户程序向回调函数传递附加参数，
订阅API不对其做任何处理，只原样传递给回调函数。此参数在同步模式下无意义。

订阅创建以后，就可以消费其数据了，同步模式下，示例代码是下面的 else 部分：

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

这里是一个 **while** 循环，用户每按一次回车键就调用一次`taos_consume`，
而`taos_consume`的返回值是查询到的结果集，与`taos_use_result`完全相同，
例子中使用这个结果集的代码是函数`print_result`：

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

其中的 `taos_print_row` 用于处理订阅到数据，在我们的例子中，它会打印出所有符合条件的记录。
而异步模式下，消费订阅到的数据则显得更为简单：

```c
void subscribe_callback(TAOS_SUB* tsub, TAOS_RES *res, void* param, int code) {
　　print_result(res, *(int*)param);
}
```

当要结束一次数据订阅时，需要调用`taos_unsubscribe`：

```c
taos_unsubscribe(tsub, keep);
```

其第二个参数，用于决定是否在客户端保留订阅的进度信息。
如果这个参数是**false**（**0**），那无论下次调用`taos_subscribe`的时的`restart`参数是什么，
订阅都只能重新开始。
另外，进度信息的保存位置是 *{DataDir}/subscribe/* 这个目录下，
每个订阅有一个与其`topic`同名的文件，删掉某个文件，同样会导致下次创建其对应的订阅时只能重新开始。

代码介绍完毕，我们来看一下实际的运行效果。假设：

* 示例代码已经下载到本地
* TDengine 也已经在同一台机器上安装好
* 示例所需的数据库、超级表、子表已经全部创建好

则可以在示例代码所在目录执行以下命令来编译并启动示例程序：

```shell
$ make
$ ./subscribe -sql='select * from meters where current > 10;'
```

示例程序启动后，打开另一个终端窗口，启动 TDengine 的 shell 向 **D1001** 插入一条电流为 12A 的数据：

```shell
$ taos
> use test;
> insert into D1001 values(now, 12, 220, 1);
```

这时，因为电流超过了10A，您应该可以看到示例程序将它输出到了屏幕上。
您可以继续插入一些数据观察示例程序的输出。

### Java 使用数据订阅功能

订阅功能也提供了 Java 开发接口，相关说明请见 [Java Connector](https://www.taosdata.com/cn/documentation20/connector/)。需要注意的是，目前 Java 接口没有提供异步订阅模式，但用户程序可以通过创建 `TimerTask` 等方式达到同样的效果。

下面以一个示例程序介绍其具体使用方法。它所完成的功能与前面介绍的 C 语言示例基本相同，也是订阅数据库中所有电流超过 10A 的记录。 

#### 准备数据

```sql
# 创建 power 库
taos> create database power;
# 切换库
taos> use power;
# 创建超级表
taos> create table meters(ts timestamp, current float, voltage int, phase int) tags(location binary(64), groupId int);
# 创建表
taos> create table d1001 using meters tags ("Beijing.Chaoyang", 2);
taos> create table d1002 using meters tags ("Beijing.Haidian", 2);
# 插入测试数据
taos> insert into d1001 values("2020-08-15 12:00:00.000", 12, 220, 1),("2020-08-15 12:10:00.000", 12.3, 220, 2),("2020-08-15 12:20:00.000", 12.2, 220, 1);
taos> insert into d1002 values("2020-08-15 12:00:00.000", 9.9, 220, 1),("2020-08-15 12:10:00.000", 10.3, 220, 1),("2020-08-15 12:20:00.000", 11.2, 220, 1);
# 从超级表 meters 查询电流大于 10A 的记录
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

#### 示例程序

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
            subscribe = ((TSDBConnection) connection).subscribe(topic, sql, true); // 创建订阅
            int count = 0;
            while (count < 10) {
                TimeUnit.SECONDS.sleep(1); // 等待1秒，避免频繁调用 consume，给服务端造成压力
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
                    subscribe.close(true); // 关闭订阅
                if (connection != null) 
                    connection.close(); 
            } catch (SQLException throwables) {
                throwables.printStackTrace();
            }
        }
    }
}
```

运行示例程序，首先，它会消费符合查询条件的所有历史数据：

```shell
# java -jar subscribe.jar 

ts: 1597464000000	current: 12.0	voltage: 220	phase: 1	location: Beijing.Chaoyang	groupid : 2
ts: 1597464600000	current: 12.3	voltage: 220	phase: 2	location: Beijing.Chaoyang	groupid : 2
ts: 1597465200000	current: 12.2	voltage: 220	phase: 1	location: Beijing.Chaoyang	groupid : 2
ts: 1597464600000	current: 10.3	voltage: 220	phase: 1	location: Beijing.Haidian	groupid : 2
ts: 1597465200000	current: 11.2	voltage: 220	phase: 1	location: Beijing.Haidian	groupid : 2
```

接着，使用 taos 客户端向表中新增一条数据：

```sql
# taos
taos> use power;
taos> insert into d1001 values("2020-08-15 12:40:00.000", 12.4, 220, 1);
```

因为这条数据的电流大于10A，示例程序会将其消费：

```shell
ts: 1597466400000	current: 12.4	voltage: 220	phase: 1	location: Beijing.Chaoyang	groupid: 2
```


## 缓存(Cache)

TDengine采用时间驱动缓存管理策略（First-In-First-Out，FIFO），又称为写驱动的缓存管理机制。这种策略有别于读驱动的数据缓存模式（Least-Recent-Use，LRU），直接将最近写入的数据保存在系统的缓存中。当缓存达到临界值的时候，将最早的数据批量写入磁盘。一般意义上来说，对于物联网数据的使用，用户最为关心最近产生的数据，即当前状态。TDengine充分利用了这一特性，将最近到达的（当前状态）数据保存在缓存中。

TDengine通过查询函数向用户提供毫秒级的数据获取能力。直接将最近到达的数据保存在缓存中，可以更加快速地响应用户针对最近一条或一批数据的查询分析，整体上提供更快的数据库查询响应能力。从这个意义上来说，可通过设置合适的配置参数将TDengine作为数据缓存来使用，而不需要再部署额外的缓存系统，可有效地简化系统架构，降低运维的成本。需要注意的是，TDengine重启以后系统的缓存将被清空，之前缓存的数据均会被批量写入磁盘，缓存的数据将不会像专门的Key-value缓存系统再将之前缓存的数据重新加载到缓存中。

TDengine分配固定大小的内存空间作为缓存空间，缓存空间可根据应用的需求和硬件资源配置。通过适当的设置缓存空间，TDengine可以提供极高性能的写入和查询的支持。TDengine中每个虚拟节点（virtual node）创建时分配独立的缓存池。每个虚拟节点管理自己的缓存池，不同虚拟节点间不共享缓存池。每个虚拟节点内部所属的全部表共享该虚拟节点的缓存池。

TDengine将内存池按块划分进行管理，数据在内存块里按照列式存储。一个vnode的内存池是在vnode创建时按块分配好的，而且每个内存块按照先进先出的原则进行管理。一张表所需要的内存块是从vnode的内存池中进行分配的，块的大小由系统配置参数cache决定。每张表最大内存块的数目由配置参数tblocks决定，每张表平均的内存块的个数由配置参数ablocks决定。因此对于一个vnode, 总的内存大小为: `cache * ablocks * tables`。内存块参数cache不宜过小，一个cache block需要能存储至少几十条以上记录，才会有效率。参数ablocks最小为2，保证每张表平均至少能分配两个内存块。

你可以通过函数last_row快速获取一张表或一张超级表的最后一条记录，这样很便于在大屏显示各设备的实时状态或采集值。例如：

```mysql
select last_row(voltage) from meters where location='Beijing.Chaoyang';
```

该SQL语句将获取所有位于北京朝阳区的电表最后记录的电压值。


## 报警监测(Alert)

在 TDengine 的应用场景中，报警监测是一个常见需求，从概念上说，它要求程序从最近一段时间的数据中筛选出符合一定条件的数据，并基于这些数据根据定义好的公式计算出一个结果，当这个结果符合某个条件且持续一定时间后，以某种形式通知用户。

为了满足用户对报警监测的需求，TDengine 以独立模块的形式提供了这一功能，有关它的安装使用方法，请参考博客 [使用 TDengine 进行报警监测](https://www.taosdata.com/blog/2020/04/14/1438.html) 。
