---
sidebar_label: 数据订阅
description: "轻量级的数据订阅与推送服务。连续写入到 TDengine 中的时序数据能够被自动推送到订阅客户端。"
title: 数据订阅
---

import Tabs from "@theme/Tabs";
import TabItem from "@theme/TabItem";
import Java from "./_sub_java.mdx";
import Python from "./_sub_python.mdx";
import Go from "./_sub_go.mdx";
import Rust from "./_sub_rust.mdx";
import Node from "./_sub_node.mdx";
import CSharp from "./_sub_cs.mdx";
import CDemo from "./_sub_c.mdx";

基于数据天然的时间序列特性，TDengine 的数据写入（insert）与消息系统的数据发布（pub）逻辑上一致，均可视为系统中插入一条带时间戳的新记录。同时，TDengine 在内部严格按照数据时间序列单调递增的方式保存数据。本质上来说，TDengine 中每一张表均可视为一个标准的消息队列。

TDengine 内嵌支持轻量级的消息订阅与推送服务。使用系统提供的 API，用户可使用普通查询语句订阅数据库中的一张或多张表。订阅的逻辑和操作状态的维护均是由客户端完成，客户端定时轮询服务器是否有新的记录到达，有新的记录到达就会将结果反馈到客户。

TDengine 的订阅与推送服务的状态是由客户端维持，TDengine 服务端并不维持。因此如果应用重启，从哪个时间点开始获取最新数据，由应用决定。

TDengine 的 API 中，与订阅相关的主要有以下三个：

```c
taos_subscribe
taos_consume
taos_unsubscribe
```

这些 API 的文档请见 [C/C++ Connector](/reference/connector/cpp)，下面仍以智能电表场景为例介绍一下它们的具体用法（超级表和子表结构请参考上一节“连续查询”），完整的示例代码可以在 [这里](https://github.com/taosdata/TDengine/blob/master/examples/c/subscribe.c) 找到。

如果我们希望当某个电表的电流超过一定限制（比如 10A）后能得到通知并进行一些处理， 有两种方法：一是分别对每张子表进行查询，每次查询后记录最后一条数据的时间戳，后续只查询这个时间戳之后的数据：

```sql
select * from D1001 where ts > {last_timestamp1} and current > 10;
select * from D1002 where ts > {last_timestamp2} and current > 10;
...
```

这确实可行，但随着电表数量的增加，查询数量也会增加，客户端和服务端的性能都会受到影响，当电表数增长到一定的程度，系统就无法承受了。

另一种方法是对超级表进行查询。这样，无论有多少电表，都只需一次查询：

```sql
select * from meters where ts > {last_timestamp} and current > 10;
```

但是，如何选择 `last_timestamp` 就成了一个新的问题。因为，一方面数据的产生时间（也就是数据时间戳）和数据入库的时间一般并不相同，有时偏差还很大；另一方面，不同电表的数据到达 TDengine 的时间也会有差异。所以，如果我们在查询中使用最慢的那台电表的数据的时间戳作为 `last_timestamp`，就可能重复读入其它电表的数据；如果使用最快的电表的时间戳，其它电表的数据就可能被漏掉。

TDengine 的订阅功能为上面这个问题提供了一个彻底的解决方案。

首先是使用 `taos_subscribe` 创建订阅：

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

TDengine 中的订阅既可以是同步的，也可以是异步的，上面的代码会根据从命令行获取的参数 `async` 的值来决定使用哪种方式。这里，同步的意思是用户程序要直接调用 `taos_consume` 来拉取数据，而异步则由 API 在内部的另一个线程中调用 `taos_consume`，然后把拉取到的数据交给回调函数 `subscribe_callback`去处理。（注意，`subscribe_callback` 中不宜做较为耗时的操作，否则有可能导致客户端阻塞等不可控的问题。）

参数 `taos` 是一个已经建立好的数据库连接，在同步模式下无特殊要求。但在异步模式下，需要注意它不会被其它线程使用，否则可能导致不可预计的错误，因为回调函数在 API 的内部线程中被调用，而 TDengine 的部分 API 不是线程安全的。

参数 `sql` 是查询语句，可以在其中使用 where 子句指定过滤条件。在我们的例子中，如果只想订阅电流超过 10A 时的数据，可以这样写：

```sql
select * from meters where current > 10;
```

注意，这里没有指定起始时间，所以会读到所有时间的数据。如果只想从一天前的数据开始订阅，而不需要更早的历史数据，可以再加上一个时间条件：

```sql
select * from meters where ts > now - 1d and current > 10;
```

订阅的 `topic` 实际上是它的名字，因为订阅功能是在客户端 API 中实现的，所以没必要保证它全局唯一，但需要它在一台客户端机器上唯一。

如果名为 `topic` 的订阅不存在，参数 `restart` 没有意义；但如果用户程序创建这个订阅后退出，当它再次启动并重新使用这个 `topic` 时，`restart` 就会被用于决定是从头开始读取数据，还是接续上次的位置进行读取。本例中，如果 `restart` 是 **true**（非零值），用户程序肯定会读到所有数据。但如果这个订阅之前就存在了，并且已经读取了一部分数据，且 `restart` 是 **false**（**0**），用户程序就不会读到之前已经读取的数据了。

`taos_subscribe`的最后一个参数是以毫秒为单位的轮询周期。在同步模式下，如果前后两次调用 `taos_consume` 的时间间隔小于此时间，`taos_consume` 会阻塞，直到间隔超过此时间。异步模式下，这个时间是两次调用回调函数的最小时间间隔。

`taos_subscribe` 的倒数第二个参数用于用户程序向回调函数传递附加参数，订阅 API 不对其做任何处理，只原样传递给回调函数。此参数在同步模式下无意义。

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

这里是一个 **while** 循环，用户每按一次回车键就调用一次 `taos_consume`，而 `taos_consume` 的返回值是查询到的结果集，与 `taos_use_result` 完全相同，例子中使用这个结果集的代码是函数 `print_result`：

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
　　　　　　taos_print_row(temp, row, fields, num_fields);
　　　　　　puts(temp);
　　　　　　nRows++;
　　　　}
　　}
　　printf("%d rows consumed.\n", nRows);
}
```

其中的 `taos_print_row` 用于处理订阅到数据，在我们的例子中，它会打印出所有符合条件的记录。而异步模式下，消费订阅到的数据则显得更为简单：

```c
void subscribe_callback(TAOS_SUB* tsub, TAOS_RES *res, void* param, int code) {
　　print_result(res, *(int*)param);
}
```

当要结束一次数据订阅时，需要调用 `taos_unsubscribe`：

```c
taos_unsubscribe(tsub, keep);
```

其第二个参数，用于决定是否在客户端保留订阅的进度信息。如果这个参数是**false**（**0**），那无论下次调用 `taos_subscribe` 时的 `restart` 参数是什么，订阅都只能重新开始。另外，进度信息的保存位置是 _{DataDir}/subscribe/_ 这个目录下（注：`taos.cfg` 配置文件中 `DataDir` 参数值默认为 **/var/lib/taos/**,但是 Windows 服务器上本身不存在该目录，所以需要在 Windows 的配置文件中修改 `DataDir` 参数值为相应的已存在目录"），每个订阅有一个与其 `topic` 同名的文件，删掉某个文件，同样会导致下次创建其对应的订阅时只能重新开始。

代码介绍完毕，我们来看一下实际的运行效果。假设：

- 示例代码已经下载到本地
- TDengine 也已经在同一台机器上安装好
- 示例所需的数据库、超级表、子表已经全部创建好

则可以在示例代码所在目录执行以下命令来编译并启动示例程序：

```bash
make
./subscribe -sql='select * from meters where current > 10;'
```

示例程序启动后，打开另一个终端窗口，启动 TDengine CLI 向 **D1001** 插入一条电流为 12A 的数据：

```sql
$ taos
> use test;
> insert into D1001 values(now, 12, 220, 1);
```

这时，因为电流超过了 10A，您应该可以看到示例程序将它输出到了屏幕上。您可以继续插入一些数据观察示例程序的输出。

## 示例程序

下面的示例程序展示是如何使用连接器订阅所有电流超过 10A 的记录。

### 准备数据

```
# create database "power"
taos> create database power;
# use "power" as the database in following operations
taos> use power;
# create super table "meters"
taos> create table meters(ts timestamp, current float, voltage int, phase int) tags(location binary(64), groupId int);
# create tabes using the schema defined by super table "meters"
taos> create table d1001 using meters tags ("California.SanFrancisco", 2);
taos> create table d1002 using meters tags ("California.LosAngeles", 2);
# insert some rows
taos> insert into d1001 values("2020-08-15 12:00:00.000", 12, 220, 1),("2020-08-15 12:10:00.000", 12.3, 220, 2),("2020-08-15 12:20:00.000", 12.2, 220, 1);
taos> insert into d1002 values("2020-08-15 12:00:00.000", 9.9, 220, 1),("2020-08-15 12:10:00.000", 10.3, 220, 1),("2020-08-15 12:20:00.000", 11.2, 220, 1);
# filter out the rows in which current is bigger than 10A
taos> select * from meters where current > 10;
           ts            |    current   |    voltage   |  phase |         location          |   groupid   |
===========================================================================================================
 2020-08-15 12:10:00.000 |    10.30000  |     220      |      1 |      California.LosAngeles      |           2 |
 2020-08-15 12:20:00.000 |    11.20000  |     220      |      1 |      California.LosAngeles      |           2 |
 2020-08-15 12:00:00.000 |    12.00000  |     220      |      1 |      California.SanFrancisco     |           2 |
 2020-08-15 12:10:00.000 |    12.30000  |     220      |      2 |      California.SanFrancisco     |           2 |
 2020-08-15 12:20:00.000 |    12.20000  |     220      |      1 |      California.SanFrancisco     |           2 |
Query OK, 5 row(s) in set (0.004896s)
```

### 示例代码

<Tabs defaultValue="java" groupId="lang">
  <TabItem label="Java" value="java">
    <Java />
  </TabItem>
  <TabItem label="Python" value="Python">
    <Python />
  </TabItem>
  {/* <TabItem label="Go" value="go">
      <Go/>
  </TabItem> */}
  <TabItem label="Rust" value="rust">
    <Rust />
  </TabItem>
  {/* <TabItem label="Node.js" value="nodejs">
      <Node/>
  </TabItem>
  <TabItem label="C#" value="csharp">
      <CSharp/>
  </TabItem> */}
  <TabItem label="C" value="c">
    <CDemo />
  </TabItem>
</Tabs>

### 运行示例程序

示例程序会先消费符合查询条件的所有历史数据：

```bash
ts: 1597464000000	current: 12.0	voltage: 220	phase: 1	location: California.SanFrancisco	groupid : 2
ts: 1597464600000	current: 12.3	voltage: 220	phase: 2	location: California.SanFrancisco	groupid : 2
ts: 1597465200000	current: 12.2	voltage: 220	phase: 1	location: California.SanFrancisco	groupid : 2
ts: 1597464600000	current: 10.3	voltage: 220	phase: 1	location: California.LosAngeles	groupid : 2
ts: 1597465200000	current: 11.2	voltage: 220	phase: 1	location: California.LosAngeles	groupid : 2
```

接着，使用 TDengine CLI 向表中新增一条数据：

```
# taos
taos> use power;
taos> insert into d1001 values(now, 12.4, 220, 1);
```

因为这条数据的电流大于 10A，示例程序会将其消费：

```
ts: 1651146662805	current: 12.4	voltage: 220	phase: 1	location: California.SanFrancisco	groupid: 2
```
