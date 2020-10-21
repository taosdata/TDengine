# 设计文档

在本文档中，您将看到TDengine扩展如何实现将数据保存到TDengine 数据库的功能。

## 使用 JDBC 还是 RESTful APIs 链接数据库?

与TDengine服务通信的方式有两种：JDBC和RESTful API。 当前，该扩展使用RESTful API 进行连接。

原因是方便用户的部署操作。 如果使用JDBC方式连接，除了需要部署扩展外，还需要部署taos.dll/libtaos.so文件。 在典型情况下，TDengine服务不会与HiveMQ托管在同一服务器/虚拟机/docker映像上。因此，JDBC所需要的文件需要单独部署在HiveMQ 服务所在的环境中。这意味着用户必须手动复制taos.dll/libtaos.so文件。详情请查看 [官方文档](https://www.taosdata.com/cn/documentation/connector-java/).

使用RESTful API易于部署，用户不需要部署其他操作。

## 超级表/子表模型

超级表是TDengine引入的一项重要功能。 它不维护原始数据，但用作物理表的模板。

正如TDengine 官方文档所说, 超级表是指某一特定类型的数据采集点的集合。同一类型的数据采集点，其表的结构是完全一样的，但每个表（数据采集点）的静态属性（标签）是不一样的。描述一个超级表（某一特定类型的数据采集点的结合），除需要定义采集量的表结构之外，还需要定义其标签的schema，标签的数据类型可以是整数、浮点数、字符串，标签可以有多个，可以事后增加、删除或修改。 如果整个系统有N个不同类型的数据采集点，就需要建立N个超级表。

在TDengine的设计里，表用来代表一个具体的数据采集点，超级表用来代表一组相同类型的数据采集点集合。当为某个具体数据采集点创建表时，用户使用超级表的定义做模板，同时指定该具体采集点（表）的标签值。与传统的关系型数据库相比，表（一个数据采集点）是带有静态标签的，而且这些标签可以事后增加、删除、修改。一张超级表包含有多张表，这些表具有相同的时序数据schema，但带有不同的标签值。

为了将来方便查询和组织数据，TDengine扩展决定使用超级表/子表模型来维护数据。TDengine扩展启动时，它将自动检查超级表是否存在。 如果超级表不存在，它将自动创建。TDengine扩展将使用设备ID作为子表名称。 如果设备ID首次出现。 TDengine扩展将立即创建一个映射到该设备ID的新子表。 然后，有关此设备ID的所有数据将存储到该新子表中。

## 并发和异步I/O

众所周知，HiveMQ在收到MQTT数据时会调用PublishInboundInterceptor::onInboundPublish()。 如果onInboundPublish()方法花费很长时间来处理数据，这是否会影响HiveMQ性能？

通过调试HiveMQ的代码，您可以注意到HiveMQ每次都使用并行库来调用onInboundPublish()函数。这对于高I/O场景非常有用。

```java
... Call stack top ...
	at com.github.micli.catfish.TDengineInterceptor.onInboundPublish(TDengineInterceptor.java:54)
	at cO.f$b.a(Unknown Source)
	at cO.f$b.apply(Unknown Source)
	at cN.q$b.a(Unknown Source)
	at cN.q$b.c(Unknown Source)
	at cN.q$b.b(Unknown Source)
	at cN.q$b.run(Unknown Source)
	at java.base/java.util.concurrent.Executors$RunnableAdapter.call(Executors.java:515)
	at java.base/java.util.concurrent.FutureTask.run(FutureTask.java:264)
	at java.base/java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1128)
	at java.base/java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:628)
	at java.base/java.lang.Thread.run(Thread.java:834)
```
这并不意味着我们不需要异步I/O来提高性能。 代码改进的下一步是将同步调用更改为异步调用。在1.1.0版中，TDengine扩展已在PublishInboundInterceptor::onInboundPublish() 方法中实现了异步处理程序。这将减少MQTT发布者和MQTT订阅者之间的时间延迟。在1.1.2版中，TDengine扩展已由同步HTTP方法(executeSQL)代替了异步HTTP访问(executeSQLAsync)。我不知道它是否有效地改善了I/O。因为所有工作负载都将移至TDengine服务。

## 自动创建数据库对象

为了使此扩展非常易于使用，在设计之初，我就考虑了如何自动维护所有数据库对象。使用此功能，用户无需手动创建数据库和表。而是通过TDengine扩展创建所有对象。

TDengine扩展的初始化步骤如下：

![process](./images/process-initialization.png)

如果是第一次启动，TDengine扩展将自动创建数据库和超级表。超级表将用作子表模板。每个新的设备ID将来都将映射到一个新的子表。

如果不是第一次启动，TDengine扩展将通过以下SQL语句检索设备ID和子表名称：

```sql
SELECT deviceId, TBNAME FROM [super table]
```

该映射数据将被加载到Java中的哈希表对象中。 当MQTT数据到达TDengine扩展时，它将通过哈希表检索表名，以进行数据插入。如果在扩展程序运行时添加了新设备，TDengine扩展程序将自动创建子表，并将派生ID-表映射添加到哈希表中。

通过上述功能，可以支持将超级表/子表模型存储在TDengine数据库中。

## 设计参考

+ [TDengine 数据模型和整体架构](https://www.taosdata.com/cn/documentation/architecture/)
+ [创建超级表](https://www.taosdata.com/cn/documentation/model/#%E5%88%9B%E5%BB%BA%E8%B6%85%E7%BA%A7%E8%A1%A8)
