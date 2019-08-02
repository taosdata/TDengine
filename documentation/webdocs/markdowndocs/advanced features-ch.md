#高级功能

## 连续查询(Continuous Query)
连续查询是TDengine定期自动执行的查询，采用滑动窗口的方式进行计算，是一种简化的时间驱动的流式计算。针对库中的表或超级表，TDengine可提供定期自动执行的连续查询，用户可让TDengine推送查询的结果，也可以将结果再写回到TDengine中。每次执行的查询是一个时间窗口，时间窗口随着时间流动向前滑动。在定义连续查询的时候需要指定时间窗口（time window, 参数interval ）大小和每次前向增量时间（forward sliding times, 参数sliding）。

TDengine的连续查询采用时间驱动模式，可以直接使用TAOS SQL进行定义，不需要额外的操作。使用连续查询，可以方便快捷地按照时间窗口生成结果，从而对原始采集数据进行降采样（down sampling）。用户通过TAOS SQL定义连续查询以后，TDengine自动在最后的一个完整的时间周期末端拉起查询，并将计算获得的结果推送给用户或者写回TDengine。

TDengine提供的连续查询与普通流计算中的时间窗口计算具有以下区别：

- 不同于流计算的实时反馈计算结果，连续查询只在时间窗口关闭以后才开始计算。例如时间周期是1天，那么当天的结果只会在23:59:59以后才会生成。

- 如果有历史记录写入到已经计算完成的时间区间，连续查询并不会重新进行计算，也不会重新将结果推送给用户。对于写回TDengine的模式，也不会更新已经存在的计算结果。

- 使用连续查询推送结果的模式，服务端并不缓存客户端计算状态，也不提供Exactly-Once的语意保证。如果用户的应用端崩溃，再次拉起的连续查询将只会从再次拉起的时间开始重新计算最近的一个完整的时间窗口。如果使用写回模式，TDengine可确保数据写回的有效性和连续性。

#### 使用连续查询

使用TAOS SQL定义连续查询的过程，需要调用API taos_stream在应用端启动连续查询。例如要对统计表FOO_TABLE 每1分钟统计一次记录数量，前向滑动的时间是30秒，SQL语句如下：

```sql
SELECT COUNT(*) 
FROM FOO_TABLE 
INTERVAL(1M) SLIDING(30S)
```

其中查询的时间窗口（time window）是1分钟，前向增量（forward sliding time）时间是30秒。也可以不使用sliding来指定前向滑动时间，此时系统将自动向前滑动一个查询时间窗口再开始下一次计算，即时间窗口长度等于前向滑动时间。

```sql
SELECT COUNT(*) 
FROM FOO_TABLE 
INTERVAL(1M)
```

如果需要将连续查询的计算结果写回到数据库中，可以使用如下的SQL语句

```sql
CREATE TABLE QUERY_RES 
  AS 
  SELECT COUNT(*) 
  FROM FOO_TABLE 
  INTERVAL(1M) SLIDING(30S)
```

此时系统将自动创建表QUERY_RES，然后将连续查询的结果写入到该表。需要注意的是，前向滑动时间不能大于时间窗口的范围。如果用户指定的前向滑动时间超过时间窗口范围，系统将自动将其设置为时间窗口的范围值。如上所示SQL语句，如果用户设置前向滑动时间超过1分钟，系统将强制将其设置为1分钟。 

此外，TDengine还支持用户指定连续查询的结束时间，默认如果不输入结束时间，连续查询将永久运行，如果用户指定了结束时间，连续查询在系统时间达到指定的时间以后停止运行。如SQL所示，连续查询将运行1个小时，1小时之后连续查询自动停止。

```sql
CREATE TABLE QUERY_RES 
  AS 
  SELECT COUNT(*) 
  FROM FOO_TABLE 
  WHERE TS > NOW AND TS <= NOW + 1H 
  INTERVAL(1M) SLIDING(30S) 
```

此外，还需要注意的是查询时间窗口的最小值是10毫秒，没有时间窗口范围的上限。

#### 管理连续查询

用户可在控制台中通过 *show streams* 命令来查看系统中全部运行的连续查询，并可以通过 *kill stream* 命令杀掉对应的连续查询。在写回模式中，如果用户可以直接将写回的表删除，此时连续查询也会自动停止并关闭。后续版本会提供更细粒度和便捷的连续查询管理命令。

## 数据订阅(Publisher/Subscriber)
基于数据天然的时间序列特性，TDengine的数据写入（insert）与消息系统的数据发布（pub）逻辑上一致，均可视为系统中插入一条带时间戳的新记录。同时，TDengine在内部严格按照数据时间序列单调递增的方式保存数据。本质上来说，TDengine中里每一张表均可视为一个标准的消息队列。

TDengine内嵌支持轻量级的消息订阅与推送服务。使用系统提供的API，用户可订阅数据库中的某一张表（或超级表）。订阅的逻辑和操作状态的维护均是由客户端完成，客户端定时轮询服务器是否有新的记录到达，有新的记录到达就会将结果反馈到客户。

TDengine的订阅与推送服务的状态是客户端维持，TDengine服务器并不维持。因此如果应用重启，从哪个时间点开始获取最新数据，由应用决定。

#### API说明

使用订阅的功能，主要API如下：

<ul>
<li><p><code>TAOS_SUB *taos_subscribe(char *host, char *user, char *pass, char *db, char *table, int64_t time, int mseconds)</code></p><p>该函数负责启动订阅服务。其中参数说明：</p></li><ul>
<li><p>host：主机IP地址</p></li>
<li><p>user：数据库登录用户名</p></li>
<li><p>pass：密码</p></li>
<li><p>db：数据库名称</p></li>
<li><p>table：(超级) 表的名称</p></li>
<li><p>time：启动时间，Unix Epoch时间，单位为毫秒。从1970年1月1日起计算的毫秒数。如果设为0，表示从当前时间开始订阅</p></li>
<li><p>mseconds：查询数据库更新的时间间隔，单位为毫秒。一般设置为1000毫秒。返回值为指向TDengine_SUB 结构的指针，如果返回为空，表示失败。</p></li>
</ul><li><p><code>TAOS_ROW taos_consume(TAOS_SUB *tsub)</code>
</p><p>该函数用来获取订阅的结果，用户应用程序将其置于一个无限循环语句。如果数据库有新记录到达，该API将返回该最新的记录。如果没有新的记录，该API将阻塞。如果返回值为空，说明系统出错。参数说明：</p></li><ul><li><p>tsub：taos_subscribe的结构体指针。</p></li></ul><li><p><code>void taos_unsubscribe(TAOS_SUB *tsub)</code></p><p>取消订阅。应用程序退出时，务必调用该函数以避免资源泄露。</p></li>
<li><p><code>int taos_num_subfields(TAOS_SUB *tsub)</code></p><p>获取返回的一行记录中数据包含多少列。</p></li>
<li><p><code>TAOS_FIELD *taos_fetch_subfields(TAOS_SUB *tsub)</code></p><p>获取每列数据的属性（数据类型、名字、长度），与taos_num_subfileds配合使用，可解析返回的每行数据。</p></li></ul>
示例代码：请看安装包中的的示范程序

## 缓存 (Cache)
TDengine采用时间驱动缓存管理策略（First-In-First-Out，FIFO），又称为写驱动的缓存管理机制。这种策略有别于读驱动的数据缓存模式（Least-Recent-Use，LRU），直接将最近写入的数据保存在系统的缓存中。当缓存达到临界值的时候，将最早的数据批量写入磁盘。一般意义上来说，对于物联网数据的使用，用户最为关心最近产生的数据，即当前状态。TDengine充分利用了这一特性，将最近到达的（当前状态）数据保存在缓存中。

TDengine通过查询函数向用户提供毫秒级的数据获取能力。直接将最近到达的数据保存在缓存中，可以更加快速地响应用户针对最近一条或一批数据的查询分析，整体上提供更快的数据库查询响应能力。从这个意义上来说，可通过设置合适的配置参数将TDengine作为数据缓存来使用，而不需要再部署额外的缓存系统，可有效地简化系统架构，降低运维的成本。需要注意的是，TDengine重启以后系统的缓存将被清空，之前缓存的数据均会被批量写入磁盘，缓存的数据将不会像专门的Key-value缓存系统再将之前缓存的数据重新加载到缓存中。

TDengine分配固定大小的内存空间作为缓存空间，缓存空间可根据应用的需求和硬件资源配置。通过适当的设置缓存空间，TDengine可以提供极高性能的写入和查询的支持。TDengine中每个虚拟节点（virtual node）创建时分配独立的缓存池。每个虚拟节点管理自己的缓存池，不同虚拟节点间不共享缓存池。每个虚拟节点内部所属的全部表共享该虚拟节点的缓存池。

一个缓存池了有很多个缓存块，缓存的大小由缓存块的个数以及缓存块的大小决定。参数cacheBlockSize决定每个缓存块的大小，参数cacheNumOfBlocks决定每个虚拟节点可用缓存块数量。因此单个虚拟节点总缓存开销为cacheBlockSize x cacheNumOfBlocks。参数numOfBlocksPerMeter决定每张表可用缓存块的数量，TDengine要求每张表至少有2个缓存块可供使用，因此cacheNumOfBlocks的数值不应该小于虚拟节点中所包含的表数量的两倍，即cacheNumOfBlocks ≤ sessionsPerVnode x 2。一般情况下cacheBlockSize可以不用调整，使用系统默认值即可，缓存块需要存储至少几十条记录才能确保TDengine更有效率地进行数据写入。

你可以通过函数last_row快速获取一张表或一张超级表的最后一条记录，这样很便于在大屏显示各设备的实时状态或采集值。例如：

```mysql
select last_row(degree) from thermometer where location='beijing';
```

该SQL语句将获取所有位于北京的传感器最后记录的温度值。
