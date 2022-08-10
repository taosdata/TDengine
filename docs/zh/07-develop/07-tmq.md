---
sidebar_label: 数据订阅
description: "数据订阅与推送服务。写入到 TDengine 中的时序数据能够被自动推送到订阅客户端。"
title: 数据订阅
---

为了帮助应用实时获取写入 TDengine 的数据，或者以事件到达顺序处理数据，TDengine提供了类似消息队列产品的数据订阅、消费接口。这样在很多场景下，采用 TDengine 的时序数据处理系统不再需要集成消息队列产品，比如 kafka, 从而简化系统设计的复杂度，降低运营维护成本。

与 kafka 一样，你需要定义 topic, 但 TDengine 的 topic 是基于一个已经存在的超级表、子表或普通表的查询条件，即一个 SELECT 语句。你可以使用 SQL 对标签、表名、列、表达式等条件进行过滤，以及对数据进行标量函数与 UDF 计算（不包括数据聚合）。与其他消息队列软件相比，这是 TDengine 数据订阅功能的最大的优势，它提供了更大的灵活性，数据的颗粒度可以由应用随时调整，而且数据的过滤与预处理交给 TDengine，而不是应用完成，有效的减少传输的数据量与应用的复杂度。

消费者订阅 topic 后，可以实时获得最新的数据。多个消费者可以组成一个消费者组 (consumer group), 一个消费者组里的多个消费者共享消费进度，便于多线程、分布式地消费数据，提高消费速度。但不同消费者组中的消费者即使消费同一个topic, 并不共享消费进度。一个消费者可以订阅多个 topic。如果订阅的是超级表，数据可能会分布在多个不同的 vnode 上，也就是多个 shard 上，这样一个消费组里有多个消费者可以提高消费效率。TDengine 的消息队列提供了消息的ACK机制，在宕机、重启等复杂环境下确保 at least once 消费。

为了实现上述功能，TDengine 会为 WAL (Write-Ahead-Log) 文件自动创建索引以支持快速随机访问，并提供了灵活可配置的文件切换与保留机制：用户可以按需指定 WAL 文件保留的时间以及大小（详见 create database 语句）。通过以上方式将 WAL 改造成了一个保留事件到达顺序的、可持久化的存储引擎（但由于 TSDB 具有远比 WAL 更高的压缩率，我们不推荐保留太长时间，一般来说，不超过几天）。 对于以 topic 形式创建的查询，TDengine 将对接 WAL 而不是 TSDB 作为其存储引擎。在消费时，TDengine 根据当前消费进度从 WAL 直接读取数据，并使用统一的查询引擎实现过滤、变换等操作，将数据推送给消费者。

本文档不对消息队列本身的基础知识做介绍，如果需要了解，请自行搜索。

## 主要数据结构和API

TMQ 的 API 中，与订阅相关的主要数据结构和API如下：

```c
typedef struct tmq_t      tmq_t;
typedef struct tmq_conf_t tmq_conf_t;
typedef struct tmq_list_t tmq_list_t;

typedef void(tmq_commit_cb(tmq_t *, int32_t code, void *param));

DLL_EXPORT tmq_list_t *tmq_list_new();
DLL_EXPORT int32_t     tmq_list_append(tmq_list_t *, const char *);
DLL_EXPORT void        tmq_list_destroy(tmq_list_t *);
DLL_EXPORT tmq_t *tmq_consumer_new(tmq_conf_t *conf, char *errstr, int32_t errstrLen);
DLL_EXPORT const char *tmq_err2str(int32_t code);

DLL_EXPORT int32_t   tmq_subscribe(tmq_t *tmq, const tmq_list_t *topic_list);
DLL_EXPORT int32_t   tmq_unsubscribe(tmq_t *tmq);
DLL_EXPORT TAOS_RES *tmq_consumer_poll(tmq_t *tmq, int64_t timeout);
DLL_EXPORT int32_t   tmq_consumer_close(tmq_t *tmq);
DLL_EXPORT int32_t   tmq_commit_sync(tmq_t *tmq, const TAOS_RES *msg);
DLL_EXPORT void      tmq_commit_async(tmq_t *tmq, const TAOS_RES *msg, tmq_commit_cb *cb, void *param);

enum tmq_conf_res_t {
  TMQ_CONF_UNKNOWN = -2,
  TMQ_CONF_INVALID = -1,
  TMQ_CONF_OK = 0,
};
typedef enum tmq_conf_res_t tmq_conf_res_t;

DLL_EXPORT tmq_conf_t    *tmq_conf_new();
DLL_EXPORT tmq_conf_res_t tmq_conf_set(tmq_conf_t *conf, const char *key, const char *value);
DLL_EXPORT void           tmq_conf_destroy(tmq_conf_t *conf);
DLL_EXPORT void           tmq_conf_set_auto_commit_cb(tmq_conf_t *conf, tmq_commit_cb *cb, void *param);
```

这些 API 的文档请见 [C/C++ Connector](/reference/connector/cpp)，下面介绍一下它们的具体用法（超级表和子表结构请参考“数据建模”一节），完整的示例代码可以在 [tmq.c](https://github.com/taosdata/TDengine/blob/3.0/examples/c/tmq.c) 看到。

## 写入数据

首先完成建库、建一张超级表和多张子表操作，然后就可以写入数据了，比如：

```sql
drop database if exists tmqdb;
create database tmqdb;
create table tmqdb.stb (ts timestamp, c1 int, c2 float, c3 varchar(16) tags(t1 int, t3 varchar(16));
create table tmqdb.ctb0 using tmqdb.stb tags(0, "subtable0");
create table tmqdb.ctb1 using tmqdb.stb tags(1, "subtable1");
create table tmqdb.ctb2 using tmqdb.stb tags(2, "subtable2");
create table tmqdb.ctb3 using tmqdb.stb tags(3, "subtable3");       
insert into tmqdb.ctb0 values(now, 0, 0, 'a0')(now+1s, 0, 0, 'a00');
insert into tmqdb.ctb1 values(now, 1, 1, 'a1')(now+1s, 11, 11, 'a11');
insert into tmqdb.ctb2 values(now, 2, 2, 'a1')(now+1s, 22, 22, 'a22');
insert into tmqdb.ctb3 values(now, 3, 3, 'a1')(now+1s, 33, 33, 'a33');
```

## 创建topic：

```sql
create topic topicName as select ts, c1, c2, c3 from tmqdb.stb where c1 > 1;
```

TMQ支持多种订阅类型：

### 列订阅

语法：CREATE TOPIC topic_name as subquery
通过select语句订阅（包括select *，或select ts, c1等指定列描述订阅，可以带条件过滤、标量函数计算，但不支持聚合函数、不支持时间窗口聚合）

- TOPIC一旦创建则schema确定
- 被订阅或用于计算的column和tag不可被删除、修改
- 若发生schema变更，新增的column不出现在结果中

### 超级表订阅
语法：CREATE TOPIC topic_name AS STABLE stbName

与select * from stbName订阅的区别是：
- 不会限制用户的schema变更
- 返回的是非结构化的数据：返回数据的schema会随之超级表的schema变化而变化
- 用户对于要处理的每一个数据块都可能有不同的schema，因此，必须重新获取schema
- 返回数据不带有tag

## 创建 consumer 以及consumer group

对于consumer, 目前支持的config包括：

| 参数名称                     | 参数值                         | 备注                                                   |
| ---------------------------- | ------------------------------ | ------------------------------------------------------ |
| group.id                     | 最大长度：192                  |                                                        |
| enable.auto.commit           | 合法值：true, false            |                                                        |
| auto.commit.interval.ms      |                                |                                                        |
| auto.offset.reset            | 合法值：earliest, latest, none |                                                        |
| td.connect.ip                | 用于连接，同taos_connect的参数 |                                                        |
| td.connect.user              | 用于连接，同taos_connect的参数 |                                                        |
| td.connect.pass              | 用于连接，同taos_connect的参数 |                                                        |
| td.connect.port              | 用于连接，同taos_connect的参数 |                                                        |
| enable.heartbeat.background  | 合法值：true, false            | 开启后台心跳，即consumer不会因为长时间不poll而认为离线 |
| experimental.snapshot.enable | 合法值：true, false            | 从wal开始消费，还是从tsbs开始消费                      |
| msg.with.table.name          | 合法值：true, false            | 从消息中能否解析表名                                   |

```sql
/* 根据需要，设置消费组(group.id)、自动提交(enable.auto.commit)、自动提交时间间隔(auto.commit.interval.ms)、用户名(td.connect.user)、密码(td.connect.pass)等参数 */
  tmq_conf_t* conf = tmq_conf_new();
  tmq_conf_set(conf, "enable.auto.commit", "true");
  tmq_conf_set(conf, "auto.commit.interval.ms", "1000");
  tmq_conf_set(conf, "group.id", "cgrpName");
  tmq_conf_set(conf, "td.connect.user", "root");
  tmq_conf_set(conf, "td.connect.pass", "taosdata");
  tmq_conf_set(conf, "auto.offset.reset", "earliest");
  tmq_conf_set(conf, "experimental.snapshot.enable", "true");
  tmq_conf_set(conf, "msg.with.table.name", "true");
  tmq_conf_set_auto_commit_cb(conf, tmq_commit_cb_print, NULL);

  tmq_t* tmq = tmq_consumer_new(conf, NULL, 0);
  tmq_conf_destroy(conf);
  return tmq;
```

上述配置中包括consumer group ID，如果多个 consumer 指定的 consumer group ID一样，则自动形成一个consumer group，共享消费进度。


## 创建 topic 列表

单个consumer支持同时订阅多个topic。

```sql
  tmq_list_t* topicList = tmq_list_new();
  tmq_list_append(topicList, "topicName");
  return topicList;
```

## 启动订阅并开始消费

```sql
  /* 启动订阅 */
  tmq_subscribe(tmq, topicList);
  tmq_list_destroy(topicList);
  
  /* 循环poll消息 */
  int32_t totalRows = 0;
  int32_t msgCnt = 0;
  int32_t timeOut = 5000;
  while (running) {
    TAOS_RES* tmqmsg = tmq_consumer_poll(tmq, timeOut);
    if (tmqmsg) {
      msgCnt++;
      totalRows += msg_process(tmqmsg);
      taos_free_result(tmqmsg);
    } else {
      break;
	}
  }
  
  fprintf(stderr, "%d msg consumed, include %d rows\n", msgCnt, totalRows);
```

这里是一个 **while** 循环，每调用一次tmq_consumer_poll()，获取一个消息，该消息与普通查询返回的结果集完全相同，可以使用相同的解析API完成消息内容的解析：

```sql
 static int32_t msg_process(TAOS_RES* msg) {
  char buf[1024];
  int32_t rows = 0;

  const char* topicName = tmq_get_topic_name(msg);
  const char* dbName    = tmq_get_db_name(msg);
  int32_t     vgroupId  = tmq_get_vgroup_id(msg);

  printf("topic: %s\n", topicName);
  printf("db: %s\n", dbName);
  printf("vgroup id: %d\n", vgroupId);

  while (1) {
    TAOS_ROW row = taos_fetch_row(msg);
    if (row == NULL) break;

    TAOS_FIELD* fields      = taos_fetch_fields(msg);
    int32_t     numOfFields = taos_field_count(msg);
    int32_t*    length      = taos_fetch_lengths(msg);
    int32_t     precision   = taos_result_precision(msg);
    const char* tbName      = tmq_get_table_name(msg);
    rows++; 
    taos_print_row(buf, row, fields, numOfFields);
    printf("row content from %s: %s\n", (tbName != NULL ? tbName : "null table"), buf);
  }

  return rows;
}
```

## 结束消费

```sql
  /* 取消订阅 */
  tmq_unsubscribe(tmq);

  /* 关闭消费 */
  tmq_consumer_close(tmq);
```

## 删除topic

如果不再需要，可以删除创建topic，但注意：只有没有被订阅的topic才能别删除。

```sql
  /* 删除topic */
  drop topic topicName;
```

## 状态查看

1、topics：查询已经创建的topic

```sql
  show topics;
```

2、consumers：查询consumer的状态及其订阅的topic

```sql
  show consumers;
```

3、subscriptions：查询consumer与vgroup之间的分配关系

```sql
  show subscriptions;
```


