---
sidebar_label: 消息队列
description: "数据订阅与推送服务。写入到 TDengine 中的时序数据能够被自动推送到订阅客户端。"
title: 消息队列
---


为了实时获取写入TDengine的数据，或者以事件到达顺序处理数据，TDengine提供了类似消息队列产品的数据订阅、消费接口，它支持以消费者组的方式，使多个消费者分布式、多线程地同时订阅一个topic，共享消费进度。并且提供了消息的ACK机制，在宕机、重启等复杂环境下确保at least once消费。

使用TDengine一体化的数据订阅功能，除了以上标准的消息队列特性，还能在订阅同时通过标签、表名、列、表达式等多种方法过滤所需数据，并且支持对数据进行函数变换、预处理（包括标量udf计算）。

为了实现上述功能，TDengine提供了多种灵活的WAL文件切换与保留机制：可以按照时间或文件大小来保留WAL文件（详见create database语句）。在消费时，TDengine从WAL中获取数据，并经过过滤、变换等操作，将数据推送给消费者。




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

一、首先完成建库、建一张超级表和多张子表，并每个子表插入若干条数据记录：

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

二、创建topic：

```sql
create topic topicName as select ts, c1, c2, c3 from tmqdb.stb where c1 > 1;
```

注：TMQ支持多种订阅类型：
1、列订阅

语法：CREATE TOPIC topic_name as subquery
通过select语句订阅（包括select *，或select ts, c1等指定列描述订阅，可以带条件过滤、标量函数计算，但不支持聚合函数、不支持时间窗口聚合）

- TOPIC一旦创建则schema确定
- 被订阅或用于计算的column和tag不可被删除、修改
- 若发生schema变更，新增的column不出现在结果中

2、超级表订阅
语法：CREATE TOPIC topic_name AS STABLE stbName

与select * from stbName订阅的区别是：
- 不会限制用户的schema变更
- 返回的是非结构化的数据：返回数据的schema会随之超级表的schema变化而变化
- 用户对于要处理的每一个数据块都可能有不同的schema，因此，必须重新获取schema
- 返回数据不带有tag

三、创建consumer

目前支持的config：

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

四、创建订阅主题列表

```sql
  tmq_list_t* topicList = tmq_list_new();
  tmq_list_append(topicList, "topicName");
  return topicList;
```

单个consumer支持同时订阅多个topic。

五、启动订阅并开始消费

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

五、结束消费

```sql
  /* 取消订阅 */
  tmq_unsubscribe(tmq);

  /* 关闭消费 */
  tmq_consumer_close(tmq);
```

六、删除topic

如果不再需要，可以删除创建topic，但注意：只有没有被订阅的topic才能别删除。

```sql
  /* 删除topic */
  drop topic topicName;
```

七、状态查看

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


