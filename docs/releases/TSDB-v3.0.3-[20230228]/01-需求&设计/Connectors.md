# Connectors

## 1. Java

TMQ over WebSocket 相较于 JNI 方式接口保持一致。

### 1.1 配置

```java
Properties properties = new Properties();
properties.setProperty(TMQConstants.CONNECT_USER, "root");
properties.setProperty(TMQConstants.CONNECT_PASS, "taosdata");
properties.setProperty(TMQConstants.MSG_WITH_TABLE_NAME, "true");
properties.setProperty(TMQConstants.ENABLE_AUTO_COMMIT, "true");
properties.setProperty(TMQConstants.GROUP_ID, "tg1");
properties.setProperty(TMQConstants.VALUE_DESERIALIZER, "com.taosdata.jdbc.tmq.ResultDeserializer");
properties.setProperty("td.connect.type", "ws");
```

增加`td.connect.type`配置用以区分使用哪种连接方式。参数为 "jni"、"ws"，此项不填时为 "jni";

### 1.2 创建 Consumer

```java
TaosConsumer<Map<String, Object>> consumer = new TaosConsumer<>(properties)
```

### 1.3 订阅 Topic

```java
void subscribe(Collection<String> topics) throws SQLException;
```

### 1.4 拉取数据 Poll

```java
ConsumerRecords<V> poll(Duration timeout) throws SQLException;
```

### 1.5 示例代码

```java
Properties properties = new Properties();
properties.setProperty(TMQConstants.CONNECT_USER, "root");
properties.setProperty(TMQConstants.CONNECT_PASS, "taosdata");
properties.setProperty(TMQConstants.MSG_WITH_TABLE_NAME, "true");
properties.setProperty(TMQConstants.ENABLE_AUTO_COMMIT, "true");
properties.setProperty(TMQConstants.GROUP_ID, "tg1");
properties.setProperty(TMQConstants.CONNECT_TYPE, "ws");
properties.setProperty(TMQConstants.VALUE_DESERIALIZER, "com.taosdata.jdbc.tmq.ResultDeserializer");

try (TaosConsumer<ResultBean> consumer = new TaosConsumer<>(properties)) {
    consumer.subscribe(Collections.singletonList(topic));
    for (int i = 0; i < 10; i++) {
        ConsumerRecords<ResultBean> consumerRecords = consumer.poll(Duration.ofMillis(100));
        int count = 0;
        for (ResultBean bean : consumerRecords) {
            count++;
            Assert.assertTrue(strings.contains(bean.getC3()));
        }
        Assert.assertEquals(3, count);
    }
}
```

## 2. Python

Python 连接器 TMQ API 重新设计如下：
```python
class Consumer:
    def subscribe(self, topics):
        pass

    def unsubscribe(self):
        pass

    def poll(self, timeout: float = 1.0):
        pass

    def close(self):
        pass

    def commit(self, message):
        pass
```

### 2.1 创建 Consumer

#### 2.1.1 配置参数

| field | type | desc | comment |
| --- | --- | --- | --- |
| td.connect.ip | string |  |  |
| td.connect.user | string |  |  |
| td.connect.pass | string |  |  |
| td.connect.port | string |  |  |
| group.id | string | 消费组 ID，同一消费组共享消费进度 | **必填项**。最大长度：192 |
| client.id | string | 客户端 ID | 最大长度：192 |
| msg.with.table.name | string | 是否允许从消息中解析表名,不适用于列订阅 | 合法值：true, false |
| enable.auto.commit | string | 启用自动提交 | 合法值：true, false |
| auto.commit.interval.ms | string | 以毫秒为单位的自动提交时间间隔 | 默认值：5000 ms |
| auto.offset.reset | string | 消费组订阅的初始位置 | 可选：earliest(default), latest, none |
| experimental.snapshot.enable | string | 是否允许从 TSDB 消费数据 | 合法值：true, false |
| enable.heartbeat.background | string | 启用后台心跳，启用后即使长时间不 poll 消息也不会造成离线 | 合法值：true, false |

#### 2.1.2 原生连接

```python
from taos.tmq import Consumer

## 3. Syntax: `consumer = Consumer(configs)`

##

## 4. Example:

consumer = Consumer({"group.id": "local", "td.connect.ip": "127.0.0.1"})
```

#### 4.0.1 WebSocket 连接

```python
from taosws import Consumer

## 5. Syntax: `consumer = Consumer(configs)`

##

## 6. Example:

consumer = Consumer({"td.connect.websocket.scheme": "ws", "group.id": "0"})
```

### 6.1 订阅 topic

#### 6.1.1 原生连接

```python
consumer.subscribe(['topic1', 'topic2'])
```

#### 6.1.2 WebSocket 连接

```python
consumer.subscribe(['topic1', 'topic2'])
```

### 6.2 消费数据

#### 6.2.1 原生连接

```python
while True:
    res = consumer.poll(1.0)
    if not res:
        continue
    err = res.error()
    if err is not None:
        raise err
    val = res.value()

    for block in val:
        print(block.fetchall())
```

#### 6.2.2 WebSocket 连接

```python
while True:
    message = consumer.poll(timeout=1.0)
    if message:
        id = message.vgroup()
        topic = message.topic()
        database = message.database()

        for block in message:
            nrows = block.nrows()
            ncols = block.ncols()
            for row in block:
                print(row)
            values = block.fetchall()
            print(nrows, ncols)

        # consumer.commit(message)
```

### 6.3 结束消费

#### 6.3.1 原生连接

```python

## 7. 取消订阅

consumer.unsubscribe()

## 8. 关闭消费

consumer.close()
```

#### 8.0.1 WebSocket 连接

```python

## 9. 取消订阅

consumer.unsubscribe()

## 10. 关闭消费

consumer.close()
```

### 10.1 完整示例代码

#### 10.1.1 原生连接

```python
from taos.tmq import Consumer
import taos


def init_tmq_env(db, topic):
    conn = taos.connect()
    conn.execute("drop topic if exists {}".format(topic))
    conn.execute("drop database if exists {}".format(db))
    conn.execute("create database if not exists {}".format(db))
    conn.select_db(db)
    conn.execute(
        "create stable if not exists stb1 (ts timestamp, c1 int, c2 float, c3 varchar(16)) tags(t1 int, t3 varchar(16))")
    conn.execute("create table if not exists tb1 using stb1 tags(1, 't1')")
    conn.execute("create table if not exists tb2 using stb1 tags(2, 't2')")
    conn.execute("create table if not exists tb3 using stb1 tags(3, 't3')")
    conn.execute("create topic if not exists {} as select ts, c1, c2, c3 from stb1".format(topic))
    conn.execute("insert into tb1 values (now, 1, 1.0, 'tmq test')")
    conn.execute("insert into tb2 values (now, 2, 2.0, 'tmq test')")
    conn.execute("insert into tb3 values (now, 3, 3.0, 'tmq test')")


def cleanup(db, topic):
    conn = taos.connect()
    conn.execute("drop topic if exists {}".format(topic))
    conn.execute("drop database if exists {}".format(db))


if __name__ == '__main__':
    init_tmq_env("tmq_test", "tmq_test_topic")  # init env
    consumer = Consumer(
        {
            "group.id": "tg2",
            "td.connect.user": "root",
            "td.connect.pass": "taosdata",
            "enable.auto.commit": "true",
        }
    )
    consumer.subscribe(["tmq_test_topic"])

    try:
        while True:
            res = consumer.poll(1)
            if not res:
                break
            err = res.error()
            if err is not None:
                raise err
            val = res.value()

            for block in val:
                print(block.fetchall())
    finally:
        consumer.unsubscribe()
        consumer.close()
        cleanup("tmq_test", "tmq_test_topic")

```

#### 10.1.2 WebSocket 连接

```python
#!/usr/bin/python3
from taosws import Consumer

conf = {
    "td.connect.websocket.scheme": "ws",
    "group.id": "0",
}
consumer = Consumer(conf)

consumer.subscribe(["test"])

while True:
    message = consumer.poll(timeout=1.0)
    if message:
        id = message.vgroup()
        topic = message.topic()
        database = message.database()

        for block in message:
            nrows = block.nrows()
            ncols = block.ncols()
            for row in block:
                print(row)
            values = block.fetchall()
            print(nrows, ncols)

        # consumer.commit(message)
    else:
        break

consumer.close()

```
