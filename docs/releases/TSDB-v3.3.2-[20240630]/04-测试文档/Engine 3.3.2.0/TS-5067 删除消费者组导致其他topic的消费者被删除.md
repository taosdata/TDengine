# TS-5067 删除消费者组导致其他topic的消费者被删除

## 1. 测试目标

在两个消费者分别订阅两个topic时，如果使用的消费者组名称一样，删除其中一个消费者的消费者组是否会对另一个消费者产生影响
现场问题：删除其中一个topic下的消费者组后，另一个消费者组下的消费者被删除，导致无法继续消费
测试期望：两个消费者的消费者组虽然名称一致，但分属不同的topic，应该相互独立，互不影响

## 2. 变更历史

| Date | Version | Owner | Memo |
| --- | --- | --- | --- |
| 2024/06/25 | 0.1 | @闫宇星 |  |

## 3. 测试结论

修复版本中，测试脚本中的多项校验均通过，测试通过

## 4. 已知问题和限制

无

## 5. 测试资源及环境

测试平台：Linux x86_64
测试机器：192.168.0.174
复现版本：TDengine 3.1 分支，commit：1dca412d65d5f40a150f9534321ec35ad591d4c8
修复版本：TDengine 3.1 分支，commit：2d4dd61d66324773cfbd5a4e516f17e80b703e7f

## 6. 测试范围及方法

### 6.1 测试范围

1. TDengine 订阅功能

### 6.2 测试方法

1. 使用开发提供的测试脚本运行测试，并添加一些测试例

## 7. 测试数据

无

## 8. 测试步骤

1. 创建数据库
2. 创建一个超级表，并插入多条数据
3. 创建两个 topic t1 和 t2
4. 第一个消费者通过 g1 消费者组订阅 topic t1，消费超级表所有数据，关闭第一个消费者
5. 第二个消费者通过 g1 消费者组订阅 topic t2
6. 使用 drop consumer group 删除第一个消费者的订阅
7. 使用 show consumers 检查第二个消费者是否依然存在，订阅是否正常，并检查是否可以正常消费 t2的所有数据，之后关闭消费者 t2
8. 第三个消费者通过 g1 消费者组订阅 topic t2，检查是否可以正常消费数据，以及订阅关系是否正常
```python
def consume_TS_5067_Test(self):
    # 创建数据库
    tdSql.execute(f'create database if not exists d1 vgroups 1')
    tdSql.execute(f'use d1')
    # 创建订阅用的超级表
    tdSql.execute(f'create table st(ts timestamp, i int) tags(t int)')
    # 向超级表插入
    tdSql.execute(f'insert into t1 using st tags(1) values(now, 1) (now+1s, 2)')
    tdSql.execute(f'insert into t2 using st tags(2) values(now, 1) (now+1s, 2)')
    tdSql.execute(f'insert into t3 using st tags(3) values(now, 1) (now+1s, 2)')
    tdSql.execute(f'insert into t1 using st tags(1) values(now+5s, 11) (now+10s, 12)')
    tdSql.query("select * from st")
    tdSql.checkRows(8)
    # 创建两个 topic
    tdSql.execute(f'create topic t1 as select * from st')
    tdSql.execute(f'create topic t2 as select * from st')
    
    # 消费者统一配置，都使用 g1 作为消费者组名称
    consumer_dict = {
        "group.id": "g1",
        "td.connect.user": "root",
        "td.connect.pass": "taosdata",
        "auto.offset.reset": "earliest",
    }
    
    # 创建第一个消费者，订阅topic t1
    consumer1 = Consumer(consumer_dict)

    try:
        consumer1.subscribe(["t1"])
    except TmqError:
        tdLog.exit(f"subscribe error")
    # 第一个消费者消费8条数据
    index = 0
    try:
        while True:
            res = consumer1.poll(1)
            if not res:
                if index != 1:
                    tdLog.exit("consume error")
                break
            val = res.value()
            if val is None:
                continue
            cnt = 0;
            for block in val:
                cnt += len(block.fetchall())

            if cnt != 8:
                tdLog.exit("consume error")

            index += 1
    finally:
        consumer1.close()
    # 创建第二个消费者，订阅 topic t2
    consumer2 = Consumer(consumer_dict)
    try:
        consumer2.subscribe(["t2"])
    except TmqError:
        tdLog.exit(f"subscribe error")

    # 列出所有的两个订阅关系
    tdSql.query(f'show subscriptions')
    tdSql.checkRows(2)
    # 列出所有消费者，因为第一个消费者已经关闭，当前只剩下第二个消费者
    tdSql.query(f'show consumers')
    tdSql.checkRows(1)
    # 删除第一个消费者的消费者组
    tdSql.execute(f'drop consumer group g1 on t1')
    # 列出所有消费者，第二个消费者还应存在
    tdSql.query(f'show consumers')
    tdSql.checkRows(1)
    # 列出订阅关系，当前只有第二个消费者的订阅存在
    tdSql.query(f'show subscriptions')
    tdSql.checkRows(1)

    # 第二个消费者不受第一个消费者订阅被删除的影响，仍能消费8条消息
    index = 0
    try:
        while True:
            res = consumer2.poll(1)
            if not res:
                if index != 1:
                    tdLog.exit("consume error")
                break
            val = res.value()
            if val is None:
                continue
            cnt = 0;
            for block in val:
                cnt += len(block.fetchall())

            if cnt != 8:
                tdLog.exit("consume error")

            index += 1
    finally:
        consumer2.close()

    # 创建第三个消费者，订阅 topic t2
    consumer3 = Consumer(consumer_dict)
    try:
        consumer3.subscribe(["t2"])
    except TmqError:
        tdLog.exit(f"subscribe error")
    # 当前仅存在第三个消费者
    tdSql.query(f'show consumers')
    tdSql.checkRows(1)
    # 插入一条数据供第三个消费者消费
    tdSql.execute(f'insert into t4 using st tags(3) values(now, 1)')
    try:
        res = consumer3.poll(1)
        if not res:
            tdLog.exit("consume1 error")
    finally:
        consumer3.close()
    # 当前不存在消费者
    tdSql.query(f'show consumers')
    tdSql.checkRows(0)
    # 当前仅存 topic t2 订阅
    tdSql.query(f'show subscriptions')
    tdSql.checkRows(1)

    tdSql.execute(f'drop topic t1')
    tdSql.execute(f'drop topic t2')
    tdSql.execute(f'drop database d1')
```

### 8.1 旧版本复现

```bash
(.testsenv) root@u0-174:/home/astro/TDengine/tests/system-test# python3 test.py -f 7-tmq/tmq_taosx.py
2024-06-25 07:20:44.897284 Procedures for tdengine deployed in u0-174

2024-06-25 07:20:45.003571 stop all dnodes, asan:0

2024-06-25 07:20:45.178380 psim is deployed and configured by /home/astro/TDengine/sim/psim/cfg/taos.cfg
2024-06-25 07:20:45.237042 dnode:1 is deployed and configured by /home/astro/TDengine/sim/dnode1/cfg/taos.cfg
2024-06-25 07:20:45.340854 taosd found: /home/astro/TDengine/debug/build/bin/taosd

2024-06-25 07:20:45.344992 dnode:1 is running with nohup /home/astro/TDengine/debug/build/bin/taosd -c /home/astro/TDengine/sim/dnode1/cfg > /dev/null 2>&1 &  
2024-06-25 07:20:46.389104 the dnode:1 has been started.
2024-06-25 07:20:46.389180 Procedures for testing self-deployment

2024-06-25 07:20:46.403288 start to excute /home/astro/TDengine/tests/system-test/7-tmq/tmq_taosx.py
2024-06-25 07:20:46.537765 sql:select * from st, queryRows:8 == expect:8

2024-06-25 07:20:53.683981 sql:show subscriptions, queryRows:2 == expect:2

2024-06-25 07:20:53.689200 sql:show consumers, queryRows:1 == expect:1

2024-06-25 07:20:53.700347 /home/astro/TDengine/tests/system-test/7-tmq/tmq_taosx.py(533) failed: sql:show consumers, queryRows:0 != expect:1
Exception ignored in: <function Consumer.__del__ at 0x75aaba304040>
Traceback (most recent call last):
  File "/home/astro/TDengine/.testsenv/lib/python3.12/site-packages/taos/tmq.py", line 371, in __del__
    self.close()
  File "/home/astro/TDengine/.testsenv/lib/python3.12/site-packages/taos/tmq.py", line 296, in close
    tmq_consumer_close(self._tmq)
  File "/home/astro/TDengine/.testsenv/lib/python3.12/site-packages/taos/cinterface.py", line 1681, in tmq_consumer_close
    raise TmqError(msg=f"failed on tmq_consumer_close(), errno={res:X}, errmsg={tmq_err2str(res)}", errno=res)
taos.error.TmqError: [0x03e6]: failed on tmq_consumer_close(), errno=-7FFFFC1A, errmsg=Consumer not exist
```

删除在 topic t1 上的 消费者，导致 topic t2 上的消费者被连带删除，问题复现

### 8.2 新版本修复

```sql
(.testsenv) root@u0-174:/home/astro/TDengine/tests/system-test# python3 test.py -f 7-tmq/tmq_taosx.py
2024-06-25 06:44:32.620774 Procedures for tdengine deployed in u0-174

2024-06-25 06:44:32.728023 stop all dnodes, asan:0

2024-06-25 06:44:33.976404 psim is deployed and configured by /home/astro/TDengine/sim/psim/cfg/taos.cfg
2024-06-25 06:44:34.022960 dnode:1 is deployed and configured by /home/astro/TDengine/sim/dnode1/cfg/taos.cfg
2024-06-25 06:44:34.127961 taosd found: /home/astro/TDengine/debug/build/bin/taosd

2024-06-25 06:44:34.131959 dnode:1 is running with nohup /home/astro/TDengine/debug/build/bin/taosd -c /home/astro/TDengine/sim/dnode1/cfg > /dev/null 2>&1 &  
2024-06-25 06:44:35.176381 the dnode:1 has been started.
2024-06-25 06:44:35.176442 Procedures for testing self-deployment

2024-06-25 06:44:35.190676 start to excute /home/astro/TDengine/tests/system-test/7-tmq/tmq_taosx.py
2024-06-25 06:44:35.325691 sql:select * from st, queryRows:8 == expect:8

2024-06-25 06:44:42.467430 sql:show subscriptions, queryRows:2 == expect:2

2024-06-25 06:44:42.472476 sql:show consumers, queryRows:1 == expect:1

2024-06-25 06:44:42.481170 sql:show consumers, queryRows:1 == expect:1

2024-06-25 06:44:42.484809 sql:show subscriptions, queryRows:1 == expect:1

2024-06-25 06:44:48.521992 sql:show consumers, queryRows:1 == expect:1

2024-06-25 06:44:52.537863 sql:show consumers, queryRows:0 == expect:0

2024-06-25 06:44:52.541900 sql:show subscriptions, queryRows:1 == expect:1

2024-06-25 06:44:52.663308 /home/astro/TDengine/tests/system-test/7-tmq/tmq_taosx.py successfully executed
```

## 9. 参考文档

JIRA：
TS-5067
