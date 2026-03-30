# TS-4592-增加清理lost状态consumer的功能 Test Spec 

## 1. 测试目标

1. python 连接器支持配置consumer 超时的两个参数。
2. 超时参数正常生效，且超时时间符合预期。
3. taosd 异常以后重启，这俩参数依然生效

## 2. 变更历史

| Date | Version | Owner | Memo |
| --- | --- | --- | --- |
| 2024.09.20 | 0.1 | 陈浩然 |  |
| 2024.09.24 | 1.0 | 陈浩然 | 1. 增加升级测试 1. 增加异常重启测试 1. 增加多 consumer 多线程测试 |

## 3. 测试范围

## 4. 测试结论

1. 能正常通过超时时间删除consumer，测试通过。
2. 注意：升级该版本以后客户端服务端都不向下兼容，

## 5. 开发质量报告

结论：本特性/优化的开发质量是优

| 统计指标 | 数量 |
| --- | --- |
| 提测被拒次数 |  |
| 基础测试用例不通过 |  |
| Bug 总数 | 1 |
| 严重 Bug 总数 |  |

## 6. 已知问题和限制

## 7. 测试环境

1. 测试平台：Linux x64
2. 测试资源：192.168.1.86（尽量使用 adress san版本运行测试用例）

## 8. 测试数据 (Optional)

创建数据
taosBenchmark -d db_sub -t 1000 -n 1000 -v 4 -y
创建 topic
create topic select_d1 as  select * from db_sub.meters;

### 8.1 测试脚本：

 python3 per_consumer.py 
```python
import os
import taos
import time
from datetime import datetime
import subprocess
from multiprocessing import Process
import threading
from taos.tmq import Consumer

try:
    conn = taos.connect()
except Exception as e:
    print(str(e))

conn.execute(f'drop topic if exists select_d1;')
result = conn.query(f'show topics;')
data = result.fetch_all()
for row in data:
    print(row)
conn.execute(f'drop topic if exists select_d1 ;')
conn.execute(f'drop topic if exists select_d1 ;')
result = conn.query(f'show topics;')
data = result.fetch_all()
for row in data:
    print(row)

## 9. conn.execute(f'drop topic if exists select_d2;')

## 10. conn.execute(f'drop topic if exists select_d3;')

## 11. conn.execute(f'drop topic if exists select_d4;')

## 12. os.system("nohup taosBenchmark -d db_sub -t 1000 -n 1000 -v 4 -y & ")

## 13. time.sleep(10)

conn.execute(f'create topic select_d1 as  select * from db_sub.meters;')

## 14. conn.execute(f'create topic select_d2 as  select * from db_sub.meters;')

## 15. conn.execute(f'create topic select_d3 as  select * from db_sub.meters;')

## 16. conn.execute(f'create topic select_d4 as  select * from db_sub.meters;')

def sub_consumer(consumer,group_id):
    if group_id < 100 :
        consumer.subscribe(["select_d1"])
    # elif group_id < 200 :
    #     consumer.subscribe(["select_d2"])
    # elif group_id < 300 :
    #     consumer.subscribe(["select_d3"])
    # else:
    #     consumer.subscribe(["select_d4"])

    nrows = 0
    while True:
        start = datetime.now()
        print(f"time:{start},consumer:{group_id}, start to consume")
        #start = datetime.now()
        #print(f"time:{start},consumer:{group_id}, start to consume")
        message = consumer.poll(timeout=10.0)
        
        if message:
            id = message.offset()
            topic = message.topic()
            database = message.database()
            
            for block in message:
                addrows = block.nrows()
                nrows += block.nrows()
                ncols = block.ncols()
                values = block.fetchall
            end = datetime.now()
            elapsed_time = end -start
            print(f"time:{end},consumer:{group_id}, elapsed time:{elapsed_time},consumer_nrows:{nrows},consumer_addrows:{addrows}, consumer_ncols:{ncols},offset:{id}")
        consumer.commit()
        print(f"consumer:{group_id},consumer_nrows:{nrows}")
            # consumer.unsubscribe()
            # consumer.close()
            # break

def sub_consumer_once(consumer,group_id):
    if group_id < 100 :
        consumer.subscribe(["select_d1"])
    # elif group_id < 200 :
    #     consumer.subscribe(["select_d2"])
    # elif group_id < 300 :
    #     consumer.subscribe(["select_d3"])
    # else:
    #     consumer.subscribe(["select_d4"])

    nrows = 0
    consumer_nrows = 0
    while True:
        start = datetime.now()
        print(f"time:{start},consumer:{group_id}, start to consume")
        #start = datetime.now()
        #print(f"time:{start},consumer:{group_id}, start to consume")
        print(f"consumer_nrows:{consumer_nrows}")
        if consumer_nrows < 1000000:
            message = consumer.poll(timeout=10.0)
        else:
            time.sleep(30)
        
        if message:
            id = message.offset()
            topic = message.topic()
            database = message.database()
            
            for block in message:
                addrows = block.nrows()
                nrows += block.nrows()
                ncols = block.ncols()
                values = block.fetchall
            end = datetime.now()
            elapsed_time = end -start
            print(f"time:{end},consumer:{group_id}, elapsed time:{elapsed_time},consumer_nrows:{nrows},consumer_addrows:{addrows}, consumer_ncols:{ncols},offset:{id}")
        consumer.commit()
        print(f"consumer:{group_id},consumer_nrows:{nrows}")
        consumer_nrows = nrows
            # consumer.unsubscribe()
            # consumer.close()
            # break

def cloud_consumer(group_id):
    conf = {
        # auth options
        "td.connect.websocket.scheme": "ws",
        # consume options
        "td.connect.ip": "yw86",
        "td.connect.port": "6241",
        "group.id": f"local{group_id}", 
        "client.id": "test_consumer_ws_py",
        "enable.auto.commit": "true",
        "auto.commit.interval.ms": "1000",
        "auto.offset.reset": "earliest",
        "msg.with.table.name": "true",
        "experimental.snapshot.enable" :"false",
    }
    consumer = Consumer(conf)
    print(consumer,group_id)
    #counsmer sub:
    while True:
        try:
            sub_consumer(consumer,group_id)
        except Exception as e:
            print(str(e))
            time.sleep(1)
            break
    
    #consumer.close()

def taosc_consumer(group_id):
    conf = {
        # auth options
        # consume options
        "td.connect.ip": "yw86",
        "group.id": f"local{group_id}", 
        "client.id": "test_consumer_py",
        "enable.auto.commit": "false",
        "auto.commit.interval.ms": "1000",
        "auto.offset.reset": "earliest",
        "msg.with.table.name": "true",
        "session.timeout.ms": "6000",
        "max.poll.interval.ms": "90000",
        "experimental.snapshot.enable" :"false",
    }
    consumer = Consumer(conf)
    print(consumer,group_id)
    #counsmer sub:
    while True:
        try:
            sub_consumer_once(consumer,group_id)
        except Exception as e:
            print(str(e))
            time.sleep(1)
            break
    
    #consumer.close()

consumer_groups_num = 1
threads = []
process_list = []

for id in range(consumer_groups_num):
    #threads.append(Process(target=cloud_consumer, args=(id,)))
    threads.append(threading.Thread(target=taosc_consumer, args=(id,)))
for tr in threads:
    tr.start()
for tr in threads:
    tr.join()
```

## 17. 测试用例

### 17.1 功能用例

| 编号 | 类型 | 测试目的 | consumer_groups_num | 测试步骤 | 预期结果 | 是否为基础用例 | 测试结果 | 备注 |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 1 |  | 验证session.timeout.ms 参数 | 1 | 1. 按着第八章节的测试数据，写入数据和 topic
1. 使用 python3 [per_consumer.py](http://per_consumer.py) 来启动 consumer 消费，不配置该参数session.timeout.ms ，即为默认值 12s。
2. 消费完成后，关闭 consumer 进程，模拟心跳丢失，等待 12s 超时时间
4.taos 客户端执行show consumer  查看consumer是否存在 | 1. 步骤 3 的 100w 数据完全被消费，步骤 4 查看consumer 12s 左右就被删除了 | Y | Pass |  |
| 2 |  | 验证session.timeout.ms 参数的边界范围 | 1 | 1. 按着第八章节的测试数据，写入数据和 topic
1. 使用 python3 [per_consumer.py](http://per_consumer.py) 来启动 consumer 消费，配置该产数session.timeout.ms 为5.9s。
2. 使用 python3 [per_consumer.py](http://per_consumer.py) 来启动 consumer 消费，配置该产数session.timeout.ms 为1801s。consumer 配置会报错
3. 使用 python3 [per_consumer.py](http://per_consumer.py) 来启动 consumer 消费，配置该产数session.timeout.ms 为6s。
4. 消费完成后，关闭 consumer 进程，模拟心跳丢失，等待 6s 超时时间
6.taos 客户端执行show consumer  查看consumer是否存在 | 1. 步骤2 的consumer 无法启动
1. 步骤 3的 consumer 无法启动 
2. 步骤6 consumer 6s 左右后被删除。 | Y | Pass |  |
| 3 |  | 验证max.poll.interval.ms 参数 | 1 | 1. 按着第八章节的测试数据，写入数据和 topic
1. 使用 python3 [per_consumer.py](http://per_consumer.py) 来启动 consumer 消费，不配置该参数max.poll.interval.ms ，即为默认值 300s。
2. 100w 数据第一次消费完成后，不再 poll 数据了，等待 300s 超时时间
4.taos 客户端执行show consumer  查看consumer是否存在 | 1. 步骤 3 的 100w 数据完全被消费，步骤 4 查看consumer 300s 左右就被删除了 | Y | Pass |  |
| 4 |  | 验证max.poll.interval.ms 参数的边界范围 | 1 | 1. 按着第八章节的测试数据，写入数据和 topic
1. 使用 python3 [per_consumer.py](http://per_consumer.py) 来启动 consumer 消费，配置该产数max.poll.interval.ms 为0.9s。
2. 使用 python3 [per_consumer.py](http://per_consumer.py) 来启动 consumer 消费，配置该产数max.poll.interval.ms为2247483647s。consumer 配置会报错
3. 使用 python3 [per_consumer.py](http://per_consumer.py) 来启动 consumer 消费，配置该产数max.poll.interval.ms为10s。
4. 100w 数据第一次消费完成后，不再 poll 数据了，等待 300s 超时时间，模拟只存在心跳但是不 poll 数据情况，等待 10s 超时时间
6.taos 客户端执行show consumer  查看consumer是否存在 | 1. 步骤 5 的 100w 数据完全被消费，步骤 4 查看consumer 10s 左右就被删除了 | Y | Pass |  |
| 5 |  | 验证所有值为默认参数且重启 taosd | 1 | 1. 按着第八章节的测试数据，写入数据和 topic
1. 使用 python3 [per_consumer.py](http://per_consumer.py) 来启动 consumer 消费，不配置该参数session.timeout.ms ，即为默认值 12s。
2. 消费一部分数据后，关闭 taosd。等待 30s
3. 重启 taosd，消费完成后，关闭 consumer 进程，模拟心跳丢失，等待 12s 超时时间
4. taos 客户端执行show consumer  查看consumer是否存在
 | 1. 步骤 4 的 100w 数据完全被消费，步骤 5 查看consumer 12s 左右就被删除了 | Y | Pass |  |
| 6 |  |  | 100 | 1. 按着第八章节的测试数据，写入数据和 topic
1. consumer_groups_num配置 100，使用 python3 [per_consumer.py](http://per_consumer.py) 来启动 consumer 消费，配置参数session.timeout.ms和max.poll.interval.ms 均为1800s。
2. 消费完成后，不在 poll 数据，等待1800s 超时
3. taos 客户端执行show consumer  查看consumer是否存在 | 1. 步骤 3 的 100w 数据完全被消费，步骤 4 查看consumer 1800s 左右就被删除了 |  |  | 这里配置 100 线程，消费很慢，需要提升性能。 |
| 7 |  | 升级测试 | 1 | 1.安装 3.3.0.0版本，按着8 章节写入数据，创建 topic，
1. 启动 python 消费程序，python3 [per_consumer.py](http://per_consumer.py) ，消费几秒后，停止 taosd，记录消费程序最终消费的个数，并停止消费程序。
2. 升级并启动 taosd，再次启动消费程序，跟步骤 2 同一个 consumer group 即可，使用 python3 [per_consumer_new.py](http://per_consumer_new.py) 启动，等待消费完成，记录消费程序最终消费的个数
3. 记录消费的总数据量为步骤 2 和 3 的消费总，跟 100w 总数据量对比 | 1. 步骤 4 消费的总数据量之和大于等于应该消费的总数据量。 | Y | Pass | 测试消费是 102w（第一次 39w,第二次 63w），大于等 100w。 |

## 18. 待讨论(Optional)

## 19. Jira


## 20. 测试计划 (Optional)


## 21. 风险评估

## 22. 测试备忘 (Optional)

## 23. 参考文档 (Optional)

[Consumer 状态优化](https://taosdata.feishu.cn/wiki/Grb0wNyPiiAC71kIQc4cLffrndg)
