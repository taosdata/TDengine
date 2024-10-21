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
# result = conn.query(f'show topics;')
# data = result.fetch_all()
# for row in data:
#     print(row)
conn.execute(f'drop topic if exists select_d1 ;')
conn.execute(f'drop topic if exists select_d1 ;')
# result = conn.query(f'show topics;')
# data = result.fetch_all()
# for row in data:
#     print(row)

# conn.execute(f'drop topic if exists select_d2;')
# conn.execute(f'drop topic if exists select_d3;')
# conn.execute(f'drop topic if exists select_d4;')
# os.system("nohup taosBenchmark -d db_sub -t 1000 -n 1000 -v 4 -y & ")
# time.sleep(10)

conn.execute(f'create topic select_d1 as  select * from dbtest.meters;')
# conn.execute(f'create topic select_d2 as  select * from db_sub.meters;')
# conn.execute(f'create topic select_d3 as  select * from db_sub.meters;')
# conn.execute(f'create topic select_d4 as  select * from db_sub.meters;')

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
                values = block.fetchall()
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
                values = block.fetchall()
                print(values)
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
        "session.timeout.ms": "10000",
        "max.poll.interval.ms": "18000",
        "experimental.snapshot.enable" :"true",
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