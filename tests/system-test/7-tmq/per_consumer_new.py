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

# conn.execute(f'drop topic if exists select_d1;')
# conn.execute(f'drop topic if exists select_d2;')
# conn.execute(f'drop topic if exists select_d3;')
# conn.execute(f'drop topic if exists select_d4;')
# os.system("nohup taosBenchmark -d db_sub -t 10000 -n 10000 -y & ")


# conn.execute(f'create topic select_d1 as  select * from db_sub.meters;')
# conn.execute(f'create topic select_d2 as  select * from db_sub.meters;')
# conn.execute(f'create topic select_d3 as  select * from db_sub.meters;')
# conn.execute(f'create topic select_d4 as  select * from db_sub.meters;')

def sub_consumer(consumer,group_id):
    if group_id < 50 :
        consumer.subscribe(["select_d1"])
    elif group_id < 100 :
        consumer.subscribe(["select_d2"])
    elif group_id < 150 :
        consumer.subscribe(["select_d3"])
    else:
        consumer.subscribe(["select_d4"])

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
                #for row in block:
                #    print(row)
                values = block.fetchall
            end = datetime.now()
            elapsed_time = end -start
            print(f"time:{end},consumer:{group_id}, elapsed time:{elapsed_time},consumer_nrows:{nrows},consumer_addrows:{addrows}, consumer_ncols:{ncols},offset:{id}")
        consumer.commit()
        print(f"consumer:{group_id},consumer_nrows:{nrows}")

            # consumer.unsubscribe()
            # consumer.close()
            # break

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
        "session.timeout.ms": "100000",
        "max.poll.interval.ms": "180000",
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

