import os
import taosws

from taosws import Consumer
from taosws import Connection as connect     

import subprocess
from multiprocessing import Process
import threading

TDENGINE_CLOUD_ENDPOINT="https://gw.us-east-1.aws.cloud.tdengine.com"
TDENGINE_CLOUD_TOKEN="85576b19811fbd2000f5120ea810279e84586c8a"

endpoint = TDENGINE_CLOUD_ENDPOINT
token = TDENGINE_CLOUD_TOKEN

try:
    conn = taosws.connect("%s?token=%s" % (endpoint, token))
except Exception as e:
    print(str(e))

conn.execute(f'drop topic if exists select_d2;')

conn.execute(f'create topic select_d2 as select * from test.d2;')

def cloud_consumer(group_id):
    conf = {
        # auth options
        "td.connect.websocket.scheme": "wss",
        "td.connect.ip": endpoint,
        "td.connect.token": token,
        # consume options
        "group.id": f"local{group_id}", 
        "client.id": f"{group_id}",
        "enable.auto.commit": "true",
        "auto.commit.interval.ms": "1000",
        "auto.offset.reset": "earliest",
        "msg.with.table.name": "true",
    }
    consumers = Consumer(conf)
    print(consumers)

consumer_groups_num = 2  
threads=[]

for id in range(consumer_groups_num):
    threads.append(threading.Thread(target=cloud_consumer, args=(id,)))
for tr in threads:
    tr.start()
for tr in threads:
    tr.join()

conn.execute(f'drop topic select_d2')