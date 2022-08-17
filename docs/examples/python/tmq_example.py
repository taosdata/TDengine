import taos
from taos.tmq import TaosConsumer
consumer = TaosConsumer('topic_ctb_column', group_id='vg2')
for msg in consumer:
    for row in msg:
        print(row)
