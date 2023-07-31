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
