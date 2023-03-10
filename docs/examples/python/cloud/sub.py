#!/usr/bin/env python
import os
from taosws import Consumer

endpoint = os.environ["TDENGINE_CLOUD_ENDPOINT"]
token = os.environ["TDENGINE_CLOUD_TOKEN"]

conf = {
    # auth options
    "td.connect.websocket.scheme": "wss",
    "td.connect.ip": endpoint,
    "td.connect.token": token,
    # consume options
    "group.id": "test_group_py",
    "client.id": "test_consumer_ws_py",
}
consumer = Consumer(conf)

consumer.subscribe(["test"])

while 1:
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
  else:
      break

consumer.close()