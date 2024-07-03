import taos

conn = taos.connect(
    host="localhost",
    user="root",
    password="taosdata",
    port=6030,
)

db = "power"
topic = "topic_meters"

conn.execute(f"DROP TOPIC IF EXISTS {topic}")
conn.execute(f"DROP DATABASE IF EXISTS {db}")
conn.execute(f"CREATE DATABASE {db}")

# change database. same as execute "USE db"
conn.select_db(db)

# create super table
conn.execute(
    "CREATE STABLE power.meters (ts TIMESTAMP, current FLOAT, voltage INT, phase FLOAT) TAGS (location BINARY(64), groupId INT)"
)

# ANCHOR: create_topic
# create topic
conn.execute(
    f"CREATE TOPIC IF NOT EXISTS {topic} AS SELECT ts, current, voltage, phase, groupid, location FROM meters"
)
# ANCHOR_END: create_topic

# ANCHOR: create_consumer
from taos.tmq import Consumer

consumer = Consumer(
    {
        "group.id": "1",
        "td.connect.user": "root",
        "td.connect.pass": "taosdata",
        "enable.auto.commit": "true",
    }
)
# ANCHOR_END: create_consumer

# ANCHOR: subscribe
consumer.subscribe([topic])
# ANCHOR_END: subscribe

try:
    # ANCHOR: consume
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
    # ANCHOR_END: consume

    # ANCHOR: assignment
    assignments = consumer.assignment()
    for assignment in assignments:
        print(assignment)
        # ANCHOR_END: assignment

        # ANCHOR: seek
        offset = taos.tmq.TopicPartition(
            topic=topic,
            partition=assignment.partition,
            offset=0,
        )
        consumer.seek(offset)
        # ANCHOR_END: seek
finally:
    # ANCHOR: unsubscribe
    consumer.unsubscribe()
    consumer.close()
    # ANCHOR_END: unsubscribe

conn.close()
