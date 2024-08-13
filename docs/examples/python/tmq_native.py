import taos

def prepareMeta():
    conn = None
    try:
        conn = taos.connect(
            host="localhost",
            user="root",
            password="taosdata",
            port=6030,
        )

        db = "power"
        topic = "topic_meters"
        conn.execute(f"CREATE DATABASE IF NOT EXISTS {db}")

        # change database. same as execute "USE db"
        conn.select_db(db)

        # create super table
        conn.execute(
            "CREATE STABLE IF NOT EXISTS power.meters (ts TIMESTAMP, current FLOAT, voltage INT, phase FLOAT) TAGS (location BINARY(64), groupId INT)"
        )

        # ANCHOR: create_topic
        # create topic
        conn.execute(
            f"CREATE TOPIC IF NOT EXISTS {topic} AS SELECT ts, current, voltage, phase, groupid, location FROM meters"
        )
        # ANCHOR_END: create_topic
        sql = """
            INSERT INTO 
            power.d1001 USING power.meters (groupid, location) TAGS(2, 'California.SanFrancisco')
                VALUES (NOW + 1a, 10.30000, 219, 0.31000) 
                (NOW + 2a, 12.60000, 218, 0.33000) (NOW + 3a, 12.30000, 221, 0.31000)
            power.d1002 USING power.meters (groupid, location)  TAGS(3, 'California.SanFrancisco') 
                VALUES (NOW + 1a, 10.30000, 218, 0.25000)
            """
        affectedRows = conn.execute(sql)
        print(f"Inserted into {affectedRows} rows to power.meters successfully.")
    except Exception as err:
        print(f"Prepare insert data error, ErrMessage:{err}")
        raise err
    finally:
        if conn:
            conn.close()

# ANCHOR: create_consumer
from taos.tmq import Consumer

def create_consumer():
    host = "localhost"
    port = 6030
    groupId = "group1"
    clientId = "1"
    try:
        consumer = Consumer(
            {
                "group.id": groupId,
                "client.id": clientId,
                "td.connect.user": "root",
                "td.connect.pass": "taosdata",
                "enable.auto.commit": "true",
                "auto.commit.interval.ms": "1000",
                "auto.offset.reset": "latest",
                "td.connect.ip": host,
                "td.connect.port": port,
            }
        )
        print(f"Create consumer successfully, host: {host}:{port}, groupId: {groupId}, clientId: {clientId}");
        return consumer
    except Exception as err:
        print(f"Failed to create native consumer, host: {host}:{port} ; ErrMessage:{err}");
        raise err
    # ANCHOR_END: create_consumer


# ANCHOR: subscribe
def subscribe(consumer):
    try:
        # subscribe to the topics
        consumer.subscribe(["topic_meters"])
        print("Subscribe topics successfully")
        for i in range(50):
            records = consumer.poll(1)
            if records:
                err = records.error()
                if err is not None:
                    print(f"Poll data error, {err}")
                    raise err

                val = records.value()
                if val:
                    for block in val:
                        data = block.fetchall()
                        print(f"data: {data}")

    except Exception as err:
        print(f"Failed to poll data, ErrMessage:{err}")
        raise err


# ANCHOR_END: subscribe

def commit_offset(consumer):
    # ANCHOR: commit_offset
    try:
        for i in range(50):
            records = consumer.poll(1)
            if records:
                err = records.error()
                if err is not None:
                    print(f"Poll data error, {err}")
                    raise err

                val = records.value()
                if val:
                    for block in val:
                        print(block.fetchall())

                # after processing the data, commit the offset manually
                consumer.commit(records)
                print("Commit offset manually successfully.");

    except Exception as err:
        print(f"Failed to poll data, ErrMessage:{err}")
        raise err
    # ANCHOR_END: commit_offset


def seek_offset(consumer):
    # ANCHOR: assignment
    try:
        assignments = consumer.assignment()
        if assignments:
            for partition in assignments:
                partition.offset = 0
                consumer.seek(partition)
                print(f"Assignment seek to beginning successfully");
    except Exception as err:
        print(f"Seek example failed; ErrMessage:{err}")
        raise err
    # ANCHOR_END: assignment

def unsubscribe(consumer):
    # ANCHOR: unsubscribe    
    try:
        consumer.unsubscribe()
        print("Consumer unsubscribed successfully.");
    except Exception as err:
        print(f"Failed to unsubscribe consumer. ErrMessage:{err}")
    finally:
        if consumer:
            consumer.close()        
    # ANCHOR_END: unsubscribe

if __name__ == "__main__":
    consumer = None
    try:
        prepareMeta()
        consumer = create_consumer()
        subscribe(consumer)
        seek_offset(consumer)
        commit_offset(consumer)
        consumer.unsubscribe()
        print("Consumer unsubscribed successfully.");
    except Exception as err:
        print(f"Failed to stmt consumer. ErrMessage:{err}")
    finally:
        consumer.unsubscribe()

