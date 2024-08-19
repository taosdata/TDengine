#!/usr/bin/python3
import taos

db              = "power"
topic           = "topic_meters"
user            = "root"
password        = "taosdata"
host            = "localhost"
port            = 6030
groupId         = "group1"
clientId        = "1"  
tdConnWsScheme  = "ws"
autoOffsetReset = "latest"
autoCommitState = "true"
autoCommitIntv  = "1000"


def prepareMeta():
    conn = None
    try:
        conn = taos.connect(host=host, user=user, password=password, port=port)
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
            power.d1002 USING power.meters (groupid, location) TAGS(3, 'California.SanFrancisco') 
                VALUES (NOW + 1a, 10.30000, 218, 0.25000)
            """
        affectedRows = conn.execute(sql)
        print(f"Inserted into {affectedRows} rows to power.meters successfully.")
    except Exception as err:
        print(f"Failed to prepareMeta, host: {host}:{port}, db: {db}, topic: {topic}, ErrMessage:{err}.")
        raise err
    finally:
        if conn:
            conn.close()

# ANCHOR: create_consumer
from taos.tmq import Consumer

def create_consumer():
    try:
        consumer = Consumer(
            {
                "group.id": groupId,
                "client.id": clientId,
                "td.connect.user": user,
                "td.connect.pass": password,
                "enable.auto.commit": autoCommitState,
                "auto.commit.interval.ms": autoCommitIntv,
                "auto.offset.reset": autoOffsetReset,
                "td.connect.ip": host,
                "td.connect.port": str(port),
            }
        )
        print(f"Create consumer successfully, host: {host}:{port}, groupId: {groupId}, clientId: {clientId}")
        return consumer
    except Exception as err:
        print(f"Failed to create native consumer, host: {host}:{port}, groupId: {groupId}, clientId: {clientId}, ErrMessage:{err}.")
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
        print(f"Failed to poll data, topic: {topic}, groupId: {groupId}, clientId: {clientId}, ErrMessage:{err}.")
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
        print(f"Failed to commit offset, topic: {topic}, groupId: {groupId}, clientId: {clientId}, ErrMessage:{err}.")
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
                print(f"Assignment seek to beginning successfully.")
    except Exception as err:
        print(f"Failed to seek offset, topic: {topic}, groupId: {groupId}, clientId: {clientId}, ErrMessage:{err}.")
        raise err
    # ANCHOR_END: assignment

def unsubscribe(consumer):
    # ANCHOR: unsubscribe    
    try:
        consumer.unsubscribe()
        print("Consumer unsubscribed successfully.");
    except Exception as err:
        print(f"Failed to unsubscribe consumer. topic: {topic}, groupId: {groupId}, clientId: {clientId}, ErrMessage:{err}.")
    finally:
        if consumer:
            consumer.close()
            print("Consumer closed successfully.");        
    # ANCHOR_END: unsubscribe

if __name__ == "__main__":
    consumer = None
    try:
        prepareMeta()
        consumer = create_consumer()
        subscribe(consumer)
        seek_offset(consumer)
        commit_offset(consumer)
    except Exception as err:
        print(f"Failed to execute consumer example, topic: {topic}, groupId: {groupId}, clientId: {clientId}, ErrMessage:{err}.")
    finally:
        unsubscribe(consumer);
