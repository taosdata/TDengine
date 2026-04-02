import os
import taosws


db = "dmeters"
topic = "topic_meters"
url = os.environ["WS_CLOUD_URL"]
token = os.environ["WS_CLOUD_TOKEN"]
groupId = "group1"
clientId = "1"
tdConnWsScheme = "wss"
autoOffsetReset = "earliest"
autoCommitState = "true"
autoCommitIntv = "1000"


def prepareMeta():
    conn = None

    try:
        conn = taosws.connect("wss://%s?token=%s" % (url, token))
        sql = """
            INSERT INTO 
            dmeters.d1001 USING dmeters.meters (groupid, location) TAGS(2, 'SanFrancisco')
                VALUES (NOW + 1a, 10.30000, 219, 0.31000) 
                (NOW + 2a, 12.60000, 218, 0.33000) (NOW + 3a, 12.30000, 221, 0.31000)
            dmeters.d1002 USING dmeters.meters (groupid, location) TAGS(3, 'SanFrancisco') 
                VALUES (NOW + 1a, 10.30000, 218, 0.25000)
            """
        affectedRows = conn.execute(sql)
        print(f"Inserted into {affectedRows} rows to power.meters successfully.")

    except Exception as err:
        print(f"Failed to prepareMeta, dsn: {url}, db: {db}, topic: {topic}, ErrMessage:{err}.")
        raise err
    finally:
        if conn:
            conn.close()


# ANCHOR: create_consumer
def create_consumer():
    try:
        consumer = taosws.Consumer(
            conf={
                "protocol": tdConnWsScheme,
                "group.id": groupId,
                "client.id": clientId,
                "auto.offset.reset": autoOffsetReset,
                "td.connect.ip": url,
                "td.connect.token": token,
                "enable.auto.commit": autoCommitState,
                "auto.commit.interval.ms": autoCommitIntv,
            }
        )
        print(f"Create consumer successfully, host: {url}, groupId: {groupId}, clientId: {clientId}.")
        return consumer
    except Exception as err:
        print(
            f"Failed to create websocket consumer, host: {url}, groupId: {groupId}, clientId: {clientId}, ErrMessage:{err}."
        )
        raise err


# ANCHOR_END: create_consumer


def seek_offset(consumer):
    # ANCHOR: assignment
    try:
        assignments = consumer.assignment()
        for assignment in assignments:
            topic = assignment.topic()
            print(f"topic: {topic}")
            for assign in assignment.assignments():
                print(
                    f"vg_id: {assign.vg_id()}, offset: {assign.offset()}, begin: {assign.begin()}, end: {assign.end()}"
                )
                consumer.seek(topic, assign.vg_id(), assign.begin())
                print("Assignment seek to beginning successfully.")

    except Exception as err:
        print(f"Failed to seek offset, topic: {topic}, groupId: {groupId}, clientId: {clientId}, ErrMessage:{err}.")
        raise err
    # ANCHOR_END: assignment


def subscribe(consumer):
    try:
        consumer.subscribe([topic])
        print("Subscribe topics successfully")
        for i in range(50):
            records = consumer.poll(timeout=1.0)
            if records:
                for block in records:
                    for row in block:
                        print(f"data: {row}")

    except Exception as err:
        print(f"Failed to poll data, topic: {topic}, groupId: {groupId}, clientId: {clientId}, ErrMessage:{err}.")
        raise err


def commit_offset(consumer):
    try:
        for i in range(5):
            records = consumer.poll(timeout=1.0)
            if records:
                for block in records:
                    for row in block:
                        print(f"data: {row}")

                #  after processing the data, commit the offset manually
                consumer.commit(records)
                print("Commit offset manually successfully.")

    except Exception as err:
        print(f"Failed to commit offset, topic: {topic}, groupId: {groupId}, clientId: {clientId}, ErrMessage:{err}.")
        raise err


def unsubscribe(consumer):
    # ANCHOR: unsubscribe
    try:
        consumer.unsubscribe()
        print("Consumer unsubscribed successfully.")
    except Exception as err:
        print(
            f"Failed to unsubscribe consumer. topic: {topic}, groupId: {groupId}, clientId: {clientId}, ErrMessage:{err}."
        )
        raise err
    finally:
        if consumer:
            consumer.close()
            print("Consumer closed successfully.")


def test_cloud_tmq():
    consumer = None
    try:
        prepareMeta()
        consumer = create_consumer()
        subscribe(consumer)
        seek_offset(consumer)
        commit_offset(consumer)
    finally:
        if consumer:
            unsubscribe(consumer)


if __name__ == "__main__":
    test_cloud_tmq()
