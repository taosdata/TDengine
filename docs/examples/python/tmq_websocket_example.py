#!/usr/bin/python3
import taosws

db              = "power"
topic           = "topic_meters"
user            = "root"
password        = "taosdata"
host            = "localhost"
port            = 6041
groupId         = "group1"
clientId        = "1"  
tdConnWsScheme  = "ws"
autoOffsetReset = "latest"
autoCommitState = "true"
autoCommitIntv  = "1000"


def prepareMeta():
    conn = None

    try:
        conn = taosws.connect(user=user, password=password, host=host, port=port)

        # create database
        rowsAffected = conn.execute(f"CREATE DATABASE IF NOT EXISTS {db}")
        assert rowsAffected == 0

        # change database.
        rowsAffected = conn.execute(f"USE {db}")
        assert rowsAffected == 0

        # create super table
        rowsAffected = conn.execute(
            "CREATE TABLE IF NOT EXISTS `meters` (`ts` TIMESTAMP, `current` FLOAT, `voltage` INT, `phase` FLOAT) TAGS (`groupid` INT, `location` BINARY(64))"
        )
        assert rowsAffected == 0

        # create table
        rowsAffected = conn.execute(
            "CREATE TABLE IF NOT EXISTS `d0` USING `meters` (groupid, location) TAGS(0, 'Los Angles')")
        assert rowsAffected == 0

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
def create_consumer():  
    try:
        consumer = taosws.Consumer(conf={
            "td.connect.websocket.scheme": tdConnWsScheme,
            "group.id": groupId,
            "client.id": clientId,
            "auto.offset.reset": autoOffsetReset,
            "td.connect.ip": host,
            "td.connect.port": port,
            "enable.auto.commit": autoCommitState,
            "auto.commit.interval.ms": autoCommitIntv,
        })
        print(f"Create consumer successfully, host: {host}:{port}, groupId: {groupId}, clientId: {clientId}.");
        return consumer;
    except Exception as err:
        print(f"Failed to create websocket consumer, host: {host}:{port}, groupId: {groupId}, clientId: {clientId}, ErrMessage:{err}.");
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
                    f"vg_id: {assign.vg_id()}, offset: {assign.offset()}, begin: {assign.begin()}, end: {assign.end()}")
                consumer.seek(topic, assign.vg_id(), assign.begin())
                print("Assignment seek to beginning successfully.")

    except Exception as err:
        print(f"Failed to seek offset, topic: {topic}, groupId: {groupId}, clientId: {clientId}, ErrMessage:{err}.")
        raise err
    # ANCHOR_END: assignment


# ANCHOR: subscribe
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


# ANCHOR_END: subscribe

# ANCHOR: commit_offset
def commit_offset(consumer):
    try:
        for i in range(50):
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


# ANCHOR_END: commit_offset
#

def unsubscribe(consumer):
# ANCHOR: unsubscribe    
    try:
        consumer.unsubscribe()
        print("Consumer unsubscribed successfully.");
    except Exception as err:
        print(f"Failed to unsubscribe consumer. topic: {topic}, groupId: {groupId}, clientId: {clientId}, ErrMessage:{err}.")
        raise err
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
    finally:
        if consumer:
            unsubscribe(consumer)
