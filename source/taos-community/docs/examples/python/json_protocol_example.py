import json

import taos
from taos import SmlProtocol, SmlPrecision

lines = [{"metric": "meters.current", "timestamp": 1648432611249, "value": 10.3, "tags": {"location": "California.SanFrancisco", "groupid": 2}},
         {"metric": "meters.voltage", "timestamp": 1648432611249, "value": 219,
          "tags": {"location": "California.LosAngeles", "groupid": 1}},
         {"metric": "meters.current", "timestamp": 1648432611250, "value": 12.6,
          "tags": {"location": "California.SanFrancisco", "groupid": 2}},
         {"metric": "meters.voltage", "timestamp": 1648432611250, "value": 221, "tags": {"location": "California.LosAngeles", "groupid": 1}}]


def get_connection():
    return taos.connect()


def create_database(conn):
    conn.execute("CREATE DATABASE test keep 36500")
    conn.execute("USE test")


def insert_lines(conn):
    global lines
    lines = json.dumps(lines)
    # note: the first parameter must be a list with only one element.
    affected_rows = conn.schemaless_insert(
        [lines], SmlProtocol.JSON_PROTOCOL, SmlPrecision.NOT_CONFIGURED)
    print(affected_rows)  # 4


if __name__ == '__main__':
    connection = get_connection()
    try:
        create_database(connection)
        insert_lines(connection)
    finally:
        connection.close()
