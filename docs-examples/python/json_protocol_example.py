import json

import taos
from taos import SmlProtocol, SmlPrecision

lines = [[{"metric": "meters.current", "timestamp": 1648432611249, "value": 10.3, "tags": {"location": "Beijing.Chaoyang", "groupid": 2}},
          {"metric": "meters.voltage", "timestamp": 1648432611249, "value": 219, "tags": {"location": "Beijing.Haidian", "groupid": 1}}],
         [{"metric": "meters.current", "timestamp": 1648432611250, "value": 12.6, "tags": {"location": "Beijing.Chaoyang", "groupid": 2}},
          {"metric": "meters.voltage", "timestamp": 1648432611250, "value": 221, "tags": {"location": "Beijing.Haidian", "groupid": 1}}]
         ]


def get_connection():
    # create connection use firstEp in taos.cfg.
    return taos.connect()


def create_database(conn):
    conn.execute("create database test")
    conn.execute("use test")


def insert_lines(conn):
    global lines
    lines = [json.dumps(line) for line in lines]
    print(lines)
    affected_rows = conn.schemaless_insert(lines, SmlProtocol.JSON_PROTOCOL, SmlPrecision.NOT_CONFIGURED)
    print(affected_rows)  # 这里有 bug, 4 条数据只写入 2 条。


if __name__ == '__main__':
    connection = get_connection()
    create_database(connection)
    insert_lines(connection)
    connection.close()
