import json

import taos
from taos import SmlProtocol, SmlPrecision

lines = [[{"metric": "meters.current", "timestamp": 1648432611249, "value": 10.3, "tags": {"location": "Beijing.Chaoyang", "groupid": 2}},
          {"metric": "meters.voltage", "timestamp": 1648432611249, "value": 219, "tags": {"location": "Beijing.Haidian", "groupid": 1}}],
         [{"metric": "meters.current", "timestamp": 1648432611250, "value": 12.6, "tags": {"location": "Beijing.Chaoyang", "groupid": 2}},
          {"metric": "meters.voltage", "timestamp": 1648432611250, "value": 221, "tags": {"location": "Beijing.Haidian", "groupid": 1}}]
         ]

# create connection use firstEP in taos.cfg.
conn = taos.connect()


def create_database():
    conn.execute("create database test")
    conn.execute("use test")


def insert_lines():
    global lines
    lines = [json.dumps(line) for line in lines]
    print(lines)
    affected_rows = conn.schemaless_insert(lines, SmlProtocol.JSON_PROTOCOL, SmlPrecision.NOT_CONFIGURED)
    print(affected_rows)  # 这里有 bug, 4 条数据只写入 2 条。


if __name__ == '__main__':
    create_database()
    insert_lines()
