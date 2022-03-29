import taos
from taos import SmlProtocol, SmlPrecision

lines = ["meters,location=Beijing.Chaoyang,groupid=2 current=10.3,voltage=219,phase=0.31 1648432611249300",
         "meters,location=Beijing.Chaoyang,groupid=2 current=12.6,voltage=218,phase=0.33 1648432611249800",
         "meters,location=Beijing.Chaoyang,groupid=2 current=12.3,voltage=221,phase=0.31 1648432611250300",
         "meters,location=Beijing.Chaoyang,groupid=3 current=10.3,voltage=218,phase=0.25 1648432611249200",
         "meters,location=Beijing.Haidian,groupid=2 current=11.8,voltage=221,phase=0.28 1648432611249000",
         "meters,location=Beijing.Haidian,groupid=2 current=13.4,voltage=223,phase=0.29 1648432611249500",
         "meters,location=Beijing.Haidian,groupid=3 current=10.8,voltage=223,phase=0.29 1648432611249300",
         "meters,location=Beijing.Haidian,groupid=3 current=11.3,voltage=221,phase=0.35 1648432611249800",
         ]


def get_connection():
    # create connection use firstEP in taos.cfg.
    return taos.connect()


def create_database(conn):
    # the default precision is ms (microsecond), but we use us(microsecond) here.
    conn.execute("create database test precision 'us'")
    conn.execute("use test")


def insert_lines(conn):
    affected_rows = conn.schemaless_insert(lines, SmlProtocol.LINE_PROTOCOL, SmlPrecision.MICRO_SECONDS)
    print(affected_rows)  # 8


if __name__ == '__main__':
    connection = get_connection()
    create_database(connection)
    insert_lines(connection)
    connection.close()
