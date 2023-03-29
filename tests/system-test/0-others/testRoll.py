from taos.tmq import Consumer
import taos
import taosrest
import socket


def init_tmq_env(db, topic):
    conn = taos.connect()
    # conn.execute("create dnode test209")
    # conn.execute("create dnode test216")
    # conn.execute("create mnode on dnode 2")
    # conn.execute("create mnode on dnode 3")

    conn.execute("drop topic if exists {}".format(topic))
    conn.execute("drop database if exists {}".format(db))
    conn.execute("create database if not exists {} replica 1 wal_retention_period 3600".format(db))
    conn.select_db(db)
    conn.execute(
        "create stable if not exists stb1 (ts timestamp, c1 int, c2 float, c3 varchar(16)) tags(t1 int, t3 varchar(16))")
    conn.execute("create table if not exists tb1 using stb1 tags(1, 't1')")
    conn.execute("create table if not exists tb2 using stb1 tags(2, 't2')")
    conn.execute("create table if not exists tb3 using stb1 tags(3, 't3')")
    conn.execute("create topic if not exists {} as select ts, c1, c2, c3 from stb1".format(topic))
    conn.execute("insert into tb1 values (now+10s, 1, 1.0, 'tmq test')")
    conn.execute("insert into tb2 values (now+100s, 2, 2.0, 'tmq test')")
    conn.execute("insert into tb3 values (now+20s, 3, 3.0, 'tmq test')")
    conn.execute("insert into tb3 values (now+30s, 4, 4.0, 'tmq test4')")

def init_tmq_rest_env(db, topic):
    host = socket.gethostname()
    conn = taosrest.connect(url=f"http://{host}:6041")

    # conn.execute("create dnode test209")
    # conn.execute("create dnode test216")
    # conn.execute("create mnode on dnode 2")
    # conn.execute("create mnode on dnode 3")

    conn.execute("drop topic if exists {}".format(topic))
    conn.execute("drop database if exists {}".format(db))
    conn.execute("create database if not exists {} replica 3 wal_retention_period 3600".format(db))
    conn.select_db(db)
    conn.execute(
        "create stable if not exists stb1 (ts timestamp, c1 int, c2 float, c3 varchar(16)) tags(t1 int, t3 varchar(16))")
    conn.execute("create table if not exists tb1 using stb1 tags(1, 't1')")
    conn.execute("create table if not exists tb2 using stb1 tags(2, 't2')")
    conn.execute("create table if not exists tb3 using stb1 tags(3, 't3')")
    conn.execute("create topic if not exists {} as select ts, c1, c2, c3 from stb1".format(topic))
    conn.execute("insert into tb1 values (now+10s, 1, 1.0, 'tmq test')")
    conn.execute("insert into tb2 values (now+100s, 2, 2.0, 'tmq test')")
    conn.execute("insert into tb3 values (now+20s, 3, 3.0, 'tmq test')")
    conn.execute("insert into tb3 values (now+30s, 4, 4.0, 'tmq test4')")




if __name__ == '__main__':
    conn = taos.connect()

    init_tmq_env("tmq_test", "tmq_test_topic")  # init env
    # init_tmq_rest_env("tmq_test", "tmq_test_topic")
    consumer = Consumer(
        {
            "group.id": "tg75",
            "client.id": "124",
            "td.connect.user": "root",
            "td.connect.pass": "taosdata",
            "enable.auto.commit": "true",
            "experimental.snapshot.enable":  "true",
        }
    )
    consumer.subscribe(["tmq_test_topic"])

    while True:
        res = consumer.poll(10)
        if not res:
            break
        err = res.error()
        if err is not None:
            raise err
        val = res.value()

        for block in val:
            print(block.fetchall())
