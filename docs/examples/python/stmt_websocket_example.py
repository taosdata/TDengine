#!
import time

import taosws

import taos


def before_test(db_name):
    taos_conn = taos.connect()
    taos_conn.execute("drop database if exists %s" % db_name)
    taos_conn.execute("create database %s keep 36500" % db_name)
    taos_conn.select_db(db_name)
    taos_conn.execute("create table t1 (ts timestamp, a int, b float, c varchar(10))")
    taos_conn.execute(
        "create table stb1 (ts timestamp, a int, b float, c varchar(10)) tags (t1 int, t2 binary(10))")
    taos_conn.close()


def after_test(db_name):
    taos_conn = taos.connect()
    taos_conn.execute("drop database if exists %s" % db_name)
    taos_conn.close()


def stmt_insert():
    db_name = 'test_ws_stmt_{}'.format(int(time.time()))
    before_test(db_name)

    conn = taosws.connect('taosws://root:taosdata@localhost:6041/%s' % db_name)

    stmt = conn.statement()
    stmt.prepare("insert into t1 values (?, ?, ?, ?)")

    stmt.bind_param([
        taosws.millis_timestamps_to_column([1686844800000, 1686844801000, 1686844802000, 1686844803000]),
        taosws.ints_to_column([1, 2, 3, 4]),
        taosws.floats_to_column([1.1, 2.2, 3.3, 4.4]),
        taosws.varchar_to_column(['a', 'b', 'c', 'd']),
    ])

    stmt.add_batch()
    rows = stmt.execute()
    assert rows == 4
    stmt.close()
    after_test(db_name)


def stmt_insert_into_stable():
    db_name = 'test_ws_stmt_{}'.format(int(time.time()))
    before_test(db_name)

    conn = taosws.connect("taosws://root:taosdata@localhost:6041/%s" % db_name)

    stmt = conn.statement()
    stmt.prepare("insert into ? using stb1 tags (?, ?) values (?, ?, ?, ?)")
    stmt.set_tbname('stb1_1')
    stmt.set_tags([
        taosws.int_to_tag(1),
        taosws.varchar_to_tag('aaa'),
    ])
    stmt.bind_param([
        taosws.millis_timestamps_to_column([1686844800000, 1686844801000, 1686844802000, 1686844803000]),
        taosws.ints_to_column([1, 2, 3, 4]),
        taosws.floats_to_column([1.1, 2.2, 3.3, 4.4]),
        taosws.varchar_to_column(['a', 'b', 'c', 'd']),
    ])

    stmt.add_batch()
    rows = stmt.execute()
    assert rows == 4
    stmt.close()
    after_test(db_name)


if __name__ == '__main__':
    stmt_insert()
    stmt_insert_into_stable()
