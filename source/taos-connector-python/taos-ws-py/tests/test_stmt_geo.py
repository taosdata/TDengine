import time
import taosws


def before_test(db_name):
    conn = taosws.connect()
    conn.execute("drop database if exists %s" % db_name)
    conn.execute("create database %s" % db_name)
    conn.execute("use %s" % db_name)
    conn.execute(
        "create table stb1 (ts timestamp, a int, b float, c varchar(10), geo geometry(512), vbinary varbinary(32)) tags (t1 int, t2 binary(10))"
    )
    conn.close()


def after_test(db_name):
    conn = taosws.connect()
    conn.execute("drop database if exists %s" % db_name)
    conn.close()


def test_stmt_insert():
    db_name = "test_ws_stmt_{}".format(int(time.time()))
    before_test(db_name)

    conn = taosws.connect("taosws://localhost:6041/%s" % db_name)

    stmt = conn.statement()

    stmt.prepare("insert into ? using stb1 tags (?, ?) values (?, ?, ?, ?, ?, ?)")
    stmt.set_tbname("stb1_1")
    stmt.set_tags([taosws.int_to_tag(1), taosws.varchar_to_tag("aaa")])
    geo = bytes(
        [
            0x01,
            0x01,
            0x00,
            0x00,
            0x00,
            0x00,
            0x00,
            0x00,
            0x00,
            0x00,
            0x00,
            0x59,
            0x40,
            0x00,
            0x00,
            0x00,
            0x00,
            0x00,
            0x00,
            0x59,
            0x40,
        ]
    )

    varbinary = bytes([0x01, 0x02, 0x03, 0x04])

    stmt.bind_param(
        [
            taosws.millis_timestamps_to_column([1686844800000, 1686844801000, 1686844802000, 1686844803000]),
            taosws.ints_to_column([1, 2, 3, 4]),
            taosws.floats_to_column([1.1, 2.2, 3.3, 4.4]),
            taosws.varchar_to_column(["a", "b", "c", "d"]),
            taosws.geometry_to_column(
                [
                    bytes(
                        [
                            0x01,
                            0x01,
                            0x00,
                            0x00,
                            0x00,
                            0x00,
                            0x00,
                            0x00,
                            0x00,
                            0x00,
                            0x00,
                            0x59,
                            0x40,
                            0x00,
                            0x00,
                            0x00,
                            0x00,
                            0x00,
                            0x00,
                            0x59,
                            0x40,
                        ]
                    ),
                    bytes(
                        [
                            0x01,
                            0x01,
                            0x00,
                            0x00,
                            0x00,
                            0x00,
                            0x00,
                            0x00,
                            0x00,
                            0x00,
                            0x00,
                            0x59,
                            0x40,
                            0x00,
                            0x00,
                            0x00,
                            0x00,
                            0x00,
                            0x00,
                            0x59,
                            0x40,
                        ]
                    ),
                    bytes(
                        [
                            0x01,
                            0x01,
                            0x00,
                            0x00,
                            0x00,
                            0x00,
                            0x00,
                            0x00,
                            0x00,
                            0x00,
                            0x00,
                            0x59,
                            0x40,
                            0x00,
                            0x00,
                            0x00,
                            0x00,
                            0x00,
                            0x00,
                            0x59,
                            0x40,
                        ]
                    ),
                    bytes(
                        [
                            0x01,
                            0x01,
                            0x00,
                            0x00,
                            0x00,
                            0x00,
                            0x00,
                            0x00,
                            0x00,
                            0x00,
                            0x00,
                            0x59,
                            0x40,
                            0x00,
                            0x00,
                            0x00,
                            0x00,
                            0x00,
                            0x00,
                            0x59,
                            0x40,
                        ]
                    ),
                ]
            ),
            taosws.varbinary_to_column(
                [
                    bytes([0x01, 0x02, 0x03, 0x04]),
                    bytes([0x01, 0x02, 0x03, 0x04]),
                    bytes([0x01, 0x02, 0x03, 0x04]),
                    bytes([0x01, 0x02, 0x03, 0x04]),
                ]
            ),
        ]
    )

    stmt.add_batch()
    rows = stmt.execute()
    assert rows == 4
    stmt.close()

    cursor = conn.cursor()
    cursor.execute("select * from stb1")
    while True:
        row = cursor.fetchone()
        if row:
            assert row[4] == geo
            assert row[5] == varbinary
        else:
            break
    after_test(db_name)
