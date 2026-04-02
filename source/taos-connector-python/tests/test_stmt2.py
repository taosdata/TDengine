import math
import taos
import pytest
from datetime import datetime
from shapely.wkb import dumps
from shapely.wkt import loads as wkt_loads
from taos import bind2
from taos import log
from taos.statement2 import *
from utils import IS_WS


# input WKT return WKB (bytes object)
def WKB(wkt, hex=False):
    if wkt is None:
        return None
    wkb = wkt_loads(wkt)
    wkb_bytes = dumps(wkb, hex)
    return wkb_bytes


@pytest.fixture
def conn():
    # type: () -> taos.TaosConnection
    taos.log.setting(True, True, True, True, True, True)
    return taos.connect()


def compareLine(oris, rows):
    n = len(oris)
    if len(rows) != n:
        return False
    log.debug(f"    len is {n} oris={oris} rows={rows}")
    for i in range(n):
        if oris[i] != rows[i]:
            if type(rows[i]) == bool:
                if bool(oris[i]) != rows[i]:
                    log.debug1(f"    diff bool i={i} oris[i] == rows[i] {oris[i]} == {rows[i]}")
                    return False
                else:
                    log.debug1(f"    float i={i} oris[i] == rows[i] {oris[i]} == {rows[i]}")
            elif type(rows[i]) == float:
                if math.isclose(oris[i], rows[i], rel_tol=1e-3) is False:
                    log.debug1(f"    diff float i={i} oris[i] == rows[i] {oris[i]} == {rows[i]}")
                    return False
                else:
                    log.debug1(f"    float i={i} oris[i] == rows[i] {oris[i]} == {rows[i]}")
            else:
                log.debug1(f"    diff i={i} oris[i] == rows[i] {oris[i]} == {rows[i]}")
                return False
        else:
            log.debug1(f"    i={i} oris[i] == rows[i] {oris[i]} == {rows[i]}")

    return True


def checkResultCorrect(conn, sql, tagsTb, datasTb):
    # column to rows
    log.debug(f"check sql correct: {sql}\n")
    oris = []
    ncol = len(datasTb)
    nrow = len(datasTb[0])

    for i in range(nrow):
        row = []
        for j in range(ncol):
            if j == 0:
                # ts column
                c0 = datasTb[j][i]
                if type(c0) is int:
                    row.append(datasTb[j][i])
                else:
                    ts = int(bind2.datetime_to_timestamp(c0, PrecisionEnum.Milliseconds).value)
                    row.append(ts)
            else:
                row.append(datasTb[j][i])

        if tagsTb is not None:
            row += tagsTb
        oris.append(row)

    # fetch all
    log.debug(sql)
    res = conn.query(sql)
    i = 0
    for row in res:
        lrow = list(row)
        lrow[0] = int(lrow[0].timestamp() * 1000)
        if compareLine(oris[i], lrow) is False:
            log.info(f"insert data differet. i={i} expect ori data={oris[i]} query from db ={lrow}")
            raise (BaseException("check insert data correct failed."))
        else:
            log.debug(f"i={i} origin data same with get from db\n")
            log.debug(f" origin data = {oris[i]} \n")
            log.debug(f" get from db = {lrow} \n")
        i += 1


def checkResultCorrects(conn, dbname, stbname, tbnames, tags, datas):
    count = len(tbnames)
    for i in range(count):
        if stbname is None:
            sql = f"select * from {dbname}.{tbnames[i]} "
        else:
            sql = f"select * from {dbname}.{stbname} where tbname='{tbnames[i]}' "

        checkResultCorrect(conn, sql, tags[i], datas[i])

    print("insert data check correct ..................... ok\n")


def prepare(conn, dbname, stbname, ntb1, ntb2):
    conn.execute("drop database if exists %s" % dbname)
    conn.execute("create database if not exists %s precision 'ms' " % dbname)
    conn.select_db(dbname)
    # stable
    sql = f"create table if not exists {dbname}.{stbname}(ts timestamp, name binary(32), sex bool, score int, remarks blob) tags(grade nchar(8), class int)"
    conn.execute(sql)
    # normal table
    sql = f"create table if not exists {dbname}.{ntb1} (ts timestamp, name varbinary(32), sex bool, score float, geo geometry(128), remarks blob)"
    conn.execute(sql)
    sql = f"create table if not exists {dbname}.{ntb2} (ts timestamp, name varbinary(32), sex bool, score float, geo geometry(128), remarks blob)"
    conn.execute(sql)


# performace is high
def insert_bind_param(conn, stmt2, dbname, stbname):
    #
    #  table info , write 5 lines to 3 child tables d0, d1, d2 with super table
    #
    tbnames = ["d1", "d2", "d3"]

    tags = [["grade1", 1], ["grade1", None], [None, 3]]
    datas = [
        # class 1
        [
            # student
            [1601481600000, 1601481600001, 1601481600002, 1601481600003, 1601481600004, 1601481600005],
            ["Mary", "Tom", "Jack", "Jane", "alex", None],
            [0, 1, 1, 0, 1, None],
            [98, 80, 60, 100, 99, None],
            [b"binary value_1", b"binary value_2", b"binary value_3", b"binary value_4", b"binary value_5", None],
        ],
        # class 2
        [
            # student
            [1601481600000, 1601481600001, 1601481600002, 1601481600003, 1601481600004, 1601481600005],
            ["Mary2", "Tom2", "Jack2", "Jane2", "alex2", None],
            [0, 1, 1, 0, 1, 0],
            [298, 280, 260, 2100, 299, None],
            [b"binary value_1", b"binary value_2", b"binary value_3", b"binary value_4", b"binary value_5", None],
        ],
        # class 3
        [
            # student
            [1601481600000, 1601481600001, 1601481600002, 1601481600003, 1601481600004, 1601481600005],
            ["Mary3", "Tom3", "Jack3", "Jane3", "alex3", "Mark"],
            [0, 1, 1, 0, 1, None],
            [398, 380, 360, 3100, 399, None],
            [b"binary value_1", b"binary value_2", b"binary value_3", b"binary value_4", b"binary value_5", None],
        ],
    ]

    stmt2.bind_param(tbnames, tags, datas)
    stmt2.execute()

    # check correct
    checkResultCorrects(conn, dbname, stbname, tbnames, tags, datas)


def insert_bind_param_normal_tables(conn, stmt2, dbname, ntb):
    tbnames = [ntb]
    tags = None
    wkts = [None, b"POINT(121.213 31.234)", b"POINT(122.22 32.222)", None, b"POINT(124.22 34.222)"]
    wkbs = [WKB(wkt) for wkt in wkts]

    datas = [
        # table 1
        [
            # student
            [
                1601481600000,
                1601481600004,
                "2024-09-19 10:00:00",
                "2024-09-19 10:00:01.123",
                datetime(2024, 9, 20, 10, 11, 12, 456),
            ],
            [b"Mary", b"tom", b"Jack", b"Jane", None],
            [0, 3.14, True, 0, 1],
            [98, 99.87, 60, 100, 99],
            wkbs,
            [b"binary value_1", b"binary value_2", b"binary value_3", b"binary value_4", b"binary value_5"],
        ]
    ]

    stmt2.bind_param(tbnames, tags, datas)
    stmt2.execute()

    # check correct
    checkResultCorrects(conn, dbname, None, tbnames, [None], datas)


def insert_bind_param_with_table(conn, stmt2, dbname, stbname, ctb):
    tbnames = None
    tags = [["grade2", 1]]

    # prepare data
    datas = [
        # table 1
        [
            # student
            [
                1601481600000,
                1601481600004,
                "2024-09-19 10:00:00",
                "2024-09-19 10:00:01.123",
                datetime(2024, 9, 20, 10, 11, 12, 456),
            ],
            ["Mary", "Tom", "Jack", "Jane", "alex"],
            [0, 1, 1, 0, 1],
            [98, 80, 60, 100, 99],
            [b"binary value_1", b"binary value_2", b"binary value_3", b"binary value_4", b"binary value_5"],
        ]
    ]

    stmt2.bind_param(tbnames, tags, datas)
    stmt2.execute()

    # check correct
    checkResultCorrects(conn, dbname, stbname, [ctb], tags, datas)


# insert with single table (performance is lower)
def insert_bind_param_with_tables(conn, stmt2, dbname, stbname):
    tbnames = ["t1", "t2", "t3"]
    tags = [["grade2", 1], ["grade2", 2], ["grade2", 3]]

    # prepare data
    datas = [
        # table 1
        [
            # student
            [
                1601481600000,
                1601481600004,
                "2024-09-19 10:00:00",
                "2024-09-19 10:00:01.123",
                datetime(2024, 9, 20, 10, 11, 12, 456),
            ],
            ["Mary", "Tom", "Jack", "Jane", "alex"],
            [0, 1, 1, 0, 1],
            [98, 80, 60, 100, 99],
            [b"binary value_1", b"binary value_2", b"binary value_3", b"binary value_4", b"binary value_5"],
        ],
        # table 2
        [
            # student
            [1601481600000, 1601481600001, 1601481600002, 1601481600003, 1601481600004],
            ["Mary2", "Tom2", "Jack2", "Jane2", "alex2"],
            [0, 1, 1, 0, 1],
            [298, 280, 260, 2100, 299],
            [b"binary value_1", b"binary value_2", b"binary value_3", b"binary value_4", b"binary value_5"],
        ],
        # table 3
        [
            # student
            [1601481600000, 1601481600001, 1601481600002, 1601481600003, 1601481600004],
            ["Mary3", "Tom3", "Jack3", "Jane3", "alex3"],
            [0, 1, 1, 0, 1],
            [398, 380, 360, 3100, 399],
            [b"binary value_1", b"binary value_2", b"binary value_3", b"binary value_4", b"binary value_5"],
        ],
    ]

    table0 = BindTable(tbnames[0], tags[0])
    table1 = BindTable(tbnames[1], tags[1])
    table2 = BindTable(tbnames[2], tags[2])

    for data in datas[0]:
        table0.add_col_data(data)
    for data in datas[1]:
        table1.add_col_data(data)
    for data in datas[2]:
        table2.add_col_data(data)

    # bind with single table
    stmt2.bind_param_with_tables([table0, table1, table2])
    stmt2.execute()

    # check correct
    checkResultCorrects(conn, dbname, stbname, tbnames, tags, datas)


def do_check_invalid(stmt2, tbnames, tags, datas):
    table0 = BindTable(tbnames[0], tags[0])
    table1 = BindTable(tbnames[1], tags[1])
    table2 = BindTable(tbnames[2], tags[2])

    for data in datas[0]:
        table0.add_col_data(data)
    for data in datas[1]:
        table1.add_col_data(data)
    for data in datas[2]:
        table2.add_col_data(data)

    # bind with single table
    try:
        stmt2.bind_param_with_tables([table0, table1, table2])
        stmt2.execute()
    except Exception as err:
        # traceback.print_stack()
        print(f"failed to do_check_invalid. err={err}")
        return

    print(f"input invalid data  passed , unexpect. \ntbnames={tbnames}\ntags={tags} \ndatas={datas} \n")
    assert False


def check_input_invalid_param(conn, stmt2, dbname, stbname):
    tbnames = ["t1", "t2", "t3"]
    tags = [["grade2", 1], ["grade2", 2], ["grade2", 3]]

    # prepare data
    datas = [
        # table 1
        [
            # student
            [
                1601481600000,
                1601481600004,
                "2024-09-19 10:00:00",
                "2024-09-19 10:00:01.123",
                datetime(2024, 9, 20, 10, 11, 12, 456),
            ],
            ["Mary", "Tom", "Jack", "Jane", "alex"],
            [0, 1, 1, 0, 1],
            [98, 80, 60, 100, 99],
        ],
        # table 2
        [
            # student
            [1601481600000, 1601481600001, 1601481600002, 1601481600003, 1601481600004],
            ["Mary2", "Tom2", "Jack2", "Jane2", "alex2"],
            [0, 1, 1, 0, 1],
            [298, 280, 260, 2100, 299],
        ],
        # table 3
        [
            # student
            [1601481600000, 1601481600001, 1601481600002, 1601481600003, 1601481600004],
            ["Mary3", "Tom3", "Jack3", "Jane3", "alex3"],
            [0, 1, 1, 0, 1],
            [398, 380, 360, 3100, 399],
        ],
    ]

    # some tags is none
    tags1 = [["grade2", 1], None, ["grade2", 3]]
    do_check_invalid(stmt2, tbnames, tags1, datas)

    # timestamp is over range
    origin = datas[0][0][0]
    datas[0][0][0] = 100000000000000000000000
    do_check_invalid(stmt2, tbnames, tags, datas)
    datas[0][0][0] = origin  # restore


# insert with single table (performance is lower)
def insert_with_normal_tables(conn, stmt2, dbname, ntb):
    tbnames = [ntb]
    tags = [None]
    # prepare data

    wkts = [None, "POINT(121.213 31.234)", "POINT(122.22 32.222)", None, "POINT(124.22 34.222)"]
    wkbs = [WKB(wkt) for wkt in wkts]

    datas = [
        # table 1
        [
            # student
            [
                1601481600000,
                1601481600004,
                "2024-09-19 10:00:00",
                "2024-09-19 10:00:01.123",
                datetime(2024, 9, 20, 10, 11, 12, 456),
            ],
            [b"Mary", b"tom", b"Jack", b"Jane", None],
            [0, 3.14, True, 0, 1],
            [98, 99.87, 60, 100, 99],
            wkbs,
            [b"binary value_1", b"binary value_2", b"binary value_3", b"binary value_4", b"binary value_5"],
        ]
    ]

    table0 = BindTable(tbnames[0], tags[0])
    for data in datas[0]:
        table0.add_col_data(data)

    # bind with single table
    stmt2.bind_param_with_tables([table0])
    stmt2.execute()

    # check correct
    checkResultCorrects(conn, dbname, None, tbnames, tags, datas)


def test_stmt2_prepare_empty_sql(conn):
    if not IS_V3:
        print(" test_stmt2_prepare_empty_sql not support TDengine 2.X version.")
        return

    try:
        # prepare
        stmt2 = conn.statement2()
        stmt2.prepare(sql="")

        # should not run here
        conn.close()
        print("prepare empty sql ............................. failed\n")
        assert False

    except StatementError as err:
        print("prepare empty sql ............................. ok\n")
        conn.close()


def test_bind_invalid_tbnames_type():
    if not IS_V3:
        print(" test_bind_invalid_tbnames_type not support TDengine 2.X version.")
        return

    dbname = "stmt2"
    stbname = "stmt2_stable"
    subtbname = "stmt2_subtable"

    try:
        conn = taos.connect()
        conn.execute(f"drop database if exists {dbname}")
        conn.execute(f"create database {dbname}")
        conn.select_db(dbname)
        conn.execute(f"create stable {stbname} (ts timestamp, a int) tags (b int);")
        conn.execute(f"create table {subtbname} using {stbname} tags(0);")

        stmt2 = conn.statement2(f"insert into ? using {dbname}.{stbname} tags(?) values(?,?)")

        tags = [[1]]
        datas = [[[1626861392589], [1]]]

        stmt2.bind_param(subtbname, tags, datas)

        # should not run here
        conn.close()
        print("bind invalid tbnames type ..................... failed\n")
        assert False

    except StatementError as err:
        print("bind invalid tbnames type ..................... ok\n")
        conn.close()


@pytest.mark.skipif(IS_WS, reason="Skip WS")
def test_stmt2_insert(conn):
    if not IS_V3:
        print(" test_stmt2_query not support TDengine 2.X version.")
        return

    dbname = "stmt2"
    stbname = "meters"
    ntb1 = "ntb1"
    ntb2 = "ntb2"

    try:
        prepare(conn, dbname, stbname, ntb1, ntb2)

        ctb = "ctb"  # child table
        stmt2 = conn.statement2(f"insert into {dbname}.{ctb} using {dbname}.{stbname} tags (?,?) values(?,?,?,?,?)")
        insert_bind_param_with_table(conn, stmt2, dbname, stbname, ctb)
        print("insert child table ........................... ok\n")
        stmt2.close()

        # prepare
        stmt2 = conn.statement2(f"insert into ? using {dbname}.{stbname} tags(?,?) values(?,?,?,?,?)")
        print("insert prepare sql ............................ ok\n")

        # insert with table
        insert_bind_param_with_tables(conn, stmt2, dbname, stbname)
        print("insert bind with tables ....................... ok\n")
        check_input_invalid_param(conn, stmt2, dbname, stbname)
        print("check input invalid params .................... ok\n")

        # # insert with split args
        insert_bind_param(conn, stmt2, dbname, stbname)
        print("insert bind ................................... ok\n")
        print("insert execute ................................ ok\n")
        stmt2.close()

        # ntb1
        stmt2 = conn.statement2(f"insert into {dbname}.{ntb1} values(?,?,?,?,?,?)")
        insert_with_normal_tables(conn, stmt2, dbname, ntb1)
        print("insert normal tables .......................... ok\n")
        stmt2.close()

        # ntb2
        stmt2 = conn.statement2(f"insert into {dbname}.{ntb2} values(?,?,?,?,?,?)")
        insert_bind_param_normal_tables(conn, stmt2, dbname, ntb2)
        print("insert normal tables (bind param) ............. ok\n")
        stmt2.close()

        conn.close()
        print("test_stmt2_insert ............................. [passed]\n")
    except Exception as err:
        # conn.execute("drop database if exists %s" % dbname)
        print("test_stmt2_insert ............................. failed\n")
        conn.close()
        raise err


#
#  ------------------------ query -------------------
#
def query_bind_param(conn, stmt2):
    # set param
    # tbnames = ["d2"]
    tbnames = None
    tags = None
    datas = [
        # class 1
        [
            # where name in ('Tom2','alex2') or score > 1000;"
            ["Tom2"],
            [100],
            [True],
        ]
    ]

    # set param
    # types = [FieldType.C_BINARY, FieldType.C_INT]
    # stmt2.set_columns_type(types)

    # bind
    stmt2.bind_param(tbnames, tags, datas)


# compare
def compare_result(conn, sql2, res2):
    lres1 = []
    lres2 = []

    # shor res2
    for row in res2:
        log.debug(f" res2 rows = {row} \n")
        lres2.append(row)

    res1 = conn.query(sql2)
    for row in res1:
        log.debug(f" res1 rows = {row} \n")
        lres1.append(row)

    row1 = len(lres1)
    row2 = len(lres2)
    col1 = len(lres1[0])
    col2 = len(lres2[0])

    # check number
    if row1 != row2:
        err = f"two results row count different. row1={row1} row2={row2}"
        raise (BaseException(err))
    if col1 != col2:
        err = f" two results column count different. col1={col1} col2={col2}"
        raise (BaseException(err))

    for i in range(row1):
        for j in range(col1):
            if lres1[i][j] != lres2[i][j]:
                raise (f" two results data different. i={i} j={j} data1={res1[i][j]} data2={res2[i][j]}\n")


# query
def test_stmt2_query(conn):
    if not IS_V3:
        print(" test_stmt2_query not support TDengine 2.X version.")
        return

    dbname = "stmt2"
    stbname = "meters"
    ntb1 = "ntb1"
    ntb2 = "ntb2"
    sql1 = f"select * from {dbname}.d2 where name in (?) and score > ? and sex = ?;"
    sql2 = f"select * from {dbname}.d2 where name in ('Tom2') and score > 100 and sex = true;"

    try:
        # prepare
        prepare(conn, dbname, stbname, ntb1, ntb2)

        # prepare
        # stmt2 = conn.statement2(f"insert into ? using {dbname}.{stbname} tags(?,?) values(?,?,?,?)")
        # insert_bind_param_with_tables(conn, stmt2, dbname, stbname)
        # insert_bind_param(conn, stmt2, dbname, stbname)
        # stmt2.close()
        # print("insert bind & execute ......................... ok\n")

        conn.execute(
            f"insert into d2 using {stbname} tags('grade1', 2) values('2020-10-01 00:00:00.000', 'Mary2', false, 298, 'XXX')"
        )
        conn.execute(
            f"insert into d2 using {stbname} tags('grade1', 2) values('2020-10-01 00:00:00.001', 'Tom2', true, 280, 'YYY')"
        )
        conn.execute(
            f"insert into d2 using {stbname} tags('grade1', 2) values('2020-10-01 00:00:00.002', 'Jack2', true, 260, 'ZZZ')"
        )
        conn.execute(
            f"insert into d2 using {stbname} tags('grade1', 2) values('2020-10-01 00:00:00.003', 'Jane2', false, 2100, 'WWW')"
        )
        conn.execute(
            f"insert into d2 using {stbname} tags('grade1', 2) values('2020-10-01 00:00:00.004', 'Tom2', true, 299, 'ZZZ')"
        )
        conn.execute(
            f"insert into d2 using {stbname} tags('grade1', 2) values('2020-10-01 00:00:00.005', 'Tom2', false, NULL, 'WWW')"
        )

        # statement2
        stmt2 = conn.statement2(sql1)
        print("query prepare sql ............................. ok\n")

        # insert with table
        # insert_bind_param_with_tables(conn, stmt2)

        # bind
        query_bind_param(conn, stmt2)
        print("query bind param .............................. ok\n")

        # query execute
        stmt2.execute()

        # fetch result
        res2 = stmt2.result()

        # check result
        compare_result(conn, sql2, res2)
        print("query check corrent ........................... ok\n")

        # conn.execute("drop database if exists %s" % dbname)
        stmt2.close()
        conn.close()
        print("test_stmt2_query .............................. [passed]\n")

    except Exception as err:
        print("query ......................................... failed\n")
        conn.close()
        raise err


def test_stmt2_example(conn):
    try:
        # create database
        rows_affected: int = conn.execute(f"CREATE DATABASE IF NOT EXISTS power")
        print(f"Create database power successfully")
        assert rows_affected == 0
        conn.select_db("power")

        conn.execute("create table if not exists stb (ts timestamp, v int) tags(jt json)")
        print(f"Create stable power.stb successfully")

        stmt = conn.statement2("INSERT INTO stb (ts, v, jt, tbname) VALUES (?,?,?,?)")

        datas = [
            [1626861392589, 7, '{"name":"value"}', "tb1"],
            [1626861392590, 8, '{"name":"value2"}', "tb2"],
            [1626861392591, 9, '{"name":"value3"}', "tb3"],
            [1626861392592, 10, '{"name":"value4"}', "tb4"],
            [1626861392593, 11, '{"name":"value4"}', "tb4"],
            [1626861392584, 7, '{"name":"value"}', "tb1"],
            [1626861392595, 8, '{"name":"value2"}', "tb2"],
            [1626861392596, 9, '{"name":"value3"}', "tb3"],
            [1626861392597, 10, '{"name":"value4"}', "tb4"],
            [1626861392598, 11, '{"name":"value4"}', "tb4"],
        ]

        stmt.bind_param(None, None, datas)
        print("bind_param done")
        stmt.execute()

        assert stmt.affected_rows == 10

        result = conn.query("SELECT ts, v, jt FROM stb limit 100", req_id=1)
        for row in result:
            print(f"ts: {row[0]}, v: {row[1]}, jt:  {row[2]}")

    except Exception as err:
        print(f"Failed to execute json_tag_example; ErrMessage:{err}")
    finally:
        if conn:
            conn.close()


if __name__ == "__main__":
    print("start stmt2 test case...\n")

    # insert
    test_stmt2_insert(taos.connect())

    # query
    test_stmt2_query(taos.connect())

    test_stmt2_example(taos.connect())
    print("end stmt2 test case.\n")
