import taos
import pytest
import utils
from dotenv import load_dotenv
from taos.cinterface import *
from utils import tear_down_database, IS_WS


load_dotenv()


def connect():
    return CTaosInterface().connect()


def teardown_module(module):
    conn = taos.connect()
    db_name = "pytest_taos_stmt"
    tear_down_database(conn, db_name)
    conn.close()


cfg = dict(
    host="localhost",
    user=utils.test_username(),
    password=utils.test_password(),
)


def test_taos_get_client_info():
    info = taos_get_client_info()
    assert info is not None
    print("pass test_taos_connect_auth")


@pytest.mark.skipif(IS_WS, reason="Skip WS")
def test_taos_connect_auth():
    if not taos.IS_V3:
        return
    conn = taos_connect_auth(
        host="localhost",
        user="root",
        auth="dcc5bed04851fec854c035b2e40263b6",
    )
    assert conn is not None
    print("pass test_taos_connect_auth")


def test_taos_connect():
    conn = taos_connect(
        host="localhost",
        user=utils.test_username(),
        password=utils.test_password(),
    )
    assert conn is not None
    print("pass test_taos_connect")


def test_taos_use_result():
    c = taos_connect(**cfg)
    sql = "show databases"
    r = taos_query(c, sql)
    try:
        fields = taos_use_result(r)
        assert isinstance(fields, list)
        print("pass test_taos_use_result")
    except Exception as e:
        print(e)
        raise e


def test_taos_load_table_info():
    c = taos_connect(**cfg)
    taos_load_table_info(c, "information_schema.ins_dnodes")
    print("pass test_taos_load_table_info")


def test_taos_validate_sql():
    c = taos_connect(**cfg)
    sql = "show databases"
    msg = taos_validate_sql(c, sql)
    assert msg is None
    print("pass test_taos_validate_sql")


@pytest.mark.skipif(IS_WS, reason="Skip WS")
def test_taos_stmt_errstr():
    if not taos.IS_V3:
        return

    conn = taos.connect(**cfg)
    dbname = "pytest_taos_stmt"

    try:
        conn.execute("drop database if exists %s" % dbname)
        conn.execute("create database if not exists %s" % dbname)
        conn.select_db(dbname)
        conn.execute(
            "create table if not exists log(ts timestamp, bo bool, nil tinyint, ti tinyint, si smallint, ii int,\
             bi bigint, tu tinyint unsigned, su smallint unsigned, iu int unsigned, bu bigint unsigned, \
             ff float, dd double, bb binary(100), nn nchar(100), tt timestamp)",
        )

        stmt = conn.statement("insert into log values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)")
        stmt.set_tbname("test")

        params = taos.new_bind_params(16)
        params[0].timestamp(1626861392589, taos.PrecisionEnum.Milliseconds)
        params[1].bool(True)
        params[2].tinyint(None)
        params[3].tinyint(2)
        params[4].smallint(3)
        params[5].int(4)
        params[6].bigint(5)
        params[7].tinyint_unsigned(6)
        params[8].smallint_unsigned(7)
        params[9].int_unsigned(8)
        params[10].bigint_unsigned(9)
        params[11].float(10.1)
        params[12].double(10.11)
        params[13].binary("hello")
        params[14].nchar("stmt")
        params[15].timestamp(1626861392589, taos.PrecisionEnum.Milliseconds)

        stmt.bind_param(params)
        stmt.execute()
        assert stmt.affected_rows == 1

        result = conn.query("select * from log")
        row = result.next()
        assert len(row) == 16
        assert row[2] is None
        for i in range(3, 11):
            assert row[i] == i - 1
        assert row[12] == 10.11
        assert row[13] == "hello"
        assert row[14] == "stmt"

        conn.execute("drop database if exists %s" % dbname)
        conn.close()

    except Exception as err:
        conn.execute("drop database if exists %s" % dbname)
        conn.close()
        raise err


def test_taos_use_result():
    conn = taos_connect(**cfg)
    sql = "show databases"
    res = taos_query(conn, sql)
    try:
        fields = taos_use_result(res)
        assert isinstance(fields, list)
        print("pass test_taos_use_result")
    except Exception as e:
        print(e)
        raise e
    finally:
        taos_free_result(res)
        taos_close(conn)


@pytest.mark.skipif(IS_WS, reason="Skip WS")
def test_parsing_decimal():
    if not IS_V3:
        print("test_parsing_decimal not support TDengine 2.X version.")
        return

    conn = taos_connect(**cfg)
    execute_sql(conn, "drop database if exists testdec")
    execute_sql(conn, "create database if not exists testdec")
    execute_sql(
        conn,
        "create table if not exists testdec.test(ts timestamp, dec64 decimal(10,6), dec128 decimal(24,10)) tags (note nchar(20))",
    )
    execute_sql(conn, "create table testdec.d0 using testdec.test(note) tags('test')")
    execute_sql(conn, "insert into testdec.d0 values(now(), '9876.123456', '123456789012.0987654321')")
    execute_sql(conn, "insert into testdec.d0 values(now()+1s, '-6789.654321', '-123456789012.0987654321')")

    def parsing_block(block, col_index=0):
        data = ctypes.cast(block, ctypes.POINTER(ctypes.c_void_p))[col_index]
        ptr = ctypes.cast(data, ctypes.POINTER(ctypes.c_char * 64))
        return ptr

    def check_decimal(res, values, block_type=True):
        if block_type:
            block, num_rows = taos_fetch_block_raw(res)
        else:
            block = taos_fetch_row_raw(res)
        ptr = parsing_block(block)
        for i, value in enumerate(values):
            assert cast(ptr[i], c_char_p).value.decode("utf-8") == value

    def check_decimal_block(conn, sql, values):
        res = taos_query(conn, sql)
        check_decimal(res, values)
        taos_free_result(res)

    def check_decimal_row(conn, sql, values):
        res = taos_query(conn, sql)
        for value in values:
            check_decimal(res, [value], False)
        taos_free_result(res)

    check_decimal_block(conn, "select dec64 from testdec.test", ["9876.123456", "-6789.654321"])
    check_decimal_block(
        conn, "select dec128 from testdec.test", ["123456789012.0987654321", "-123456789012.0987654321"]
    )

    check_decimal_row(conn, "select dec64 from testdec.test", ["9876.123456", "-6789.654321"])
    check_decimal_row(conn, "select dec128 from testdec.test", ["123456789012.0987654321", "-123456789012.0987654321"])

    res = taos_query(conn, "select dec64, dec128 from testdec.test")
    fields: TaosFieldEs = taos_fetch_fields_e(res)
    print("count: %d" % fields.count)
    for field in fields:
        print(field)
    row = taos_fetch_row_raw(res)
    print("row: %s" % taos_print_row(row, fields.fields, fields.count))
    taos_free_result(res)

    taos_close(conn)
    print("pass test_parsing_decimal")


############################################ stmt2 begin ############################################


def execute_sql(conn, sql):
    res = taos_query(conn, sql)
    taos_free_result(res)


global_reqid = 0


def test_taos_stmt2_option_default_reqid():
    if not taos.IS_V3:
        return

    import threading

    def worker():
        global global_reqid
        for i in range(5):
            option = taos.TaosStmt2Option()
            with threading.Lock():
                assert option.reqid != global_reqid
                global_reqid = option.reqid

    threads = []
    for i in range(3):
        t = threading.Thread(target=worker)
        threads.append(t)
        t.start()

    for t in threads:
        t.join()

    print("pass test_taos_stmt2_option_default_reqid")


def test_taos_stmt2_option_reqid_wrong_type():
    if not taos.IS_V3:
        return

    try:
        option = taos.TaosStmt2Option(reqid=True)
        assert False
    except StatementError as err:
        pass

    print("pass test_taos_stmt2_option_reqid_wrong_type")


def test_taos_stmt2_init_without_option():
    if not taos.IS_V3:
        return

    conn = taos_connect(**cfg)
    option = None
    stmt2 = taos_stmt2_init(conn, option)
    assert stmt2 is not None
    taos_stmt2_close(stmt2)
    taos_close(conn)
    print("pass test_taos_stmt2_init_without_option")


def test_taos_stmt2_init_with_option():
    if not taos.IS_V3:
        return

    conn = taos_connect(**cfg)
    option = taos.TaosStmt2Option(reqid=1, single_stb_insert=True, single_table_bind_once=False).get_impl()
    stmt2 = taos_stmt2_init(conn, option)
    assert stmt2 is not None
    taos_stmt2_close(stmt2)
    taos_close(conn)
    print("pass test_taos_stmt2_init_with_option")


def test_taos_stmt2_bind_without_prepare():
    if not taos.IS_V3:
        return

    from taos.bind2 import TaosStmt2Bind, new_stmt2_binds, new_bindv

    conn = taos_connect(**cfg)
    option = None
    stmt2 = taos_stmt2_init(conn, option)
    assert stmt2 is not None
    insert_flag = taos_stmt2_is_insert(stmt2)
    assert insert_flag == False

    # prepare data
    tbanmes = ["d1"]
    tags = [["grade1", 1]]
    datas = [
        # class 1
        [
            # student
            [1601481600000, 1601481600001, 1601481600002, 1601481600003, 1601481600004],
            ["Mary", "Tom", "Jack", "Jane", "alex"],
            [0, 1, 1, 0, 1],
            [98, 80, 60, 100, 99],
        ],
    ]

    cnt_tbls = 1
    cnt_tags = 2
    cnt_cols = 4

    # tags
    stmt2_tags = []
    for tag_list in tags:
        n = len(tag_list)
        assert n == cnt_tags
        binds: Array[TaosStmt2Bind] = new_stmt2_binds(n)
        binds[0].binary(tag_list[0])
        binds[1].int(tag_list[1])
        stmt2_tags.append(binds)
    #

    # cols
    stmt2_cols = []
    for data_list in datas:
        n = len(data_list)
        assert n == cnt_cols
        binds: Array[TaosStmt2Bind] = new_stmt2_binds(n)
        binds[0].timestamp(data_list[0])
        binds[1].binary(data_list[1])
        binds[2].bool(data_list[2])
        binds[3].int(data_list[3])
        stmt2_cols.append(binds)
    #

    bindv = new_bindv(cnt_tbls, tbanmes, stmt2_tags, stmt2_cols)

    try:
        taos_stmt2_bind_param(stmt2, bindv.get_address(), -1)
        assert 1 == 2
    except taos.error.StatementError as e:
        pass
    taos_stmt2_close(stmt2)
    taos_close(conn)
    print("pass test_taos_stmt2_bind_without_prepare")


def test_taos_stmt2_insert():
    if not taos.IS_V3:
        return

    from taos.bind2 import TaosStmt2Bind, new_stmt2_binds, new_bindv

    conn = taos_connect(**cfg)
    option = None
    stmt2 = taos_stmt2_init(conn, option)

    dbname = "stmt2"
    stbname = "meters"
    execute_sql(conn, "drop database if exists %s" % dbname)
    execute_sql(conn, "create database if not exists %s" % dbname)
    taos_select_db(conn, dbname)

    sql = f"create table if not exists {stbname}(ts timestamp, name binary(32), sex bool, score int) tags(grade binary(24), class int)"
    execute_sql(conn, sql)

    sql = f"insert into ? using {stbname} tags(?,?) values(?,?,?,?)"
    taos_stmt2_prepare(stmt2, sql)

    # prepare data
    tbanmes = ["d1", "d2", "d3"]
    tags = [["grade1", 1], ["grade1", 2], ["grade1", 3]]
    datas = [
        # class 1
        [
            # student
            [1601481600000, 1601481600001, 1601481600002, 1601481600003, 1601481600004],
            ["Mary", "Tom", "Jack", "Jane", "alex"],
            [0, 1, 1, 0, 1],
            [98, 80, 60, 100, 99],
        ],
        # class 2
        [
            # student
            [1601481600000, 1601481600001, 1601481600002, 1601481600003, 1601481600004],
            ["Mary2", "Tom2", "Jack2", "Jane2", "alex2"],
            [0, 1, 1, 0, 1],
            [298, 280, 260, 2100, 299],
        ],
        # class 3
        [
            # student
            [1601481600000, 1601481600001, 1601481600002, 1601481600003, 1601481600004],
            ["Mary3", "Tom3", "Jack3", "Jane3", "alex3"],
            [0, 1, 1, 0, 1],
            [398, 380, 360, 3100, 399],
        ],
    ]

    cnt_tbls = 3
    cnt_tags = 2
    cnt_cols = 4
    cnt_rows = 5

    # tags
    stmt2_tags = []
    for tag_list in tags:
        n = len(tag_list)
        assert n == cnt_tags
        binds: Array[TaosStmt2Bind] = new_stmt2_binds(n)
        binds[0].binary(tag_list[0])
        binds[1].int(tag_list[1])
        stmt2_tags.append(binds)
    #

    # cols
    stmt2_cols = []
    for data_list in datas:
        n = len(data_list)
        assert n == cnt_cols
        binds: Array[TaosStmt2Bind] = new_stmt2_binds(n)
        binds[0].timestamp(data_list[0])
        binds[1].binary(data_list[1])
        binds[2].bool(data_list[2])
        binds[3].int(data_list[3])
        stmt2_cols.append(binds)
    #

    bindv = new_bindv(cnt_tbls, tbanmes, stmt2_tags, stmt2_cols)

    taos_stmt2_bind_param(stmt2, bindv.get_address(), -1)

    affected_rows = taos_stmt2_exec(stmt2)
    assert affected_rows == cnt_tbls * cnt_rows

    taos_stmt2_close(stmt2)
    taos_close(conn)

    print("pass test_taos_stmt2_insert")


def test_taos_stmt2_get_fields():
    if not taos.IS_V3:
        return

    from taos.bind2 import TaosStmt2Bind, new_stmt2_binds, new_bindv

    conn = taos_connect(**cfg)
    option = None
    stmt2 = taos_stmt2_init(conn, option)

    dbname = "stmt2"
    stbname = "meters"
    execute_sql(conn, "drop database if exists %s" % dbname)
    execute_sql(conn, "create database if not exists %s" % dbname)
    taos_select_db(conn, dbname)

    sql = f"create table if not exists {stbname}(ts timestamp, name binary(32), sex bool, score int) tags(grade binary(24), class int)"
    execute_sql(conn, sql)

    sql = f"insert into ? using {stbname} tags(?,?) values(?,?,?,?)"
    taos_stmt2_prepare(stmt2, sql)

    # prepare data
    tbanmes = ["d1", "d2", "d3"]
    tags = [["grade1", 1], ["grade1", 2], ["grade1", 3]]
    datas = [
        # class 1
        [
            # student
            [1601481600000, 1601481600001, 1601481600002, 1601481600003, 1601481600004],
            ["Mary", "Tom", "Jack", "Jane", "alex"],
            [0, 1, 1, 0, 1],
            [98, 80, 60, 100, 99],
        ],
        # class 2
        [
            # student
            [1601481600000, 1601481600001, 1601481600002, 1601481600003, 1601481600004],
            ["Mary2", "Tom2", "Jack2", "Jane2", "alex2"],
            [0, 1, 1, 0, 1],
            [298, 280, 260, 2100, 299],
        ],
        # class 3
        [
            # student
            [1601481600000, 1601481600001, 1601481600002, 1601481600003, 1601481600004],
            ["Mary3", "Tom3", "Jack3", "Jane3", "alex3"],
            [0, 1, 1, 0, 1],
            [398, 380, 360, 3100, 399],
        ],
    ]

    cnt_tbls = 3
    cnt_tags = 2
    cnt_cols = 4
    cnt_rows = 5

    # tags
    stmt2_tags = []
    for tag_list in tags:
        n = len(tag_list)
        assert n == cnt_tags
        binds: Array[TaosStmt2Bind] = new_stmt2_binds(n)
        binds[0].binary(tag_list[0])
        binds[1].int(tag_list[1])
        stmt2_tags.append(binds)

    # cols
    stmt2_cols = []
    for data_list in datas:
        n = len(data_list)
        assert n == cnt_cols
        binds: Array[TaosStmt2Bind] = new_stmt2_binds(n)
        binds[0].timestamp(data_list[0])
        binds[1].binary(data_list[1])
        binds[2].bool(data_list[2])
        binds[3].int(data_list[3])
        stmt2_cols.append(binds)

    bindv = new_bindv(cnt_tbls, tbanmes, stmt2_tags, stmt2_cols)

    taos_stmt2_bind_param(stmt2, bindv.get_address(), -1)

    check_fields = [
        TaosFieldAllCls("tbname", 8, 0, 0, 271, 4),
        TaosFieldAllCls("grade", 8, 0, 0, 26, 2),
        TaosFieldAllCls("class", 4, 0, 0, 4, 2),
        TaosFieldAllCls("ts", 9, 0, 0, 8, 1),
        TaosFieldAllCls("name", 8, 0, 0, 34, 1),
        TaosFieldAllCls("sex", 1, 0, 0, 1, 1),
        TaosFieldAllCls("score", 4, 0, 0, 4, 1),
    ]
    count, fields = taos_stmt2_get_fields(stmt2)
    print("count: %d, fields: %s" % (count, fields))
    assert count == cnt_tags + cnt_cols + 1
    assert len(fields) == count
    for i in range(count):
        assert fields[i].name == check_fields[i].name
        assert fields[i].type == check_fields[i].type
        assert fields[i].field_type == check_fields[i].field_type

    taos_stmt2_close(stmt2)
    taos_close(conn)

    print("pass test_taos_stmt2_get_fields")


def test_taos_stmt2_query():
    if not taos.IS_V3:
        return

    from taos.bind2 import TaosStmt2Bind, new_stmt2_binds, new_bindv
    from taos.result import TaosResult

    conn = taos_connect(**cfg)
    option = None
    stmt2 = taos_stmt2_init(conn, option)

    dbname = "stmt2"
    stbname = "meters"
    execute_sql(conn, "drop database if exists %s" % dbname)
    execute_sql(conn, "create database if not exists %s" % dbname)
    taos_select_db(conn, dbname)

    sql = f"create table if not exists {stbname}(ts timestamp, name binary(32), sex bool, score int) tags(grade binary(24), class int)"
    execute_sql(conn, sql)

    execute_sql(
        conn, f"insert into d1 using {stbname} tags('grade1', 1) values('2020-10-01 00:00:00.000', 'Mary', false, 98)"
    )
    execute_sql(
        conn, f"insert into d1 using {stbname} tags('grade1', 1) values('2020-10-01 00:00:00.001', 'Tom', true, 80)"
    )
    execute_sql(
        conn, f"insert into d1 using {stbname} tags('grade1', 1) values('2020-10-01 00:00:00.002', 'Jack', true, 60)"
    )
    execute_sql(
        conn, f"insert into d1 using {stbname} tags('grade1', 1) values('2020-10-01 00:00:00.003', 'Jane', false, 100)"
    )
    execute_sql(
        conn, f"insert into d1 using {stbname} tags('grade1', 1) values('2020-10-01 00:00:00.004', 'alex', true, 99)"
    )

    execute_sql(
        conn,
        f"insert into d2 using {stbname} tags('grade1', 2) values('2020-10-01 00:00:00.000', 'Mary2', false, 298)",
    )
    execute_sql(
        conn, f"insert into d2 using {stbname} tags('grade1', 2) values('2020-10-01 00:00:00.001', 'Tom2', true, 280)"
    )
    execute_sql(
        conn, f"insert into d2 using {stbname} tags('grade1', 2) values('2020-10-01 00:00:00.002', 'Jack2', true, 260)"
    )
    execute_sql(
        conn,
        f"insert into d2 using {stbname} tags('grade1', 2) values('2020-10-01 00:00:00.003', 'Jane2', false, 2100)",
    )
    execute_sql(
        conn, f"insert into d2 using {stbname} tags('grade1', 2) values('2020-10-01 00:00:00.004', 'alex2', true, 299)"
    )

    execute_sql(
        conn,
        f"insert into d3 using {stbname} tags('grade1', 3) values('2020-10-01 00:00:00.000', 'Mary3', false, 398)",
    )
    execute_sql(
        conn, f"insert into d3 using {stbname} tags('grade1', 3) values('2020-10-01 00:00:00.001', 'Tom3', true, 380)"
    )
    execute_sql(
        conn, f"insert into d3 using {stbname} tags('grade1', 3) values('2020-10-01 00:00:00.002', 'Jack3', true, 360)"
    )
    execute_sql(
        conn,
        f"insert into d3 using {stbname} tags('grade1', 3) values('2020-10-01 00:00:00.003', 'Jane3', false, 3100)",
    )
    execute_sql(
        conn, f"insert into d3 using {stbname} tags('grade1', 3) values('2020-10-01 00:00:00.004', 'alex3', true, 399)"
    )

    sql = f"select * from {stbname} where name = ? and score = ?"

    taos_stmt2_prepare(stmt2, sql)

    # prepare data
    tbanmes = None
    tags = None
    datas = [
        # class 1
        [
            # student
            ["Mary"],
            [98],
        ]
    ]

    cnt_tbls = 1
    cnt_tags = 0
    cnt_cols = 2

    # tags
    stmt2_tags = None

    # cols
    stmt2_cols = []
    for data_list in datas:
        n = len(data_list)
        assert n == cnt_cols
        binds: Array[TaosStmt2Bind] = new_stmt2_binds(n)
        binds[0].binary(data_list[0])
        binds[1].int(data_list[1])
        stmt2_cols.append(binds)
    #

    bindv = new_bindv(cnt_tbls, tbanmes, stmt2_tags, stmt2_cols)

    taos_stmt2_bind_param(stmt2, bindv.get_address(), -1)

    affected_rows = taos_stmt2_exec(stmt2)
    assert affected_rows == 0

    assert taos_stmt2_is_insert(stmt2) == False

    # TODO: fetch query result
    res = taos_stmt2_result(stmt2)
    rows = TaosResult(res, close_after=False, decode_binary=True)

    row_list = list()
    for row in rows:
        row_list.append(row)
    #
    assert len(row_list) == 1
    row = row_list[0]
    assert row[1] == datas[0][0][0]
    assert row[3] == datas[0][1][0]

    rows.close()
    taos_stmt2_close(stmt2)
    taos_close(conn)

    print("pass test_taos_stmt2_query")


############################################ stmt2 end ############################################

if __name__ == "__main__":
    test_taos_stmt2_get_fields()
