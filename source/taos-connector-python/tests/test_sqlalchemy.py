import taos
import utils
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy import inspect
from sqlalchemy import text
from utils import PORT


load_dotenv()

taos.log.setting(True, True, True, True, True, True)

host = "localhost"


def prepare(conn, dbname, stbname, ntb1, ntb2):
    conn.execute(text("drop database if exists %s" % dbname))
    conn.execute(text("create database if not exists %s precision 'ms' " % dbname))
    # stable
    sql = f"create table if not exists {dbname}.{stbname}(ts timestamp, name binary(32), sex bool, score int, remarks varbinary(500)) tags(grade nchar(8), class int)"
    conn.execute(text(sql))
    # normal table
    sql = f"create table if not exists {dbname}.{ntb1} (ts timestamp, name varbinary(32), sex bool, score float, geo geometry(128), remarks varbinary(500))"
    conn.execute(text(sql))
    sql = f"create table if not exists {dbname}.{ntb2} (ts timestamp, name varbinary(32), sex bool, score float, geo geometry(128), remarks varbinary(500))"
    conn.execute(text(sql))


def test_stmt2_query():
    engine = create_engine(
        f"taos://{utils.test_username()}:{utils.test_password()}@{host}:{PORT}?timezone=Asia/Shanghai"
    )
    conn = engine.connect()
    dbname = "stmt2"
    stbname = "meters"
    ntb1 = "ntb1"
    ntb2 = "ntb2"
    # sql1 = f"select * from {dbname}.d2 where name in (?) or score > ? ;"
    sql1 = f"select * from {dbname}.d2 where score > :minscore and score < :maxscore;"
    try:
        # prepare
        prepare(conn, dbname, stbname, ntb1, ntb2)

        conn.execute(
            text(
                f"insert into {dbname}.d2 using {dbname}.{stbname} tags('grade1', 2) values('2020-10-01 00:00:00.000', 'Mary2', false, 298, 'XXX')"
            )
        )
        conn.execute(
            text(
                f"insert into {dbname}.d2 using {dbname}.{stbname} tags('grade1', 2) values('2020-10-01 00:00:00.001', 'Tom2', true, 280, 'YYY')"
            )
        )
        conn.execute(
            text(
                f"insert into {dbname}.d2 using {dbname}.{stbname} tags('grade1', 2) values('2020-10-01 00:00:00.002', 'Jack2', true, 260, 'ZZZ')"
            )
        )
        conn.execute(
            text(
                f"insert into {dbname}.d2 using {dbname}.{stbname} tags('grade1', 2) values('2020-10-01 00:00:00.003', 'Jane2', false, 2100, 'WWW')"
            )
        )
        conn.execute(
            text(
                f"insert into {dbname}.d2 using {dbname}.{stbname} tags('grade1', 2) values('2020-10-01 00:00:00.004', 'alex2', true, 299, 'ZZZ')"
            )
        )
        conn.execute(
            text(
                f"insert into {dbname}.d2 using {dbname}.{stbname} tags('grade1', 2) values('2020-10-01 00:00:00.005', NULL, false, NULL, 'WWW')"
            )
        )

        result = conn.execute(text(sql1), {"minscore": 280, "maxscore": 1000})
        count = 0
        for row in result:
            count += 1
            assert 280 < row[3] < 1000

        assert count == 2
        print("test_stmt2_query .............................. [passed]\n")

    except Exception as err:
        print("query ......................................... failed\n")
        raise err
    finally:
        conn.close()


def insert_data(conn):
    if conn is None:
        c = taos.connect()
    else:
        c = conn
    c.execute(text("drop database if exists test"))
    c.execute(text("create database if not exists test"))
    c.execute(text("create table test.meters (ts timestamp, c1 int, c2 double) tags(t1 int)"))
    c.execute(
        text("insert into test.d0 using test.meters tags(0) values (1733189403001, 1, 1.11) (1733189403002, 2, 2.22)")
    )
    c.execute(
        text("insert into test.d1 using test.meters tags(1) values (1733189403003, 3, 3.33) (1733189403004, 4, 4.44)")
    )
    c.execute(text("create table test.ntb(ts timestamp, age int)"))
    c.execute(text("insert into test.ntb values(now, 23)"))


def insert_stmt_sqlalchemy_data(conn):
    if conn is None:
        raise (BaseException("conn is null failed."))

    conn.execute(text("drop database if exists test"))
    conn.execute(text("create database if not exists test"))
    conn.execute(text("create table test.meters (ts timestamp, c1 int, c2 double) tags(t1 int)"))

    data = [
        {"ts": 1626861392589, "c1": 1, "c2": 2.0, "t1": 1, "tbname": "tb1"},
        {"ts": 1626861392590, "c1": 2, "c2": 2.5, "t1": 2, "tbname": "tb2"},
        {"ts": 1626861392591, "c1": 3, "c2": 3.0, "t1": 3, "tbname": "tb3"},
    ]

    sql = text("INSERT INTO test.meters (ts, c1, c2, t1, tbname) VALUES (:ts, :c1, :c2, :t1, :tbname)")
    rows = conn.execute(sql, data)
    print(f"inserted data done, rows={rows}")
    result = conn.execute(
        text("select * from test.meters where ts > :start and ts < :end"),
        {"start": 1626861392589, "end": 1626861392598},
    )
    print(f"result: {result}")
    for row in result:
        print(f" result rows = {row} \n")


# compare list
def check_list_equal(list1, list2, tips):
    if list1 != list2:
        print(f"{tips} failed. two list item not equal. list1={list1} list2={list2}")
        raise (BaseException(f"list not euqal. {list1} != {list2}"))


# check result
def check_result_equal(result1, result2, tips):
    if result1 != result2:
        print(f"{tips} failed. result not equal. result1={result1} result2={result2}")
        raise (BaseException("result not euqal."))


# check baisc function
def check_basic(conn, inspection, subTables=["meters", "ntb"]):
    # get schema names
    databases = inspection.get_schema_names()
    if "test" not in databases:
        print(f"test not in {databases}")
        raise (BaseException("get_schema_names failed."))

    # get table names
    tables = subTables
    check_list_equal(inspection.get_table_names("test"), tables, "check get_table_names()")

    # get_columns
    cols2 = [
        {"name": "ts", "type": inspection.dialect._resolve_type("TIMESTAMP")},
        {"name": "c1", "type": inspection.dialect._resolve_type("INT")},
        {"name": "c2", "type": inspection.dialect._resolve_type("DOUBLE")},
        {"name": "t1", "type": inspection.dialect._resolve_type("INT")},
    ]
    cols1 = inspection.get_columns("meters", "test")
    for i in range(len(cols1)):
        cname1 = cols1[i]["name"]
        ctype1 = cols1[i]["type"]
        cname2 = cols2[i]["name"]
        ctype2 = cols2[i]["type"]

        if cname1 != cname2:
            print(f"two name diff, name1={cname1} name2={cname2}")
            raise ("name diff")

        if type(ctype1) != ctype2:
            print(f"two type diff, type1={ctype1} | {type(ctype1)} type2={str(ctype2)} | {type(ctype2)}")
            raise ("type diff")

    # has tabled
    check_result_equal(inspection.has_table("meters", "test"), True, "check has_table()")
    # has_schema
    check_result_equal(inspection.dialect.has_schema(conn, "test"), True, "check has_schema()")

    # get_indexes
    print("inspection.get_indexes", inspection.get_indexes("test.meters"))

    res = conn.execute(text("select * from test.meters"))
    print("res", res.fetchall())

    # import_dbapi
    print("inspection.dialect.import_dbapi", inspection.dialect.import_dbapi())

    # _resolve_type
    print("inspection.dialect._resolve_type", inspection.dialect._resolve_type("int"))

    conn.close()


# taos
def test_read_from_sqlalchemy_taos():
    if not taos.IS_V3:
        return
    engine = create_engine(
        f"taos://{utils.test_username()}:{utils.test_password()}@{host}:{PORT}?timezone=Asia/Shanghai"
    )
    conn = engine.connect()
    insert_data(conn)
    inspection = inspect(engine)
    check_basic(conn, inspection)


# taos
def test_sqlalchemy_format_stmt_taos():
    if not taos.IS_V3:
        return
    engine = create_engine(
        f"taos://{utils.test_username()}:{utils.test_password()}@{host}:{PORT}?timezone=Asia/Shanghai"
    )
    conn = engine.connect()
    insert_stmt_sqlalchemy_data(conn)
    inspection = inspect(engine)
    check_basic(conn, inspection, subTables=["meters"])


def test_read_from_sqlalchemy_taosrest():
    if not taos.IS_V3:
        return
    engine = create_engine(
        f"taosrest://{utils.test_username()}:{utils.test_password()}@{host}:6041?timezone=Asia/Shanghai"
    )
    conn = engine.connect()
    insert_data(conn)
    inspection = inspect(engine)
    check_basic(conn, inspection)


# main test
if __name__ == "__main__":
    print("hello, test sqlalcemy db api. do nothing\n")
    test_read_from_sqlalchemy_taos()
    print("Test taos api ..................................... [OK]\n")
    test_read_from_sqlalchemy_taosrest()
    print("Test taosrest api ................................. [OK]\n")
