import taos
import pytest
from ctypes import *
from taos import *
from taos.cinterface import *
from taos.error import SchemalessError
from utils import IS_WS


@pytest.fixture
def conn():
    return connect()


@pytest.mark.skipif(IS_WS, reason="Skip WS")
def test_schemaless_insert_update_2(conn):
    dbname = "test_schemaless_insert_update_2"
    try:
        conn.execute("drop database if exists %s" % dbname)
        if taos.IS_V3:
            conn.execute("create database if not exists %s schemaless 1 precision 'ns'" % dbname)
        else:
            conn.execute("create database if not exists %s update 2 precision 'ns'" % dbname)
        conn.select_db(dbname)

        lines = [
            'st,t1=4i64,t3="t4",t2=5f64,t4=5f64 c1=3i64,c3=L"passitagin, abc",c2=true,c4=5f64,c5=5f64,c6=7u64 1626006933640000000',
        ]
        res = conn.schemaless_insert(lines, 1, 0)
        print("affected rows: ", res)
        assert res == 1

        result = conn.query("select * from st")
        [before] = result.fetch_all_into_dict()
        assert before["c3"] == "passitagin, abc"

        lines = [
            'st,t1=4i64,t3="t4",t2=5f64,t4=5f64 c1=3i64,c3=L"passitagin",c2=true,c4=5f64,c5=5f64,c6=7u64 1626006933640000000',
        ]
        res = conn.schemaless_insert(lines, 1, 0)
        result = conn.query("select * from st")
        [after] = result.fetch_all_into_dict()
        assert after["c3"] == "passitagin"

        conn.execute("drop database if exists %s" % dbname)
        conn.close()
    except InterfaceError as err:
        conn.execute("drop database if exists %s" % dbname)
        conn.close()
        print(err)
    except Exception as err:
        conn.execute("drop database if exists %s" % dbname)
        conn.close()
        print(err)
        raise err


def test_schemaless_insert(conn):
    dbname = "pytest_taos_schemaless_insert"
    try:
        conn.execute("drop database if exists %s" % dbname)
        if taos.IS_V3:
            conn.execute("create database if not exists %s schemaless 1 precision 'ns'" % dbname)
        else:
            conn.execute("create database if not exists %s update 2 precision 'ns'" % dbname)

        conn.select_db(dbname)

        lines = [
            'st,t1=3i64,t2=4f64,t3="t3" c1=3i64,c3=L"passit",c2=false,c4=4f64 1626006833639000000',
            'st,t1=4i64,t3="t4",t2=5f64,t4=5f64 c1=3i64,c3=L"passitagin, abc",c2=true,c4=5f64,c5=5f64,c6=7u64 1626006933640000000',
            'stf,t1=4i64,t3="t4",t2=5f64,t4=5f64 c1=3i64,c3=L"passitagin_stf",c2=false,c5=5f64,c6=7u64 1626006933641000000',
        ]
        res = conn.schemaless_insert(lines, 1, 0)
        print("affected rows: ", res)
        assert res == 3

        lines = [
            'stf,t1=5i64,t3="t4",t2=5f64,t4=5f64 c1=3i64,c3=L"passitagin_stf",c2=false,c5=5f64,c6=7u64 1626006933641000000',
        ]
        res = conn.schemaless_insert(lines, 1, 0)
        print("affected rows: ", res)
        assert res == 1
        result = conn.query("select * from st")

        dict2 = result.fetch_all_into_dict()
        print(dict2)
        print(result.row_count)
        all = result.rows_iter()
        for row in all:
            print(row)
        result.close()
        assert result.row_count == 2

        # error test
        lines = [
            ',t1=3i64,t2=4f64,t3="t3" c1=3i64,c3=L"passit",c2=false,c4=4f64 1626006833639000000',
        ]
        try:
            res = conn.schemaless_insert(lines, 1, 0)
            print(res)
            # assert(False)
        except SchemalessError as err:
            print("**** error: ", err)
            # assert (err.msg == 'Invalid data format')

        result = conn.query("select * from st")
        print(result.row_count)
        all = result.rows_iter()
        for row in all:
            print(row)
        result.close()

        conn.execute("drop database if exists %s" % dbname)
        conn.close()
    except InterfaceError as err:
        conn.execute("drop database if exists %s" % dbname)
        conn.close()
        print(err)
    except Exception as err:
        conn.execute("drop database if exists %s" % dbname)
        conn.close()
        print(err)
        raise err


def test_schemaless_insert_ttl(conn: TaosConnection) -> None:
    dbname = "pytest_taos_schemaless_insert"
    try:
        conn.execute("drop database if exists %s" % dbname)
        if taos.IS_V3:
            conn.execute("create database if not exists %s schemaless 1 precision 'ns'" % dbname)
        else:
            conn.execute("create database if not exists %s update 2 precision 'ns'" % dbname)

        conn.select_db(dbname)

        lines = [
            'st,t1=3i64,t2=4f64,t3="t3" c1=3i64,c3=L"passit",c2=false,c4=4f64 1626006833639000000',
            'st,t1=4i64,t3="t4",t2=5f64,t4=5f64 c1=3i64,c3=L"passitagin, abc",c2=true,c4=5f64,c5=5f64,c6=7u64 1626006933640000000',
            'stf,t1=4i64,t3="t4",t2=5f64,t4=5f64 c1=3i64,c3=L"passitagin_stf",c2=false,c5=5f64,c6=7u64 1626006933641000000',
        ]
        ttl = 1000
        res = conn.schemaless_insert(lines, 1, 0, ttl=ttl)
        print("affected rows: ", res)
        assert res == 3

        lines = [
            'stf,t1=5i64,t3="t4",t2=5f64,t4=5f64 c1=3i64,c3=L"passitagin_stf",c2=false,c5=5f64,c6=7u64 1626006933641000000',
        ]
        ttl = 1000
        res = conn.schemaless_insert(lines, 1, 0, ttl=ttl)
        print("affected rows: ", res)
        assert res == 1
        result = conn.query("select * from st")

        dict2 = result.fetch_all_into_dict()
        print(dict2)
        print(result.row_count)
        all = result.rows_iter()
        for row in all:
            print(row)
        result.close()
        assert result.row_count == 2

        # error test
        lines = [
            ',t1=3i64,t2=4f64,t3="t3" c1=3i64,c3=L"passit",c2=false,c4=4f64 1626006833639000000',
        ]
        try:
            ttl = 1000
            res = conn.schemaless_insert(lines, 1, 0, ttl=ttl)
            print(res)
            # assert(False)
        except SchemalessError as err:
            print("**** error: ", err)
            # assert (err.msg == 'Invalid data format')

        result = conn.query("select * from st")
        print(result.row_count)
        all = result.rows_iter()
        for row in all:
            print(row)
        result.close()

        conn.execute("drop database if exists %s" % dbname)
        conn.close()
    except InterfaceError as err:
        conn.execute("drop database if exists %s" % dbname)
        conn.close()
        print(err)
    except Exception as err:
        conn.execute("drop database if exists %s" % dbname)
        conn.close()
        print(err)
        raise err


def test_schemaless_insert_ttl_with_req_id(conn: TaosConnection) -> None:
    dbname = "pytest_taos_schemaless_insert"
    try:
        conn.execute("drop database if exists %s" % dbname)
        if taos.IS_V3:
            conn.execute("create database if not exists %s schemaless 1 precision 'ns'" % dbname)
        else:
            conn.execute("create database if not exists %s update 2 precision 'ns'" % dbname)

        conn.select_db(dbname)

        lines = [
            'st,t1=3i64,t2=4f64,t3="t3" c1=3i64,c3=L"passit",c2=false,c4=4f64 1626006833639000000',
            'st,t1=4i64,t3="t4",t2=5f64,t4=5f64 c1=3i64,c3=L"passitagin, abc",c2=true,c4=5f64,c5=5f64,c6=7u64 1626006933640000000',
            'stf,t1=4i64,t3="t4",t2=5f64,t4=5f64 c1=3i64,c3=L"passitagin_stf",c2=false,c5=5f64,c6=7u64 1626006933641000000',
        ]
        ttl = 1000
        req_id = utils.gen_req_id()
        res = conn.schemaless_insert(lines, 1, 0, ttl=ttl, req_id=req_id)
        print("affected rows: ", res)
        assert res == 3

        lines = [
            'stf,t1=5i64,t3="t4",t2=5f64,t4=5f64 c1=3i64,c3=L"passitagin_stf",c2=false,c5=5f64,c6=7u64 1626006933641000000',
        ]
        ttl = 1000
        req_id = utils.gen_req_id()
        res = conn.schemaless_insert(lines, 1, 0, ttl=ttl, req_id=req_id)
        print("affected rows: ", res)
        assert res == 1
        result = conn.query("select * from st")

        dict2 = result.fetch_all_into_dict()
        print(dict2)
        print(result.row_count)
        all = result.rows_iter()
        for row in all:
            print(row)
        result.close()
        assert result.row_count == 2

        # error test
        lines = [
            ',t1=3i64,t2=4f64,t3="t3" c1=3i64,c3=L"passit",c2=false,c4=4f64 1626006833639000000',
        ]
        try:
            ttl = 1000
            req_id = utils.gen_req_id()
            res = conn.schemaless_insert(lines, 1, 0, ttl=ttl, req_id=req_id)
            print(res)
            # assert(False)
        except SchemalessError as err:
            print("**** error: ", err)
            # assert (err.msg == 'Invalid data format')

        result = conn.query("select * from st")
        print(result.row_count)
        all = result.rows_iter()
        for row in all:
            print(row)
        result.close()

        conn.execute("drop database if exists %s" % dbname)
        conn.close()
    except InterfaceError as err:
        conn.execute("drop database if exists %s" % dbname)
        conn.close()
        print(err)
    except Exception as err:
        conn.execute("drop database if exists %s" % dbname)
        conn.close()
        print(err)
        raise err


def test_schemaless_insert_raw(conn: TaosConnection) -> None:
    dbname = "pytest_taos_schemaless_insert"
    try:
        conn.execute("drop database if exists %s" % dbname)

        if taos.IS_V3:
            conn.execute("create database if not exists %s schemaless 1 precision 'ns'" % dbname)
        else:
            conn.execute("create database if not exists %s update 2 precision 'ns'" % dbname)

        conn.select_db(dbname)

        lines = """st,t1=3i64,t2=4f64,t3="t3" c1=3i64,c3=L"passit",c2=false,c4=4f64 1626006833639000000
        st,t1=4i64,t3="t4",t2=5f64,t4=5f64 c1=3i64,c3=L"passitagin, abc",c2=true,c4=5f64,c5=5f64,c6=7u64 1626006933640000000
        stf,t1=4i64,t3="t4",t2=5f64,t4=5f64 c1=3i64,c3=L"passitagin_stf",c2=false,c5=5f64,c6=7u64 1626006933641000000"""

        res = conn.schemaless_insert_raw(lines, 1, 0)
        print("affected rows: ", res)
        assert res == 3

        lines = """stf,t1=5i64,t3="t4",t2=5f64,t4=5f64 c1=3i64,c3=L"passitagin_stf",c2=false,c5=5f64,c6=7u64 1626006933641000000"""
        res = conn.schemaless_insert_raw(lines, 1, 0)
        print("affected rows: ", res)
        assert res == 1

        result = conn.query("select * from st")
        dict2 = result.fetch_all_into_dict()
        print(dict2)
        print(result.row_count)

        all = result.rows_iter()
        for row in all:
            print(row)
        result.close()
        assert result.row_count == 2

        # error test
        lines = """,t1=3i64,t2=4f64,t3="t3" c1=3i64,c3=L"passit",c2=false,c4=4f64 1626006833639000000"""
        try:
            res = conn.schemaless_insert_raw(lines, 1, 0)
            print(res)
            # assert(False)
        except SchemalessError as err:
            print("**** error: ", err)
            # assert (err.msg == 'Invalid data format')

        result = conn.query("select * from st")
        print(result.row_count)
        all = result.rows_iter()
        for row in all:
            print(row)
        result.close()

        conn.execute("drop database if exists %s" % dbname)
        conn.close()
    except InterfaceError as err:
        conn.execute("drop database if exists %s" % dbname)
        conn.close()
        print(err)
    except SchemalessError as err:
        conn.execute("drop database if exists %s" % dbname)
        conn.close()
        print(err)
    except Exception as err:
        conn.execute("drop database if exists %s" % dbname)
        conn.close()
        print(err)
        raise err


def test_schemaless_insert_raw_with_req_id(conn: TaosConnection) -> None:
    dbname = "pytest_taos_schemaless_insert"
    try:
        conn.execute("drop database if exists %s" % dbname)

        if taos.IS_V3:
            conn.execute("create database if not exists %s schemaless 1 precision 'ns'" % dbname)
        else:
            conn.execute("create database if not exists %s update 2 precision 'ns'" % dbname)

        conn.select_db(dbname)

        lines = """st,t1=3i64,t2=4f64,t3="t3" c1=3i64,c3=L"passit",c2=false,c4=4f64 1626006833639000000
        st,t1=4i64,t3="t4",t2=5f64,t4=5f64 c1=3i64,c3=L"passitagin, abc",c2=true,c4=5f64,c5=5f64,c6=7u64 1626006933640000000
        stf,t1=4i64,t3="t4",t2=5f64,t4=5f64 c1=3i64,c3=L"passitagin_stf",c2=false,c5=5f64,c6=7u64 1626006933641000000"""

        req_id = utils.gen_req_id()
        res = conn.schemaless_insert_raw(lines, 1, 0, req_id=req_id)
        print("affected rows: ", res)
        assert res == 3

        lines = """stf,t1=5i64,t3="t4",t2=5f64,t4=5f64 c1=3i64,c3=L"passitagin_stf",c2=false,c5=5f64,c6=7u64 1626006933641000000"""
        req_id = utils.gen_req_id()
        res = conn.schemaless_insert_raw(lines, 1, 0, req_id=req_id)
        print("affected rows: ", res)
        assert res == 1

        result = conn.query("select * from st")
        dict2 = result.fetch_all_into_dict()
        print(dict2)
        print(result.row_count)

        all = result.rows_iter()
        for row in all:
            print(row)
        result.close()
        assert result.row_count == 2

        # error test
        lines = """,t1=3i64,t2=4f64,t3="t3" c1=3i64,c3=L"passit",c2=false,c4=4f64 1626006833639000000"""
        try:
            req_id = utils.gen_req_id()
            res = conn.schemaless_insert_raw(lines, 1, 0, req_id=req_id)
            print(res)
            # assert(False)
        except SchemalessError as err:
            print("**** error: ", err)
            # assert (err.msg == 'Invalid data format')

        result = conn.query("select * from st")
        print(result.row_count)
        all = result.rows_iter()
        for row in all:
            print(row)
        result.close()

        conn.execute("drop database if exists %s" % dbname)
        conn.close()
    except InterfaceError as err:
        conn.execute("drop database if exists %s" % dbname)
        conn.close()
        print(err)
    except Exception as err:
        conn.execute("drop database if exists %s" % dbname)
        conn.close()
        print(err)
        raise err


def test_schemaless_insert_raw_ttl(conn: TaosConnection) -> None:
    dbname = "pytest_taos_schemaless_insert"
    try:
        conn.execute("drop database if exists %s" % dbname)

        if taos.IS_V3:
            conn.execute("create database if not exists %s schemaless 1 precision 'ns'" % dbname)
        else:
            conn.execute("create database if not exists %s update 2 precision 'ns'" % dbname)

        conn.select_db(dbname)

        lines = """st,t1=3i64,t2=4f64,t3="t3" c1=3i64,c3=L"passit",c2=false,c4=4f64 1626006833639000000
        st,t1=4i64,t3="t4",t2=5f64,t4=5f64 c1=3i64,c3=L"passitagin, abc",c2=true,c4=5f64,c5=5f64,c6=7u64 1626006933640000000
        stf,t1=4i64,t3="t4",t2=5f64,t4=5f64 c1=3i64,c3=L"passitagin_stf",c2=false,c5=5f64,c6=7u64 1626006933641000000"""

        ttl = 1000
        res = conn.schemaless_insert_raw(lines, 1, 0, ttl=ttl)
        print("affected rows: ", res)
        assert res == 3

        lines = """stf,t1=5i64,t3="t4",t2=5f64,t4=5f64 c1=3i64,c3=L"passitagin_stf",c2=false,c5=5f64,c6=7u64 1626006933641000000"""
        ttl = 1000
        res = conn.schemaless_insert_raw(lines, 1, 0, ttl=ttl)
        print("affected rows: ", res)
        assert res == 1

        result = conn.query("select * from st")
        dict2 = result.fetch_all_into_dict()
        print(dict2)
        print(result.row_count)

        all = result.rows_iter()
        for row in all:
            print(row)
        result.close()
        assert result.row_count == 2

        # error test
        lines = """,t1=3i64,t2=4f64,t3="t3" c1=3i64,c3=L"passit",c2=false,c4=4f64 1626006833639000000"""
        try:
            ttl = 1000
            res = conn.schemaless_insert_raw(lines, 1, 0, ttl=ttl)
            print(res)
            # assert(False)
        except SchemalessError as err:
            print("**** error: ", err)
            # assert (err.msg == 'Invalid data format')

        result = conn.query("select * from st")
        print(result.row_count)
        all = result.rows_iter()
        for row in all:
            print(row)
        result.close()

        conn.execute("drop database if exists %s" % dbname)
        conn.close()
    except InterfaceError as err:
        conn.execute("drop database if exists %s" % dbname)
        conn.close()
        print(err)
    except Exception as err:
        conn.execute("drop database if exists %s" % dbname)
        conn.close()
        print(err)
        raise err


def test_schemaless_insert_raw_ttl_with_req_id(conn: TaosConnection) -> None:
    dbname = "pytest_taos_schemaless_insert"
    try:
        conn.execute("drop database if exists %s" % dbname)

        if taos.IS_V3:
            conn.execute("create database if not exists %s schemaless 1 precision 'ns'" % dbname)
        else:
            conn.execute("create database if not exists %s update 2 precision 'ns'" % dbname)

        conn.select_db(dbname)

        lines = """st,t1=3i64,t2=4f64,t3="t3" c1=3i64,c3=L"passit",c2=false,c4=4f64 1626006833639000000
        st,t1=4i64,t3="t4",t2=5f64,t4=5f64 c1=3i64,c3=L"passitagin, abc",c2=true,c4=5f64,c5=5f64,c6=7u64 1626006933640000000
        stf,t1=4i64,t3="t4",t2=5f64,t4=5f64 c1=3i64,c3=L"passitagin_stf",c2=false,c5=5f64,c6=7u64 1626006933641000000"""

        ttl = 1000
        req_id = utils.gen_req_id()
        res = conn.schemaless_insert_raw(lines, 1, 0, ttl=ttl, req_id=req_id)
        print("affected rows: ", res)
        assert res == 3

        lines = """stf,t1=5i64,t3="t4",t2=5f64,t4=5f64 c1=3i64,c3=L"passitagin_stf",c2=false,c5=5f64,c6=7u64 1626006933641000000"""
        ttl = 1000
        req_id = utils.gen_req_id()
        res = conn.schemaless_insert_raw(lines, 1, 0, ttl=ttl, req_id=req_id)
        print("affected rows: ", res)
        assert res == 1

        result = conn.query("select * from st")
        dict2 = result.fetch_all_into_dict()
        print(dict2)
        print(result.row_count)

        all = result.rows_iter()
        for row in all:
            print(row)
        result.close()
        assert result.row_count == 2

        # error test
        lines = """,t1=3i64,t2=4f64,t3="t3" c1=3i64,c3=L"passit",c2=false,c4=4f64 1626006833639000000"""
        try:
            ttl = 1000
            req_id = utils.gen_req_id()
            res = conn.schemaless_insert_raw(lines, 1, 0, ttl=ttl, req_id=req_id)
            print(res)
            # assert(False)
        except SchemalessError as err:
            print("**** error: ", err)
            # assert (err.msg == 'Invalid data format')

        result = conn.query("select * from st")
        print(result.row_count)
        all = result.rows_iter()
        for row in all:
            print(row)
        result.close()

        conn.execute("drop database if exists %s" % dbname)
        conn.close()
    except InterfaceError as err:
        conn.execute("drop database if exists %s" % dbname)
        conn.close()
        print(err)
    except Exception as err:
        conn.execute("drop database if exists %s" % dbname)
        conn.close()
        print(err)
        raise err


def test_schemaless_insert_with_req_id(conn):
    # type: (TaosConnection) -> None

    dbname = "pytest_taos_schemaless_insert"
    try:
        conn.execute("drop database if exists %s" % dbname)
        if taos.IS_V3:
            conn.execute("create database if not exists %s schemaless 1 precision 'ns'" % dbname)
        else:
            conn.execute("create database if not exists %s update 2 precision 'ns'" % dbname)

        conn.select_db(dbname)

        lines = [
            'st,t1=3i64,t2=4f64,t3="t3" c1=3i64,c3=L"passit",c2=false,c4=4f64 1626006833639000000',
            'st,t1=4i64,t3="t4",t2=5f64,t4=5f64 c1=3i64,c3=L"passitagin, abc",c2=true,c4=5f64,c5=5f64,c6=7u64 1626006933640000000',
            'stf,t1=4i64,t3="t4",t2=5f64,t4=5f64 c1=3i64,c3=L"passitagin_stf",c2=false,c5=5f64,c6=7u64 1626006933641000000',
        ]
        req_id = utils.gen_req_id()
        res = conn.schemaless_insert(lines, 1, 0, req_id)
        print("affected rows: ", res)
        assert res == 3

        lines = [
            'stf,t1=5i64,t3="t4",t2=5f64,t4=5f64 c1=3i64,c3=L"passitagin_stf",c2=false,c5=5f64,c6=7u64 1626006933641000000',
        ]
        req_id = utils.gen_req_id()
        res = conn.schemaless_insert(lines, 1, 0, req_id)
        print("affected rows: ", res)
        assert res == 1
        result = conn.query("select * from st")

        dict2 = result.fetch_all_into_dict()
        print(dict2)
        result.row_count
        all = result.rows_iter()
        for row in all:
            print(row)
        result.close()
        assert result.row_count == 2

        # error test
        lines = [
            ',t1=3i64,t2=4f64,t3="t3" c1=3i64,c3=L"passit",c2=false,c4=4f64 1626006833639000000',
        ]
        try:
            req_id = utils.gen_req_id()
            res = conn.schemaless_insert(lines, 1, 0, req_id)
            print(res)
            # assert(False)
        except SchemalessError as e:
            print(e)

        req_id = utils.gen_req_id()
        result = conn.query("select * from st", req_id)
        all = result.rows_iter()
        for row in all:
            print(row)
        result.close()

        conn.execute("drop database if exists %s" % dbname)
        conn.close()
    except InterfaceError as err:
        conn.execute("drop database if exists %s" % dbname)
        conn.close()
        print(err)
    except Exception as err:
        conn.execute("drop database if exists %s" % dbname)
        conn.close()
        print(err)
        raise err


if __name__ == "__main__":
    test_schemaless_insert(connect())
    test_schemaless_insert_update_2(connect())
