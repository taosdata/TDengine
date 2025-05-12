import taos
from taos import utils
from taos import TaosConnection
from taos.cinterface import *
from taos.error import OperationalError, SchemalessError

conn = taos.connect()
dbname = "taos_schemaless_insert"
try:
    conn.execute("drop database if exists %s" % dbname)

    if taos.IS_V3:
        conn.execute("create database if not exists %s schemaless 1 precision 'ns' keep 36500" % dbname)
    else:
        conn.execute("create database if not exists %s update 2 precision 'ns' keep 36500" % dbname)

    conn.select_db(dbname)

    lines = '''st,t1=3i64,t2=4f64,t3="t3" c1=3i64,c3=L"passit",c2=false,c4=4f64 1626006833639000000
    st,t1=4i64,t3="t4",t2=5f64,t4=5f64 c1=3i64,c3=L"passitagin, abc",c2=true,c4=5f64,c5=5f64,c6=7u64 1626006933640000000
    stf,t1=4i64,t3="t4",t2=5f64,t4=5f64 c1=3i64,c3=L"passitagin_stf",c2=false,c5=5f64,c6=7u64 1626006933641000000'''

    res = conn.schemaless_insert_raw(lines, 1, 0)
    print("affected rows: ", res)
    assert (res == 3)

    lines = '''stf,t1=5i64,t3="t4",t2=5f64,t4=5f64 c1=3i64,c3=L"passitagin_stf",c2=false,c5=5f64,c6=7u64 1626006933641000000'''
    res = conn.schemaless_insert_raw(lines, 1, 0)
    print("affected rows: ", res)
    assert (res == 1)

    result = conn.query("select * from st")
    dict2 = result.fetch_all_into_dict()
    print(dict2)
    print(result.row_count)

    all = result.rows_iter()
    for row in all:
        print(row)
    result.close()
    assert (result.row_count == 2)

    # error test
    lines = ''',t1=3i64,t2=4f64,t3="t3" c1=3i64,c3=L"passit",c2=false,c4=4f64 1626006833639000000'''
    try:
        res = conn.schemaless_insert_raw(lines, 1, 0)
        print(res)
        # assert(False)
    except SchemalessError as err:
        print('**** error: ', err)
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
