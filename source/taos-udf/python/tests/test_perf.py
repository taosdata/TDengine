import taos
import pytest
import datetime
import os

rows = 1024
times = 1024;

def exec_ignore_except(conn, sql):
    try:
        conn.execute(sql)
    except:
        pass

@pytest.fixture(scope="module", name="conn")
def setup():
    conn = taos.connect()
    exec_ignore_except(conn, 'drop function udf1')
    exec_ignore_except(conn, 'drop function udf2')
    exec_ignore_except(conn, 'drop function bit_and')
    exec_ignore_except(conn, 'drop function pybitand')
    exec_ignore_except(conn, 'drop function l2norm')
    exec_ignore_except(conn, 'drop function pyl2norm')
    exec_ignore_except(conn, 'drop database udf')
    try:
        conn.execute('drop function udf1')
    except:
        pass
    try:
        conn.execute('drop function udf2')
    except:
        pass
    try:
        conn.execute('drop database udf')
    except:
        pass
    conn.execute('create database udf')
    conn.select_db('udf')
    conn.execute('create table t(ts timestamp, i int)')
    sql = 'insert into t values'
    for i in range(rows):
        sql = sql + '(now + {}a, {})'.format(i, i)
    conn.execute(sql)
    conn.execute("create function udf1 as './py-udf/udf1.py' outputtype int language 'python'")
    conn.execute("create aggregate function udf2 as './py-udf/udf2.py' outputtype int bufSize 128 language 'python'")
    os.system("./c-udf/compile_udf.sh")
    conn.execute("create function bit_and as './c-udf/libbitand.so' outputtype int");
    conn.execute("create aggregate function l2norm as './c-udf/libl2norm.so' outputtype double bufSize 8")
    conn.execute("create function pybitand as './py-udf/pybitand.py' outputtype int language 'python'")
    conn.execute("create aggregate function pyl2norm as './py-udf/pyl2norm.py' outputtype double bufSize 128 language 'python'")
    yield conn

    conn.close()

def test_scalar_perf(conn):
    t1 = datetime.datetime.now()
    for i in range(times):
        q = conn.query('select udf1(i) from udf.t')
        r = q.fetch_all()
    t2 = datetime.datetime.now()
    print('scalar 1M rows time:', t2-t1)
    assert len(r) == rows
    assert len(r[0]) == 1
    for i in range(rows):
        assert r[i][0] == i

def test_agg_perf(conn): 
    t3 = datetime.datetime.now()
    for i in range(times):
        q2 = conn.query('select udf2(i) from udf.t')
        r2 = q2.fetch_all()
    t4 = datetime.datetime.now()
    print('agg 1M rows time:', t4-t3)
    assert len(r2) == 1
    assert len(r2[0]) == 1

def test_c_bitand_perf(conn):
    t1 = datetime.datetime.now()
    for i in range(times):
        q = conn.query('select bit_and(i,i) from udf.t')
        r = q.fetch_all()
    t2 = datetime.datetime.now();
    print('c scalar bit_and 1M rows', t2-t1);

def test_py_bitand_perf(conn):
    t1 = datetime.datetime.now()
    for i in range(times):
        q = conn.query('select pybitand(i,i) from udf.t')
        r = q.fetch_all()
    t2 = datetime.datetime.now();
    print('py scalar bit_and 1M rows', t2-t1);


def test_c_l2norm_perf(conn):
    t1 = datetime.datetime.now()
    for i in range(times):
        q = conn.query('select l2norm(i) from udf.t')
        r = q.fetch_all()
    t2 = datetime.datetime.now();
    print('c scalar bit_and 1M rows', t2-t1);

def test_py_l2norm_perf(conn):
    t1 = datetime.datetime.now()
    for i in range(times):
        q = conn.query('select pyl2norm(i) from udf.t')
        r = q.fetch_all()
    t2 = datetime.datetime.now();
    print('py scalar bit_and 1M rows', t2-t1);


