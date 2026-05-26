import taos
import pytest
import datetime

rows = 3
times = 1;

def fill_table(conn):
    conn.execute(" create stable st1 (fts timestamp, \
                 fbool bool, ftiny tinyint, fsmall smallint, fint int, fbig bigint, futiny tinyint unsigned, fusmall smallint unsigned, fuint int unsigned, fubig bigint unsigned, \
                ffloat float, fdouble double, fbin binary(10), fnchar nchar(10)) \
                tags(tts timestamp, \
                    tbool bool, ttiny tinyint, tsmall smallint, tint int, tbig bigint, tutiny tinyint unsigned, tusmall smallint unsigned, tuint int unsigned, tubig bigint unsigned, \
                        tfloat float, tdouble double, tbin binary(10), tnchar nchar(10))")
    conn.execute(" create table tb1 using st1 tags('2022-07-10 16:31:00', true, 1, 1, 1, 1, 1, 1, 1, 1, 1.0, 1.0, 'a', 'a')")
    conn.execute(" create table tb2 using st1 tags('2022-07-10 16:32:00', false, 2, 2, 2, 2, 2, 2, 2, 2, 2.0, 2.0, 'b', 'b')")
    conn.execute(" create table tb3 using st1 tags('2022-07-10 16:33:00', true, 3, 3, 3, 3, 3, 3, 3, 3, 3.0, 3.0, 'c', 'c')")

    conn.execute(" insert into tb1 values ('2022-07-10 16:31:01', false, 1, 1, 1, 1, 1, 1, 1, 1, 1.0, 1.0, 'a', 'a')")
    conn.execute(" insert into tb1 values ('2022-07-10 16:31:02', true, 2, 2, 2, 2, 2, 2, 2, 2, 2.0, 2.0, 'b', 'b')")
    conn.execute(" insert into tb1 values ('2022-07-10 16:31:03', false, 3, 3, 3, 3, 3, 3, 3, 3, 3.0, 3.0, 'c', 'c')")
    conn.execute(" insert into tb1 values ('2022-07-10 16:31:04', true, 4, 4, 4, 4, 4, 4, 4, 4, 4.0, 4.0, 'd', 'd')")
    conn.execute(" insert into tb1 values ('2022-07-10 16:31:05', false, 5, 5, 5, 5, 5, 5, 5, 5, 5.0, 5.0, 'e', 'e')")
    conn.execute(" insert into tb1(fts) values ('2023-03-09 15:38:00')")

    conn.execute(" insert into tb2 values ('2022-07-10 16:32:01', false, 1, 1, 1, 1, 1, 1, 1, 1, 1.0, 1.0, 'a', 'a')")
    conn.execute(" insert into tb2 values ('2022-07-10 16:32:02', true, 2, 2, 2, 2, 2, 2, 2, 2, 2.0, 2.0, 'b', 'b')")
    conn.execute(" insert into tb2 values ('2022-07-10 16:32:03', false, 3, 3, 3, 3, 3, 3, 3, 3, 3.0, 3.0, 'c', 'c')")
    conn.execute(" insert into tb2 values ('2022-07-10 16:32:04', true, 4, 4, 4, 4, 4, 4, 4, 4, 4.0, 4.0, 'd', 'd')")
    conn.execute(" insert into tb2 values ('2022-07-10 16:32:05', false, 5, 5, 5, 5, 5, 5, 5, 5, 5.0, 5.0, 'e', 'e')")
    conn.execute(" insert into tb2(fts) values ('2023-03-09 15:39:00')")

    conn.execute(" insert into tb3 values ('2022-07-10 16:33:01', false, 1, 1, 1, 1, 1, 1, 1, 1, 1.0, 1.0, 'a', 'a')")
    conn.execute(" insert into tb3 values ('2022-07-10 16:33:02', true, 2, 2, 2, 2, 2, 2, 2, 2, 2.0, 2.0, 'b', 'b')")
    conn.execute(" insert into tb3 values ('2022-07-10 16:33:03', false, 3, 3, 3, 3, 3, 3, 3, 3, 3.0, 3.0, 'c', 'c')")
    conn.execute(" insert into tb3 values ('2022-07-10 16:33:04', true, 4, 4, 4, 4, 4, 4, 4, 4, 4.0, 4.0, 'd', 'd')")
    conn.execute(" insert into tb3 values ('2022-07-10 16:33:05', false, 5, 5, 5, 5, 5, 5, 5, 5, 5.0, 5.0, 'e', 'e')")
    conn.execute(" insert into tb3(fts) values ('2023-03-09 15:40:00')")

@pytest.fixture(scope="module", name="conn")
def setup():
    conn = taos.connect()
    try:
        conn.execute('drop function udfagg')
    except:
        pass
    try:
        conn.execute('drop database udf')
    except:
        pass
    
    conn.execute('create database udf')
    conn.select_db('udf')
    conn.execute("create aggregate function udfagg as './py-udf/udfagg.py' outputtype binary(16384) bufsize 65536 language 'python'")
    fill_table(conn)
    yield conn

    conn.close()

def test_udfagg_var(conn):    
    q = conn.query('select udfagg(fbin, fnchar) from udf.st1 partition by tbname order by tbname')
    rs = q.fetch_all()
    assert len(rs) == 3    
    r0 = eval(rs[0][0])
    assert r0 ==  [[b'a', b'a\x00\x00\x00'], [b'b', b'b\x00\x00\x00'], [b'c', b'c\x00\x00\x00'], [b'd', b'd\x00\x00\x00'], [b'e', b'e\x00\x00\x00'], [None, None]]
    r1 = eval(rs[1][0])
    assert r1 ==  [[b'a', b'a\x00\x00\x00'], [b'b', b'b\x00\x00\x00'], [b'c', b'c\x00\x00\x00'], [b'd', b'd\x00\x00\x00'], [b'e', b'e\x00\x00\x00'], [None, None]]
    r2 = eval(rs[2][0])
    assert r2 ==  [[b'a', b'a\x00\x00\x00'], [b'b', b'b\x00\x00\x00'], [b'c', b'c\x00\x00\x00'], [b'd', b'd\x00\x00\x00'], [b'e', b'e\x00\x00\x00'], [None, None]]
    
def test_udfagg_fixed(conn):
    q = conn.query('select udfagg(fbool, ftiny, fsmall, fint, fbig, futiny, fusmall, fuint, fubig, ffloat, fdouble) from udf.st1')
    rs = q.fetch_all()
    assert len(rs) == 1
    r0 = eval(rs[0][0])
    assert r0 ==  [[False, 1, 1, 1, 1, 1, 1, 1, 1, 1.0, 1.0], [True, 2, 2, 2, 2, 2, 2, 2, 2, 2.0, 2.0], [False, 3, 3, 3, 3, 3, 3, 3, 3, 3.0, 3.0], [True, 4, 4, 4, 4, 4, 4, 4, 4, 4.0, 4.0], [False, 5, 5, 5, 5, 5, 5, 5, 5, 5.0, 5.0], [None, None, None, None, None, None, None, None, None, None, None], [False, 1, 1, 1, 1, 1, 1, 1, 1, 1.0, 1.0], [True, 2, 2, 2, 2, 2, 2, 2, 2, 2.0, 2.0], [False, 3, 3, 3, 3, 3, 3, 3, 3, 3.0, 3.0], [True, 4, 4, 4, 4, 4, 4, 4, 4, 4.0, 4.0], [False, 5, 5, 5, 5, 5, 5, 5, 5, 5.0, 5.0], [None, None, None, None, None, None, None, None, None, None, None], [False, 1, 1, 1, 1, 1, 1, 1, 1, 1.0, 1.0], [True, 2, 2, 2, 2, 2, 2, 2, 2, 2.0, 2.0], [False, 3, 3, 3, 3, 3, 3, 3, 3, 3.0, 3.0], [True, 4, 4, 4, 4, 4, 4, 4, 4, 4.0, 4.0], [False, 5, 5, 5, 5, 5, 5, 5, 5, 5.0, 5.0], [None, None, None, None, None, None, None, None, None, None, None]]    

def test_udfscl_ts(conn):
    q = conn.query('select udfagg(fts, tts, tint, fint) u from udf.st1 partition by tbname order by tbname')
    rs = q.fetch_all()
    
    assert len(rs) == 3
    r0 = eval(rs[0][0])
    r1 = eval(rs[1][0])
    r2 = eval(rs[2][0])
    
    assert r0 ==  [[1657441861000, 1657441860000, 1, 1], [1657441862000, 1657441860000, 1, 2], [1657441863000, 1657441860000, 1, 3], [1657441864000, 1657441860000, 1, 4], [1657441865000, 1657441860000, 1, 5], [1678347480000, 1657441860000, 1, None]]
    assert r1 ==  [[1657441921000, 1657441920000, 2, 1], [1657441922000, 1657441920000, 2, 2], [1657441923000, 1657441920000, 2, 3], [1657441924000, 1657441920000, 2, 4], [1657441925000, 1657441920000, 2, 5], [1678347540000, 1657441920000, 2, None]] 
