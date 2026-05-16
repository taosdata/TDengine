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
        conn.execute('drop function udfscl')
    except:
        pass
    try:
        conn.execute('drop database udf')
    except:
        pass
    
    conn.execute('create database udf')
    conn.select_db('udf')
    conn.execute("create function udfscl as './py-udf/udfscalar.py' outputtype binary(16384) language 'python'")
    fill_table(conn)
    yield conn

    conn.close()

def test_udfscl_var(conn):    
    q = conn.query('select udfscl(fbin, fnchar) from udf.st1 partition by tbname order by tbname,fts')
    rs = q.fetch_all()
    assert len(rs) == 18
    ri5 = eval(rs[5][0])
    ri11 = eval(rs[11][0])
    ri17 = eval(rs[17][0])
    assert ri5[0] == None
    assert ri5[1] == None
    assert ri5 == ri17
    assert ri5 == ri11
    ri1 = eval(rs[1][0])
    assert ri1[0] == b'b'
    assert ri1[1] == b'b\x00\x00\x00'
    ri2 = eval(rs[2][0])
    assert ri2[0] == b'c'
    assert ri2[1] == b'c\x00\x00\x00'
    for i in range(5):
        r1 = eval(rs[i][0])
        r2 = eval(rs[i+6][0])
        r3 = eval(rs[i+12][0])
        assert r1 == r2
        assert r1 == r3


def test_udfscl_fixed(conn):
    q = conn.query('select udfscl(fbool, ftiny, fsmall, fint, fbig, futiny, fusmall, fuint, fubig, ffloat, fdouble) u\
        ,tbname from udf.st1 partition by tbname order by tbname, fts');
    rs = q.fetch_all()
    assert(len(rs) == 18)
    assert(len(rs[0])==2)
    r0 = eval(rs[0][0])
    r1 = eval(rs[1][0])
    r2 = eval(rs[2][0])
    r5 = eval(rs[5][0])
    assert r0 == [False, 1, 1, 1, 1, 1, 1, 1, 1, 1.0, 1.0]
    assert r1 == [True, 2, 2, 2, 2, 2, 2, 2, 2, 2.0, 2.0]
    assert r2 == [False, 3, 3, 3, 3, 3, 3, 3, 3, 3.0, 3.0]
    assert r5 == [None, None, None, None, None, None, None, None, None, None, None]
    for i in range(6):
        r_tb1 = eval(rs[i][0])
        r_tb2 = eval(rs[i+6][0])
        r_tb3 = eval(rs[i+12][0])
        assert r_tb1 == r_tb2
        assert r_tb2 == r_tb3
        
def test_udfscl_ts(conn):
    q = conn.query('select udfscl(fts, tts, tint, fint) u,tbname from udf.st1 partition by tbname order by tbname, fts')
    rs = q.fetch_all()
    
    assert len(rs) == 18
    
    r5 = eval(rs[5][0])
    r11 = eval(rs[11][0])
    r17 = eval(rs[17][0])
    assert r5 == [1678347480000, 1657441860000, 1, None]
    assert r11 == [1678347540000, 1657441920000, 2, None]
    assert r17 == [1678347600000, 1657441980000, 3, None]        

    
    
