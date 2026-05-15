import taos
import pytest

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
        conn.execute('drop function udf1int')
    except:
        pass
    try:
        conn.execute('drop function udf1var')
    except:
        pass
    try:
        conn.execute('drop function udf1nchar')
    except:
        pass        
    try:
        conn.execute('drop function udf1double')
    except:
        pass
    try:
        conn.execute('drop function udf2double')
    except:
        pass
    try:
        conn.execute('drop function udf2float')
    except:
        pass    
    try:
        conn.execute('drop database udf')
    except:
        pass
    conn.execute('create database udf')
    conn.select_db('udf')
    conn.execute("create function udf1int as './py-udf/udf1.py' outputtype int language 'python'")
    conn.execute("create function udf1var as './py-udf/udf1.py' outputtype binary(10) language 'python'" )
    conn.execute("create function udf1nchar as './py-udf/udf1.py' outputtype nchar(10) language 'python'" )
    conn.execute("create function udf1double as './py-udf/udf1.py' outputtype double language 'python'")
    conn.execute("create aggregate function udf2double as './py-udf/udf2.py' outputtype double bufsize 128 language 'python'")
    conn.execute("create aggregate function udf2float as './py-udf/udf2.py' outputtype float bufsize 128 language 'python'")

    fill_table(conn)
    yield conn

    conn.close()

def test_scalar_int(conn):
    q = conn.query('select udf1int(fint) from udf.st1 order by tbname, fts')
    rs = q.fetch_all()
    assert len(rs) == 18
    assert rs ==  [(1,), (2,), (3,), (4,), (5,), (None,), (1,), (2,), (3,), (4,), (5,), (None,), (1,), (2,), (3,), (4,), (5,), (None,)]


def test_scalar_var(conn):    
    q = conn.query('select udf1var(fbin) from udf.st1 order by tbname, fts')
    rs = q.fetch_all()
    assert len(rs) == 18
    assert rs ==  [('a',), ('b',), ('c',), ('d',), ('e',), (None,), ('a',), ('b',), ('c',), ('d',), ('e',), (None,), ('a',), ('b',), ('c',), ('d',), ('e',), (None,)]    

def test_scalar_nchar(conn):    
    q = conn.query('select udf1nchar(fnchar) from udf.st1 order by tbname, fts')
    rs = q.fetch_all()
    assert len(rs) == 18
    assert rs ==  [('a',), ('b',), ('c',), ('d',), ('e',), (None,), ('a',), ('b',), ('c',), ('d',), ('e',), (None,), ('a',), ('b',), ('c',), ('d',), ('e',), (None,)]

def test_scalar_double(conn):    
    q = conn.query('select udf1double(fdouble) from udf.st1 order by tbname,fts ')
    rs = q.fetch_all()
    assert len(rs) == 18
    assert rs ==  [(1.0,), (2.0,), (3.0,), (4.0,), (5.0,), (None,), (1.0,), (2.0,), (3.0,), (4.0,), (5.0,), (None,), (1.0,), (2.0,), (3.0,), (4.0,), (5.0,), (None,)]

def test_agg_double(conn):
    q = conn.query('select udf2double(fdouble) from udf.st1')
    rs = q.fetch_all()
    assert len(rs) == 1
    assert rs[0][0] == 45.0        

def test_agg_float(conn):
    q = conn.query('select udf2float(ffloat) from udf.st1')
    rs = q.fetch_all()
    assert len(rs) == 1
    assert rs[0][0] == 45.0  
