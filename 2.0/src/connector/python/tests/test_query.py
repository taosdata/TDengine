from datetime import datetime
import taos
import pytest

@pytest.fixture
def conn():
    return taos.connect()

def test_query(conn):
    """This test will use fetch_block for rows fetching, significantly faster than rows_iter"""
    result = conn.query("select * from log.log limit 10000")
    fields = result.fields
    for field in fields:
        print("field: %s" % field)
    start = datetime.now()
    for row in result:
        # print(row)
        None
    end = datetime.now()
    elapsed = end - start
    print("elapsed time: ", elapsed)
    result.close()
    conn.close()

def test_query_row_iter(conn):
    """This test will use fetch_row for each row fetching, this is the only way in async callback"""
    result = conn.query("select * from log.log limit 10000")
    fields = result.fields
    for field in fields:
        print("field: %s" % field)
    start = datetime.now()
    for row in result.rows_iter():
        # print(row)
        None
    end = datetime.now()
    elapsed = end - start
    print("elapsed time: ", elapsed)
    result.close()
    conn.close()

if __name__ == "__main__":
    test_query(taos.connect(database = "log"))
    test_query_row_iter(taos.connect(database = "log"))
