from taos import *
from ctypes import *
import time

def fetch_callback(p_param, p_result, num_of_rows):
    print("fetched ", num_of_rows, "rows")
    p = cast(p_param, POINTER(Counter))
    result = TaosResult(p_result)

    if num_of_rows == 0:
        print("fetching completed")
        p.contents.done = True
        # should explicitly close the result in fetch completed or cause error
        result.close()
        return
    if num_of_rows < 0:
        p.contents.done = True
        result.check_error(num_of_rows)
        result.close()
        return None
    
    for row in result.rows_iter(num_of_rows):
        # print(row)
        None
    p.contents.count += result.row_count
    result.fetch_rows_a(fetch_callback, p_param)
    


def query_callback(p_param, p_result, code):
    # type: (c_void_p, c_void_p, c_int) -> None
    if p_result == None:
        return
    result = TaosResult(p_result)
    if code == 0:
        result.fetch_rows_a(fetch_callback, p_param)
    result.check_error(code)
    # explicitly close result while query failed
    result.close()


class Counter(Structure):
    _fields_ = [("count", c_int), ("done", c_bool)]

    def __str__(self):
        return "{ count: %d, done: %s }" % (self.count, self.done)


def test_query(conn):
    # type: (TaosConnection) -> None
    counter = Counter(count=0)
    conn.query_a("select * from log.log", query_callback, byref(counter))

    while not counter.done:
        print("wait query callback")
        time.sleep(1)
    print(counter)
    # conn.close()


if __name__ == "__main__":
    test_query(connect())