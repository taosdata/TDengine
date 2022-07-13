import time
from ctypes import *

from taos import *


def fetch_callback(p_param, p_result, num_of_rows):
    print("fetched ", num_of_rows, "rows")
    p = cast(p_param, POINTER(Counter))
    result = TaosResult(p_result)

    if num_of_rows == 0:
        print("fetching completed")
        p.contents.done = True
        result.close()
        return
    if num_of_rows < 0:
        p.contents.done = True
        result.check_error(num_of_rows)
        result.close()
        return None

    for row in result.rows_iter(num_of_rows):
        print(row)
    p.contents.count += result.row_count
    result.fetch_rows_a(fetch_callback, p_param)


def query_callback(p_param, p_result, code):
    if p_result is None:
        return
    result = TaosResult(p_result)
    if code == 0:
        result.fetch_rows_a(fetch_callback, p_param)
    result.check_error(code)


class Counter(Structure):
    _fields_ = [("count", c_int), ("done", c_bool)]

    def __str__(self):
        return "{ count: %d, done: %s }" % (self.count, self.done)


def test_query(conn):
    counter = Counter(count=0)
    conn.query_a("select ts, current, voltage from power.meters", query_callback, byref(counter))

    while not counter.done:
        print(counter)
        time.sleep(1)
    print(counter)
    conn.close()


if __name__ == "__main__":
    test_query(connect())

# possible output:
# { count: 0, done: False }
# fetched  8 rows
# 1538548685000 10.300000 219
# 1538548695000 12.600000 218
# 1538548696800 12.300000 221
# 1538548696650 10.300000 218
# 1538548685500 11.800000 221
# 1538548696600 13.400000 223
# 1538548685500 10.800000 223
# 1538548686500 11.500000 221
# fetched  0 rows
# fetching completed
# { count: 8, done: True }
