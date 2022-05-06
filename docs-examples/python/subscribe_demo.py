"""
Python asynchronous subscribe demo.
run on Linux system with: python3 subscribe_demo.py
"""

from ctypes import c_void_p

import taos
import time


def query_callback(p_sub, p_result, p_param, code):
    """
    :param p_sub: pointer returned by native API -- taos_subscribe
    :param p_result: pointer to native TAOS_RES
    :param p_param: None
    :param code: error code
    :return: None
    """
    print("in callback")
    result = taos.TaosResult(c_void_p(p_result))
    # raise exception if error occur
    result.check_error(code)
    for row in result.rows_iter():
        print(row)
    print(f"{result.row_count} rows consumed.")


if __name__ == '__main__':
    conn = taos.connect()
    restart = True
    topic = "topic-meter-current-bg"
    sql = "select * from power.meters where current > 10"  # Error sql
    interval = 2000  # consumption interval in microseconds.
    _ = conn.subscribe(restart, topic, sql, interval, query_callback)
    # Note: we received the return value as _ above, to avoid the TaosSubscription object to be deleted by gc.
    while True:
        time.sleep(10)  # use Ctrl + C to interrupt
