# encoding:UTF-8
from dotenv import load_dotenv

from taos.cinterface import *
import taos
from taos.connection import *

load_dotenv()


def test_default_connect():
    if not taos.IS_V3:
        return
    #
    conn = TaosConnection()
    conn.close()
    print("pass test_default_connect")


def test_stmt2_invalid_conn():
    if not taos.IS_V3:
        return
    #
    conn = TaosConnection()
    conn.close()
    stmt2 = conn.statement2()
    assert stmt2 is None
    print("pass test_stmt2_invalid_conn")
