from taos.cinterface import *

from taos import *

import pytest

@pytest.fixture
def conn():
    return connect()

def test_client_info():
    print(taos_get_client_info())
    None

def test_server_info(conn):
    # type: (TaosConnection) -> None
    print(conn.client_info)
    print(conn.server_info)
    None

if __name__ == "__main__":
    test_client_info()
    test_server_info(connect())
