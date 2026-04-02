import taosws
import time
import os
import utils
import pytest


def test_ws_connect():
    print("-" * 40)
    print("test_ws_connect")
    conn = taosws.connect(f"taosws://{utils.test_username()}:{utils.test_password()}@localhost:6041")
    r = conn.query_with_req_id("show dnodes", 1)
    print("r: ", r.fields)
    print("test_ws_connect done")
    print("-" * 40)


def test_default_connect():
    print("-" * 40)
    print("test_default_connect")
    conn = taosws.connect()
    r = conn.query_with_req_id("show dnodes", 1)
    print("r: ", r.fields)
    print("test_default_connect done")
    print("-" * 40)


def test_connect_invalid_user():
    print("-" * 40)
    print("test_connect_invalid_user")
    try:
        conn = taosws.connect(
            user="taos",
            password="taosdata",
            host="localhost",
            port=6041,
        )
        r = conn.query_with_req_id("show dnodes", 1)
        print("r: ", r.fields)
    except Exception as e:
        print("except invalid_user: ", e)
    print("test_connect_invalid_user done")
    print("-" * 40)


def test_report_connector_info():
    test = os.getenv("TEST_TD_3360")
    if test is not None:
        return

    connector_info = utils.get_connector_info()
    print("connector_info:", connector_info)

    conn = taosws.connect()
    time.sleep(2)
    res = conn.query("show connections")
    assert any(connector_info == col for row in res for col in row)
    conn.close()

    conn = taosws.connect(
        user=utils.test_username(),
        password=utils.test_password(),
        host="localhost",
        port=6041,
    )
    time.sleep(2)
    res = conn.query("show connections")
    assert any(connector_info == col for row in res for col in row)
    conn.close()


@pytest.mark.skipif(not utils.TEST_TD_ENTERPRISE, reason="only for TDengine Enterprise")
def test_connect_with_token():
    conn = taosws.connect("ws://localhost:6041")
    try:
        conn.execute("drop user token_user")
    except Exception:
        pass

    conn.execute("create user token_user pass 'token_pass_1'")
    rs = conn.query("create token test_bearer_token from user token_user")
    token = next(iter(rs))[0]

    conn1 = taosws.connect("ws://localhost:6041?bearer_token=" + token)
    rs = conn1.query("select 1")
    res = next(iter(rs))[0]
    assert res == 1
    conn1.close()

    conn2 = taosws.connect(
        host="localhost",
        port=6041,
        bearer_token=token,
    )
    rs = conn2.query("select 1")
    res = next(iter(rs))[0]
    assert res == 1
    conn2.close()

    conn.execute("drop user token_user")
    conn.close()


@pytest.mark.skipif(not utils.TEST_TD_ENTERPRISE, reason="only for TDengine Enterprise")
def test_connect_with_totp():
    conn = taosws.connect("ws://localhost:6041")
    try:
        conn.execute("drop user totp_user")
    except Exception:
        pass

    totp_seed = utils.generate_totp_seed(255)
    conn.execute("create user totp_user pass 'totp_pass_1' totpseed '%s'" % totp_seed)

    rs = conn.query("select generate_totp_secret('%s')" % totp_seed)
    totp_secret = next(iter(rs))[0]

    secret = utils.totp_secret_decode(totp_secret)
    code = utils.generate_totp_code(secret)

    conn1 = taosws.connect("ws://totp_user:totp_pass_1@localhost:6041?totp_code=" + code)
    rs = conn1.query("select 1")
    res = next(iter(rs))[0]
    assert res == 1
    conn1.close()

    conn2 = taosws.connect(
        host="localhost",
        port=6041,
        user="totp_user",
        password="totp_pass_1",
        totp_code=code,
    )
    rs = conn2.query("select 1")
    res = next(iter(rs))[0]
    assert res == 1
    conn2.close()

    conn.execute("drop user totp_user")
    conn.close()


def show_env():
    import os

    print("-" * 40)
    print("TAOS_LIBRARY_PATH: ", os.environ.get("TAOS_LIBRARY_PATH"))
    print("TAOS_CONFIG_DIR: ", os.environ.get("TAOS_CONFIG_DIR"))
    print("TAOS_C_CLIENT_VERSION: ", os.environ.get("TAOS_C_CLIENT_VERSION"))
    print("TAOS_C_CLIENT_VERSION_STR: ", os.environ.get("TAOS_C_CLIENT_VERSION_STR"))
    print("taosws.__version__", taosws.__version__)
    print("-" * 40)


if __name__ == "__main__":
    show_env()
    test_ws_connect()
    test_default_connect()
    test_connect_invalid_user()
    test_report_connector_info()
