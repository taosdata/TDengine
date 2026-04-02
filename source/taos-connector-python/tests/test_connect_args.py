import time
import pytest
from taos import *
from taos.constants import TSDB_CONNECTIONS_MODE, TSDB_OPTION_CONNECTION
from taos.error import ConnectionError
from utils import *


@pytest.mark.skipif(IS_WS, reason="Skip WS")
def test_connect_args():
    """
    DO NOT DELETE THIS TEST CASE!

    Useless args, prevent mistakenly deleted args in connect init.
    Because some case in CI of earlier version may use it.
    """

    host = "localhost:6030"
    conn = connect(
        host=host, charset="utf8", timezone="UTC", user_app="python client 1", user_ip="127.2.2.2", bi_mode=True
    )

    time.sleep(3)
    bClient = False
    bHost = False
    result = conn.query("show connections")
    assert result is not None
    for row in result:
        if row[7] == "python client 1":
            bClient = True
        if row[8] == "127.2.2.2":
            bHost = True

    assert bClient and bHost

    result = conn.query("select timezone()")
    assert result is not None
    for row in result:
        print(row)
        assert row[0] == "UTC (UTC, +0000)"

    conn.set_option(TSDB_OPTION_CONNECTION.TSDB_OPTION_CONNECTION_CHARSET.value, "utf8")
    conn.set_option(TSDB_OPTION_CONNECTION.TSDB_OPTION_CONNECTION_TIMEZONE.value, "UTC")
    conn.set_option(TSDB_OPTION_CONNECTION.TSDB_OPTION_CONNECTION_USER_IP.value, "127.0.0.2")
    conn.set_option(TSDB_OPTION_CONNECTION.TSDB_OPTION_CONNECTION_USER_APP.value, "python client")
    conn.set_mode(TSDB_CONNECTIONS_MODE.TSDB_CONNECTIONS_MODE_BI.value, 1)

    time.sleep(3)
    bClient = False
    bHost = False
    result = conn.query("show connections")
    assert result is not None
    for row in result:
        print(row)
        if row[7] == "python client":
            bClient = True
        if row[8] == "127.0.0.2":
            bHost = True

    assert bClient and bHost

    result = conn.query("select timezone()")
    assert result is not None
    for row in result:
        print(row)
        assert row[0] == "UTC (UTC, +0000)"

    assert conn is not None
    conn.close()


@pytest.mark.skipif(not TEST_TD_ENTERPRISE, reason="only for TDengine Enterprise")
def test_connect_with_totp():
    conn = connect()
    try:
        conn.execute("drop user totp_user")
    except Exception:
        pass

    totp_seed = generate_totp_seed(255)
    conn.execute("create user totp_user pass 'totp_pass_1' totpseed '%s'" % totp_seed)

    rs = conn.query("select generate_totp_secret('%s')" % totp_seed)
    totp_secret = next(iter(rs))[0]

    secret = totp_secret_decode(totp_secret)
    code = generate_totp_code(secret)

    conn1 = connect(
        host="localhost",
        port=6030,
        user="totp_user",
        password="totp_pass_1",
        totp_code=code,
    )
    rs = conn1.query("select 1")
    res = next(iter(rs))[0]
    assert res == 1
    conn1.close()

    conn.execute("drop user totp_user")
    conn.close()


@pytest.mark.skipif(not TEST_TD_ENTERPRISE, reason="only for TDengine Enterprise")
def test_connect_with_invalid_totp():
    conn = connect()
    try:
        conn.execute("drop user it_totp_user")
    except Exception:
        pass

    totp_seed = generate_totp_seed(255)
    conn.execute("create user it_totp_user pass 'totp_pass_1' totpseed '%s'" % totp_seed)

    try:
        connect(
            host="localhost",
            port=6030,
            user="it_totp_user",
            password="totp_pass_1",
            totp_code="000000",
        )
    except ConnectionError as e:
        assert "Wrong TOTP code" in str(e)
        pass

    conn.execute("drop user it_totp_user")
    conn.close()


@pytest.mark.skipif(not TEST_TD_ENTERPRISE, reason="only for TDengine Enterprise")
def test_connect_with_token():
    conn = connect()
    try:
        conn.execute("drop user token_user")
    except Exception:
        pass

    conn.execute("create user token_user pass 'token_pass_1'")
    rs = conn.query("create token test_bearer_token from user token_user")
    token = next(iter(rs))[0]

    conn1 = connect(
        host="localhost",
        port=6030,
        bearer_token=token,
    )
    rs = conn1.query("select 1")
    res = next(iter(rs))[0]
    assert res == 1
    conn1.close()

    conn.execute("drop user token_user")
    conn.close()


@pytest.mark.skipif(not TEST_TD_ENTERPRISE, reason="only for TDengine Enterprise")
def test_connect_with_invalid_token():
    conn = connect()
    try:
        conn.execute("drop user it_token_user")
    except Exception:
        pass

    conn.execute("create user it_token_user pass 'token_pass_1'")

    try:
        connect(
            host="localhost",
            port=6030,
            bearer_token="invalid_token",
        )
    except ConnectionError as e:
        assert "Invalid token" in str(e)
        pass

    conn.execute("drop user it_token_user")
    conn.close()


@pytest.mark.skipif(not TEST_TD_ENTERPRISE, reason="only for TDengine Enterprise")
def test_connect_with_totp_and_token():
    conn = connect()
    try:
        conn.execute("drop user tt_user")
    except Exception:
        pass

    totp_seed = generate_totp_seed(255)
    conn.execute("create user tt_user pass 'totp_pass_1' totpseed '%s'" % totp_seed)

    rs = conn.query("select generate_totp_secret('%s')" % totp_seed)
    totp_secret = next(iter(rs))[0]

    secret = totp_secret_decode(totp_secret)
    code = generate_totp_code(secret)

    conn1 = connect(
        host="localhost",
        port=6030,
        user="tt_user",
        password="totp_pass_1",
        totp_code=code,
    )
    rs = conn1.query("select 1")
    res = next(iter(rs))[0]
    assert res == 1
    conn1.close()

    rs = conn.query("create token test_tt_token from user tt_user")
    token = next(iter(rs))[0]

    conn2 = connect(
        host="localhost",
        port=6030,
        bearer_token=token,
    )
    rs = conn2.query("select 1")
    res = next(iter(rs))[0]
    assert res == 1
    conn2.close()

    conn3 = connect(
        host="localhost",
        port=6030,
        user="tt_user",
        password="totp_pass_1",
        bearer_token=token,
    )
    rs = conn3.query("select 1")
    res = next(iter(rs))[0]
    assert res == 1
    conn3.close()

    conn.execute("drop user tt_user")
    conn.close()


@pytest.mark.skipif(not TEST_TD_ENTERPRISE, reason="only for TDengine Enterprise")
def test_connect_test():
    conn = connect()
    try:
        conn.execute("drop user totp_test_user")
    except Exception:
        pass

    totp_seed = generate_totp_seed(255)
    conn.execute("create user totp_test_user pass 'totp_pass_1' totpseed '%s'" % totp_seed)

    rs = conn.query("select generate_totp_secret('%s')" % totp_seed)
    totp_secret = next(iter(rs))[0]

    secret = totp_secret_decode(totp_secret)
    code = generate_totp_code(secret)

    connect_test(
        host="localhost",
        port=6030,
        user="totp_test_user",
        password="totp_pass_1",
        totp_code=code,
    )

    conn.execute("drop user totp_test_user")
    conn.close()


@pytest.mark.skipif(not TEST_TD_ENTERPRISE, reason="only for TDengine Enterprise")
def test_connect_test_with_invalid_totp():
    conn = connect()
    try:
        conn.execute("drop user it_totp_test_user")
    except Exception:
        pass

    totp_seed = generate_totp_seed(255)
    conn.execute("create user it_totp_test_user pass 'totp_pass_1' totpseed '%s'" % totp_seed)

    try:
        connect_test(
            host="localhost",
            port=6030,
            user="it_totp_test_user",
            password="totp_pass_1",
            totp_code="123456",
        )
    except ConnectionError as e:
        assert "Wrong TOTP code" in str(e)
        pass

    conn.execute("drop user it_totp_test_user")
    conn.close()


@pytest.mark.skipif(not TEST_TD_ENTERPRISE, reason="only for TDengine Enterprise")
def test_connect_test_without_totp():
    try:
        connect_test()
    except TypeError as e:
        assert "missing 1 required" in str(e)
        pass
