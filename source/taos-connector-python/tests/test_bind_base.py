# encoding:UTF-8

from datetime import datetime
from dotenv import load_dotenv

import taos
from taos.cinterface import *
from taos.bind_base import datetime_to_timestamp

load_dotenv()


def test_datetime_to_timestamp_null():
    if not taos.IS_V3:
        return
    #
    dt_str = "2020-01-01 00:00:00"
    dt = datetime.strptime(dt_str, "%Y-%m-%d %H:%M:%S")
    # datetime
    assert datetime_to_timestamp(None, taos.PrecisionEnum.Milliseconds) == taos.FieldType.C_BIGINT_NULL
    assert datetime_to_timestamp(dt, taos.PrecisionEnum.Milliseconds, 2) == taos.FieldType.C_BIGINT_NULL
    print("pass test_datetime_to_timestamp_null")


def check_timestamp(dt_str, dt, seconds_diff):
    # datetime
    assert datetime_to_timestamp(dt, taos.PrecisionEnum.Milliseconds).value == seconds_diff * 10**3
    assert datetime_to_timestamp(dt, taos.PrecisionEnum.Microseconds).value == seconds_diff * 10**6
    try:
        datetime_to_timestamp(dt, 9)
        assert 1 == 2
    except:
        pass
    # float
    assert datetime_to_timestamp(seconds_diff * 1.0, taos.PrecisionEnum.Milliseconds).value == seconds_diff * 10**3
    assert datetime_to_timestamp(seconds_diff * 1.0, taos.PrecisionEnum.Microseconds).value == seconds_diff * 10**6
    try:
        datetime_to_timestamp(seconds_diff * 1.0, 9)
        assert 1 == 2
    except:
        pass
    # int
    assert (
        datetime_to_timestamp(seconds_diff, taos.PrecisionEnum.Milliseconds).value
        == ctypes.c_int64(seconds_diff).value
    )
    # str
    assert datetime_to_timestamp(dt_str, taos.PrecisionEnum.Milliseconds).value == seconds_diff * 10**3
    assert datetime_to_timestamp(dt_str, taos.PrecisionEnum.Microseconds).value == seconds_diff * 10**6
    try:
        datetime_to_timestamp(dt_str, 9)
        assert 1 == 2
    except:
        pass
    # c_int64
    assert (
        datetime_to_timestamp(ctypes.c_int64(seconds_diff), taos.PrecisionEnum.Milliseconds).value
        == ctypes.c_int64(seconds_diff).value
    )
    # other
    assert datetime_to_timestamp(list(), taos.PrecisionEnum.Milliseconds) == FieldType.C_BIGINT_NULL


def test_datetime_to_timestamp_default():
    if not taos.IS_V3:
        return
    #
    now = datetime.now()
    timezone_info = now.astimezone().tzinfo
    print(f"Current timezone: {timezone_info}")

    dt_str = "2020-01-01 00:00:00"
    dt = datetime.strptime(dt_str, "%Y-%m-%d %H:%M:%S")
    taos.field.set_tz(None)
    # Time zone: Asia/Shanghai (CST, +0800)
    # seconds_diff = 1577808000
    seconds_diff = int(dt.timestamp())
    check_timestamp(dt_str, dt, seconds_diff)
    print("pass test_datetime_to_timestamp_default")


def test_datetime_to_timestamp_set_timezone():
    if not taos.IS_V3:
        return
    #
    import pytz

    dt_str = "2020-01-01 00:00:00"
    dt = datetime.strptime(dt_str, "%Y-%m-%d %H:%M:%S")
    # set timezone
    west_eight_zone = pytz.timezone("America/Los_Angeles")
    taos.field.set_tz(west_eight_zone)
    seconds_diff = 1577865180
    check_timestamp(dt_str, dt, seconds_diff)
    print("pass test_datetime_to_timestamp_set_timezone")
