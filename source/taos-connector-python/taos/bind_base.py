# encoding:UTF-8

from ctypes import *
from datetime import datetime, timezone

from taos.constants import FieldType
from taos.precision import PrecisionEnum, PrecisionError


from taos.field import get_tz


def parse_datetime_to_utc_timestamp(dt) -> int:
    if isinstance(dt, str):
        dt_naive = datetime.fromisoformat(dt)
    elif isinstance(dt, datetime):
        dt_naive = dt
    else:
        raise TypeError(f"dt type error, expected str or datetime type but got {type(dt)}.")
    #
    if dt_naive.tzinfo is None or dt_naive.tzinfo.utcoffset(dt_naive) is None:
        tz = get_tz()
        if tz is not None:
            dt_aware = dt_naive.replace(tzinfo=tz)
        else:
            dt_aware = dt_naive
    else:
        dt_aware = dt_naive.astimezone(timezone.utc)
    #
    result = int(dt_aware.timestamp())
    return result


def datetime_to_timestamp(value, precision, is_null_type=0):
    # type: (datetime | float | int | str | c_int64, PrecisionEnum, int) -> c_int64
    if is_null_type > 0 or value is None:
        return FieldType.C_BIGINT_NULL

    if type(value) is datetime or isinstance(value, str):
        utc_timestamp = parse_datetime_to_utc_timestamp(value)
        if precision == PrecisionEnum.Milliseconds:
            ret_value = c_int64(utc_timestamp * 10**3)
            return ret_value
        elif precision == PrecisionEnum.Microseconds:
            ret_value = c_int64(utc_timestamp * 10**6)
            return ret_value
        else:
            raise PrecisionError("datetime do not support nanosecond precision")
    elif type(value) is float:
        if precision == PrecisionEnum.Milliseconds:
            return c_int64(round(value * 10**3))
        elif precision == PrecisionEnum.Microseconds:
            return c_int64(round(value * 10**6))
        else:
            raise PrecisionError("time float do not support nanosecond precision")
    elif isinstance(value, int) and not isinstance(value, bool):
        return c_int64(value)
    elif isinstance(value, c_int64):
        return value
    return FieldType.C_BIGINT_NULL
