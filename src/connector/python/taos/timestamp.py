
class TimestampType(object):
    """Choose which type that parsing TDengine timestamp data to

    - DATETIME: use python datetime.datetime, note that it does not support nanosecond precision,
        and python taos will use raw c_int64 as a fallback for nanosecond results.
    - NUMPY: use numpy.datetime64 type.
    - RAW: use raw c_int64.
    - TAOS: use taos' TaosTimestamp.
    """
    DATETIME = 0,
    NUMPY = 1,
    RAW = 2,
    TAOS = 3,

class TaosTimestamp:
    pass
