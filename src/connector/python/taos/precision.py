class PrecisionEnum(object):
    """Precision enums"""

    Milliseconds = 0
    Microseconds = 1
    Nanoseconds = 2


class PrecisionError(Exception):
    """Python datetime does not support nanoseconds error"""

    pass
