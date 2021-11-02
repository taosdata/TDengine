
class SmlPrecision:
    """Schemaless timestamp precision constants"""
    NOT_CONFIGURED = 0 # C.TSDB_SML_TIMESTAMP_NOT_CONFIGURED
    HOURS = 1
    MINUTES = 2
    SECONDS = 3
    MILLI_SECONDS = 4
    MICRO_SECONDS = 5
    NANO_SECONDS = 6

class SmlProtocol:
    """Schemaless protocol constants"""
    UNKNOWN_PROTOCOL = 0
    LINE_PROTOCOL = 1
    TELNET_PROTOCOL = 2
    JSON_PROTOCOL = 3