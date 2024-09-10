from typing import Any, BinaryIO, List, Dict, NewType
from enum import Enum

DirPath = NewType('DirPath', str)

QueryResult = NewType('QueryResult', List[List[Any]])

class TdDataType(Enum):
    '''
    Use a Python Enum types of represent all the data types in TDengine.

    Ref: https://www.taosdata.com/cn/documentation/taos-sql#data-type
    '''
    TIMESTAMP   = 'TIMESTAMP'
    INT         = 'INT'
    BIGINT      = 'BIGINT'
    FLOAT       = 'FLOAT'
    DOUBLE      = 'DOUBLE'
    BINARY      = 'BINARY'
    BINARY16    = 'BINARY(16)'  # TODO: get rid of this hack
    BINARY200   = 'BINARY(200)'
    SMALLINT    = 'SMALLINT'
    TINYINT     = 'TINYINT'
    BOOL        = 'BOOL'
    NCHAR16     = 'NCHAR(16)'
    UTINYINT    = 'TINYINT UNSIGNED'
    USMALLINT   = 'SMALLINT UNSIGNED'
    UINT        = 'INT UNSIGNED'
    UBIGINT     = 'BIGINT UNSIGNED'
    VARCHAR16   = 'VARCHAR(16)'
    VARBINARY16 = 'VARBINARY(16)'
    GEOMETRY32  = 'GEOMETRY(32)'
    JSON        = 'JSON'

class DataBoundary(Enum):
    TINYINT_BOUNDARY = [-128, 127]
    SMALLINT_BOUNDARY = [-32768, 32767]
    INT_BOUNDARY = [-2147483648, 2147483647]
    BIGINT_BOUNDARY = [-9223372036854775808, 9223372036854775807]
    UTINYINT_BOUNDARY = [0, 255]
    USMALLINT_BOUNDARY = [0, 65535]
    UINT_BOUNDARY = [0, 4294967295]
    UBIGINT_BOUNDARY = [0, 18446744073709551615]
    FLOAT_BOUNDARY = [-3.40E+38, 3.40E+38]
    DOUBLE_BOUNDARY = [-1.7e+308, 1.7e+308]
    BOOL_BOUNDARY = [True, False]
    # GEOMETRY_BOUNDARY = ["point(1.0 1.0)", "LINESTRING(1.0 1.0, 2.0 2.0)", "POLYGON((1.0 1.0, 2.0 2.0, 1.0 1.0))"]
    GEOMETRY_BOUNDARY = ["point(1.0 1.0)"]
    BINARY_MAX_LENGTH = 65517
    NCHAR_MAX_LENGTH = 16379
    DBNAME_MAX_LENGTH = 64
    STBNAME_MAX_LENGTH = 192
    TBNAME_MAX_LENGTH = 192
    CHILD_TBNAME_MAX_LENGTH = 192
    TAG_KEY_MAX_LENGTH = 64
    COL_KEY_MAX_LENGTH = 64
    MAX_TAG_COUNT = 128
    MAX_TAG_COL_COUNT = 4096
    mnodeShmSize = [6292480, 2147483647]
    mnodeShmSize_default = 6292480
    vnodeShmSize = [6292480, 2147483647]
    vnodeShmSize_default = 31458304
    DB_PARAM_BUFFER_CONFIG = {"create_name": "buffer", "query_name": "buffer", "vnode_json_key": "szBuf", "boundary": [3, 16384], "default": 256}
    DB_PARAM_CACHELAST_CONFIG = {"create_name": "cachemodel", "query_name": "cachemodel", "vnode_json_key": "cacheLast", "boundary": {None:0, 'last_row':1, 'last_value':2, 'both':3}, "default": {None:0}}
    DB_PARAM_COMP_CONFIG = {"create_name": "comp", "query_name": "compression", "vnode_json_key": "", "boundary": [0, 1, 2], "default": 2}
    DB_PARAM_DURATION_CONFIG = {"create_name": "duration", "query_name": "duration", "vnode_json_key": "daysPerFile", "boundary": [1, 3650, '60m', '5256000m', '1h', '87600h', '1d', '3650d'], "default": "14400m"}
    DB_PARAM_FSYNC_CONFIG = {"create_name": "wal_fsync_period", "query_name": "wal_fsync_period", "vnode_json_key": "wal.fsyncPeriod", "boundary": [0, 180000], "default": 3000}
    DB_PARAM_KEEP_CONFIG = {"create_name": "keep", "query_name": "keep", "vnode_json_key": "", "boundary": [1, 365000,'1440m','525600000m','24h','8760000h','1d','365000d'], "default": "5256000m,5256000m,5256000m"}
    DB_PARAM_MAXROWS_CONFIG = {"create_name": "maxrows", "query_name": "maxrows", "vnode_json_key": "maxRows", "boundary": [200, 10000000], "default": 4096}
    DB_PARAM_MINROWS_CONFIG = {"create_name": "minrows", "query_name": "minrows", "vnode_json_key": "minRows", "boundary": [10, 1000000], "default": 100}
    DB_PARAM_NTABLES_CONFIG = {"create_name": "ntables", "query_name": "ntables", "vnode_json_key": "", "boundary": 0, "default": 0}
    DB_PARAM_PAGES_CONFIG = {"create_name": "pages", "query_name": "pages", "vnode_json_key": "szCache", "boundary": [64], "default": 256}
    DB_PARAM_PAGESIZE_CONFIG = {"create_name": "pagesize", "query_name": "pagesize", "vnode_json_key": "szPage", "boundary": [1, 16384], "default": 4}
    DB_PARAM_PRECISION_CONFIG = {"create_name": "precision", "query_name": "precision", "vnode_json_key": "", "boundary": ['ms', 'us', 'ns'], "default": "ms"}
    DB_PARAM_REPLICA_CONFIG = {"create_name": "replica", "query_name": "replica", "vnode_json_key": "syncCfg.replicaNum", "boundary": [1], "default": 1}
    DB_PARAM_SINGLE_STABLE_CONFIG = {"create_name": "single_stable", "query_name": "single_stable", "vnode_json_key": "", "boundary": [0, 1], "default": 0}
    DB_PARAM_STRICT_CONFIG = {"create_name": "strict", "query_name": "strict", "vnode_json_key": "", "boundary": {"off": "off", "on": "on"}, "default": "off"}
    DB_PARAM_VGROUPS_CONFIG = {"create_name": "vgroups", "query_name": "vgroups", "vnode_json_key": "", "boundary": [1, 1024], "default": 2}
    DB_PARAM_WAL_CONFIG = {"create_name": "wal_level", "query_name": "wal_level", "vnode_json_key": "wal.level", "boundary": [0, 2], "default": 1}
    SAMPLE_BOUNDARY = [1, 1000]
    TIMEZONE_BOUNDARY = [0, 1]
    HISTOGRAM_BOUNDARY = [0, 1]
    IGNORE_NEGATIVE_BOUNDARY = [0, 1]
    DIFF_IGNORE_BOUNDARY = [0, 1, 2, 3]
    PERCENTILE_BOUNDARY = [1, 10]
    LEASTSQUARES_BOUNDARY = [1, 10]
    SUBSTR_BOUNDARY = [1, 10]
    TAIL_BOUNDARY = [1, 100]
    TAIL_OFFSET_BOUNDARY = [0, 100]
    TOP_BOUNDARY = [1, 100]
    MAVG_BOUNDARY = [1, 1000]
    CONCAT_BOUNDARY = [2, 8]
    MINROWS_BOUNDARY = [10, 1000000]
    MAXROWS_BOUNDARY = [200, 10000000]
    STT_TRIGGER_BOUNDARY = [1, 16]
    PRECISION_BOUNDARY = ['ms', 'us', 'ns']
    COMP_BOUNDARY = [0, 1, 2]
    CACHESIZE_BOUNDARY = [1, 65536]
    LIMIT_BOUNDARY = 100
    TIME_UNIT = ['b', 'u', 'a', 's', 'm', 'h', 'd', 'w']
    STATECOUNT_UNIT = ["LT", "LE", "EQ", "NE", "GE", "GT"]
    TO_CHAR_UNIT = ['AM,am,PM,pm', 'A.M.,a.m.,P.M.,p.m.', 'YYYY,yyyy', 'YYY,yyy', 'YY,yy', 'Y,y','MONTH', 'Month',
                    'month', 'MON', 'Mon', 'mon', 'MM,mm', 'DD,dd', 'DAY', 'Day', 'day', 'DY', 'Dy', 'dy', 'DDD',
                    'D,d', 'HH24,hh24', 'hh12,HH12, hh, HH', 'MI,mi', 'SS,ss', 'MS,ms', 'US,us', 'NS,ns', 'TZH,tzh']
    ALL_TYPE_UNIT = ['TINYINT', 'SMALLINT', 'INT', 'BIGINT', 'TINYINT UNSIGNED', 'SMALLINT UNSIGNED', 'INT UNSIGNED', 'BIGINT UNSIGNED', 'FLOAT', 'DOUBLE', 'BINARY', 'VARCHAR', 'VARBINARY', 'NCHAR', 'BOOL', 'TIMESTAMP', 'GEOMETRY(64)']
    WINDOW_UNIT = ['INTERVAL', 'SESSION', 'STATE_WINDOW', 'COUNT_WINDOW', 'EVENT_WINDOW']
    FILL_UNIT = ["NULL", "PREV", "NEXT", "LINEAR", "VALUE, 0", "NULL_F", "VALUE_F, 0"]
    SYSTABLE_UNIT = ['INFORMATION_SCHEMA', 'PERFORMANCE_SCHEMA']
    SHOW_UNIT = ['SHOW CLUSTER', 'SHOW CLUSTER ALIVE', 'SHOW CLUSTER VARIABLES', 'SHOW LOCAL VARIABLES', 'SHOW CLUSTER MACHINES', 'SHOW CONNECTIONS', 'SHOW MNODES', 'SHOW DNODES', 'SHOW QNODES', 'SHOW VNODES', 'SHOW SNODES',
                'SHOW VGROUPS', 'SHOW STREAMS', 'SHOW VIEWS', 'SHOW APPS', 'SHOW DNODE ID 1 VARIABLES', 'SHOW CREATE', 'SHOW GRANTS', 'SHOW GRANTS LOGS', 'SHOW GRANTS FULL', 'SHOW DATABASES', 'SHOW STABLES', 'SHOW TABLES',
                'SHOW USERS', 'SHOW LICENCES', 'SHOW TRANSACTIONS', 'SHOW TABLE DISTRIBUTED', 'SHOW TABLE LIKE "%a%"', 'SHOW TAGS FROM', 'SHOW TOPICS', 'SHOW SUBCRIPTIONS', 'SHOW CONSUMERS', 'SHOW FUNCTIONS', 'SHOW SCORES', 'SHOW INDEXES']

class FunctionMap(Enum):
    # TODO TO_JSON
    NUMERIC = {
        'types': ['TINYINT', 'SMALLINT', 'INT', 'BIGINT', 'TINYINT UNSIGNED', 'SMALLINT UNSIGNED', 'INT UNSIGNED', 'BIGINT UNSIGNED', 'FLOAT', 'DOUBLE'],
        'mathFuncs': ['ABS', 'ACOS', 'ASIN', 'ATAN', 'CEIL', 'COS', 'FLOOR', 'LOG', 'POW', 'ROUND', 'SIN', 'SQRT', 'TAN'],
        'strFuncs': [],
        'timeFuncs': ['NOW', 'TIMEZONE', 'TODAY'],
        'aggFuncs': ['APERCENTILE', 'AVG', 'COUNT', 'LEASTSQUARES', 'SPREAD', 'STDDEV', 'SUM', 'HYPERLOGLOG', 'PERCENTILE'],
        'selectFuncs': ['FIRST', 'LAST', 'LAST_ROW', 'MAX', 'MIN', 'MODE'],
        'specialFuncs': ['IRATE', 'TWA'],
        'VariableFuncs': ['BOTTOM', 'INTERP', 'UNIQUE', 'TOP', 'TAIL', 'SAMPLE', 'DIFF', 'CSUM', 'MAVG', 'DERIVATIVE', 'STATECOUNT', 'STATEDURATION', 'HISTOGRAM'],
        'castFuncs': ['CAST', 'TO_ISO8601'],
        'castTypes': ['TINYINT', 'SMALLINT', 'INT', 'BIGINT', 'TINYINT UNSIGNED', 'SMALLINT UNSIGNED', 'INT UNSIGNED', 'BIGINT UNSIGNED', 'FLOAT', 'DOUBLE', 'BINARY', 'VARCHAR', 'NCHAR', 'BOOL', 'TIMESTAMP', 'GEOMETRY(64)']
    }
    TEXT = {
        'types': ['BINARY', 'VARCHAR', 'NCHAR'],
        'mathFuncs': [],
        'strFuncs': ['CHAR_LENGTH', 'CONCAT', 'CONCAT_WS', 'LENGTH', 'LOWER', 'LTRIM', 'RTRIM', 'SUBSTR', 'UPPER'],
        'timeFuncs': ['NOW', 'TIMETRUNCATE', 'TIMEZONE', 'TODAY'],
        'aggFuncs': ['COUNT', 'HYPERLOGLOG'],
        'selectFuncs': ['FIRST', 'LAST', 'LAST_ROW', 'MODE'],
        'specialFuncs': [],
        'VariableFuncs': ['BOTTOM', 'INTERP', 'UNIQUE', 'TAIL', 'SAMPLE'],
        'castFuncs': ['CAST', 'TO_UNIXTIMESTAMP'],
        'castTypes': DataBoundary.ALL_TYPE_UNIT.value
    }
    BINARY = {
        'types': ['VARBINARY'],
        'mathFuncs': [],
        'strFuncs': ['LENGTH'],
        'timeFuncs': ['NOW', 'TIMETRUNCATE', 'TIMEZONE', 'TODAY'],
        'aggFuncs': ['COUNT', 'HYPERLOGLOG'],
        'selectFuncs': ['FIRST', 'LAST', 'LAST_ROW', 'MODE'],
        'specialFuncs': [],
        'VariableFuncs': ['UNIQUE', 'TAIL', 'SAMPLE'],
        'castFuncs': [],
        'castTypes': []
    }
    BOOLEAN = {
        'types': ['BOOL'],
        'mathFuncs': [],
        'strFuncs': [],
        'timeFuncs': ['NOW'],
        'aggFuncs': ['COUNT', 'HYPERLOGLOG'],
        'selectFuncs': ['FIRST', 'LAST', 'LAST_ROW', 'MODE'],
        'specialFuncs': [],
        'VariableFuncs': ['UNIQUE', 'TAIL', 'SAMPLE'],
        'castFuncs': ['CAST'],
        'castTypes': ['TINYINT', 'SMALLINT', 'INT', 'BIGINT', 'TINYINT UNSIGNED', 'SMALLINT UNSIGNED', 'INT UNSIGNED', 'BIGINT UNSIGNED', 'FLOAT', 'DOUBLE', 'BINARY', 'VARCHAR', 'NCHAR', 'BOOL', 'TIMESTAMP', 'GEOMETRY(64)']
    }
    TIMESTAMP = {
        'types': ['TIMESTAMP'],
        'mathFuncs': [],
        'strFuncs': [],
        'timeFuncs': ['NOW', 'TIMEDIFF', 'TIMETRUNCATE', 'TIMEZONE', 'TODAY'],
        'aggFuncs': ['ELAPSED', 'SPREAD'],
        'selectFuncs': ['FIRST', 'LAST', 'LAST_ROW', 'MODE'],
        'specialFuncs': [],
        'VariableFuncs': ['UNIQUE', 'SAMPLE'],
        'castFuncs': ['CAST', 'TO_ISO8601', 'TO_CHAR'],
        'castTypes': ['TINYINT', 'SMALLINT', 'INT', 'BIGINT', 'TINYINT UNSIGNED', 'SMALLINT UNSIGNED', 'INT UNSIGNED', 'BIGINT UNSIGNED', 'FLOAT', 'DOUBLE', 'BINARY', 'VARCHAR', 'NCHAR', 'BOOL', 'TIMESTAMP', 'GEOMETRY(64)']
    }
    # TODO sysinfoFuncs and geometryFuncs

TdColumns = Dict[str, TdDataType]
TdTags    = Dict[str, TdDataType]

IpcStream = NewType('IpcStream', BinaryIO)

print(TdColumns)