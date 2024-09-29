
import random
from enum import Enum
import re
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
    MAX_DELAY_UNIT = ["5s", "1m", "1h", "1d", "1w", "1M", "1y"]
    DELETE_MARK_UNIT = ["5s", "1m", "1h", "1d", "1w", "1M", "1y"]
    WATERMARK_UNIT = ["5s", "1m", "1h", "1d", "1w", "1M", "1y"]
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
        'castTypes': ['TINYINT', 'SMALLINT', 'INT', 'BIGINT', 'TINYINT UNSIGNED', 'SMALLINT UNSIGNED', 'INT UNSIGNED', 'BIGINT UNSIGNED', 'FLOAT', 'DOUBLE', 'BINARY', 'VARCHAR', 'NCHAR', 'BOOL', 'TIMESTAMP', 'GEOMETRY(64)'],
        'unsupported': ['LEASTSQUARES', 'PERCENTILE', 'BOTTOM', 'TOP', 'INTERP', 'DERIVATIVE', 'IRATE', 'DIFF', 'STATECOUNT', 'STATEDURATION', 'CSUM', 'MAVG', 'SAMPLE', 'TAIL', 'UNIQUE', 'MODE', 'IRATE', 'TWA', 'HISTOGRAM']
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
        'castTypes': DataBoundary.ALL_TYPE_UNIT.value,
        'unsupported': ['BOTTOM', 'INTERP', 'SAMPLE', 'TAIL', 'UNIQUE', 'MODE']
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
        'castTypes': [],
        'unsupported': ['SAMPLE', 'TAIL', 'UNIQUE', 'MODE']
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
        'castTypes': ['TINYINT', 'SMALLINT', 'INT', 'BIGINT', 'TINYINT UNSIGNED', 'SMALLINT UNSIGNED', 'INT UNSIGNED', 'BIGINT UNSIGNED', 'FLOAT', 'DOUBLE', 'BINARY', 'VARCHAR', 'NCHAR', 'BOOL', 'TIMESTAMP', 'GEOMETRY(64)'],
        'unsupported': ['TAIL', 'UNIQUE', 'MODE', 'SAMPLE']
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
        'castTypes': ['TINYINT', 'SMALLINT', 'INT', 'BIGINT', 'TINYINT UNSIGNED', 'SMALLINT UNSIGNED', 'INT UNSIGNED', 'BIGINT UNSIGNED', 'FLOAT', 'DOUBLE', 'BINARY', 'VARCHAR', 'NCHAR', 'BOOL', 'TIMESTAMP', 'GEOMETRY(64)'],
        'unsupported': ['ELAPSED', 'UNIQUE', 'MODE', 'SAMPLE']
    }

# class StreamFunctionMap(Enum):
    # # TODO TO_JSON
    # NUMERIC = {
    #     'types': ['TINYINT', 'SMALLINT', 'INT', 'BIGINT', 'TINYINT UNSIGNED', 'SMALLINT UNSIGNED', 'INT UNSIGNED', 'BIGINT UNSIGNED', 'FLOAT', 'DOUBLE'],
    #     'mathFuncs': ['ABS', 'ACOS', 'ASIN', 'ATAN', 'CEIL', 'COS', 'FLOOR', 'LOG', 'POW', 'ROUND', 'SIN', 'SQRT', 'TAN'],
    #     'strFuncs': [],
    #     'timeFuncs': ['NOW', 'TIMEZONE', 'TODAY'],
    #     'aggFuncs': ['APERCENTILE', 'AVG', 'COUNT', 'LEASTSQUARES', 'SPREAD', 'STDDEV', 'SUM', 'HYPERLOGLOG', 'PERCENTILE'],
    #     'selectFuncs': ['FIRST', 'LAST', 'LAST_ROW', 'MAX', 'MIN', 'MODE'],
    #     'specialFuncs': ['IRATE', 'TWA'],
    #     'VariableFuncs': ['BOTTOM', 'INTERP', 'UNIQUE', 'TOP', 'TAIL', 'SAMPLE', 'DIFF', 'CSUM', 'MAVG', 'DERIVATIVE', 'STATECOUNT', 'STATEDURATION', 'HISTOGRAM'],
    #     'castFuncs': ['CAST', 'TO_ISO8601'],
    #     'castTypes': ['TINYINT', 'SMALLINT', 'INT', 'BIGINT', 'TINYINT UNSIGNED', 'SMALLINT UNSIGNED', 'INT UNSIGNED', 'BIGINT UNSIGNED', 'FLOAT', 'DOUBLE', 'BINARY', 'VARCHAR', 'NCHAR', 'BOOL', 'TIMESTAMP', 'GEOMETRY(64)'],
    #     'unsupported': ['LEASTSQUARES', 'PERCENTILE', 'BOTTOM', 'TOP', 'INTERP', 'DERIVATIVE', 'IRATE', 'DIFF', 'STATECOUNT', 'STATEDURATION', 'CSUM', 'MAVG', 'SAMPLE', 'TAIL', 'UNIQUE', 'MODE']
    # }
    # TEXT = {
    #     'types': ['BINARY', 'VARCHAR', 'NCHAR'],
    #     'mathFuncs': [],
    #     'strFuncs': ['CHAR_LENGTH', 'CONCAT', 'CONCAT_WS', 'LENGTH', 'LOWER', 'LTRIM', 'RTRIM', 'SUBSTR', 'UPPER'],
    #     'timeFuncs': ['NOW', 'TIMETRUNCATE', 'TIMEZONE', 'TODAY'],
    #     'aggFuncs': ['COUNT', 'HYPERLOGLOG'],
    #     'selectFuncs': ['FIRST', 'LAST', 'LAST_ROW', 'MODE'],
    #     'specialFuncs': [],
    #     'VariableFuncs': ['BOTTOM', 'INTERP', 'UNIQUE', 'TAIL', 'SAMPLE'],
    #     'castFuncs': ['CAST', 'TO_UNIXTIMESTAMP'],
    #     'castTypes': DataBoundary.ALL_TYPE_UNIT.value,
    #     'unsupported': ['INTERP', 'SAMPLE', 'TAIL', 'UNIQUE', 'MODE']
    # }
    # BINARY = {
    #     'types': ['VARBINARY'],
    #     'mathFuncs': [],
    #     'strFuncs': ['LENGTH'],
    #     'timeFuncs': ['NOW', 'TIMETRUNCATE', 'TIMEZONE', 'TODAY'],
    #     'aggFuncs': ['COUNT', 'HYPERLOGLOG'],
    #     'selectFuncs': ['FIRST', 'LAST', 'LAST_ROW', 'MODE'],
    #     'specialFuncs': [],
    #     'VariableFuncs': ['UNIQUE', 'TAIL', 'SAMPLE'],
    #     'castFuncs': [],
    #     'castTypes': [],
    #     'unsupported': ['SAMPLE', 'TAIL', 'UNIQUE', 'MODE']
    # }
    # BOOLEAN = {
    #     'types': ['BOOL'],
    #     'mathFuncs': [],
    #     'strFuncs': [],
    #     'timeFuncs': ['NOW'],
    #     'aggFuncs': ['COUNT', 'HYPERLOGLOG'],
    #     'selectFuncs': ['FIRST', 'LAST', 'LAST_ROW', 'MODE'],
    #     'specialFuncs': [],
    #     'VariableFuncs': ['UNIQUE', 'TAIL', 'SAMPLE'],
    #     'castFuncs': ['CAST'],
    #     'castTypes': ['TINYINT', 'SMALLINT', 'INT', 'BIGINT', 'TINYINT UNSIGNED', 'SMALLINT UNSIGNED', 'INT UNSIGNED', 'BIGINT UNSIGNED', 'FLOAT', 'DOUBLE', 'BINARY', 'VARCHAR', 'NCHAR', 'BOOL', 'TIMESTAMP', 'GEOMETRY(64)'],
    #     'unsupported': ['TAIL', 'UNIQUE', 'MODE']
    # }
    # TIMESTAMP = {
    #     'types': ['TIMESTAMP'],
    #     'mathFuncs': [],
    #     'strFuncs': [],
    #     'timeFuncs': ['NOW', 'TIMEDIFF', 'TIMETRUNCATE', 'TIMEZONE', 'TODAY'],
    #     'aggFuncs': ['ELAPSED', 'SPREAD'],
    #     'selectFuncs': ['FIRST', 'LAST', 'LAST_ROW', 'MODE'],
    #     'specialFuncs': [],
    #     'VariableFuncs': ['UNIQUE', 'SAMPLE'],
    #     'castFuncs': ['CAST', 'TO_ISO8601', 'TO_CHAR'],
    #     'castTypes': ['TINYINT', 'SMALLINT', 'INT', 'BIGINT', 'TINYINT UNSIGNED', 'SMALLINT UNSIGNED', 'INT UNSIGNED', 'BIGINT UNSIGNED', 'FLOAT', 'DOUBLE', 'BINARY', 'VARCHAR', 'NCHAR', 'BOOL', 'TIMESTAMP', 'GEOMETRY(64)'],
    #     'unsupported': ['ELAPSED', 'UNIQUE', 'MODE']
    # }


class SQLLancer:
    def formatTimediff(self, expr1):
        # 1b(纳秒), 1u(微秒)，1a(毫秒)，1s(秒)，1m(分)，1h(小时)，1d(天), 1w(周)
        time_unit = random.choice(DataBoundary.TIME_UNIT.value)
        expr2 = f'{expr1}+1{time_unit}'
        return f"TIMEDIFF({expr1}, {expr2})"

    def formatDiff(self, expr1):
        ignore_val = random.choice(DataBoundary.DIFF_IGNORE_BOUNDARY.value)
        if ignore_val == 0:
            return f"DIFF({expr1})"
        else:
            return f"DIFF({expr1}, {ignore_val})"

    def formatTimeTruncate(self, expr):
        time_unit = random.choice(DataBoundary.TIME_UNIT.value[2:])
        use_current_timezone = random.choice(DataBoundary.TIMEZONE_BOUNDARY.value)
        return f'TIMETRUNCATE({expr}, 1{time_unit}, {use_current_timezone})'

    def formatHistogram(self, expr):
        user_input1 = [f'HISTOGRAM({expr}, "user_input", "[1, 3, 5, 7]", {random.choice([0, 1])})']
        linear_bin  = [f'HISTOGRAM({expr}, "linear_bin", \'{{"start": 0.0, "width": 5.0, "count": 5, "infinity": true}}\', {random.choice(DataBoundary.HISTOGRAM_BOUNDARY.value)})']
        user_input2 = [f'HISTOGRAM({expr}, "user_input", \'{{"start":1.0, "factor": 2.0, "count": 5, "infinity": true}}\', {random.choice(DataBoundary.HISTOGRAM_BOUNDARY.value)})']
        funcList = user_input1 + linear_bin + user_input2
        return random.choice(funcList)

    def formatPercentile(self, expr):
        rcnt = random.randint(DataBoundary.PERCENTILE_BOUNDARY.value[0], DataBoundary.PERCENTILE_BOUNDARY.value[1])
        p = random.sample(range(DataBoundary.PERCENTILE_BOUNDARY.value[0], DataBoundary.PERCENTILE_BOUNDARY.value[1]*10), rcnt)
        return f'PERCENTILE({expr},{",".join(map(str, p))})'

    def formatStatecount(self, expr):
        val = random.randint(DataBoundary.PERCENTILE_BOUNDARY.value[0], DataBoundary.PERCENTILE_BOUNDARY.value[1])
        oper = random.choice(DataBoundary.STATECOUNT_UNIT.value)
        return f'STATECOUNT({expr}, "{oper}", {val})'

    def formatStateduration(self, expr):
        val = random.randint(DataBoundary.PERCENTILE_BOUNDARY.value[0], DataBoundary.PERCENTILE_BOUNDARY.value[1])
        oper = random.choice(DataBoundary.STATECOUNT_UNIT.value)
        unit = random.choice(DataBoundary.TIME_UNIT.value)
        return f'STATEDURATION({expr}, "{oper}", {val}, 1{unit})'

    def formatConcat(self, expr, *args):
        print("---1args:", *args)
        print("---2args:", args)
        base = f'CONCAT("pre_", cast({expr} as nchar({DataBoundary.CONCAT_BOUNDARY.value[1]})))'
        argsVals = list()
        for i in range(len(args[:DataBoundary.CONCAT_BOUNDARY.value[1]-1])):
            print("---args[i]:", args[i])
            argsVals.append(f'cast({args[i]} as nchar({DataBoundary.CONCAT_BOUNDARY.value[1]}))')
        if len(argsVals) == 0:
            return base
        else:
            return f'CONCAT({base}, {", ".join(argsVals)})'

    def formatConcatWs(self, expr, *args):
        separator_expr = random.choice([",", ":", ";", "_", "-"])
        base = f'CONCAT_WS("{separator_expr}", "pre_", cast({expr} as nchar({DataBoundary.CONCAT_BOUNDARY.value[1]})))'
        print("----base", base)
        argsVals = list()
        for i in range(len(args[:DataBoundary.CONCAT_BOUNDARY.value[1]-1])):
            argsVals.append(f'cast({args[i]} as nchar({DataBoundary.CONCAT_BOUNDARY.value[1]}))')
            print("----argsVals", argsVals)
        if len(argsVals) == 0:
            return base
        else:
            return f'CONCAT_WS("{separator_expr}", {base}, {", ".join(argsVals)})'

    def formatSubstr(self, expr):
        pos = random.choice(DataBoundary.SUBSTR_BOUNDARY.value)
        length = random.choice(DataBoundary.SUBSTR_BOUNDARY.value)
        return f'SUBSTR({expr}, {pos}, {length})'

    def formatCast(self, expr, castTypeList):
        return f'CAST({expr} AS {random.choice(castTypeList)})'

    def formatFunc(self, func, colname, castTypeList="nchar", *args, **kwarg):
        if func in ['ABS', 'ACOS', 'ASIN', 'ATAN', 'CEIL', 'COS', 'FLOOR', 'LOG', 'ROUND', 'SIN', 'SQRT', 'TAN'] + \
                    ['AVG', 'COUNT', 'SPREAD', 'STDDEV', 'SUM', 'HYPERLOGLOG'] + \
                    ['FIRST', 'LAST', 'LAST_ROW', 'MAX', 'MIN', 'MODE', 'UNIQUE'] + \
                    ['CSUM', 'IRATE', 'TWA'] + \
                    ['CHAR_LENGTH', 'LENGTH', 'LOWER', 'LTRIM', 'RTRIM', 'UPPER', 'TO_JSON']:
            return f"{func}({colname})"
        elif func in ['NOW', 'TODAY', 'TIMEZONE', 'DATABASES', 'CLIENT_VERSION', 'SERVER_VERSION', 'SERVER_STATUS', 'CURRENT_USER']:
            return f"{func}()"
        elif func in ['TIMEDIFF']:
            return self.formatTimediff(colname)
        elif func in ['TIMEDIFF']:
            return self.formatDiff(colname)
        elif func in ['TIMETRUNCATE']:
            return self.formatTimeTruncate(colname)
        elif func in ['APERCENTILE']:
            return f'{func}({colname}, {random.randint(DataBoundary.PERCENTILE_BOUNDARY.value[0], DataBoundary.PERCENTILE_BOUNDARY.value[1])}, "{random.choice(["default", "t-digest"])}")'
        elif func in ['LEASTSQUARES']:
            return f"{func}({colname}, {random.randint(1, DataBoundary.LEASTSQUARES_BOUNDARY.value[1])}, {random.randint(1, DataBoundary.LEASTSQUARES_BOUNDARY.value[1])})"
        elif func in ['HISTOGRAM']:
            return self.formatHistogram(colname)
        elif func in ['PERCENTILE']:
            return self.formatPercentile(colname)
        elif func in ['ELAPSED']:
            return f"{func}({colname}, 1{random.choice(DataBoundary.TIME_UNIT.value)})"
        elif func in ['POW']:
            return f"{func}({colname}, {random.choice(DataBoundary.SAMPLE_BOUNDARY.value)})"
        elif func in ['INTERP', 'DIFF', 'TO_UNIXTIMESTAMP']:
            return f"{func}({colname}, {random.choice(DataBoundary.IGNORE_NEGATIVE_BOUNDARY.value)})"
        elif func in ['SAMPLE']:
            return f"{func}({colname}, {random.choice(DataBoundary.SAMPLE_BOUNDARY.value)})"
        elif func in ['TAIL']:
            return f"{func}({colname}, {random.choice(DataBoundary.TAIL_BOUNDARY.value)}, {random.choice(DataBoundary.TAIL_OFFSET_BOUNDARY.value)})"
        elif func in ['TOP', 'BOTTOM']:
            return f"{func}({colname}, {random.choice(DataBoundary.TOP_BOUNDARY.value)})"
        elif func in ['DERIVATIVE']:
            return f"{func}({colname}, 1{random.choice(DataBoundary.TIME_UNIT.value[3:])}, {random.choice(DataBoundary.IGNORE_NEGATIVE_BOUNDARY.value)})"
        elif func in ['MAVG']:
            return f"{func}({colname}, {random.choice(DataBoundary.MAVG_BOUNDARY.value)})"
        elif func in ['STATECOUNT']:
            return self.formatStatecount(colname)
        elif func in ['STATEDURATION']:
            return self.formatStateduration(colname)
        elif func in ['CONCAT']:
            print("----args", args)
            return self.formatConcat(colname, *args)
        elif func in ['CONCAT_WS']:
            return self.formatConcatWs(colname, *args)
        elif func in ['SUBSTR']:
            return self.formatSubstr(colname, *args)
        elif func in ['CAST']:
            return self.formatCast(colname, castTypeList)
        elif func in ['TO_ISO8601']:
            timezone = random.choice(["+00:00", "-00:00", "+08:00", "-08:00", "+12:00", "-12:00"])
            return f'{func}({colname}, "{timezone}")'
        elif func in ['TO_CHAR', 'TO_TIMESTAMP']:
            return f'{func}({colname}, "{random.choice(DataBoundary.TO_CHAR_UNIT.value)}")'
        else:
            pass

    def getShowSql(self, dbname, tbname, ctbname):
        showSql = random.choice(DataBoundary.SHOW_UNIT.value)
        if "DISTRIBUTED" in showSql:
            return f'{showSql} {tbname};'
        elif "SHOW CREATE DATABASE" in showSql:
            return f'{showSql} {dbname};'
        elif "SHOW CREATE STABLE" in showSql:
            return f'{showSql} {tbname};'
        elif "SHOW CREATE TABLE" in showSql:
            return f'{showSql} {ctbname};'
        elif "SHOW TAGS" in showSql:
            return f'{showSql} {ctbname};'
        else:
            return f'{showSql};'

    def getSystableSql(self):
        '''
        sysdb = random.choice(DataBoundary.SYSTABLE_UNIT.value
        self.tdSql.query(f'show {sysdb}.tables')
        systableList = list(map(lambda x:x[0], self.tdSql.query_data))
        systable = random.choice(systableList)
        if systable == "ins_tables":
            return f'select * from {systable} limit({DataBoundary.LIMIT_BOUNDARY.value})'
        else:
            return f'select * from {systable}'
        '''
        return "select * from information_schema.ins_stables;"

    def getSlimitValue(self, rand=None):
        useTag = random.choice([True, False]) if rand is None else True
        if useTag:
            slimitValList = [f'SLIMIT {random.randint(1, DataBoundary.LIMIT_BOUNDARY.value)}', f'SLIMIT {random.randint(1, DataBoundary.LIMIT_BOUNDARY.value)}, {random.randint(1, DataBoundary.LIMIT_BOUNDARY.value)}']
            slimitVal = random.choice(slimitValList)
        else:
            slimitVal = ""
        return slimitVal

    def setGroupTag(self, fm, funcList):
        """
        Check if there are common elements between `funcList` and `selectFuncs` in `fm`.

        Args:
            fm (dict): The dictionary containing the 'selectFuncs' key.
            funcList (list): The list of functions to compare with 'selectFuncs'.

        Returns:
            bool: True if there are common elements, False otherwise.
        """
        selectFuncs = fm['selectFuncs']
        s1 = set(funcList)
        s2 = set(selectFuncs)
        common_elements = s1 & s2
        if len(common_elements) > 1:
            return True
        else:
            return False

    def selectFuncsFromType(self, fm, colname, column_type, doAggr, subquery=False):
        if doAggr == 0:
            categoryList = ['aggFuncs']
        elif doAggr == 1:
            categoryList = ['mathFuncs', 'strFuncs', 'timeFuncs', 'selectFuncs', 'castFuncs']
        elif doAggr == 2:
            categoryList = ['VariableFuncs']
        elif doAggr == 3:
            categoryList = ['specialFuncs']
        else:
            return
        funcList = list()
        print("----categoryList", categoryList)
        print("----fm", fm)

        for category in categoryList:
            funcList += fm[category]
        print("----funcList", funcList)
        if subquery:
            funcList = [func for func in funcList if func not in fm['unsupported']]
        selectItems = random.sample(funcList, random.randint(1, len(funcList))) if len(funcList) > 0 else list()
        funcStrList = list()
        for func in selectItems:
            print("----func", func)
            funcStr = self.formatFunc(func, colname, fm["castTypes"])
            print("----funcStr", funcStr)
            funcStrList.append(funcStr)
        print("-------funcStrList:", funcStrList)
        print("-------funcStr:", ",".join(funcStrList))
        print("----selectItems", selectItems)

        if "INT" in column_type:
            groupKey = colname if self.setGroupTag(fm, funcList) else ""
        else:
            groupKey = colname if self.setGroupTag(fm, funcList) else ""
        if doAggr == 2:
            if len(funcStrList) > 0:
                return ",".join([random.choice(funcStrList)]), groupKey
            else:
                return "", groupKey
        return ",".join(funcStrList), groupKey


        # # colTypes = ['NUMERIC', 'TEXT', 'BINARY', 'BOOLEAN', 'TIMESTAMP'...]
        # colTypes = [member.name for member in FunctionMap]
        # # cateDict = {'NUMERIC': '......', 'TEXT': '......', 'BINARY': '......', 'BOOLEAN': '......', 'TIMESTAMP': '......'...}
        # print(fm)
        # cateDict = fm.value
        # # categories = ['mathFuncs', 'strFuncs', 'timeFuncs', 'aggFuncs', 'selectFuncs', 'specialFuncs']
        # categories = [key for key in cateDict.keys() if "Funcs" in key]
        # for category in categories:
        #     if FunctionMap[category]:
        #         func = random.choice()
        #         pass
        '''
    fm:
        {
            'types': ['TINYINT', 'SMALLINT', 'INT', 'BIGINT', 'TINYINT UNSIGNED', 'SMALLINT UNSIGNED', 'INT UNSIGNED', 'BIGINT UNSIGNED', 'FLOAT', 'DOUBLE'],
            'mathFuncs': ['ABS', 'ACOS', 'ASIN', 'ATAN', 'CEIL', 'COS', 'FLOOR', 'LOG', 'POW', 'ROUND', 'SIN', 'SQRT', 'TAN'],
            'strFuncs': [],
            'timeFuncs': ['NOW', 'TIMEDIFF', 'TIMEZONE', 'TODAY'],
            'aggFuncs': ['APERCENTILE', 'AVG', 'COUNT', 'LEASTSQUARES', 'SPREAD', 'STDDEV', 'SUM', 'HYPERLOGLOG', 'HISTOGRAM', 'PERCENTILE'],
            'selectFuncs': ['BOTTOM', 'FIRST', 'INTERP', 'LAST', 'LAST_ROW', 'MAX', 'MIN', 'MODE', 'SAMPLE', 'TAIL', 'TOP', 'UNIQUE'],
            'specialFuncs': ['CSUM', 'DERIVATIVE', 'DIFF', 'IRATE', 'MAVG', 'STATECOUNT', 'STATEDURATION', 'TWA'],
            'castFuncs': ['CAST', 'TO_ISO8601'],
            'castTypes': ['TINYINT', 'SMALLINT', 'INT', 'BIGINT', 'TINYINT UNSIGNED', 'SMALLINT UNSIGNED', 'INT UNSIGNED', 'BIGINT UNSIGNED', 'FLOAT', 'DOUBLE', 'BINARY', 'VARCHAR', 'NCHAR', 'BOOL', 'TIMESTAMP']
        }
        '''
        # for category in categories:
        #     if fm[category]:
        #         func = random.choice(fm[category])
        #         if func == 'CAST':
        #             cast_type = random.choice(fm['castTypes'])
        #             return f"{func}({colname} AS {cast_type})"
        #         return f"{func}({colname})"
        # return f"COUNT({colname})"  # Default fallback

    def getFuncCategory(self, doAggr):
        if doAggr == 0:
            return "aggFuncs"
        elif doAggr == 1:
            return "nFuncs"
        else:
            return "spFuncs"

    def getRandomTimeUnitStr(self, stream=False):
        """
        Generates a random time unit string.

        Returns:
            str: A string representing a random time unit.
        """
        if stream:
            return f'{random.randint(*DataBoundary.SAMPLE_BOUNDARY.value)}{random.choice(DataBoundary.TIME_UNIT.value[3:])}'
        else:
            return f'{random.randint(*DataBoundary.SAMPLE_BOUNDARY.value)}{random.choice(DataBoundary.TIME_UNIT.value)}'

    def sepTimeStr(self, timeStr):
        match = re.match(r"(\d+)(\D+)", timeStr)
        return int(match.group(1)), match.group(2)

    def getOffsetFromInterval(self, interval):
        _, unit = self.sepTimeStr(interval)
        idx = DataBoundary.TIME_UNIT.value.index(unit)
        return f'{random.randint(*DataBoundary.SAMPLE_BOUNDARY.value)}{random.choice(DataBoundary.TIME_UNIT.value[2:idx])}'

    def getRandomWindow(self):
        """
        Returns a random window from the available window units.

        Returns:
            str: A random window unit.
        """
        return random.choice(DataBoundary.WINDOW_UNIT.value)

    def getOffsetValue(self, rand=None):
        """
        Returns the offset value for a SQL query.

        Args:
            rand (bool, optional): If True, a random offset value will be used. If False, a time unit string will be used. Defaults to None.

        Returns:
            str: The offset value for the SQL query.
        """
        useTag = random.choice([True, False]) if rand is None else True
        offsetVal = f',{self.getRandomTimeUnitStr()}' if useTag else ""
        return offsetVal

    def getSlidingValue(self, rand=None):
        """
        Get the sliding value for the SQL query.

        Parameters:
        - rand (bool, optional): If True, use a random value for the sliding value. If False, use a predefined value. Defaults to None.

        Returns:
        - slidingVal (str): The sliding value for the SQL query.
        """
        useTag = random.choice([True, False]) if rand is None else True
        slidingVal = f'SLIDING({self.getRandomTimeUnitStr()})' if useTag else ""
        return slidingVal

    def getOrderByValue(self, col, rand=None):
        useTag = random.choice([True, False]) if rand is None else True
        orderType = random.choice(["ASC", "DESC", ""]) if useTag else ""
        orderVal = f'ORDER BY {col} {orderType}' if useTag else ""
        return orderVal

    def getFillValue(self, rand=None):
        """
        Returns a fill value for SQL queries.

        Parameters:
        - rand (bool, optional): If True, a random fill value will be used. If False, an empty string will be returned. Defaults to None.

        Returns:`
        - str: The fill value for SQL queries.
        """
        useTag = random.choice([True, False]) if rand is None else True
        fillVal = f'FILL({random.choice(DataBoundary.FILL_UNIT.value)})' if useTag else ""
        return fillVal

    def getTimeRange(self, tbname, tsCol):
        # self.tdSql.query(f"SELECT first({tsCol}), last({tsCol}) FROM {tbname} tail({tsCol}, 100, 50);")
        # res = self.tdSql.query_data
        res = [('2024-08-26 14:00:00.000',), ('2024-08-26 18:00:00.000',)]
        return [res[0][0], res[1][0]]

    def getTimeRangeFilter(self, tbname, tsCol, rand=None, doAggr=0):
        useTag = random.choice([True, False]) if rand is None else True
        if useTag:
            start_time, end_time = self.getTimeRange(tbname, tsCol)
            timeRangeFilter = random.choice([f'WHERE {tsCol} BETWEEN "{start_time}" AND "{end_time}"', f'where {tsCol} > "{start_time}" AND {tsCol} < "{end_time}"'])
            if doAggr != 0:
                timeRangeFilter = random.choice([timeRangeFilter, "", "", "", ""])
        else:
            timeRangeFilter = ""
        return timeRangeFilter

    def getPartitionValue(self, col, rand=None):
        useTag = random.choice([True, False]) if rand is None else True
        partitionVal = f'PARTITION BY {col}' if useTag else ""
        return partitionVal

    def getPartitionObj(self, rand=None):
        useTag = random.choice([True, False]) if rand is None else True
        partitionObj = f'PARTITION BY {random.choice(DataBoundary.ALL_TYPE_UNIT.value)}' if useTag else ""
        return partitionObj

    def getEventWindowCondition(self, colDict):
        conditionList = list()
        lteList = ["<", "<="]
        gteList = [">", ">="]
        enqList = ["=", "!=", "<>"]
        nullList = ["is null", "is not null"]
        inList = ["in", "not in"]
        betweenList = ["between", "not between"]
        likeList = ["like", "not like"]
        matchList = ["match", "nmatch"]
        tinyintRangeList = DataBoundary.TINYINT_BOUNDARY.value
        intHalfBf = random.randint(tinyintRangeList[0], round((tinyintRangeList[1]+tinyintRangeList[0])/2))
        intHalfAf = random.randint(round((tinyintRangeList[1]+tinyintRangeList[0])/2), tinyintRangeList[1])
        for columnName, columnType in colDict.items():
            print(f"-----column_name, column_type: {columnName}, {columnType}")
            if columnType in ['TINYINT', 'SMALLINT', 'INT', 'BIGINT', 'TINYINT UNSIGNED', 'SMALLINT UNSIGNED', 'INT UNSIGNED', 'BIGINT UNSIGNED', ]:
                startTriggerCondition = f'{columnName} {inList[0]} {tuple(random.randint(0, 100) for _ in range(10))} or {columnName} {betweenList[0]} {intHalfBf} and {intHalfAf}'
                endTriggerCondition = f'{columnName} {inList[1]} {tuple(random.randint(0, 100) for _ in range(10))} and {columnName} {betweenList[1]} {intHalfBf} and {intHalfAf}'
            elif columnType in ['FLOAT', 'DOUBLE']:
                startTriggerCondition = f'{columnName} {random.choice(lteList)} {intHalfBf}'
                endTriggerCondition = f'{columnName} {random.choice(gteList)} {intHalfAf}'
            elif columnType in ['BINARY', 'VARCHAR', 'NCHAR']:
                startTriggerCondition = f'{columnName} {likeList[0]} "%a%" or {columnName} {matchList[1]} ".*a.*"'
                endTriggerCondition = f'{columnName} {likeList[1]} "_a_" and {columnName} {matchList[0]} ".*a.*"'
            else:
                startTriggerCondition = f'{columnName} {random.choice(enqList)} {intHalfBf} or {columnName} {nullList[0]}'
                endTriggerCondition = f'{columnName} {nullList[1]} and {columnName} {random.choice(enqList[1:])} {intHalfAf}'
            conditionList.append(f'EVENT_WINDOW start with {startTriggerCondition} end with {endTriggerCondition}')
        return random.choice(conditionList)

    def getWindowStr(self, window, colDict, tsCol="ts", stateUnit="1", countUnit="2", stream=False):
        if window == "INTERVAL":
            interval = self.getRandomTimeUnitStr(stream=stream)
            offset = self.getOffsetFromInterval(interval)
            return f"{window}({interval},{offset})"
        elif window == "SESSION":
            return f"{window}({tsCol}, {self.getRandomTimeUnitStr(stream=stream)})"
        elif window == "STATE_WINDOW":
            return f"{window}({stateUnit})"
        elif window == "COUNT_WINDOW":
            return f"{window}({countUnit})"
        elif window == "EVENT_WINDOW":
            return self.getEventWindowCondition(colDict)
        else:
            return ""

    def getFillHistoryValue(self, rand=None):
        useTag = random.choice([True, False]) if rand is None else True
        fillHistoryVal = f'FILL_HISTORY 1' if useTag else random.choice(["FILL_HISTORY 0", ""])
        return fillHistoryVal

    def getExpiredValue(self, rand=None, countWindow=False):
        if countWindow:
            return random.choice(["IGNORE EXPIRED 1", ""])
        useTag = random.choice([True, False]) if rand is None else True
        expiredVal = f'IGNORE EXPIRED 0' if useTag else random.choice(["IGNORE EXPIRED 1", ""])
        return expiredVal

    def getUpdateValue(self, rand=None):
        useTag = random.choice([True, False]) if rand is None else True
        updateVal = f'IGNORE UPDATE 0' if useTag else random.choice(["IGNORE UPDATE 1", ""])
        return updateVal

    def getTriggerValue(self, forceTrigger=None):
        if forceTrigger is not None:
            return f"TRIGGER {forceTrigger}"
        maxDelayTime = random.choice(DataBoundary.MAX_DELAY_UNIT.value)
        return random.choice(["TRIGGER AT_ONCE", "TRIGGER WINDOW_CLOSE", f"TRIGGER MAX_DELAY {maxDelayTime}", ""])

    def getDeleteMarkValue(self):
        deleteMarkTime = random.choice(DataBoundary.DELETE_MARK_UNIT.value)
        return random.choice([f"DELETE_MARK {deleteMarkTime}", ""])

    def getWatermarkValue(self, rand=None):
        useTag = random.choice([True, False]) if rand is None else True
        timeVal = random.choice(DataBoundary.WATERMARK_UNIT.value)
        watermarkTime = f"WATERMARK {timeVal}" if useTag else random.choice([f"WATERMARK {timeVal}", ""])
        return watermarkTime

    def getSubtableValue(self, partitionList):
        subTablePre = "pre"
        partitionList = ['tbname']
        for colname in partitionList:
            subtable = f'CONCAT("{subTablePre}", {colname})'
        return random.choice([f'SUBTABLE({subtable})', ""])

    def remove_duplicates(self, selectPartStr):
        parts = selectPartStr.split(',')
        seen_now = seen_today = seen_timezone = False
        result = []
        for part in parts:
            part_stripped = part.strip()
            if part_stripped == "NOW()":
                if not seen_now:
                    seen_now = True
                    result.append(part)
            elif part_stripped == "TODAY()":
                if not seen_today:
                    seen_today = True
                    result.append(part)
            elif part_stripped == "TIMEZONE()":
                if not seen_timezone:
                    seen_timezone = True
                    result.append(part)
            else:
                result.append(part)
        return ', '.join(result)

# CREATE STREAM IF NOT EXISTS stm_stb TRIGGER WINDOW_CLOSE WATERMARK 1y  IGNORE EXPIRED 1  into stm_stb_target   SUBTABLE(CONCAT("pre", tbname)) AS SELECT TO_CHAR(ts, "dy"),TIMETRUNCATE(ts, 1d, 1), POW(c1, 1),ROUND(c1),TODAY(),SIN(c1),ASIN(c1),TO_ISO8601(c1, "-08:00"),CEIL(c1),CAST(c1 AS BIGINT),MAX(c1),SQRT(c1),LOG(c1),MIN(c1),LAST_ROW(c1),TAN(c1),FIRST(c1),ABS(c1),FLOOR(c1),ACOS(c1),COS(c1),TIMEZONE(), CONCAT("pre_", cast(c2 as nchar(8))),CONCAT_WS(",", "pre_", cast(c2 as nchar(8))), LAST(c3),CAST(c3 AS SMALLINT),NOW(),FIRST(c3),LAST_ROW(c3) FROM test.stb partition BY ts,c1,c2,c3  ;;
# CREATE STREAM IF NOT EXISTS stm_stb3 TRIGGER AT_ONCE WATERMARK 1y  IGNORE EXPIRED 1  into stm_stb_target3   SUBTABLE(CONCAT("pre", tbname)) AS SELECT ts, TO_CHAR(ts, "dy"),TIMETRUNCATE(ts, 1d, 1), POW(c1, 1),ROUND(c1),TODAY(),SIN(c1),ASIN(c1),TO_ISO8601(c1, "-08:00"),CEIL(c1),CAST(c1 AS BIGINT),MAX(c1),SQRT(c1),LOG(c1),MIN(c1),LAST_ROW(c1),TAN(c1),FIRST(c1),ABS(c1),FLOOR(c1),ACOS(c1),COS(c1),TIMEZONE(), CONCAT("pre_", cast(c2 as nchar(8))),CONCAT_WS(",", "pre_", cast(c2 as nchar(8))), LAST(c3),CAST(c3 AS SMALLINT),NOW(),FIRST(c3),LAST_ROW(c3) FROM test.stb partition by tbname group BY ts,c1,c2,c3 ;;
# CREATE STREAM  stm_stb TRIGGER at_once into stm_stb_target AS SELECT ts,TO_CHAR(ts, "dy"),TIMETRUNCATE(ts, 1d, 1),SIN(c1) FROM test.stb partition by tbname;;
    def generateRandomSubQuery(self, colDict, tbname, subtable=False, doAggr=0):
        self._dbName = "test"
        selectPartList = []
        groupKeyList = []
        colTypes = [member.name for member in FunctionMap]
        tsCol = "ts"
        for column_name, column_type in colDict.items():
            if column_type == "TIMESTAMP":
                tsCol = column_name
            for fm in FunctionMap:
                if column_type in fm.value['types']:
                    selectStrs, groupKey = self.selectFuncsFromType(fm.value, column_name, column_type, doAggr, True)
                    if len(selectStrs) > 0:
                        selectPartList.append(selectStrs)
                    if len(groupKey) > 0:
                        groupKeyList.append(groupKey)

        if doAggr == 2:
            selectPartList = [random.choice(selectPartList)] if len(selectPartList) > 0 else ["count(*)"]
        if doAggr == 1:
            selectPartList = [tsCol] + selectPartList
        selectPartStr = ', '.join(selectPartList)
        selectPartStr = self.remove_duplicates(selectPartStr)
        if len(groupKeyList) > 0:
            groupKeyStr = ",".join(groupKeyList)
            partitionVal = self.getPartitionValue(groupKeyStr)
            if subtable:
                partitionVal = f'{partitionVal},tbname' if len(partitionVal) > 0 else "partition by tbname"
            return f"SELECT {selectPartStr} FROM {self._dbName}.{tbname} {partitionVal} GROUP BY {groupKeyStr} {self.getSlimitValue()};"
        else:
            groupKeyStr = "tbname"
            partitionVal = "partition by tbname" if subtable else ""
        randomSelectPart = f'`{random.choice(selectPartList)}`' if len(selectPartList) > 0 else groupKeyStr
        windowStr = self.getWindowStr(self.getRandomWindow(), colDict, stream=True)
        if ("COUNT_WINDOW" in windowStr or "STATE_WINDOW" in windowStr or "EVENT_WINDOW" in windowStr) and "partition" not in partitionVal:
            windowStr = f"partition by tbname {windowStr}"
        return f"SELECT {selectPartStr} FROM {self._dbName}.{tbname} {self.getTimeRangeFilter(tbname, tsCol, doAggr=doAggr)} {partitionVal} {windowStr};"


    def genCreateStreamSql(self, colDict, tbname):
        doAggr = random.choice([0, 1, 2])
        forceTrigger = "AT_ONCE" if doAggr == 1 else None
        streamName = f'{self._dbName}_stm_{tbname}'
        target = f'stm_{tbname}_target'
        # TODO
        existStbFields = ""
        customTags = ""
        subtable = self.getSubtableValue(colDict.keys())
        subQuery = self.generateRandomSubQuery(colDict, tbname, subtable, doAggr)
        expiredValue = self.getExpiredValue(countWindow=True) if "COUNT_WINDOW" in subQuery else self.getExpiredValue()
        streamOps = f'{self.getTriggerValue(forceTrigger)} {self.getWatermarkValue()} {self.getFillHistoryValue()} {expiredValue} {self.getUpdateValue()}'
        if ("COUNT_WINDOW" in subQuery or "STATE_WINDOW" in subQuery) and "WATERMARK" not in streamOps:
            streamOps = f'{self.getTriggerValue(forceTrigger)} {self.getWatermarkValue(True)} {self.getFillHistoryValue()} {expiredValue} {self.getUpdateValue()}'
        return f"CREATE STREAM IF NOT EXISTS {streamName} {streamOps} into {target} {existStbFields} {customTags} {subtable} AS {subQuery};"

    def generateRandomSql(self, colDict, tbname):
        selectPartList = []
        groupKeyList = []
        # colTypes = ['NUMERIC', 'TEXT', 'BINARY', 'BOOLEAN', 'TIMESTAMP'...]
        colTypes = [member.name for member in FunctionMap]
        print("-----colDict", colDict)
        print("-----colTypes", colTypes)
        doAggr = random.choice([0, 1, 2, 3, 4, 5])
        if doAggr == 4:
            return self.getShowSql("test", "stb", "ctb")
        if doAggr == 5:
            return self.getSystableSql()
        tsCol = "ts"
        for column_name, column_type in colDict.items():
            print(f"-----column_name, column_type: {column_name}, {column_type}")
            if column_type == "TIMESTAMP":
                tsCol = column_name
            for fm in FunctionMap:
                if column_type in fm.value['types']:
                    # cateDict = {'NUMERIC': '......', 'TEXT': '......', 'BINARY': '......', 'BOOLEAN': '......', 'TIMESTAMP': '......'...}
                    # cateDict = fm.value
                    # print("-----cateDict", cateDict)
                    # categories = ['mathFuncs', 'strFuncs', 'timeFuncs', 'aggFuncs', 'selectFuncs', 'specialFuncs']
                    # categories = [key for key in cateDict.keys() if "Funcs" in key]
                    # print("-----categories", categories)
                    # / selectCate = 'mathFuncs'
                    # selectCate = random.choice(categories)
                    # print("-----selectCate", selectCate)
                    selectStrs, groupKey = self.selectFuncsFromType(fm.value, column_name, column_type, doAggr)
                    print("-----selectStrs", selectStrs)
                    if len(selectStrs) > 0:
                        selectPartList.append(selectStrs)
                    print("-----selectPartList", selectPartList)
                    if len(groupKey) > 0:
                        groupKeyList.append(groupKey)
                    
                    # / funcs = ['ABS', 'ACOS', 'ASIN'....]
                    # funcs = random.sample(fm.value[selectFunc], random.randint(1, len(fm.value[selectFunc])))
                    # print("-----funcs", funcs)
                    print("\n")
                    # if func == 'CAST':
                    #     cast_type = random.choice(FunctionMap['castTypes'])
                    #     func_expression = f"{func}({column_name} AS {cast_type})"
                    #     select_parts.append(func_expression)
                    # else:
                    #     func_expression = f"{func}({column_name})"
                    #     select_parts.append(func_expression)
                    # pass
                    # func_expression = self.selectFuncsFromType(fm.value, column_name)
                    # select_parts.append(func_expression)
        
        if doAggr == 2:
            selectPartList = [random.choice(selectPartList)]
        if len(groupKeyList) > 0:
            groupKeyStr = ",".join(groupKeyList)
            return f"SELECT {', '.join(selectPartList)} FROM {tbname} GROUP BY {groupKeyStr} {self.getOrderByValue(groupKeyStr)} {self.getSlimitValue()};"
        else:
            groupKeyStr = "tbname"
        randomSelectPart = f'`{random.choice(selectPartList)}`'
        return f"SELECT {', '.join(selectPartList)} FROM {tbname} {self.getTimeRangeFilter(tbname, tsCol)} {self.getPartitionValue(groupKeyStr)} {self.getWindowStr(self.getRandomWindow(), colDict)} {self.getSlidingValue()} {self.getOrderByValue(randomSelectPart)} {self.getSlimitValue()};"

sqllancer = SQLLancer()
colDict = {"ts": "TIMESTAMP", "c1": "INT", "c2": "NCHAR", "c3": "BOOL"}
tbname = "stb"
# print(sqllancer.formatConcat("c1", "c2", "c3"))
# randomSql = sqllancer.generateRandomSql(colDict, tbname)
# print(randomSql)
randomStream = sqllancer.genCreateStreamSql(colDict, tbname)
print(randomStream)


    # def generateQueries_n(self, dbc: DbConn, selectItems) -> List[SqlQuery]:
    #     ''' Generate queries to test/exercise this super table '''
    #     ret = []  # type: List[SqlQuery]


    #     for rTbName in self.getRegTables(dbc):  # regular tables

    #         filterExpr = Dice.choice([  # TODO: add various kind of WHERE conditions
    #             None
    #         ])

    #         # Run the query against the regular table first
    #         doAggr = (Dice.throw(2) == 0)  # 1 in 2 chance
    #         if not doAggr:  # don't do aggregate query, just simple one
    #             query_parts = []
    #             for colName, colType in selectItems.items():
    #                 for groupKey, group in FunctionMap:
    #                     query_parts.append(colName)

    #             commonExpr = Dice.choice([
    #                 '*',
    #                 'abs(speed)',
    #                 'acos(speed)',
    #                 'asin(speed)',
    #                 'atan(speed)',
    #                 'ceil(speed)',
    #                 'cos(speed)',
    #                 'cos(speed)',
    #                 'floor(speed)',
    #                 'log(speed,2)',
    #                 'pow(speed,2)',
    #                 'round(speed)',
    #                 'sin(speed)',
    #                 'sqrt(speed)',
    #                 'char_length(color)',
    #                 'concat(color,color)',
    #                 'concat_ws(" ", color,color," ")',
    #                 'length(color)',
    #                 'lower(color)',
    #                 'ltrim(color)',
    #                 'substr(color , 2)',
    #                 'upper(color)',
    #                 'cast(speed as double)',
    #                 'cast(ts as bigint)',
    #                 # 'TO_ISO8601(color)',
    #                 # 'TO_UNIXTIMESTAMP(ts)',
    #                 'now()',
    #                 'timediff(ts,now)',
    #                 'timezone()',
    #                 'TIMETRUNCATE(ts,1s)',
    #                 'TIMEZONE()',
    #                 'TODAY()',
    #                 'distinct(color)'
    #             ]
    #             )
    #             ret.append(SqlQuery(  # reg table
    #                 "select {} from {}.{}".format(commonExpr, self._dbName, rTbName)))
    #             ret.append(SqlQuery(  # super table
    #                 "select {} from {}.{}".format(commonExpr, self._dbName, self.getName())))
    #         else:  # Aggregate query
    #             aggExpr = Dice.choice([
    #                 'count(*)',
    #                 'avg(speed)',
    #                 # 'twa(speed)', # TODO: this one REQUIRES a where statement, not reasonable
    #                 'sum(speed)',
    #                 'stddev(speed)',
    #                 # SELECTOR functions
    #                 'min(speed)',
    #                 'max(speed)',
    #                 'first(speed)',
    #                 'last(speed)',
    #                 'top(speed, 50)',  # TODO: not supported?
    #                 'bottom(speed, 50)',  # TODO: not supported?
    #                 'apercentile(speed, 10)',  # TODO: TD-1316
    #                 'last_row(*)',  # TODO: commented out per TD-3231, we should re-create
    #                 # Transformation Functions
    #                 # 'diff(speed)', # TODO: no supported?!
    #                 'spread(speed)',
    #                 'elapsed(ts)',
    #                 'mode(speed)',
    #                 'bottom(speed,1)',
    #                 'top(speed,1)',
    #                 'tail(speed,1)',
    #                 'unique(color)',
    #                 'csum(speed)',
    #                 'DERIVATIVE(speed,1s,1)',
    #                 'diff(speed,1)',
    #                 'irate(speed)',
    #                 'mavg(speed,3)',
    #                 'sample(speed,5)',
    #                 'STATECOUNT(speed,"LT",1)',
    #                 'STATEDURATION(speed,"LT",1)',
    #                 'twa(speed)'

    #             ])  # TODO: add more from 'top'

    #             # if aggExpr not in ['stddev(speed)']: # STDDEV not valid for super tables?! (Done in TD-1049)
    #             sql = "select {} from {}.{}".format(aggExpr, self._dbName, self.getName())
    #             if Dice.throw(3) == 0:  # 1 in X chance
    #                 partion_expr = Dice.choice(['color', 'tbname'])
    #                 sql = sql + ' partition BY ' + partion_expr + ' order by ' + partion_expr
    #                 Progress.emit(Progress.QUERY_GROUP_BY)
    #                 # Logging.info("Executing GROUP-BY query: " + sql)
    #             ret.append(SqlQuery(sql))

    #     return ret

