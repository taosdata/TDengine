from random import randrange
import time
import threading
import secrets
from util.log import *
from util.sql import *
from util.cases import *
from util.dnodes import *
from util.common import *
# from tmqCommon import *

ROUND = 100

ignore_some_tests: int = 1

class TSMA:
    def __init__(self):
        self.tsma_name = ''
        self.db_name = ''
        self.original_table_name = ''
        self.funcs = []
        self.cols = []
        self.interval: str = ''


class UsedTsma:
    TS_MIN = '-9223372036854775808'
    TS_MAX = '9223372036854775806'
    TSMA_RES_STB_POSTFIX = '_tsma_res_stb_'

    def __init__(self) -> None:
        self.name = ''  # tsma name or table name
        self.time_range_start: float = float(UsedTsma.TS_MIN)
        self.time_range_end: float = float(UsedTsma.TS_MAX)
        self.is_tsma_ = False

    def __eq__(self, __value: object) -> bool:
        if isinstance(__value, self.__class__):
            return self.name == __value.name \
                and self.time_range_start == __value.time_range_start \
                and self.time_range_end == __value.time_range_end \
                and self.is_tsma_ == __value.is_tsma_
        else:
            return False

    def __ne__(self, __value: object) -> bool:
        return not self.__eq__(__value)

    def __str__(self) -> str:
        return "%s: from %s to %s is_tsma: %d" % (self.name, self.time_range_start, self.time_range_end, self.is_tsma_)

    def __repr__(self) -> str:
        return self.__str__()

    def setIsTsma(self):
        self.is_tsma_ = self.name.endswith(self.TSMA_RES_STB_POSTFIX)
        if not self.is_tsma_:
            self.is_tsma_ = len(self.name) == 32  # for tsma output child table

class TSMAQueryContext:
    def __init__(self) -> None:
        self.sql = ''
        self.used_tsmas: List[UsedTsma] = []
        self.ignore_tsma_check_ = False
        self.ignore_res_order_ = False

    def __eq__(self, __value) -> bool:
        if isinstance(__value, self.__class__):
            if self.ignore_tsma_check_ or __value.ignore_tsma_check_:
                return True
            if len(self.used_tsmas) != len(__value.used_tsmas):
                return False
            for used_tsma1, used_tsma2 in zip(self.used_tsmas, __value.used_tsmas):
                if not used_tsma1 == used_tsma2:
                    return False
            return True
        else:
            return False

    def __ne__(self, __value: object) -> bool:
        return self.__eq__(__value)

    def __str__(self) -> str:
        return str(self.used_tsmas)

    def has_tsma(self) -> bool:
        for tsma in self.used_tsmas:
            if tsma.is_tsma_:
                return True
        return False


class TSMAQCBuilder:
    def __init__(self) -> None:
        self.qc_: TSMAQueryContext = TSMAQueryContext()

    def get_qc(self) -> TSMAQueryContext:
        return self.qc_

    def with_sql(self, sql: str):
        self.qc_.sql = sql
        return self

    def to_timestamp(self, ts: str) -> float:
        if ts == UsedTsma.TS_MAX or ts == UsedTsma.TS_MIN:
            return float(ts)
        tdSql.query(
            "select to_timestamp('%s', 'yyyy-mm-dd hh24-mi-ss.ms')" % (ts))
        res = tdSql.queryResult[0][0]
        return res.timestamp() * 1000

    def md5(self, buf: str) -> str:
        tdSql.query(f'select md5("{buf}")')
        res = tdSql.queryResult[0][0]
        return res

    def should_query_with_table(self, tb_name: str, ts_begin: str = UsedTsma.TS_MIN, ts_end: str = UsedTsma.TS_MAX) -> 'TSMAQCBuilder':
        used_tsma: UsedTsma = UsedTsma()
        used_tsma.name = tb_name
        used_tsma.time_range_start = self.to_timestamp(ts_begin)
        used_tsma.time_range_end = self.to_timestamp(ts_end)
        used_tsma.is_tsma_ = False
        self.qc_.used_tsmas.append(used_tsma)
        return self

    def should_query_with_tsma_ctb(self, db_name: str, tsma_name: str, ctb_name: str, ts_begin: str = UsedTsma.TS_MIN, ts_end: str = UsedTsma.TS_MAX) -> 'TSMAQCBuilder':
        used_tsma: UsedTsma = UsedTsma()
        name = f'1.{db_name}.{tsma_name}_{ctb_name}'
        used_tsma.name = self.md5(name)
        used_tsma.time_range_start = self.to_timestamp(ts_begin)
        used_tsma.time_range_end = self.to_timestamp(ts_end)
        used_tsma.is_tsma_ = True
        self.qc_.used_tsmas.append(used_tsma)
        return self

    def ignore_query_table(self):
        self.qc_.ignore_tsma_check_ = True
        return self

    def ignore_res_order(self, ignore: bool):
        self.qc_.ignore_res_order_ = ignore
        return self

    def should_query_with_tsma(self, tsma_name: str, ts_begin: str = UsedTsma.TS_MIN, ts_end: str = UsedTsma.TS_MAX, child_tb: bool = False) -> 'TSMAQCBuilder':
        used_tsma: UsedTsma = UsedTsma()
        if child_tb:
            used_tsma.name = tsma_name
        else:
            used_tsma.name = tsma_name + UsedTsma.TSMA_RES_STB_POSTFIX
        used_tsma.time_range_start = self.to_timestamp(ts_begin)
        used_tsma.time_range_end = self.to_timestamp(ts_end)
        used_tsma.is_tsma_ = True
        self.qc_.used_tsmas.append(used_tsma)
        return self


class TSMATester:
    def __init__(self, tdSql: TDSql) -> None:
        self.tsmas = []
        self.tdSql: TDSql = tdSql

    def explain_sql(self, sql: str):
        tdSql.execute("alter local 'querySmaOptimize' '1'")
        sql = "explain verbose true " + sql
        tdSql.query(sql, queryTimes=1)
        res = self.tdSql.queryResult
        if self.tdSql.queryResult is None:
            raise
        return res

    def get_tsma_query_ctx(self, sql: str):
        explain_res = self.explain_sql(sql)
        query_ctx: TSMAQueryContext = TSMAQueryContext()
        query_ctx.sql = sql
        query_ctx.used_tsmas = []
        used_tsma: UsedTsma = UsedTsma()
        for row in explain_res:
            row = str(row)
            if len(used_tsma.name) == 0:
                idx = row.find("Table Scan on ")
                if idx >= 0:
                    words = row[idx:].split(' ')
                    used_tsma.name = words[3]
                    used_tsma.setIsTsma()
            else:
                idx = row.find('Time Range:')
                if idx >= 0:
                    row = row[idx:].split('[')[1]
                    row = row.split(']')[0]
                    words = row.split(',')
                    used_tsma.time_range_start = float(words[0].strip())
                    used_tsma.time_range_end = float(words[1].strip())
                    query_ctx.used_tsmas.append(used_tsma)
                    used_tsma = UsedTsma()

        deduplicated_tsmas: list[UsedTsma] = []
        if len(query_ctx.used_tsmas) > 0:
            deduplicated_tsmas.append(query_ctx.used_tsmas[0])
            for tsma in query_ctx.used_tsmas:
                if tsma == deduplicated_tsmas[-1]:
                    continue
                else:
                    deduplicated_tsmas.append(tsma)
            query_ctx.used_tsmas = deduplicated_tsmas

        return query_ctx

    def check_explain(self, sql: str, expect: TSMAQueryContext) -> TSMAQueryContext:
        query_ctx = self.get_tsma_query_ctx(sql)
        if not query_ctx == expect:
            tdLog.exit('check explain failed for sql: %s \nexpect: %s \nactual: %s' % (
                sql, str(expect), str(query_ctx)))
        elif expect.has_tsma():
            tdLog.debug('check explain succeed for sql: %s \ntsma: %s' %
                        (sql, str(expect.used_tsmas)))
        has_tsma = False
        for tsma in query_ctx.used_tsmas:
            has_tsma = has_tsma or tsma.is_tsma_
        if not has_tsma and len(query_ctx.used_tsmas) > 1:
            tdLog.exit(
                f'explain err for sql: {sql}, has multi non tsmas, {query_ctx.used_tsmas}')
        return query_ctx

    def check_result(self, sql: str, skip_order: bool = False):
        tdSql.execute("alter local 'querySmaOptimize' '1'")
        tsma_res = tdSql.getResult(sql)

        tdSql.execute("alter local 'querySmaOptimize' '0'")
        no_tsma_res = tdSql.getResult(sql)

        if no_tsma_res is None or tsma_res is None:
            if no_tsma_res != tsma_res:
                tdLog.exit("comparing tsma res for: %s got different rows of result: without tsma: %s, with tsma: %s" % (
                    sql, str(no_tsma_res), str(tsma_res)))
            else:
                return

        if len(no_tsma_res) != len(tsma_res):
            tdLog.exit("comparing tsma res for: %s got different rows of result: \nwithout tsma: %s\nwith    tsma: %s" % (
                sql, str(no_tsma_res), str(tsma_res)))
        if skip_order:
            try:
                no_tsma_res.sort(
                    key=lambda x: [v is None for v in x] + list(x))
                tsma_res.sort(key=lambda x: [v is None for v in x] + list(x))
            except Exception as e:
                tdLog.exit("comparing tsma res for: %s got different data: \nno tsma res: %s \n   tsma res: %s err: %s" % (
                    sql, str(no_tsma_res), str(tsma_res), str(e)))

        for row_no_tsma, row_tsma in zip(no_tsma_res, tsma_res):
            if row_no_tsma != row_tsma:
                tdLog.exit("comparing tsma res for: %s got different row data: no tsma row: %s, tsma row: %s \nno tsma res: %s \n   tsma res: %s" % (
                    sql, str(row_no_tsma), str(row_tsma), str(no_tsma_res), str(tsma_res)))
        tdLog.info('result check succeed for sql: %s. \n   tsma-res: %s. \nno_tsma-res: %s' %
                   (sql, str(tsma_res), str(no_tsma_res)))

    def check_sql(self, sql: str, expect: TSMAQueryContext):
        tdLog.debug(f"start to check sql: {sql}")
        actual_ctx = self.check_explain(sql, expect=expect)
        tdLog.debug(f"ctx: {actual_ctx}")
        if actual_ctx.has_tsma():
            self.check_result(sql, expect.ignore_res_order_)

    def check_sqls(self, sqls, expects):
        for sql, query_ctx in zip(sqls, expects):
            self.check_sql(sql, query_ctx)


class TSMATesterSQLGeneratorOptions:
    def __init__(self) -> None:
        self.ts_min: int = 1537146000000 - 1000 * 60 * 60
        self.ts_max: int = 1537150999000 + 1000 * 60 * 60
        self.times: int = 100
        self.pk_col: str = 'ts'
        self.column_prefix: str = 'c'
        self.column_num: int = 9  # c1 - c10
        self.tags_prefix: str = 't'
        self.tag_num: int = 6  # t1 - t6
        self.str_tag_idx: List = [2, 3]
        self.child_table_name_prefix: str = 't'
        self.child_table_num: int = 10  # t0 - t9
        self.interval: bool = False
        # 70% generating a partition by, 30% no partition by, same as group by
        self.partition_by: bool = False
        self.group_by: bool = False
        # generating no ts range condition is also possible
        self.where_ts_range: bool = False
        self.where_tbname_func: bool = False
        self.where_tag_func: bool = False
        self.where_col_func: bool = False
        self.slimit_max = 10
        self.limit_max = 10
        self.norm_tb = False


class TSMATesterSQLGeneratorRes:
    def __init__(self):
        self.has_where_ts_range: bool = False
        self.has_interval: bool = False
        self.partition_by: bool = False
        self.group_by: bool = False
        self.has_slimit: bool = False
        self.has_limit: bool = False
        self.has_user_order_by: bool = False

    def can_ignore_res_order(self):
        return not (self.has_limit and self.has_slimit)


class TSMATestSQLGenerator:
    def __init__(self, opts: TSMATesterSQLGeneratorOptions = TSMATesterSQLGeneratorOptions()):
        self.db_name_: str = ''
        self.tb_name_: str = ''
        self.ts_scan_range_: List[float] = [
            float(UsedTsma.TS_MIN), float(UsedTsma.TS_MAX)]
        self.agg_funcs_: List[str] = []
        self.tsmas_: List[TSMA] = []  # currently created tsmas
        self.opts_: TSMATesterSQLGeneratorOptions = opts
        self.res_: TSMATesterSQLGeneratorRes = TSMATesterSQLGeneratorRes()

        self.select_list_: List[str] = []
        self.where_list_: List[str] = []
        self.group_or_partition_by_list: List[str] = []
        self.interval: str = ''

    def get_depth_one_str_funcs(self, name: str) -> List[str]:
        concat1 = f'CONCAT({name}, "_concat")'
        concat2 = f'CONCAT({name}, {name})'
        concat3 = f'CONCAT({name}, {name}, {name})'
        start = random.randint(1, 3)
        len = random.randint(0, 3)
        substr = f'SUBSTR({name}, {start}, {len})'
        lower = f'LOWER({name})'
        ltrim = f'LTRIM({name})'
        return [concat1, concat2, concat3, substr, substr, lower, lower, ltrim, name]

    def generate_depthed_str_func(self, name: str, depth: int) -> str:
        if depth == 1:
            return random.choice(self.get_depth_one_str_funcs(name))
        name = self.generate_depthed_str_func(name, depth - 1)
        return random.choice(self.get_depth_one_str_funcs(name))

    def generate_str_func(self, column_name: str, depth: int = 0) -> str:
        if depth == 0:
            depth = random.randint(1, 3)

        ret = self.generate_depthed_str_func(column_name, depth)
        tdLog.debug(f'generating str func: {ret}')
        return ret

    def get_random_type(self, funcs):
        rand: int = randrange(1, len(funcs))
        return funcs[rand-1]()

    def generate_select_list(self, user_select_list: str, partition_by_list: str):
        res = user_select_list
        if self.res_.has_interval and random.random() < 0.8:
            res = res + ',_wstart, _wend'
        if self.res_.partition_by or self.res_.group_by and random.random() < 0.8:
            res = res + f',{partition_by_list}'
        return res

    def generate_order_by(self, user_order_by: str, partition_by_list: str):
        auto_order_by = 'ORDER BY'
        has_limit = self.res_.has_limit or self.res_.has_slimit
        if has_limit and (self.res_.group_by or self.res_.partition_by):
            auto_order_by = f'{auto_order_by} {partition_by_list},'
        if has_limit and self.res_.has_interval:
            auto_order_by = f'{auto_order_by} _wstart, _wend,'
        if len(user_order_by) > 0:
            self.res_.has_user_order_by = True
            auto_order_by = f'{auto_order_by} {user_order_by},'
        if auto_order_by == 'ORDER BY':
            return ''
        else:
            return auto_order_by[:-1]

    def generate_one(self, select_list: str, possible_tbs: List, order_by_list: str, interval_list: List[str] = []) -> str:
        tb = random.choice(possible_tbs)
        where = self.generate_where()
        interval = self.generate_interval(interval_list)
        (partition_by, partition_by_list) = self.generate_partition_by()
        limit = self.generate_limit()
        auto_select_list = self.generate_select_list(
            select_list, partition_by_list)
        order_by = self.generate_order_by(order_by_list, partition_by_list)
        sql = f"SELECT {auto_select_list} FROM {tb} {where} {partition_by} {partition_by_list} {interval} {order_by} {limit}"
        tdLog.debug(sql)
        return sql

    def can_ignore_res_order(self):
        return self.res_.can_ignore_res_order()

    def generate_where(self) -> str:
        v = random.random()
        where = ''
        if not self.opts_.norm_tb:
            if v < 0.2:
                where = f'{self.generate_tbname_where()}'
            elif v < 0.5:
                where = f'{self.generate_tag_where()}'
            elif v < 0.7:
                op = random.choice(['AND', 'OR'])
                where = f'{self.generate_tbname_where()} {op} {self.generate_tag_where()}'
        ts_where = self.generate_ts_where_range()
        if len(ts_where) > 0 or len(where) > 0:
            op = ''
            if len(where) > 0 and len(ts_where) > 0:
                op = random.choice(['AND', 'AND', 'AND', 'AND', 'OR'])
            return f'WHERE {ts_where} {op} {where}'
        return ''

    def generate_str_equal_operator(self, column_name: str, opts: List) -> str:
        opt = random.choice(opts)
        return f'{column_name} = "{opt}"'

    # TODO support it
    def generate_str_in_operator(self, column_name: str, opts: List) -> str:
        opt = random.choice(opts)
        IN = f'"{",".join(opts)}"'
        return f'{column_name} in ({IN})'

    def generate_str_like_operator(self, column_name: str, opts: List) -> str:
        opt = random.choice(opts)
        return f'{column_name} like "{opt}"'

    def generate_tbname_where(self) -> str:
        tbs = []
        for idx in range(1, self.opts_.tag_num + 1):
            tbs.append(f'{self.opts_.child_table_name_prefix}{idx}')

        if random.random() < 0.5:
            return self.generate_str_equal_operator('tbname', tbs)
        else:
            return self.generate_str_like_operator('tbname', ['t%', '%2'])

    def generate_tag_where(self) -> str:
        idx = random.randrange(1, self.opts_.tag_num + 1)
        if random.random() < 0.5 and idx in self.opts_.str_tag_idx:
            if random.random() < 0.5:
                return self.generate_str_equal_operator(f'{self.opts_.tags_prefix}{idx}', [f'tb{random.randint(1,100)}'])
            else:
                return self.generate_str_like_operator(f'{self.opts_.tags_prefix}{idx}', ['%1', 'tb%', 'tb1%', '%1%'])
        else:
            operator = random.choice(['>', '>=', '<', '<=', '=', '!='])
            val = random.randint(1, 100)
            return f'{self.opts_.tags_prefix}{idx} {operator} {val}'

    def generate_timestamp(self, min: float = -1, max: float = 0) -> int:
        milliseconds_aligned: float = random.randint(int(min), int(max))
        seconds_aligned = int(milliseconds_aligned / 1000) * 1000
        if seconds_aligned < min:
            seconds_aligned = int(min)
        minutes_aligned = int(milliseconds_aligned / 1000 / 60) * 1000 * 60
        if minutes_aligned < min:
            minutes_aligned = int(min)
        hour_aligned = int(milliseconds_aligned / 1000 /
                           60 / 60) * 1000 * 60 * 60
        if hour_aligned < min:
            hour_aligned = int(min)

        return random.choice([milliseconds_aligned, seconds_aligned, seconds_aligned, minutes_aligned, minutes_aligned, hour_aligned, hour_aligned])

    def generate_ts_where_range(self):
        if not self.opts_.where_ts_range:
            return ''
        left_operators = ['>', '>=', '']
        right_operators = ['<', '<=', '']
        left_operator = left_operators[random.randrange(0, 3)]
        right_operator = right_operators[random.randrange(0, 3)]
        a = ''
        left_value = None
        if left_operator:
            left_value = self.generate_timestamp(
                self.opts_.ts_min, self.opts_.ts_max)
            a += f'{self.opts_.pk_col} {left_operator} {left_value}'
        if right_operator:
            if left_value:
                start = left_value
            else:
                start = self.opts_.ts_min
            right_value = self.generate_timestamp(start, self.opts_.ts_max)
            if left_operator:
                a += ' AND '
            a += f'{self.opts_.pk_col} {right_operator} {right_value}'
        # tdLog.debug(f'{self.opts_.pk_col} range with: {a}')
        if len(a) > 0:
            self.res_.has_where_ts_range = True
        return a

    def generate_limit(self) -> str:
        ret = ''
        can_have_slimit = self.res_.partition_by or self.res_.group_by
        if can_have_slimit:
            if random.random() < 0.4:
                ret = f'SLIMIT {random.randint(0, self.opts_.slimit_max)}'
                self.res_.has_slimit = True
        if random.random() < 0.4:
            self.res_.has_limit = True
            ret = ret + f' LIMIT {random.randint(0, self.opts_.limit_max)}'
        return ret

    ## if offset is True, offset cannot be the same as interval
    def generate_random_offset_sliding(self, interval: str, offset: bool = False) -> str:
        unit = interval[-1]
        hasUnit = unit.isalpha()
        if not hasUnit:
            start = 1
            if offset:
                start = 2
            ret: int = int(int(interval) / random.randint(start, 5))
            return str(ret)
        return ''

    # add sliding offset
    def generate_interval(self, intervals: List[str]) -> str:
        if not self.opts_.interval:
            return ''
        if random.random() < 0.4:  # no interval
            return ''
        value = random.choice(intervals)
        self.res_.has_interval = True
        has_offset = False
        offset = ''
        has_sliding = False
        sliding = ''
        num: int = int(value[:-1])
        unit = value[-1]
        if has_offset and num > 1:
            offset = f', {self.generate_random_offset_sliding(value, True)}'
        if has_sliding:
            sliding = f'sliding({self.generate_random_offset_sliding(value)})'
        return f'INTERVAL({value} {offset}) {sliding}'

    def generate_tag_list(self):
        used_tag_num = random.randrange(1, self.opts_.tag_num + 1)
        ret = ''
        for _ in range(used_tag_num):
            tag_idx = random.randint(1, self.opts_.tag_num)
            tag_name = self.opts_.tags_prefix + f'{tag_idx}'
            if random.random() < 0.5 and tag_idx in self.opts_.str_tag_idx:
                tag_func = self.generate_str_func(tag_name, 2)
            else:
                tag_func = tag_name
            ret = ret + f'{tag_func},'
        return ret[:-1]

    def generate_tbname_tag_list(self):
        tag_num = random.randrange(1, self.opts_.tag_num)
        ret = ''
        tbname_idx = random.randint(0, tag_num + 1)
        for i in range(tag_num + 1):
            if i == tbname_idx:
                ret = ret + 'tbname,'
            else:
                tag_idx = random.randint(1, self.opts_.tag_num)
                ret = ret + self.opts_.tags_prefix + f'{tag_idx},'
        return ret[:-1]

    def generate_partition_by(self):
        if not self.opts_.partition_by and not self.opts_.group_by:
            return ('', '')
        # no partition or group
        if random.random() < 0.3:
            return ('', '')
        ret = ''
        rand = random.random()
        if rand < 0.4:
            if random.random() < 0.5:
                ret = self.generate_str_func('tbname', 3)
            else:
                ret = 'tbname'
        elif rand < 0.8:
            ret = self.generate_tag_list()
        else:
            # tbname and tag
            ret = self.generate_tbname_tag_list()
        # tdLog.debug(f'partition by: {ret}')
        if self.res_.has_interval or random.random() < 0.5:
            self.res_.partition_by = True
            return (str('PARTITION BY'), f'{ret}')
        else:
            self.res_.group_by = True
            return (str('GROUP BY'),  f'{ret}')

    def generate_where_tbname(self) -> str:
        return self.generate_str_func('tbname')

    def generate_where_tag(self) -> str:
        # tag_idx = random.randint(1, self.opts_.tag_num)
        # tag = self.opts_.tags_prefix + str(tag_idx)
        return self.generate_str_func('t3')

    def generate_where_conditions(self) -> str:

        pass

    # generate func in tsmas(select list)
    def _generate_agg_func_for_select(self) -> str:
        pass

    # order by, limit, having, subquery...


class TDTestCase:
    updatecfgDict = {'asynclog': 0, 'ttlUnit': 1, 'ttlPushInterval': 5, 'ratioOfVnodeStreamThrea': 4, 'maxTsmaNum': 3}

    def __init__(self):
        self.vgroups = 4
        self.ctbNum = 10
        self.rowsPerTbl = 10000
        self.duraion = '1h'

    def init(self, conn, logSql, replicaVar=1):
        self.replicaVar = int(replicaVar)
        tdLog.debug(f"start to excute {__file__}")
        tdSql.init(conn.cursor(), False)
        self.tsma_tester: TSMATester = TSMATester(tdSql)
        self.tsma_sql_generator: TSMATestSQLGenerator = TSMATestSQLGenerator()

    def create_database(self, tsql, dbName, dropFlag=1, vgroups=2, replica=1, duration: str = '1d'):
        if dropFlag == 1:
            tsql.execute("drop database if exists %s" % (dbName))

        tsql.execute("create database if not exists %s vgroups %d replica %d duration %s" % (
            dbName, vgroups, replica, duration))
        tdLog.debug("complete to create database %s" % (dbName))
        return

    def create_stable(self, tsql, paraDict):
        colString = tdCom.gen_column_type_str(
            colname_prefix=paraDict["colPrefix"], column_elm_list=paraDict["colSchema"])
        tagString = tdCom.gen_tag_type_str(
            tagname_prefix=paraDict["tagPrefix"], tag_elm_list=paraDict["tagSchema"])
        sqlString = f"create table if not exists %s.%s (%s) tags (%s)" % (
            paraDict["dbName"], paraDict["stbName"], colString, tagString)
        tdLog.debug("%s" % (sqlString))
        tsql.execute(sqlString)
        return

    def create_ctable(self, tsql=None, dbName='dbx', stbName='stb', ctbPrefix='ctb', ctbNum=1, ctbStartIdx=0):
        for i in range(ctbNum):
            sqlString = "create table %s.%s%d using %s.%s tags(%d, 'tb%d', 'tb%d', %d, %d, %d)" % (dbName, ctbPrefix, i+ctbStartIdx, dbName, stbName, (i+ctbStartIdx) % 5, i+ctbStartIdx + random.randint(
                1, 100), i+ctbStartIdx + random.randint(1, 100), i+ctbStartIdx + random.randint(1, 100), i+ctbStartIdx + random.randint(1, 100), i+ctbStartIdx + random.randint(1, 100))
            tsql.execute(sqlString)

        tdLog.debug("complete to create %d child tables by %s.%s" %
                    (ctbNum, dbName, stbName))
        return

    def init_normal_tb(self, tsql, db_name: str, tb_name: str, rows: int, start_ts: int, ts_step: int):
        sql = 'CREATE TABLE %s.%s (ts timestamp, c1 INT, c2 INT, c3 INT, c4 double, c5 VARCHAR(255))' % (
            db_name, tb_name)
        tsql.execute(sql)
        sql = 'INSERT INTO %s.%s values' % (db_name, tb_name)
        for j in range(rows):
            sql += f'(%d, %d,%d,%d,{random.random()},"varchar_%d"),' % (start_ts + j * ts_step + randrange(500), j %
                                                     10 + randrange(200), j % 10, j % 10, j % 10 + randrange(100))
        tsql.execute(sql)

    def insert_data(self, tsql, dbName, ctbPrefix, ctbNum, rowsPerTbl, batchNum, startTs, tsStep):
        tdLog.debug("start to insert data ............")
        tsql.execute("use %s" % dbName)
        pre_insert = "insert into "
        sql = pre_insert

        for i in range(ctbNum):
            rowsBatched = 0
            sql += " %s.%s%d values " % (dbName, ctbPrefix, i)
            for j in range(rowsPerTbl):
                if (i < ctbNum/2):
                    sql += "(%d, %d, %d, %d,%d,%d,%d,true,'binary%d', 'nchar%d') " % (startTs + j*tsStep + randrange(
                        500), j % 10 + randrange(100), j % 10 + randrange(200), j % 10, j % 10, j % 10, j % 10, j % 10, j % 10)
                else:
                    sql += "(%d, %d, NULL, %d,NULL,%d,%d,true,'binary%d', 'nchar%d') " % (
                        startTs + j*tsStep + randrange(500), j % 10, j % 10, j % 10, j % 10, j % 10, j % 10)
                rowsBatched += 1
                if ((rowsBatched == batchNum) or (j == rowsPerTbl - 1)):
                    tsql.execute(sql)
                    rowsBatched = 0
                    if j < rowsPerTbl - 1:
                        sql = "insert into %s.%s%d values " % (dbName, ctbPrefix, i)
                    else:
                        sql = "insert into "
        if sql != pre_insert:
            tsql.execute(sql)
        tdLog.debug("insert data ............ [OK]")
        return

    def init_data(self, db: str = 'test', ctb_num: int = 10, rows_per_ctb: int = 10000, start_ts: int = 1537146000000, ts_step: int = 500):
        tdLog.printNoPrefix(
            "======== prepare test env include database, stable, ctables, and insert data: ")
        paraDict = {'dbName':     db,
                    'dropFlag':   1,
                    'vgroups':    2,
                    'stbName':    'meters',
                    'colPrefix':  'c',
                    'tagPrefix':  't',
                    'colSchema':   [{'type': 'INT', 'count': 1}, {'type': 'BIGINT', 'count': 1}, {'type': 'FLOAT', 'count': 1}, {'type': 'DOUBLE', 'count': 1}, {'type': 'smallint', 'count': 1}, {'type': 'tinyint', 'count': 1}, {'type': 'bool', 'count': 1}, {'type': 'binary', 'len': 10, 'count': 1}, {'type': 'nchar', 'len': 10, 'count': 1}],
                    'tagSchema':   [{'type': 'INT', 'count': 1}, {'type': 'nchar', 'len': 20, 'count': 1}, {'type': 'binary', 'len': 20, 'count': 1}, {'type': 'BIGINT', 'count': 1}, {'type': 'smallint', 'count': 1}, {'type': 'DOUBLE', 'count': 1}],
                    'ctbPrefix':  't',
                    'ctbStartIdx': 0,
                    'ctbNum':     ctb_num,
                    'rowsPerTbl': rows_per_ctb,
                    'batchNum':   3000,
                    'startTs':    start_ts,
                    'tsStep':     ts_step}

        paraDict['vgroups'] = self.vgroups
        paraDict['ctbNum'] = ctb_num
        paraDict['rowsPerTbl'] = rows_per_ctb

        tdLog.info("create database")
        self.create_database(tsql=tdSql, dbName=paraDict["dbName"], dropFlag=paraDict["dropFlag"],
                             vgroups=paraDict["vgroups"], replica=self.replicaVar, duration=self.duraion)

        tdLog.info("create stb")
        self.create_stable(tsql=tdSql, paraDict=paraDict)

        tdLog.info("create child tables")
        self.create_ctable(tsql=tdSql, dbName=paraDict["dbName"],
                           stbName=paraDict["stbName"], ctbPrefix=paraDict["ctbPrefix"],
                           ctbNum=paraDict["ctbNum"], ctbStartIdx=paraDict["ctbStartIdx"])
        self.insert_data(tsql=tdSql, dbName=paraDict["dbName"],
                         ctbPrefix=paraDict["ctbPrefix"], ctbNum=paraDict["ctbNum"],
                         rowsPerTbl=paraDict["rowsPerTbl"], batchNum=paraDict["batchNum"],
                         startTs=paraDict["startTs"], tsStep=paraDict["tsStep"])
        self.init_normal_tb(tdSql, paraDict['dbName'], 'norm_tb',
                            paraDict['rowsPerTbl'], paraDict['startTs'], paraDict['tsStep'])

    def wait_for_tsma_calculation(self, func_list: list, db: str, tb: str, interval: str, tsma_name: str, timeout_seconds: int =600):
        start_time = time.time()  
        while True:
            current_time = time.time()  
            if current_time - start_time > timeout_seconds: 
                error_message = f"Timeout occurred while waiting for TSMA calculation to complete."
                tdLog.exit(error_message)
            sql = 'select %s from %s.%s interval(%s)' % (
                ', '.join(func_list), db, tb, interval)
            tdLog.debug(
                f'waiting for tsma {db}.{tsma_name} to be useful with sql {sql}')
            ctx: TSMAQueryContext = self.tsma_tester.get_tsma_query_ctx(sql)
            if ctx.has_tsma():
                if ctx.used_tsmas[0].name == tsma_name + UsedTsma.TSMA_RES_STB_POSTFIX:
                    break
                elif len(ctx.used_tsmas[0].name) == 32:
                    name = f'1.{db}.{tsma_name}_{tb}'
                    if ctx.used_tsmas[0].name == TSMAQCBuilder().md5(name):
                        break
                    else:
                        time.sleep(1)
                else:
                    time.sleep(1)
            else:
                time.sleep(1)
            time.sleep(1)

    def create_tsma(self, tsma_name: str, db: str, tb: str, func_list: list, interval: str, check_tsma_calculation : str=True):
        tdSql.execute('use %s' % db)
        sql = "CREATE TSMA %s ON %s.%s FUNCTION(%s) INTERVAL(%s)" % (
            tsma_name, db, tb, ','.join(func_list), interval)
        tdSql.execute(sql, queryTimes=1)
        if check_tsma_calculation == True:
            self.wait_for_tsma_calculation(func_list, db, tb, interval, tsma_name)

    def create_error_tsma(self, tsma_name: str, db: str, tb: str, func_list: list, interval: str, expectedErrno: int):
        tdSql.execute('use %s' % db)
        sql = "CREATE TSMA %s ON %s.%s FUNCTION(%s) INTERVAL(%s)" % (
            tsma_name, db, tb, ','.join(func_list), interval)
        tdSql.error(sql, expectedErrno)

    def create_recursive_tsma(self, base_tsma_name: str, new_tsma_name: str, db: str, interval: str, tb_name: str, func_list: List[str] = ['avg(c1)']):
        tdSql.execute('use %s' % db, queryTimes=1)
        sql = 'CREATE RECURSIVE TSMA %s ON %s.%s INTERVAL(%s)' % (
            new_tsma_name, db, base_tsma_name, interval)
        tdSql.execute(sql, queryTimes=1)
        self.wait_for_tsma_calculation(
            func_list, db, tb_name, interval, new_tsma_name)

    def drop_tsma(self, tsma_name: str, db: str):
        sql = 'DROP TSMA %s.%s' % (db, tsma_name)
        tdSql.execute(sql, queryTimes=1)

    def check_explain_res_has_row(self, plan_str_expect: str, explain_output):
        plan_found = False
        for row in explain_output:
            if str(row).find(plan_str_expect) >= 0:
                tdLog.debug("plan: [%s] found in: [%s]" %
                            (plan_str_expect, str(row)))
                plan_found = True
                break
        if not plan_found:
            tdLog.exit("plan: %s not found in res: [%s]" % (
                plan_str_expect, str(explain_output)))

    def check(self, ctxs: List):
        for ctx in ctxs:
            self.tsma_tester.check_sql(ctx.sql, ctx)

    def run(self):
        self.test_bigger_tsma_interval()

    def test_create_recursive_tsma_interval(self, db: str, tb: str, func, interval: str, recursive_interval: str, succ: bool, code: int):
        self.create_tsma('tsma1', db, tb, func, interval)
        sql = f'CREATE RECURSIVE TSMA tsma2 ON {db}.tsma1 INTERVAL({recursive_interval})'
        if not succ:
            tdSql.error(sql, code)
        else:
            self.create_recursive_tsma('tsma1', 'tsma2', db, recursive_interval, tb, func)
            self.drop_tsma('tsma2', db)
        self.drop_tsma('tsma1', db)

    def test_bigger_tsma_interval_query(self, func_list: List):
        ## 3 tsmas, 12h, 1n, 1y
        ctxs = []
        interval_list = ['2h', '8h', '1d', '1n', '3n', '1w', '1y', '2y']
        opts: TSMATesterSQLGeneratorOptions = TSMATesterSQLGeneratorOptions()
        opts.interval = True
        opts.where_ts_range = True
        for _ in range(1, ROUND):
            opts.partition_by = True
            opts.group_by = True
            opts.norm_tb = False
            sql_generator = TSMATestSQLGenerator(opts)
            sql = sql_generator.generate_one(
                ','.join(func_list), ['db.meters', 'db.meters', 'db.t1', 'db.t9'], '', interval_list)
            ctxs.append(TSMAQCBuilder().with_sql(sql).ignore_query_table(
            ).ignore_res_order(sql_generator.can_ignore_res_order()).get_qc())
        return ctxs

    def test_bigger_tsma_interval(self):
        db = 'db'
        tb = 'meters'
        func = ['max(c1)', 'min(c1)', 'min(c2)', 'max(c2)', 'avg(c1)', 'count(ts)']
        self.init_data(db,10, 10000, 1500000000000, 11000000)
        examples = [
                ('10m', '1h', True), ('10m','1d',True), ('1m', '120s', True), ('1h','1d',True),
                ('12h', '1y', False), ('1h', '1n', True), ('1h', '1y', True),
                ('12n', '1y', False), ('2d','1n',False), ('55m', '55h', False), ('7m','7d',False),
                ]
        tdSql.execute('use db')
        for (i, ri, ret) in examples:
            self.test_create_recursive_tsma_interval(db, tb, func, i, ri, ret, -2147471099)

        self.create_tsma('tsma1', db, tb, func, '1h')
        self.create_recursive_tsma('tsma1', 'tsma2', db, '1n', tb, func)
        self.create_recursive_tsma('tsma2', 'tsma3', db, '1y', tb, func)
        self.check(self.test_bigger_tsma_interval_query(func))

        ctxs = []
        ctxs.append(TSMAQCBuilder().with_sql('SELECT max(c1) FROM db.meters').should_query_with_tsma('tsma3').get_qc())
        ctxs.append(TSMAQCBuilder()
                    .with_sql('SELECT max(c1) FROM db.meters WHERE ts > "2024-09-03 18:40:00.324"')
                    .should_query_with_table('meters', '2024-09-03 18:40:00.325', '2024-12-31 23:59:59.999')
                    .should_query_with_tsma('tsma3', '2025-01-01 00:00:00.000', UsedTsma.TS_MAX)
                    .get_qc())

        ctxs.append(TSMAQCBuilder()
                    .with_sql('SELECT max(c1) FROM db.meters WHERE ts >= "2024-09-03 18:00:00.000"')
                    .should_query_with_tsma('tsma1', '2024-09-03 18:00:00.000', '2024-12-31 23:59:59.999')
                    .should_query_with_tsma('tsma3', '2025-01-01 00:00:00.000', UsedTsma.TS_MAX)
                    .get_qc())

        ctxs.append(TSMAQCBuilder()
                    .with_sql('SELECT max(c1) FROM db.meters WHERE ts >= "2024-09-01 00:00:00.000"')
                    .should_query_with_tsma('tsma2', '2024-09-01 00:00:00.000', '2024-12-31 23:59:59.999')
                    .should_query_with_tsma('tsma3', '2025-01-01 00:00:00.000', UsedTsma.TS_MAX)
                    .get_qc())

        ctxs.append(TSMAQCBuilder()
                    .with_sql("SELECT max(c1) FROM db.meters INTERVAL(12n)")
                    .should_query_with_tsma('tsma3')
                    .get_qc())

        ctxs.append(TSMAQCBuilder()
                    .with_sql("SELECT max(c1) FROM db.meters INTERVAL(13n)")
                    .should_query_with_tsma('tsma2')
                    .get_qc())

        ctxs.append(TSMAQCBuilder()
                    .with_sql("SELECT max(c1),min(c1),min(c2),max(c2),avg(c1),count(ts) FROM db.t9 WHERE ts > '2018-09-17 08:16:00'")
                    .should_query_with_table('t9', '2018-09-17 08:16:00.001', '2018-12-31 23:59:59:999')
                    .should_query_with_tsma_ctb('db', 'tsma3', 't9', '2019-01-01')
                    .get_qc())

        ctxs.append(TSMAQCBuilder()
                    .with_sql("SELECT max(c1), _wstart FROM db.meters WHERE ts >= '2024-09-03 18:40:00.324' INTERVAL(1d)")
                    .should_query_with_table('meters', '2024-09-03 18:40:00.324', '2024-09-03 23:59:59:999')
                    .should_query_with_tsma('tsma1', '2024-09-04 00:00:00.000')
                    .get_qc())

        ctxs.append(TSMAQCBuilder()
                    .with_sql("SELECT max(c1), _wstart FROM db.meters WHERE ts >= '2024-09-03 18:40:00.324' INTERVAL(1n)")
                    .should_query_with_table('meters', '2024-09-03 18:40:00.324', '2024-09-30 23:59:59:999')
                    .should_query_with_tsma('tsma2', '2024-10-01 00:00:00.000')
                    .get_qc())

        self.check(ctxs)
        tdSql.execute('drop database db')

    def stop(self):
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")


event = threading.Event()

tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
