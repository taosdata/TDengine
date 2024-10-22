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

ROUND = 1000

ignore_some_tests: int = 1
wait_query_seconds = 30

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

    def create_tsma(self, tsma_name: str, db: str, tb: str, func_list: list, interval: str, check_tsma_calculation : str=True, expected_tsma_name: str = ''):
        tdSql.execute('use %s' % db)
        sql = "CREATE TSMA %s ON %s.%s FUNCTION(%s) INTERVAL(%s)" % (
            tsma_name, db, tb, ','.join(func_list), interval)
        tdSql.execute(sql, queryTimes=1)
        tsma_name_trim = tsma_name
        if tsma_name[0] == '`':
            tsma_name_trim = tsma_name[1:-1]
        if expected_tsma_name != '':
            tsma_name_trim = expected_tsma_name
        if check_tsma_calculation == True:
            self.wait_for_tsma_calculation(func_list, db, tb, interval, tsma_name_trim)

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

    def test_query_with_tsma(self):
        self.create_tsma('tsma1', 'test', 'meters', ['avg(c1)', 'avg(c2)'], '5m')
        self.create_tsma('tsma2', 'test', 'meters', ['avg(c1)', 'avg(c2)'], '30m')
        self.create_tsma('tsma5', 'test', 'norm_tb', ['avg(c1)', 'avg(c2)'], '10m')

        self.test_query_with_tsma_interval()
        self.test_query_with_tsma_agg()
        if not ignore_some_tests:
            self.test_recursive_tsma()
        self.test_query_interval_sliding()
        self.test_union()
        self.test_query_child_table()
        self.test_skip_tsma_hint()
        self.test_long_tsma_name()
        self.test_long_ctb_name()
        self.test_add_tag_col()
        self.test_modify_col_name_value()
        self.test_alter_tag_val()
        if not ignore_some_tests:
            self.test_ins_tsma()

    def test_ins_tsma(self):
        tdSql.execute('use performance_schema')
        tdSql.query('show performance_schema.tsmas')
        tdSql.checkRows(0)
        tdSql.execute('use test')
        tdSql.query('show test.tsmas')
        tdSql.checkRows(3)
        tdSql.query('select * from information_schema.ins_tsmas')
        tdSql.checkRows(3)
        tdSql.execute('create database dd')
        tdSql.execute('use dd')
        tdSql.execute('create table dd.norm_tb (ts timestamp, c1 int)')
        tdSql.execute('insert into dd.norm_tb values(now, 1)')
        self.create_tsma('tsma_norm_tb_dd', 'dd', 'norm_tb', ['avg(c1)', 'sum(c1)', 'min(c1)'], '10m')
        tdSql.query('show dd.tsmas')
        tdSql.checkRows(1)
        tdSql.query('select * from information_schema.ins_tsmas')
        tdSql.checkRows(4)
        tdSql.query('show test.tsmas')
        tdSql.checkRows(3)
        tdSql.execute('use test')
        tdSql.query('show dd.tsmas')
        tdSql.checkRows(1)
        tdSql.execute('drop database dd')
        tdSql.query('select * from information_schema.ins_tsmas')
        tdSql.checkRows(3)
        tdSql.execute('use test')

    def test_alter_tag_val(self):
        sql = 'alter table test.t1 set tag t1 = 999'
        tdSql.error(sql, -2147471088)

    def test_query_interval_sliding(self):
        pass

    def test_union(self):
        ctxs = []
        sql = 'select avg(c1) from test.meters union select avg(c1) from test.norm_tb'
        ctx = TSMAQCBuilder().with_sql(sql).should_query_with_tsma('tsma2').should_query_with_tsma_ctb('test', 'tsma5', 'norm_tb').get_qc()
        ctxs.append(ctx)
        sql = 'select avg(c1), avg(c2) from test.meters where ts between "2018-09-17 09:00:00.000" and "2018-09-17 10:00:00.000" union select avg(c1), avg(c2) from  test.meters where ts between "2018-09-17 09:00:00.200" and "2018-09-17 10:23:19.800"'
        ctxs.append(TSMAQCBuilder().with_sql(sql)
                    .should_query_with_tsma('tsma2', '2018-09-17 09:00:00', '2018-09-17 09:59:59:999')
                    .should_query_with_table("meters", '2018-09-17 10:00:00', '2018-09-17 10:00:00')
                    .should_query_with_table('meters', '2018-09-17 09:00:00.200', '2018-09-17 09:29:59:999')
                    .should_query_with_tsma('tsma2', '2018-09-17 09:30:00', '2018-09-17 09:59:59.999')
                    .should_query_with_table('meters', '2018-09-17 10:00:00.000', '2018-09-17 10:23:19.800').get_qc())
        tdSql.query('show create table test.meters')
        self.check(ctxs)
        if not ignore_some_tests:
            tdSql.execute('create database db2')
            tdSql.execute('use db2')
            tdSql.execute('create table db2.norm_tb(ts timestamp, c2 int)')
            tdSql.execute('insert into db2.norm_tb values(now, 1)')
            tdSql.execute('insert into db2.norm_tb values(now, 2)')
            self.create_tsma('tsma_db2_norm_t', 'db2', 'norm_tb', ['avg(c2)', 'last(ts)'], '10m')
            sql = 'select avg(c1) as avg_c1 from test.meters union select avg(c2) from  db2.norm_tb order by avg_c1'
            self.check([TSMAQCBuilder().with_sql(sql).should_query_with_tsma('tsma2').should_query_with_tsma_ctb('db2', 'tsma_db2_norm_t', 'norm_tb').get_qc()])
            tdSql.execute('drop database db2')
            tdSql.execute('use test')

    def test_modify_col_name_value(self):
        tdSql.error('alter table  test.norm_tb rename column c1 c1_new', -2147471088) ## tsma must be dropped

        ## modify tag name
        tdSql.error('alter stable  test.meters rename tag t1 t1_new;', -2147482637) ## stream must be dropped

    def test_add_tag_col(self):
        ## query with newly add tag will skip all tsmas not have this tag
        tdSql.execute('alter table  test.meters add tag tag_new int', queryTimes=1)
        sql = 'select avg(c1) from  test.meters partition by tag_new'
        self.check([TSMAQCBuilder().with_sql(sql).should_query_with_table('meters').get_qc()])
        sql = 'select avg(c1) from  test.meters partition by abs(tag_new)'
        self.check([TSMAQCBuilder().with_sql(sql).should_query_with_table('meters').get_qc()])
        sql = 'select avg(c1) from  test.meters where abs(tag_new) > 100'
        self.check([TSMAQCBuilder().with_sql(sql).should_query_with_table('meters').get_qc()])

        tdSql.execute('alter table test.meters drop tag tag_new', queryTimes=1)

    def generate_random_string(self, length):
        letters_and_digits = string.ascii_lowercase
        result_str = ''.join(random.choice(letters_and_digits) for i in range(length))
        return result_str

    def test_long_tsma_name(self):
        self.drop_tsma('tsma2', 'test')
        name = self.generate_random_string(178)
        tsma_func_list = ['avg(c2)', 'avg(c3)', 'min(c4)', 'max(c3)', 'sum(c2)', 'count(ts)', 'count(c2)', 'first(c5)', 'last(c5)', 'spread(c2)', 'stddev(c3)', 'last(ts)']
        if not ignore_some_tests:
            tsma_func_list.append('hyperloglog(c2)')
        self.create_tsma(name, 'test', 'meters', tsma_func_list, '55m')
        sql = 'select last(c5), spread(c2) from test.meters interval(55m)'
        ctx = TSMAQCBuilder().with_sql(sql).should_query_with_tsma(name).get_qc()
        self.check([ctx])
        tdSql.execute(f'drop tsma test.{name}')

        name = self.generate_random_string(180)
        tdSql.error(f'create tsma {name} on test.meters function({",".join(tsma_func_list)}) interval(1h)', -2147471087)

        name = self.generate_random_string(179)
        tdSql.error(f'create tsma {name} on test.meters function({",".join(tsma_func_list)}) interval(1h)', -2147471087)

        name = self.generate_random_string(178)
        self.create_recursive_tsma('tsma1', name, 'test', '60m', 'meters', ['avg(c1)','avg(c2)'])
        sql = 'select avg(c1) from test.meters interval(60m)'
        self.check([TSMAQCBuilder().with_sql(sql).should_query_with_tsma(name).get_qc()])

        tdSql.execute(f'drop tsma test.{name}')

    def test_long_ctb_name(self):
        tb_name = self.generate_random_string(192)
        tsma_name = self.generate_random_string(178)
        tdSql.execute('create database db2')
        tdSql.execute('use db2')
        db_name = 'db2'
        tdSql.execute(f'create table {db_name}.{tb_name}(ts timestamp, c2 int)')
        tdSql.execute(f'insert into  {db_name}.{tb_name} values(now, 1)')
        tdSql.execute(f'insert into  {db_name}.{tb_name} values(now, 2)')
        self.create_tsma(tsma_name, 'db2', tb_name, ['avg(c2)', 'last(ts)'], '10m')
        sql = f'select avg(c2), last(ts) from  {db_name}.{tb_name}'
        self.check([TSMAQCBuilder().with_sql(sql).should_query_with_tsma_ctb('db2', tsma_name, tb_name).get_qc()])
        tdSql.execute('drop database db2')
        tdSql.execute('use test')

    def test_skip_tsma_hint(self):
        ctxs = []
        sql = 'select /*+ skip_tsma()*/avg(c1), avg(c2) from test.meters interval(5m)'
        ctxs.append(TSMAQCBuilder().with_sql(sql).should_query_with_table('meters').get_qc())

        sql = 'select avg(c1), avg(c2) from test.meters interval(5m)'
        ctxs.append(TSMAQCBuilder().with_sql(sql).should_query_with_tsma('tsma1').get_qc())
        self.check(ctxs)

    def test_query_child_table(self):
        sql = 'select avg(c1) from test.t1'
        ctx = TSMAQCBuilder().with_sql(sql).should_query_with_tsma_ctb('test', 'tsma2', 't1', UsedTsma.TS_MIN, UsedTsma.TS_MAX).get_qc()
        self.tsma_tester.check_sql(sql, ctx)
        sql = 'select avg(c1) from test.t3'
        ctx = TSMAQCBuilder().with_sql(sql).should_query_with_tsma_ctb('test', 'tsma2', 't3').get_qc()
        self.tsma_tester.check_sql(sql, ctx)

    def test_recursive_tsma(self):
        tdSql.execute('drop tsma test.tsma2')
        tsma_func_list = ['last(ts)', 'avg(c2)', 'avg(c3)', 'min(c4)', 'max(c3)', 'sum(c2)', 'count(ts)', 'count(c2)', 'first(c5)', 'last(c5)', 'spread(c2)', 'stddev(c3)']
        if not ignore_some_tests:
            tsma_func_list.append('hyperloglog(c2)')
        select_func_list: List[str] = tsma_func_list.copy()
        select_func_list.append('count(*)')
        self.create_tsma('tsma3', 'test', 'meters', tsma_func_list, '5m')
        self.create_recursive_tsma(
            'tsma3', 'tsma4', 'test', '20m', 'meters', tsma_func_list)
        # now we have 5m, 10m, 30m, 1h 4 tsmas
        sql = 'select avg(c2), "recursive test.tsma4" from test.meters'
        ctx = TSMAQCBuilder().with_sql(sql).should_query_with_tsma(
            'tsma4', UsedTsma.TS_MIN, UsedTsma.TS_MAX).get_qc()
        self.tsma_tester.check_sql(sql, ctx)
        self.check(self.test_query_tsma_all(select_func_list))
        self.create_recursive_tsma(
            'tsma4', 'tsma6', 'test', '5h', 'meters', tsma_func_list)
        ctx = TSMAQCBuilder().with_sql(sql).should_query_with_tsma(
            'tsma6', UsedTsma.TS_MIN, UsedTsma.TS_MAX).get_qc()
        self.tsma_tester.check_sql(sql, ctx)

        self.check(self.test_query_tsma_all(select_func_list))
        #time.sleep(999999)

        tdSql.error('drop tsma test.tsma3', -2147482491)
        tdSql.error('drop tsma test.tsma4', -2147482491)
        tdSql.execute('drop tsma test.tsma6')
        tdSql.execute('drop tsma test.tsma4')
        tdSql.execute('drop tsma test.tsma3')
        self.create_tsma('tsma2', 'test', 'meters', ['avg(c1)', 'avg(c2)'], '30m')

        # test query with dropped tsma tsma4 and tsma6
        sql = 'select avg(c2), "test.tsma2" from test.meters'
        ctx = TSMAQCBuilder().with_sql(sql).should_query_with_tsma(
            'tsma2', UsedTsma.TS_MIN, UsedTsma.TS_MAX).get_qc()
        self.check([ctx])

        # test recrusive tsma on norm_tb
        tsma_name = 'tsma_recursive_on_norm_tb'
        self.create_recursive_tsma('tsma5', tsma_name, 'test', '20m', 'norm_tb', ['avg(c1)', 'avg(c2)'])
        sql = 'select avg(c1), avg(c2), tbname from test.norm_tb partition by tbname interval(20m)'
        self.check([TSMAQCBuilder().with_sql(sql).should_query_with_tsma_ctb('test', tsma_name, 'norm_tb').get_qc()])
        tdSql.execute(f'drop tsma test.{tsma_name}')

    def test_query_with_tsma_interval(self):
        self.check(self.test_query_with_tsma_interval_possibly_partition())
        self.check(self.test_query_with_tsma_interval_partition_by_col())

    def test_query_tsma_all(self, func_list: List = ['avg(c1)', 'avg(c2)']) -> List:
        ctxs = []
        interval_list = ['1s', '5s', '59s', '60s', '1m', '120s', '10m', '20m',
                         '30m', '1h', '90m', '2h', '8h', '1d']
        opts: TSMATesterSQLGeneratorOptions = TSMATesterSQLGeneratorOptions()
        opts.interval = True
        opts.where_ts_range = True
        for _ in range(1, ROUND):
            opts.partition_by = True
            opts.group_by = True
            opts.norm_tb = False
            sql_generator = TSMATestSQLGenerator(opts)
            sql = sql_generator.generate_one(
                ','.join(func_list), ['test.meters', 'test.meters', 'test.t1', 'test.t9'], '', interval_list)
            ctxs.append(TSMAQCBuilder().with_sql(sql).ignore_query_table(
            ).ignore_res_order(sql_generator.can_ignore_res_order()).get_qc())

            if random.random() > 0.7:
                continue
            opts.partition_by = False
            opts.group_by = False
            opts.norm_tb = True
            sql_generator = TSMATestSQLGenerator(opts)
            sql = sql_generator.generate_one(
                ','.join(func_list), ['test.norm_tb', 'test.t5'], '', interval_list)
            ctxs.append(TSMAQCBuilder().with_sql(sql).ignore_query_table(
            ).ignore_res_order(sql_generator.can_ignore_res_order()).get_qc())
        return ctxs

    def test_query_with_tsma_interval_possibly_partition(self,db_name: str = 'test'):
        ctxs: List[TSMAQueryContext] = []
        sql = f'select avg(c1), avg(c2) from {db_name}.meters interval(5m)'
        ctxs.append(TSMAQCBuilder().with_sql(sql)
                    .should_query_with_tsma('tsma1', UsedTsma.TS_MIN, UsedTsma.TS_MAX).get_qc())

        sql = f'select avg(c1), avg(c2) from {db_name}.meters interval(10m)'
        ctxs.append(TSMAQCBuilder().with_sql(sql)
                    .should_query_with_tsma('tsma1', UsedTsma.TS_MIN, UsedTsma.TS_MAX).get_qc())
        sql = f'select avg(c1), avg(c2) from {db_name}.meters interval(30m)'
        ctxs.append(TSMAQCBuilder().with_sql(sql)
                    .should_query_with_tsma('tsma2', UsedTsma.TS_MIN, UsedTsma.TS_MAX).get_qc())
        sql = f'select avg(c1), avg(c2) from {db_name}.meters interval(60m)'
        ctxs.append(TSMAQCBuilder().with_sql(sql)
                    .should_query_with_tsma('tsma2', UsedTsma.TS_MIN, UsedTsma.TS_MAX).get_qc())
        sql = f'select avg(c1), avg(c2) from {db_name}.meters interval(60m, 30m) SLIDING(30m)'
        ctxs.append(TSMAQCBuilder().with_sql(sql)
                    .should_query_with_tsma('tsma2', UsedTsma.TS_MIN, UsedTsma.TS_MAX).get_qc())
        sql = f'select avg(c1), avg(c2) from {db_name}.meters interval(60m, 25m) SLIDING(25m)'
        ctxs.append(TSMAQCBuilder().with_sql(sql)
                    .should_query_with_tsma('tsma1', UsedTsma.TS_MIN, UsedTsma.TS_MAX).get_qc())

        sql = f"select avg(c1), avg(c2) from {db_name}.meters where ts >= '2018-09-17 09:00:00.009' and ts < '2018-09-17 10:23:19.665' interval(30m)"
        ctxs.append(TSMAQCBuilder().with_sql(sql)
                    .should_query_with_table('meters', '2018-09-17 09:00:00.009', '2018-09-17 09:29:59.999')
                    .should_query_with_tsma('tsma2', '2018-09-17 09:30:00', '2018-09-17 09:59:59.999')
                    .should_query_with_table('meters', '2018-09-17 10:00:00.000', '2018-09-17 10:23:19.664').get_qc())

        sql = f"SELECT avg(c1), avg(c2) FROM {db_name}.meters WHERE ts >= '2018-09-17 09:00:00.009' AND ts < '2018-09-17 10:23:19.665' PARTITION BY t5 INTERVAL(30m)"
        ctxs.append(TSMAQCBuilder().with_sql(sql)
                    .should_query_with_table('meters', '2018-09-17 09:00:00.009', '2018-09-17 09:29:59.999')
                    .should_query_with_tsma('tsma2', '2018-09-17 09:30:00', '2018-09-17 09:59:59.999')
                    .should_query_with_table('meters', '2018-09-17 10:00:00.000', '2018-09-17 10:23:19.664').get_qc())

        sql = f"select avg(c1), avg(c2) from {db_name}.meters where ts >= '2018-09-17 09:00:00.009' and ts < '2018-09-17 10:23:19.665' interval(30m, 25m) SLIDING(10m)"
        ctxs.append(TSMAQCBuilder().with_sql(sql)
                    .should_query_with_table('meters', '2018-09-17 09:00:00.009', '2018-09-17 09:04:59.999')
                    .should_query_with_tsma('tsma1', '2018-09-17 09:05:00', '2018-09-17 09:54:59.999')
                    .should_query_with_table('meters', '2018-09-17 09:55:00.000', '2018-09-17 10:23:19.664').get_qc())

        sql = f"SELECT avg(c1), avg(c2),_wstart, _wend,t3,t4,t5,t2 FROM {db_name}.meters WHERE ts >= '2018-09-17 8:00:00' AND ts < '2018-09-17 09:03:18.334' PARTITION BY t3,t4,t5,t2 INTERVAL(1d);"
        ctxs.append(TSMAQCBuilder().with_sql(sql)
                    .should_query_with_table('meters', '2018-09-17 8:00:00', '2018-09-17 09:03:18.333').get_qc())
        ctxs.extend(self.test_query_tsma_all())
        return ctxs

    def test_query_with_tsma_interval_partition_by_col(self):
        return []

    def test_query_with_tsma_agg(self):
        self.check(self.test_query_with_tsma_agg_no_group_by())
        self.check(self.test_query_with_tsma_agg_group_by_tbname())
        self.check(self.test_query_with_tsma_with_having())

    def test_query_with_tsma_agg_no_group_by(self, db_name: str = 'test'):
        ctxs: List[TSMAQueryContext] = []
        sql = f'select avg(c1), avg(c2) from {db_name}.meters'
        ctxs.append(TSMAQCBuilder().with_sql(
            sql).should_query_with_tsma('tsma2').get_qc())

        sql = f'select avg(c1), avg(c2) from {db_name}.meters where ts between "2018-09-17 09:00:00.000" and "2018-09-17 10:00:00.000"'
        ctxs.append(TSMAQCBuilder().with_sql(sql)
                    .should_query_with_tsma('tsma2', '2018-09-17 09:00:00', '2018-09-17 09:59:59:999')
                    .should_query_with_table("meters", '2018-09-17 10:00:00', '2018-09-17 10:00:00').get_qc())

        sql = f'select avg(c1), avg(c2) from {db_name}.meters where ts between "2018-09-17 09:00:00.200" and "2018-09-17 10:23:19.800"'
        ctxs.append(TSMAQCBuilder().with_sql(sql)
                    .should_query_with_table('meters', '2018-09-17 09:00:00.200', '2018-09-17 09:29:59:999')
                    .should_query_with_tsma('tsma2', '2018-09-17 09:30:00', '2018-09-17 09:59:59.999')
                    .should_query_with_table('meters', '2018-09-17 10:00:00.000', '2018-09-17 10:23:19.800').get_qc())

        sql = f'select avg(c1) + avg(c2), avg(c2) from {db_name}.meters where ts between "2018-09-17 09:00:00.200" and "2018-09-17 10:23:19.800"'
        ctxs.append(TSMAQCBuilder().with_sql(sql)
                    .should_query_with_table('meters', '2018-09-17 09:00:00.200', '2018-09-17 09:29:59:999')
                    .should_query_with_tsma('tsma2', '2018-09-17 09:30:00', '2018-09-17 09:59:59.999')
                    .should_query_with_table('meters', '2018-09-17 10:00:00.000', '2018-09-17 10:23:19.800').get_qc())

        sql = f'select avg(c1) + avg(c2), avg(c2) + 1 from {db_name}.meters where ts between "2018-09-17 09:00:00.200" and "2018-09-17 10:23:19.800"'
        ctxs.append(TSMAQCBuilder().with_sql(sql)
                    .should_query_with_table('meters', '2018-09-17 09:00:00.200', '2018-09-17 09:29:59:999')
                    .should_query_with_tsma('tsma2', '2018-09-17 09:30:00', '2018-09-17 09:59:59.999')
                    .should_query_with_table('meters', '2018-09-17 10:00:00.000', '2018-09-17 10:23:19.800').get_qc())

        sql = f"select avg(c1) + 1, avg(c2) from {db_name}.meters where ts >= '2018-09-17 9:30:00.118' and ts < '2018-09-17 10:50:00'"
        ctxs.append(TSMAQCBuilder().with_sql(sql)
                    .should_query_with_table('meters', '2018-09-17 9:30:00.118', '2018-09-17 9:59:59.999')
                    .should_query_with_tsma('tsma2', '2018-09-17 10:00:00', '2018-09-17 10:29:59.999')
                    .should_query_with_tsma('tsma1', '2018-09-17 10:30:00.000', '2018-09-17 10:49:59.999').get_qc())

        sql = f"select avg(c1), avg(c2) from {db_name}.meters where ts >= '2018-09-17 9:00:00' and ts < '2018-09-17 9:45:00' limit 2"
        ctxs.append(TSMAQCBuilder().with_sql(sql)
                    .should_query_with_tsma('tsma2', '2018-09-17 9:00:00', '2018-09-17 9:29:59.999')
                    .should_query_with_tsma('tsma1', '2018-09-17 9:30:00', '2018-09-17 9:44:59.999').get_qc())

        sql = f'select avg(c1) + avg(c2) from {db_name}.meters where tbname like "%t1%"'
        ctxs.append(TSMAQCBuilder().with_sql(
            sql).should_query_with_tsma('tsma2').get_qc())

        sql = f'select avg(c1), avg(c2) from {db_name}.meters where c1 is not NULL'
        ctxs.append(TSMAQCBuilder().with_sql(
            sql).should_query_with_table('meters').get_qc())

        sql = f'select avg(c1), avg(c2), spread(c4) from {db_name}.meters'
        ctxs.append(TSMAQCBuilder().with_sql(
            sql).should_query_with_table('meters').get_qc())

        sql = f'select avg(c1), avg(c2) from {db_name}.meters where tbname = \'t1\''
        ctxs.append(TSMAQCBuilder().with_sql(
            sql).should_query_with_tsma('tsma2').get_qc())

        sql = f'select avg(c1), avg(c2) from {db_name}.meters where tbname = \'t1\' or tbname = \'t2\''
        ctxs.append(TSMAQCBuilder().with_sql(
            sql).should_query_with_tsma('tsma2').get_qc())

        sql = f'''select avg(c1), avg(c2) from {db_name}.meters where tbname = 't1' and c1 is not NULL'''
        ctxs.append(TSMAQCBuilder().with_sql(
            sql).should_query_with_table('meters').get_qc())

        sql = f'select avg(c1+c2) from {db_name}.meters'
        ctxs.append(TSMAQCBuilder().with_sql(
            sql).should_query_with_table('meters').get_qc())

        sql = f'select avg(c1), avg(c2) from {db_name}.meters where ts >= "2018-09-17 9:25:00" and ts < "2018-09-17 10:00:00" limit 6'
        ctxs.append(TSMAQCBuilder().with_sql(sql).should_query_with_tsma('tsma1', '2018-09-17 9:25:00', '2018-09-17 9:29:59.999')
                    .should_query_with_tsma('tsma2', '2018-09-17 9:30:00', '2018-09-17 9:59:59.999').get_qc())

        return ctxs

    def test_query_with_tsma_agg_group_by_tbname(self,  db_name: str = 'test'):
        ctxs: List[TSMAQueryContext] = []
        sql = f'select avg(c1) as a, avg(c2) as b, tbname from {db_name}.meters group by tbname order by tbname, a, b'
        ctxs.append(TSMAQCBuilder().with_sql(
            sql).should_query_with_tsma('tsma2').get_qc())

        sql = f'select avg(c1) as a, avg(c2) + 1 as b, tbname from {db_name}.meters where c1 > 10 group by tbname order by tbname, a, b'
        ctxs.append(TSMAQCBuilder().with_sql(
            sql).should_query_with_table('meters').get_qc())

        sql = f'select avg(c1) + avg(c2) as a, avg(c2) + 1 as b, tbname from {db_name}.meters where ts between "2018-09-17 09:00:00.200" and "2018-09-17 10:23:19.800" group by tbname order by tbname, a, b'
        ctxs.append(TSMAQCBuilder().with_sql(sql)
                    .should_query_with_table('meters', '2018-09-17 09:00:00.200', '2018-09-17 09:29:59:999')
                    .should_query_with_tsma('tsma2', '2018-09-17 09:30:00', '2018-09-17 09:59:59.999')
                    .should_query_with_table('meters', '2018-09-17 10:00:00.000', '2018-09-17 10:23:19.800').get_qc())

        sql = f'select avg(c1) + avg(c2) + 3 as a, substr(tbname, 1) as c from {db_name}.meters group by substr(tbname, 1) order by c, a'
        ctxs.append(TSMAQCBuilder().with_sql(
            sql).should_query_with_tsma('tsma2').get_qc())

        sql = f'select avg(c1) + avg(c2) as a, avg(c2) + 1 as b, substr(tbname, 1, 1) as c from {db_name}.meters where ts between "2018-09-17 09:00:00.200" and "2018-09-17 10:23:19.800" group by substr(tbname, 1, 1) order by c, a, b'
        ctxs.append(TSMAQCBuilder().with_sql(sql)
                    .should_query_with_table('meters', '2018-09-17 09:00:00.200', '2018-09-17 09:29:59:999')
                    .should_query_with_tsma('tsma2', '2018-09-17 09:30:00', '2018-09-17 09:59:59.999')
                    .should_query_with_table('meters', '2018-09-17 10:00:00.000', '2018-09-17 10:23:19.800').get_qc())

        sql = f'select avg(c1), tbname from {db_name}.meters group by tbname having avg(c1) > 0 order by tbname'
        ctxs.append(TSMAQCBuilder().with_sql(
            sql).should_query_with_tsma('tsma2').get_qc())
        sql = f'select avg(c1), tbname from {db_name}.meters group by tbname having avg(c1) > 0 and tbname = "t1"'
        ctxs.append(TSMAQCBuilder().with_sql(
            sql).should_query_with_tsma('tsma2').get_qc())

        sql = f'select avg(c1), tbname from {db_name}.meters group by tbname having avg(c1) > 0 and tbname = "t1" order by tbname'
        ctxs.append(TSMAQCBuilder().with_sql(
            sql).should_query_with_tsma('tsma2').get_qc())

        sql = f'select avg(c1) + 1, tbname from {db_name}.meters group by tbname having avg(c1) > 0 and tbname = "t1" order by tbname'
        ctxs.append(TSMAQCBuilder().with_sql(
            sql).should_query_with_tsma('tsma2').get_qc())
        sql = f'select avg(c1) + 1, tbname from {db_name}.meters group by tbname having avg(c1) > 0 and tbname like "t%" order by tbname'
        ctxs.append(TSMAQCBuilder().with_sql(
            sql).should_query_with_tsma('tsma2').get_qc())

        return ctxs

    def test_query_with_tsma_with_having(self):
        return []

    def test_ddl(self):
        self.test_create_tsma()
        self.test_drop_tsma()
        self.test_tb_ddl_with_created_tsma()
    
    def run(self):
        self.init_data()
        self.test_ddl()
        self.test_query_with_tsma()
        # bug to fix
        self.test_flush_query()
        
        #cluster test
        cluster_dnode_list = tdSql.get_cluseter_dnodes()
        clust_dnode_nums = len(cluster_dnode_list)
        if clust_dnode_nums > 1:
            self.test_redistribute_vgroups()
            
    def test_create_tsma(self):
        function_name = sys._getframe().f_code.co_name
        tdLog.debug(f'-----{function_name}------')
        self.test_create_tsma_on_stable()
        self.test_create_tsma_on_norm_table()
        self.test_create_tsma_on_child_table()
        self.test_create_recursive_tsma()
        self.test_create_tsma_maxlist_function()
        self.test_create_diffrent_tsma_name()
        self.test_create_illegal_tsma_sql()
        # self.test_drop_stable() ## drop stable and recreate a stable
        # self.test_drop_ctable()
        self.test_drop_db()

    def wait_query(self, sql: str, expected_row_num: int, timeout_in_seconds: float, is_expect_row = None):
        timeout = timeout_in_seconds
        tdSql.query(sql)
        rows: int = 0
        for row in tdSql.queryResult:
            if is_expect_row is None or is_expect_row(row):
                rows = rows + 1
        while timeout > 0 and rows != expected_row_num:
            tdLog.debug(f'start to wait query: {sql} to return {expected_row_num}, got: {str(tdSql.queryResult)} useful rows: {rows}, remain: {timeout_in_seconds - timeout}')
            time.sleep(1)
            timeout = timeout - 1
            tdSql.query(sql)
            rows = 0
            for row in tdSql.queryResult:
                if is_expect_row is None or is_expect_row(row):
                    rows = rows + 1
        if timeout <= 0:
            tdLog.exit(f'failed to wait query: {sql} to return {expected_row_num} rows timeout: {timeout_in_seconds}s')
        else:
            tdLog.debug(f'wait query succeed: {sql} to return {expected_row_num}, got: {tdSql.getRows()}')

    def test_drop_tsma(self):
        function_name = sys._getframe().f_code.co_name
        tdLog.debug(f'-----{function_name}------')
        self.create_tsma('tsma1', 'test', 'meters', ['avg(c1)', 'avg(c2)'], '5m')
        self.create_recursive_tsma('tsma1', 'tsma2', 'test', '15m', 'meters')

        # drop recursive tsma first
        tdSql.error('drop tsma test.tsma1', -2147482491)
        tdSql.execute('drop tsma test.tsma2', queryTimes=1)
        tdSql.execute('drop tsma test.tsma1', queryTimes=1)
        self.wait_query('show transactions', 0, wait_query_seconds, lambda row: row[3] != 'stream-chkpt-u')
        tdSql.execute('drop database test', queryTimes=1)

        self.init_data()

    def test_drop_db(self):
        function_name = sys._getframe().f_code.co_name
        tdLog.debug(f'-----{function_name}------')
        tdSql.execute('create database nsdb precision "ns"', queryTimes=1)
        tdSql.execute('use nsdb', queryTimes=1)
        tdSql.execute(
            'create table nsdb.meters(ts timestamp, c1 int, c2 int) tags(t1 int, t2 int)', queryTimes=1)
        # TODO insert data
        self.create_tsma('tsma1', 'nsdb', 'meters', [
                         'avg(c1)', 'avg(c2)'], '5m')
        self.create_recursive_tsma('tsma1', 'tsma2', 'nsdb', '10m', 'meters')
        tdSql.query('select avg(c1) from nsdb.meters', queryTimes=1)
        tdSql.execute('drop database nsdb')
    
    def test_tb_ddl_with_created_tsma(self):
        function_name = sys._getframe().f_code.co_name
        tdLog.debug(f'-----{function_name}------')
        tdSql.execute('create database nsdb precision "ns"', queryTimes=1)
        tdSql.execute('use nsdb', queryTimes=1)
        tdSql.execute(
            'create table nsdb.meters(ts timestamp, c1 int, c2 int) tags(t1 int, t2 int)', queryTimes=1)
        self.create_tsma('tsma1', 'nsdb', 'meters', ['avg(c1)', 'avg(c2)'], '5m')
        # drop column, drop tag
        tdSql.error('alter table nsdb.meters drop column c1', -2147482637)
        tdSql.error('alter table nsdb.meters drop tag t1', -2147482637)
        tdSql.error('alter table nsdb.meters drop tag t2', -
                    2147482637)  # Stream must be dropped first
        tdSql.execute('drop tsma nsdb.tsma1', queryTimes=1)

        # add tag
        tdSql.execute('alter table nsdb.meters add tag t3 int', queryTimes=1)
        # Invalid tsma func param, only one non-tag column allowed
        tdSql.error(
            'create tsma tsma1 on nsdb.meters function(avg(c1), avg(c2), avg(t3)) interval(5m)', -2147471096) 
        
        tdSql.execute('alter table nsdb.meters drop tag t3', queryTimes=1)
        self.wait_query('show transactions', 0, wait_query_seconds, lambda row: row[3] != 'stream-chkpt-u')
        tdSql.execute('drop database nsdb')

        # drop norm table
        self.create_tsma('tsma_norm_tb', 'test', 'norm_tb', ['avg(c1)', 'avg(c2)'], '5m')
        tdSql.error('drop table test.norm_tb', -2147471088)

        # drop no tsma table
        tdSql.execute('drop table test.t2, test.t1')

        # test ttl drop table
        self.create_tsma('tsma1', 'test', 'meters', ['avg(c1)', 'avg(c2)'], '5m')
        tdSql.execute('alter table test.t0 ttl 2', queryTimes=1)
        tdSql.execute('flush database test')
        res_tb = TSMAQCBuilder().md5('1.test.tsma1_t0')
        self.wait_query(f'select * from information_schema.ins_tables where table_name = "{res_tb}"', 0, wait_query_seconds)

        # test drop multi tables
        tdSql.execute('drop table test.t3, test.t4')
        res_tb = TSMAQCBuilder().md5('1.test.tsma1_t3')
        self.wait_query(f'select * from information_schema.ins_tables where table_name = "{res_tb}"', 0, wait_query_seconds)
        res_tb = TSMAQCBuilder().md5('1.test.tsma1_t4')
        self.wait_query(f'select * from information_schema.ins_tables where table_name = "{res_tb}"', 0, wait_query_seconds)
        time.sleep(9999999)

        # test drop stream
        tdSql.error('drop stream tsma1', -2147471088) ## TSMA must be dropped first

        self.wait_query('show transactions', 0, wait_query_seconds, lambda row: row[3] != 'stream-chkpt-u')
        tdSql.execute('drop database test', queryTimes=1)
        self.init_data()

    def test_create_tsma_on_stable(self):
        function_name = sys._getframe().f_code.co_name
        tdLog.debug(f'-----{function_name}------')
        tdSql.execute('create database nsdb precision "ns"', queryTimes=1)
        tdSql.execute('use nsdb', queryTimes=1)
        tdSql.execute(
            'create table nsdb.meters(ts timestamp, c1 int, c2 int, c3 varchar(255)) tags(t1 int, t2 int)', queryTimes=1)
        self.create_tsma('tsma1', 'nsdb', 'meters', ['avg(c1)', 'avg(c2)'], '5m')
        # Invalid tsma interval, 1ms ~ 1h is allowed
        def _():
            tdSql.error(
                'create tsma tsma2 on nsdb.meters function(avg(c1), avg(c2)) interval(2h)', -2147471097)
            tdSql.error(
                'create tsma tsma2 on nsdb.meters function(avg(c1), avg(c2)) interval(3601s)', -2147471097)
            tdSql.error(
                'create tsma tsma2 on nsdb.meters function(avg(c1), avg(c2)) interval(3600001a)', -2147471097)
            tdSql.error(
                'create tsma tsma2 on nsdb.meters function(avg(c1), avg(c2)) interval(3600001000u)', -2147471097)
            tdSql.error(
                'create tsma tsma2 on nsdb.meters function(avg(c1), avg(c2)) interval(999999b)', -2147471097)
            tdSql.error(
                'create tsma tsma2 on nsdb.meters function(avg(c1), avg(c2)) interval(999u)', -2147471097)
        # invalid tsma func param
        tdSql.error(
            'create tsma tsma2 on nsdb.meters function(avg(c1, c2), avg(c2)) interval(10m)',  -2147471096)
        # invalid param data type
        tdSql.error(
            'create tsma tsma2 on nsdb.meters function(avg(ts), avg(c2)) interval(10m)',  -2147473406)
        tdSql.error(
            'create tsma tsma2 on nsdb.meters function(avg(c3), avg(c2)) interval(10m)',  -2147473406)
        # invalid tsma func param
        tdSql.error(
            'create tsma tsma2 on nsdb.meters function(avg(c1+1), avg(c2)) interval(10m)', -2147471096)
        # invalid tsma func param
        tdSql.error(
            'create tsma tsma2 on nsdb.meters function(avg(c1*c2), avg(c2)) interval(10m)', -2147471096)

        # sma already exists in different db
         # SMA already exists in db    # Stream already exists 
        tdSql.error(
            'create tsma tsma1 on test.meters function(avg(c1), avg(c2)) interval(10m)', -2147482496) 
       

        # Invalid tsma interval, error format,including sliding and interval_offset
        tdSql.error(
            'create tsma tsma_error_interval on nsdb.meters function(count(c2)) interval(10)') #syntax error
        tdSql.error(
            'create tsma tsma_error_interval on nsdb.meters function(count(c2)) interval("10m")')
        tdSql.error(
            'create tsma tsma_error_interval on nsdb.meters function(count(c2)) interval(10,10m)') 
        tdSql.error(
            'create tsma tsma_error_interval on nsdb.meters function(count(c2)) interval(10s,10m)') 
        tdSql.error(
            'create tsma tsma_error_interval on nsdb.meters function(count(c2)) interval(10s) sliding(1m)')  


        if not ignore_some_tests:
            # max tsma num 8
            self.create_tsma('tsma2', 'nsdb', 'meters', ['avg(c1)', 'avg(c2)'], '10s')
            self.create_tsma('tsma_test3', 'test', 'meters', ['avg(c1)', 'avg(c2)'], '100s')
            self.create_tsma('tsma4', 'nsdb', 'meters', ['avg(c1)', 'avg(c2)'], '101s')
            self.create_tsma('tsma5', 'nsdb', 'meters', ['avg(c1)', 'count(ts)'], '102s')
            self.create_tsma('tsma6', 'nsdb', 'meters', ['avg(c1)', 'avg(c2)'], '103s')
            self.create_tsma('tsma7', 'nsdb', 'meters', ['avg(c1)', 'count(c2)'], '104s')
            self.create_tsma('tsma8', 'test', 'meters', ['avg(c1)', 'sum(c2)'], '105s')
            tdSql.error('create tsma tsma9 on nsdb.meters function(count(ts), count(c1), sum(c2)) interval(99s)', -2147482490)
            tdSql.error('create recursive tsma tsma9 on test.tsma8 interval(210s)', -2147482490)

            # modify  maxTsmaNum para
            tdSql.error('alter dnode 1  "maxTsmaNum" "13";')    
            tdSql.error('alter dnode 1  "maxTsmaNum" "-1";')    

            # tdSql.error('alter dnode 1  "maxTsmaNum" "abc";')
            # tdSql.error('alter dnode 1  "maxTsmaNum" "1.2";')

            tdSql.execute("alter dnode 1  'maxTsmaNum' '0';", queryTimes=1)
            tdSql.error('create tsma tsma9 on nsdb.meters function(count(ts), count(c1), sum(c2)) interval(99s)', -2147482490)        
            tdSql.execute("alter dnode 1  'maxTsmaNum' '12';", queryTimes=1)
            tdSql.execute('create tsma tsma9 on nsdb.meters function(count(ts), count(c1), sum(c2)) interval(109s)')
            tdSql.execute('create tsma tsma10 on nsdb.meters function(count(ts), count(c1), sum(c2)) interval(110s)')
            tdSql.execute('create tsma tsma11 on nsdb.meters function(count(ts), count(c1), sum(c2)) interval(111s)')
            tdSql.execute('create tsma tsma12 on nsdb.meters function(count(ts), count(c1), sum(c2)) interval(112s)')
            tdSql.query("show nsdb.tsmas", queryTimes=1)
            print(tdSql.queryResult)
            tdSql.query("show test.tsmas", queryTimes=1)
            print(tdSql.queryResult)
            tdSql.error('create tsma tsma13 on nsdb.meters function(count(ts), count(c1), sum(c2)) interval(113s)', -2147482490)


        # drop tsma
        tdSql.execute('drop tsma nsdb.tsma1', queryTimes=1)
        tdSql.execute('use test', queryTimes=1)
        tdSql.execute(
            'create tsma tsma1 on nsdb.meters function(avg(c1), avg(c2)) interval(10m)', queryTimes=1)
        self.wait_for_tsma_calculation(
            ['avg(c1)', 'avg(c2)'], 'nsdb', 'meters', '10m', 'tsma1')
        tdSql.execute('drop tsma nsdb.tsma1', queryTimes=1)

        self.wait_query('show transactions', 0, wait_query_seconds, lambda row: row[3] != 'stream-chkpt-u')
        tdSql.execute('drop database nsdb')

    def test_create_tsma_on_norm_table(self):
        function_name = sys._getframe().f_code.co_name
        tdLog.debug(f'-----{function_name}------')

    def test_create_tsma_on_child_table(self):
        function_name = sys._getframe().f_code.co_name
        tdLog.debug(f'-----{function_name}------')
        # Invalid table to create tsma, only stable or normal table allowed
        tdSql.error(
            'create tsma tsma1 on test.t1 function(avg(c1), avg(c2)) interval(1m)', -2147471098)

    def test_create_recursive_tsma(self):
        function_name = sys._getframe().f_code.co_name
        tdLog.debug(f'-----{function_name}------')
        tdSql.execute('use test')
        self.create_tsma('tsma1', 'test', 'meters', [
                         'avg(c1)', 'avg(c2)'], '5m')
        sql = 'create recursive tsma tsma2 on test.tsma1 function(avg(c1)) interval(1m)'
        tdSql.error(sql, -2147473920)  # syntax error 

        sql = 'create recursive tsma tsma2 on test.tsma1 interval(1m)'
        tdSql.error(sql, -2147471086)  # invalid tsma interval

        sql = 'create recursive tsma tsma2 on test.tsma1 interval(7m)'
        tdSql.error(sql, -2147471086)  # invalid tsma interval

        sql = 'create recursive tsma tsma2 on test.tsma1 interval(11m)'
        tdSql.error(sql, -2147471086)  # invalid tsma interval

        self.create_recursive_tsma('tsma1', 'tsma2', 'test', '20m', 'meters')

        sql = 'create recursive tsma tsma3 on test.tsma2 interval(30m)'
        tdSql.error(sql, -2147471086)  # invalid tsma interval

        self.create_recursive_tsma('tsma2', 'tsma3', 'test', '40m', 'meters')

        tdSql.execute('drop tsma test.tsma3', queryTimes=1)
        tdSql.execute('drop tsma test.tsma2', queryTimes=1)
        tdSql.execute('drop tsma test.tsma1', queryTimes=1)

    def test_create_tsma_maxlist_function(self):
        function_name = sys._getframe().f_code.co_name
        tdLog.debug(f'-----{function_name}------')
        json_file = "2-query/compa4096_tsma.json"
        tdCom.update_json_file_replica(json_file, self.replicaVar)
        os.system(f"taosBenchmark -f {json_file} -y ")
        # max number of list is 4093: 4096 - 3 - 2(原始表tag个数) - 1(tbname)
        tdSql.execute('use db4096')

        self.create_tsma('tsma_4050', 'db4096', 'stb0', self.generate_tsma_function_list_columns(4050), '5m',check_tsma_calculation=True)

        self.create_tsma('tsma_4090', 'db4096', 'stb0', self.generate_tsma_function_list_columns(4090), '6m',check_tsma_calculation=True)

        self.create_error_tsma('tsma_4091', 'db4096', 'stb0', self.generate_tsma_function_list_columns(4091), '5m',  -2147473856)  #Too many columns

        self.drop_tsma('tsma_4050', 'db4096')
        self.drop_tsma('tsma_4090', 'db4096')

    def generate_tsma_function_list_columns(self,max_column: int =4093):
        columns = []
        self.tsma_support_func = ["max", "min", "count", "sum"]
        num_items = len(self.tsma_support_func)
        for i in range(max_column):
            random_index = secrets.randbelow(num_items)
            tsma_column_element = self.tsma_support_func[random_index] + '(c' + str(i) + ')'
            columns.append(tsma_column_element)
        return columns
    
    def test_create_diffrent_tsma_name(self):
        function_name = sys._getframe().f_code.co_name
        tdLog.debug(f'-----{function_name}------')
        tdSql.execute('use test')
        
        self.create_tsma('tsma_repeat', 'test', 'meters', ['avg(c1)', 'avg(c2)'], '5m')
        tdSql.error('create tsma tsma_repeat on  test.meters function(avg(c1), avg(c2)) interval(10m)', -2147482496)  # DB error: SMA already exists in db 

        # same name with dbname, stable and child table
        self.test_create_and_drop_tsma('meters', 'test', 'meters', ['count(c1)', 'avg(c2)'], '5m')
        self.test_create_and_drop_tsma('t1', 'test', 'meters', ['count(c1)', 'avg(c2)'], '5m')
        self.test_create_and_drop_tsma('test', 'test', 'meters', ['count(c1)', 'avg(c2)'], '5m')

        # tsma name is key word
        tdSql.error("CREATE TSMA tsma ON test.meters FUNCTION(avg(c1)) INTERVAL(5m); ", -2147473920) # syntax error near
        tdSql.error("CREATE TSMA bool ON test.meters FUNCTION(avg(c1)) INTERVAL(5m); ", -2147473920)

        # tsma name is illegal
        tdSql.error("CREATE TSMA 129_tsma ON test.meters FUNCTION(count(c1)) INTERVAL(5m); ", -2147473920)
        tdSql.error("CREATE TSMA T*\-sma129_ ON test.meters FUNCTION(count(c1))  INTERVAL(5m); ", -2147473920)
        tdSql.error("CREATE TSMA Tsma_repeat ON test.meters FUNCTION(count(c1))  INTERVAL(5m); ", -2147482496)

        self.drop_tsma('tsma_repeat', 'test')
        # tsma name include escape character

        self.create_tsma('`129_tsma`', 'test', 'meters', ['count(c3)'], '5m')
        self.create_tsma('`129_Tsma`', 'test', 'meters', ['count(c3)'], '9m')
        self.create_tsma('`129_T*\-sma`', 'test', 'meters', ['count(c3)'], '10m', expected_tsma_name='129_T*\\\\-sma')
        tdSql.execute("drop tsma test.`129_tsma`")
        tdSql.execute("drop tsma test.`129_Tsma`")
        tdSql.execute("drop tsma test.`129_T*\-sma`")

    def test_create_and_drop_tsma(self, tsma_name: str, db_name: str = 'test', table_name: str = 'meters', func_list: List = ['avg(c1)', 'avg(c2)'], interval: str = '5m'):
        function_name = sys._getframe().f_code.co_name
        tdLog.debug(f'-----{function_name}------')
        tdSql.execute('use test')
        self.create_tsma(tsma_name, db_name, table_name , func_list, interval)
        self.drop_tsma(tsma_name, db_name)

    def test_create_illegal_tsma_sql(self):
        function_name = sys._getframe().f_code.co_name
        tdLog.debug(f'-----{function_name}------')
        tdSql.execute('use test')

        # DB error: Table does not exist
        tdSql.error('create tsma tsma_illegal on test.meterss function(count(c5), avg(c2)) interval(5m)',-2147473917) 

        # syntax error near
        tdSql.error('create tsma tsma_illegal on test.meters function() interval(5m)',-2147473920)  
        tdSql.error('create tsma tsma_illegal on test.meters function("count(c5)") interval(5m)',-2147473920)  
        tdSql.error('create tsma tsma_illegal on test.meters function(count(c5)+1) interval(5m)',-2147473920)  
        tdSql.error('create tsma tsma_illegal on test.meters function(avg(c1)+avg(c2)) interval(5m)',-2147473920) 

        # Invalid tsma func param, only one column allowed
        tdSql.error('create tsma tsma_illegal on test.meters function(count(1)) interval(5m)',-2147471096) 
        tdSql.error('create tsma tsma_illegal on test.meters function(count(c1,c2)) interval(5m)',-2147471096)                   
        tdSql.error('create tsma tsma_illegal on test.meters function(count(_wstart)) interval(5m)',-2147471096)          
        tdSql.error('create tsma tsma_illegal on test.meters function(count(_wend)) interval(5m)',-2147471096)   
        tdSql.error('create tsma tsma_illegal on test.meters function(count(_wduration)) interval(5m)',-2147471096)   

        # Tsma func not supported
        # TODO add all of funcs  not supported
        tdSql.error('create tsma tsma_illegal on test.meters function(abs(c1)) interval(5m)',-2147471095)  
        tdSql.error('create tsma tsma_illegal on test.meters function(last_row(c1)) interval(5m)',-2147471095) 

        # mixed tsma func not supported
        tdSql.error('create tsma tsma_illegal on test.meters function(last(c1),last_row(c1)) interval(5m)',-2147471095) 

        #  Invalid function para type 
        tdSql.error('create tsma tsma_illegal on test.meters function(avg(c8)) interval(5m)',-2147473406)  

    def test_flush_query(self):
        tdSql.execute('insert into test.norm_tb (ts,c1,c2) values (now,1,2)(now+1s,2,3)(now+2s,2,3)(now+3s,2,3) (now+4s,1,2)(now+5s,2,3)(now+6s,2,3)(now+7s,2,3); select  /*+ skip_tsma()*/  avg(c1),avg(c2) from  test.norm_tb interval(10m);select  avg(c1),avg(c2) from  test.norm_tb interval(10m);select * from information_schema.ins_stream_tasks;', queryTimes=1)
        tdSql.execute('flush database test', queryTimes=1)
        tdSql.query('select count(*) from test.meters', queryTimes=1)
        tdSql.checkData(0,0,100000)
        tdSql.query('select count(*) from test.norm_tb', queryTimes=1)
        tdSql.checkData(0,0,10008)
        tdSql.execute('flush database test', queryTimes=1)
        tdSql.query('select count(*) from test.meters', queryTimes=1)
        tdSql.checkData(0,0,100000)
        tdSql.query('select count(*) from test.norm_tb', queryTimes=1)
        tdSql.checkData(0,0,10008)

    def test_redistribute_vgroups(self):
        tdSql.redistribute_db_all_vgroups('test', self.replicaVar)
        tdSql.redistribute_db_all_vgroups('db4096', self.replicaVar)

    # def test_replica_dnode(self):
        
    # def test_split_dnode(self):

    def stop(self):
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")


event = threading.Event()

tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
