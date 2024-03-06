from random import randrange
import time
import threading

from util.log import *
from util.sql import *
from util.cases import *
from util.dnodes import *
from util.common import *
# from tmqCommon import *


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
        self.name = '' ## tsma name or table name
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
            pos = self.name.find('_') ## for tsma output child table
            if pos == 32:
                self.is_tsma_ = True

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
        tdSql.query("select to_timestamp('%s', 'yyyy-mm-dd hh24-mi-ss.ms')" % (ts))
        res = tdSql.queryResult[0][0]
        return res.timestamp() * 1000

    def should_query_with_table(self, tb_name: str, ts_begin: str = UsedTsma.TS_MIN, ts_end: str = UsedTsma.TS_MAX) -> 'TSMAQCBuilder':
        used_tsma: UsedTsma = UsedTsma()
        used_tsma.name = tb_name
        used_tsma.time_range_start = self.to_timestamp(ts_begin)
        used_tsma.time_range_end = self.to_timestamp(ts_end)
        used_tsma.is_tsma_ = False
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
        used_tsma : UsedTsma = UsedTsma()
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
            tdLog.exit('check explain failed for sql: %s \nexpect: %s \nactual: %s' % (sql, str(expect), str(query_ctx)))
        elif expect.has_tsma():
            tdLog.debug('check explain succeed for sql: %s \ntsma: %s' % (sql, str(expect.used_tsmas)))
        has_tsma = False
        for tsma in query_ctx.used_tsmas:
            has_tsma = has_tsma or tsma.is_tsma_
        if not has_tsma and len(query_ctx.used_tsmas) > 1:
            tdLog.exit(f'explain err for sql: {sql}, has multi non tsmas, {query_ctx.used_tsmas}')
        return query_ctx

    def check_result(self, sql: str, skip_order: bool = False):
        tdSql.execute("alter local 'querySmaOptimize' '1'")
        tsma_res = tdSql.getResult(sql)

        tdSql.execute("alter local 'querySmaOptimize' '0'")
        no_tsma_res = tdSql.getResult(sql)

        if no_tsma_res is None or tsma_res is None:
            if no_tsma_res != tsma_res:
                tdLog.exit("comparing tsma res for: %s got different rows of result: without tsma: %s, with tsma: %s" % (sql, str(no_tsma_res), str(tsma_res)))
            else:
                return

        if len(no_tsma_res) != len(tsma_res):
            tdLog.exit("comparing tsma res for: %s got different rows of result: \nwithout tsma: %s\nwith    tsma: %s" % (sql, str(no_tsma_res), str(tsma_res)))
        if skip_order:
            no_tsma_res.sort()
            tsma_res.sort()

        for row_no_tsma, row_tsma in zip(no_tsma_res, tsma_res):
            if row_no_tsma != row_tsma:
                tdLog.exit("comparing tsma res for: %s got different row data: no tsma row: %s, tsma row: %s \nno tsma res: %s \n  tsma res: %s" % (sql, str(row_no_tsma), str(row_tsma), str(no_tsma_res), str(tsma_res)))
        tdLog.info('result check succeed for sql: %s. \n   tsma-res: %s. \nno_tsma-res: %s' % (sql, str(tsma_res), str(no_tsma_res)))

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
        self.column_num: int = 9 ### c1 - c10
        self.tags_prefix: str = 't'
        self.tag_num: int = 6  ### t1 - t6
        self.char_tag_idx: List = [2,3]
        self.child_table_name_prefix: str = 't'
        self.child_table_num: int = 10  #### t0 - t9
        self.interval: bool = False
        self.partition_by: bool = False ## 70% generating a partition by, 30% no partition by, same as group by
        self.group_by: bool = False
        self.where_ts_range: bool = False ## generating no ts range condition is also possible
        self.where_tbname_func: bool = False
        self.where_tag_func: bool = False
        self.where_col_func: bool = False
        self.slimit_max = 10
        self.limit_max = 10

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
        self.ts_scan_range_: List[float] = [float(UsedTsma.TS_MIN), float(UsedTsma.TS_MAX)]
        self.agg_funcs_: List[str] = []
        self.tsmas_: List[TSMA] = [] ## currently created tsmas
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
        len  =random.randint(0,3)
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
            depth = random.randint(1,3)

        ret = self.generate_depthed_str_func(column_name, depth)
        tdLog.debug(f'generating str func: {ret}')
        return ret
    
    def generate_integer_func(self, column_name: str, depth: int = 0) -> str:
        pass

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
        auto_select_list = self.generate_select_list(select_list, partition_by_list)
        order_by = self.generate_order_by(order_by_list, partition_by_list)
        sql = f"SELECT {auto_select_list} FROM {tb} {where} {partition_by} {partition_by_list} {interval} {order_by} {limit}"
        return sql

    def can_ignore_res_order(self):
        return self.res_.can_ignore_res_order()

    def generate_where(self) -> str:
        return self.generate_ts_where_range()

    def generate_timestamp(self, min: float = -1, max: float = 0) -> int:
        milliseconds_aligned: float = random.randint(int(min), int(max))
        seconds_aligned = int( milliseconds_aligned/ 1000) * 1000
        if seconds_aligned < min:
            seconds_aligned = int(min)
        minutes_aligned = int(milliseconds_aligned / 1000 / 60) * 1000 * 60
        if minutes_aligned < min:
            minutes_aligned = int(min)
        hour_aligned = int(milliseconds_aligned / 1000 / 60 / 60) * 1000 * 60 * 60
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
            left_value = self.generate_timestamp(self.opts_.ts_min, self.opts_.ts_max)
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
        #tdLog.debug(f'{self.opts_.pk_col} range with: {a}')
        if len(a) > 0:
            self.res_.has_where_ts_range = True
            return f'WHERE {a}'
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

    ## add sliding offset
    def generate_interval(self, intervals: List[str]) -> str:
        if not self.opts_.interval:
            return ''
        if random.random() < 0.4: ## no interval
            return ''
        value = random.choice(intervals)
        self.res_.has_interval = True
        return f'INTERVAL({value})'

    def generate_tag_list(self):
        used_tag_num = random.randrange(1, self.opts_.tag_num)
        ret = ''
        for _ in range(used_tag_num):
            tag_idx = random.randint(1,self.opts_.tag_num)
            tag_name = self.opts_.tags_prefix + f'{tag_idx}'
            if random.random() < 0.5 and tag_idx in self.opts_.char_tag_idx:
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
                tag_idx = random.randint(1,self.opts_.tag_num)
                ret = ret + self.opts_.tags_prefix + f'{tag_idx},'
        return ret[:-1]
            

    ## TODO add tbname, tag functions
    def generate_partition_by(self):
        if not self.opts_.partition_by and not self.opts_.group_by:
            return ('','')
        ## no partition or group
        if random.random() < 0.3:
            return ('','')
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
            ## tbname and tag
            ret = self.generate_tbname_tag_list()
        tdLog.debug(f'partition by: {ret}')
        if self.res_.has_interval or random.random() < 0.5:
            self.res_.partition_by = True
            return (str('PARTITION BY'), f'{ret}')
        else:
            self.res_.group_by = True
            return (str('GROUP BY'),  f'{ret}')
    
    def generate_where_tbname(self) -> str:
        return self.generate_str_func('tbname')

    def generate_where_tag(self) -> str:
        #tag_idx = random.randint(1, self.opts_.tag_num)
        #tag = self.opts_.tags_prefix + str(tag_idx)
        return self.generate_str_func('t3')

    def generate_where_conditions(self) -> str:

        pass

    ## generate func in tsmas(select list)
    def _generate_agg_func_for_select(self) -> str:
        pass

    ## order by, limit, having, subquery...

class TDTestCase:
    updatecfgDict = {'debugFlag': 143, 'asynclog': 0}
    def __init__(self):
        self.vgroups    = 4
        self.ctbNum     = 10
        self.rowsPerTbl = 10000
        self.duraion = '1h'

    def init(self, conn, logSql, replicaVar=1):
        self.replicaVar = int(replicaVar)
        tdLog.debug(f"start to excute {__file__}")
        tdSql.init(conn.cursor(), False)
        self.tsma_tester: TSMATester = TSMATester(tdSql)
        self.tsma_sql_generator: TSMATestSQLGenerator = TSMATestSQLGenerator()

    def create_database(self,tsql, dbName,dropFlag=1,vgroups=2,replica=1, duration:str='1d'):
        if dropFlag == 1:
            tsql.execute("drop database if exists %s"%(dbName))

        tsql.execute("create database if not exists %s vgroups %d replica %d duration %s"%(dbName, vgroups, replica, duration))
        tdLog.debug("complete to create database %s"%(dbName))
        return

    def create_stable(self,tsql, paraDict):
        colString = tdCom.gen_column_type_str(colname_prefix=paraDict["colPrefix"], column_elm_list=paraDict["colSchema"])
        tagString = tdCom.gen_tag_type_str(tagname_prefix=paraDict["tagPrefix"], tag_elm_list=paraDict["tagSchema"])
        sqlString = f"create table if not exists %s.%s (%s) tags (%s)"%(paraDict["dbName"], paraDict["stbName"], colString, tagString)
        tdLog.debug("%s"%(sqlString))
        tsql.execute(sqlString)
        return

    def create_ctable(self,tsql=None, dbName='dbx',stbName='stb',ctbPrefix='ctb',ctbNum=1,ctbStartIdx=0):
        for i in range(ctbNum):
            sqlString = "create table %s.%s%d using %s.%s tags(%d, 'tb%d', 'tb%d', %d, %d, %d)" % \
                    (dbName,ctbPrefix,i+ctbStartIdx,dbName,stbName,(i+ctbStartIdx) % 5,i+ctbStartIdx,i+ctbStartIdx,i+ctbStartIdx,i+ctbStartIdx,i+ctbStartIdx)
            tsql.execute(sqlString)

        tdLog.debug("complete to create %d child tables by %s.%s" %(ctbNum, dbName, stbName))
        return

    def init_normal_tb(self, tsql, db_name: str, tb_name: str, rows: int, start_ts: int, ts_step: int):
        sql = 'CREATE TABLE %s.%s (ts timestamp, c1 INT, c2 INT, c3 VARCHAR(255), c4 INT)' % (db_name, tb_name)
        tsql.execute(sql)
        sql = 'INSERT INTO %s.%s values' % (db_name, tb_name)
        for j in range(rows):
            sql += '(%d, %d,%d,"varchar_%d",%d),' % (start_ts + j * ts_step + randrange(500), j % 10 + randrange(100), j % 10 + randrange(200), j % 10, j % 10)
        tsql.execute(sql)

    def insert_data(self,tsql,dbName,ctbPrefix,ctbNum,rowsPerTbl,batchNum,startTs,tsStep):
        tdLog.debug("start to insert data ............")
        tsql.execute("use %s" %dbName)
        pre_insert = "insert into "
        sql = pre_insert

        for i in range(ctbNum):
            rowsBatched = 0
            sql += " %s%d values "%(ctbPrefix,i)
            for j in range(rowsPerTbl):
                if (i < ctbNum/2):
                    sql += "(%d, %d, %d, %d,%d,%d,%d,true,'binary%d', 'nchar%d') "%(startTs + j*tsStep + randrange(500), j%10 + randrange(100), j%10 + randrange(200), j%10, j%10, j%10, j%10, j%10, j%10)
                else:
                    sql += "(%d, %d, NULL, %d,NULL,%d,%d,true,'binary%d', 'nchar%d') "%(startTs + j*tsStep + randrange(500), j%10, j%10, j%10, j%10, j%10, j%10)
                rowsBatched += 1
                if ((rowsBatched == batchNum) or (j == rowsPerTbl - 1)):
                    tsql.execute(sql)
                    rowsBatched = 0
                    if j < rowsPerTbl - 1:
                        sql = "insert into %s%d values " %(ctbPrefix,i)
                    else:
                        sql = "insert into "
        if sql != pre_insert:
            tsql.execute(sql)
        tdLog.debug("insert data ............ [OK]")
        return

    def init_data(self, ctb_num: int = 10, rows_per_ctb: int = 10000, start_ts : int = 1537146000000 , ts_step : int = 500):
        tdLog.printNoPrefix("======== prepare test env include database, stable, ctables, and insert data: ")
        paraDict = {'dbName':     'test',
                    'dropFlag':   1,
                    'vgroups':    2,
                    'stbName':    'meters',
                    'colPrefix':  'c',
                    'tagPrefix':  't',
                    'colSchema':   [{'type': 'INT', 'count':1},{'type': 'BIGINT', 'count':1},{'type': 'FLOAT', 'count':1},{'type': 'DOUBLE', 'count':1},{'type': 'smallint', 'count':1},{'type': 'tinyint', 'count':1},{'type': 'bool', 'count':1},{'type': 'binary', 'len':10, 'count':1},{'type': 'nchar', 'len':10, 'count':1}],
                    'tagSchema':   [{'type': 'INT', 'count':1},{'type': 'nchar', 'len':20, 'count':1},{'type': 'binary', 'len':20, 'count':1},{'type': 'BIGINT', 'count':1},{'type': 'smallint', 'count':1},{'type': 'DOUBLE', 'count':1}],
                    'ctbPrefix':  't',
                    'ctbStartIdx': 0,
                    'ctbNum':     ctb_num,
                    'rowsPerTbl': rows_per_ctb,
                    'batchNum':   3000,
                    'startTs':    start_ts,
                    'tsStep':     ts_step}

        paraDict['vgroups'] = self.vgroups
        paraDict['ctbNum'] = self.ctbNum
        paraDict['rowsPerTbl'] = self.rowsPerTbl

        tdLog.info("create database")
        self.create_database(tsql=tdSql, dbName=paraDict["dbName"], dropFlag=paraDict["dropFlag"], vgroups=paraDict["vgroups"], replica=self.replicaVar, duration=self.duraion)

        tdLog.info("create stb")
        self.create_stable(tsql=tdSql, paraDict=paraDict)

        tdLog.info("create child tables")
        self.create_ctable(tsql=tdSql, dbName=paraDict["dbName"], \
                stbName=paraDict["stbName"],ctbPrefix=paraDict["ctbPrefix"],\
                ctbNum=paraDict["ctbNum"],ctbStartIdx=paraDict["ctbStartIdx"])
        self.insert_data(tsql=tdSql, dbName=paraDict["dbName"],\
                ctbPrefix=paraDict["ctbPrefix"],ctbNum=paraDict["ctbNum"],\
                rowsPerTbl=paraDict["rowsPerTbl"],batchNum=paraDict["batchNum"],\
                startTs=paraDict["startTs"],tsStep=paraDict["tsStep"])
        self.init_normal_tb(tdSql, paraDict['dbName'], 'norm_tb', paraDict['rowsPerTbl'], paraDict['startTs'], paraDict['tsStep'])

    def wait_for_tsma_calculation(self, func_list: list, db: str, tb: str, interval: str, tsma_name: str):
        while True:
            sql = 'select %s from %s.%s interval(%s)' % (', '.join(func_list), db, tb, interval)
            tdLog.debug(f'waiting for tsma {db}.{tsma_name} to be useful with sql {sql}')
            ctx: TSMAQueryContext = self.tsma_tester.get_tsma_query_ctx(sql)
            if ctx.has_tsma():
                if ctx.used_tsmas[0].name == tsma_name + UsedTsma.TSMA_RES_STB_POSTFIX:
                    break
                elif ctx.used_tsmas[0].name.find('_') == 32 and ctx.used_tsmas[0].name[33:] == tb:
                    break
                else:
                    time.sleep(1)
            else:
                time.sleep(1)


    def create_tsma(self, tsma_name: str, db: str, tb: str, func_list: list, interval: str):
        tdSql.execute('use %s' % db)
        sql = "CREATE TSMA %s ON %s.%s FUNCTION(%s) INTERVAL(%s)" % (tsma_name, db, tb, ','.join(func_list), interval)
        tdSql.execute(sql, queryTimes=1)
        self.wait_for_tsma_calculation(func_list, db, tb, interval, tsma_name)

    def create_recursive_tsma(self, base_tsma_name: str, new_tsma_name: str, db: str, interval: str, tb_name: str, func_list: List[str] = ['avg(c1)']):
        tdSql.execute('use %s' % db, queryTimes=1)
        sql = 'CREATE RECURSIVE TSMA %s ON %s.%s INTERVAL(%s)' % (new_tsma_name, db, base_tsma_name, interval)
        tdSql.execute(sql, queryTimes=1)
        self.wait_for_tsma_calculation(func_list, db, tb_name, interval, new_tsma_name)

    def drop_tsma(self, tsma_name: str, db: str):
        sql = 'DROP TSMA %s.%s' % (db, tsma_name)
        tdSql.execute(sql, queryTimes=1)

    def check_explain_res_has_row(self, plan_str_expect: str, explain_output):
        plan_found = False
        for row in explain_output:
            if str(row).find(plan_str_expect) >= 0:
                tdLog.debug("plan: [%s] found in: [%s]" % (plan_str_expect, str(row)))
                plan_found = True
                break
        if not plan_found:
            tdLog.exit("plan: %s not found in res: [%s]" % (plan_str_expect, str(explain_output)))

    def check(self, func):
        for ctx in func():
            self.tsma_tester.check_sql(ctx.sql, ctx)

    def test_query_with_tsma(self):
        self.create_tsma('tsma1', 'test', 'meters', ['avg(c1)', 'avg(c2)'], '5m')
        self.create_tsma('tsma2', 'test', 'meters', ['avg(c1)', 'avg(c2)'], '30m')
        self.create_tsma('tsma5', 'test', 'norm_tb', ['avg(c1)', 'avg(c2)'], '10m')

        self.test_query_with_tsma_interval()
        self.test_query_with_tsma_agg()
        self.test_recursive_tsma()
        ## self.test_query_with_drop_tsma()
        ## self.test_query_with_add_tag()
        ## self.test_union()
        self.test_query_sub_table()

    def test_query_sub_table(self):
        sql = 'select avg(c1) from t1'
        ctx = TSMAQCBuilder().with_sql(sql).should_query_with_tsma('e8945e7385834f8c22705546d4016539_t1', UsedTsma.TS_MIN, UsedTsma.TS_MAX, child_tb=True).get_qc()
        self.tsma_tester.check_sql(sql, ctx)
        sql = 'select avg(c1) from t3'
        ctx = TSMAQCBuilder().with_sql(sql).should_query_with_tsma('e8945e7385834f8c22705546d4016539_t3', child_tb=True).get_qc()
        self.tsma_tester.check_sql(sql, ctx)

    def test_recursive_tsma(self):
        tdSql.execute('drop tsma tsma2')
        func_list: List[str] = ['avg(c2)', 'avg(c3)']
        self.create_tsma('tsma3', 'test', 'meters', func_list, '5m')
        self.create_recursive_tsma('tsma3', 'tsma4', 'test', '20m', 'meters', func_list)
        ## now we have 5m, 10m, 30m, 1h 4 tsmas
        sql = 'select avg(c2), "recursive tsma4" from meters'
        ctx = TSMAQCBuilder().with_sql(sql).should_query_with_tsma('tsma4', UsedTsma.TS_MIN,UsedTsma.TS_MAX).get_qc()
        self.tsma_tester.check_sql(sql, ctx)
        self.create_recursive_tsma('tsma4', 'tsma6', 'test', '1h', 'meters', func_list)
        ctx = TSMAQCBuilder().with_sql(sql).should_query_with_tsma('tsma6', UsedTsma.TS_MIN,UsedTsma.TS_MAX).get_qc()
        self.tsma_tester.check_sql(sql, ctx)

        tdSql.error('drop tsma tsma3', -2147482491)
        tdSql.error('drop tsma tsma4', -2147482491)
        tdSql.execute('drop tsma tsma6')
        tdSql.execute('drop tsma tsma4')
        tdSql.execute('drop tsma tsma3')
        self.create_tsma('tsma2', 'test', 'meters', ['avg(c1)', 'avg(c2)'], '30m')

    def test_query_with_tsma_interval(self):
        self.check(self.test_query_with_tsma_interval_possibly_partition)
        self.check(self.test_query_with_tsma_interval_partition_by_col)
    
    def test_query_with_tsma_interval_possibly_partition(self) -> List[TSMAQueryContext]:
        ctxs: List[TSMAQueryContext] = []
        sql = 'select avg(c1), avg(c2) from meters interval(5m)'
        ctxs.append(TSMAQCBuilder().with_sql(sql) \
                    .should_query_with_tsma('tsma1', UsedTsma.TS_MIN,UsedTsma.TS_MAX).get_qc())

        sql = 'select avg(c1), avg(c2) from meters interval(10m)'
        ctxs.append(TSMAQCBuilder().with_sql(sql) \
                    .should_query_with_tsma('tsma1', UsedTsma.TS_MIN,UsedTsma.TS_MAX).get_qc())
        sql = 'select avg(c1), avg(c2) from meters interval(30m)'
        ctxs.append(TSMAQCBuilder().with_sql(sql) \
                    .should_query_with_tsma('tsma2', UsedTsma.TS_MIN,UsedTsma.TS_MAX).get_qc())
        sql = 'select avg(c1), avg(c2) from meters interval(60m)'
        ctxs.append(TSMAQCBuilder().with_sql(sql) \
                    .should_query_with_tsma('tsma2', UsedTsma.TS_MIN,UsedTsma.TS_MAX).get_qc())

        sql = "select avg(c1), avg(c2) from meters where ts >= '2018-09-17 09:00:00.009' and ts < '2018-09-17 10:23:19.665' interval(30m)"
        ctxs.append(TSMAQCBuilder().with_sql(sql) \
                    .should_query_with_table('meters', '2018-09-17 09:00:00.009','2018-09-17 09:29:59.999') \
                        .should_query_with_tsma('tsma2', '2018-09-17 09:30:00','2018-09-17 09:59:59.999') \
                            .should_query_with_table('meters', '2018-09-17 10:00:00.000','2018-09-17 10:23:19.664').get_qc())

        sql = "SELECT avg(c1), avg(c2),_wstart, _wend,t3,t4,t5,t2 FROM meters WHERE ts >= '2018-09-17 8:00:00' AND ts < '2018-09-17 09:03:18.334' PARTITION BY t3,t4,t5,t2 INTERVAL(1d);"
        ctxs.append(TSMAQCBuilder().with_sql(sql) \
                    .should_query_with_table('meters', '2018-09-17 8:00:00', '2018-09-17 09:03:18.333' ).get_qc())


        interval_list = ['1s', '5s', '60s', '1m', '10m', '20m', '30m', '59s', '1h', '120s', '1200', '2h', '90m', '1d']
        opts: TSMATesterSQLGeneratorOptions = TSMATesterSQLGeneratorOptions()
        opts.interval = True
        opts.where_ts_range = True
        for _ in range(1, 100):
            opts.partition_by = True
            sql_generator = TSMATestSQLGenerator(opts)
            sql = sql_generator.generate_one('avg(c1), avg(c2)', ['meters', 't1', 't9'], '', interval_list)
            ctxs.append(TSMAQCBuilder().with_sql(sql).ignore_query_table().ignore_res_order(sql_generator.can_ignore_res_order()).get_qc())

            opts.partition_by = False
            sql_generator = TSMATestSQLGenerator(opts)
            sql = sql_generator.generate_one('avg(c1), avg(c2)', ['norm_tb', 't5'], '', interval_list)
            ctxs.append(TSMAQCBuilder().with_sql(sql).ignore_query_table().ignore_res_order(sql_generator.can_ignore_res_order()).get_qc())
        return ctxs

    def test_query_with_tsma_interval_partition_by_col(self):
        return []

    def test_query_with_tsma_agg(self):
        self.check(self.test_query_with_tsma_agg_no_group_by)
        self.check(self.test_query_with_tsma_agg_group_by_tbname)
        self.check(self.test_query_with_tsma_with_having)

    def test_query_with_tsma_agg_no_group_by(self):
        ctxs: List[TSMAQueryContext] = []
        sql = 'select avg(c1), avg(c2) from meters'
        ctxs.append(TSMAQCBuilder().with_sql(sql).should_query_with_tsma('tsma2').get_qc())

        sql = 'select avg(c1), avg(c2) from meters where ts between "2018-09-17 09:00:00.000" and "2018-09-17 10:00:00.000"'
        ctxs.append(TSMAQCBuilder().with_sql(sql) \
                    .should_query_with_tsma('tsma2', '2018-09-17 09:00:00','2018-09-17 09:59:59:999') \
                        .should_query_with_table("meters", '2018-09-17 10:00:00','2018-09-17 10:00:00').get_qc())

        sql = 'select avg(c1), avg(c2) from meters where ts between "2018-09-17 09:00:00.200" and "2018-09-17 10:23:19.800"'
        ctxs.append(TSMAQCBuilder().with_sql(sql) \
                    .should_query_with_table('meters', '2018-09-17 09:00:00.200','2018-09-17 09:29:59:999') \
                        .should_query_with_tsma('tsma2', '2018-09-17 09:30:00','2018-09-17 09:59:59.999') \
                            .should_query_with_table('meters', '2018-09-17 10:00:00.000','2018-09-17 10:23:19.800').get_qc())

        sql = 'select avg(c1) + avg(c2), avg(c2) from meters where ts between "2018-09-17 09:00:00.200" and "2018-09-17 10:23:19.800"'
        ctxs.append(TSMAQCBuilder().with_sql(sql) \
                    .should_query_with_table('meters', '2018-09-17 09:00:00.200','2018-09-17 09:29:59:999') \
                        .should_query_with_tsma('tsma2', '2018-09-17 09:30:00','2018-09-17 09:59:59.999') \
                            .should_query_with_table('meters', '2018-09-17 10:00:00.000','2018-09-17 10:23:19.800').get_qc())

        sql = 'select avg(c1) + avg(c2), avg(c2) + 1 from meters where ts between "2018-09-17 09:00:00.200" and "2018-09-17 10:23:19.800"'
        ctxs.append(TSMAQCBuilder().with_sql(sql) \
                    .should_query_with_table('meters', '2018-09-17 09:00:00.200','2018-09-17 09:29:59:999') \
                        .should_query_with_tsma('tsma2', '2018-09-17 09:30:00','2018-09-17 09:59:59.999') \
                            .should_query_with_table('meters', '2018-09-17 10:00:00.000','2018-09-17 10:23:19.800').get_qc())

        sql = "select avg(c1) + 1, avg(c2) from meters where ts >= '2018-09-17 9:30:00.118' and ts < '2018-09-17 10:50:00'"
        ctxs.append(TSMAQCBuilder().with_sql(sql) \
                    .should_query_with_table('meters', '2018-09-17 9:30:00.118', '2018-09-17 9:59:59.999') \
                        .should_query_with_tsma('tsma2', '2018-09-17 10:00:00', '2018-09-17 10:29:59.999') \
                            .should_query_with_tsma('tsma1', '2018-09-17 10:30:00.000', '2018-09-17 10:49:59.999').get_qc())
        
        sql = "select avg(c1), avg(c2) from meters where ts >= '2018-09-17 9:00:00' and ts < '2018-09-17 9:45:00' limit 2"
        ctxs.append(TSMAQCBuilder().with_sql(sql) \
                    .should_query_with_tsma('tsma2', '2018-09-17 9:00:00', '2018-09-17 9:29:59.999') \
                        .should_query_with_tsma('tsma1', '2018-09-17 9:30:00', '2018-09-17 9:44:59.999').get_qc())
        
        sql = 'select avg(c1) + avg(c2) from meters where tbname like "%t1%"'
        ctxs.append(TSMAQCBuilder().with_sql(sql).should_query_with_tsma('tsma2').get_qc())

        sql = 'select avg(c1), avg(c2) from meters where c1 is not NULL'
        ctxs.append(TSMAQCBuilder().with_sql(sql).should_query_with_table('meters').get_qc())

        sql = 'select avg(c1), avg(c2), spread(c4) from meters'
        ctxs.append(TSMAQCBuilder().with_sql(sql).should_query_with_table('meters').get_qc())

        sql = 'select avg(c1), avg(c2) from meters where tbname = \'t1\''
        ctxs.append(TSMAQCBuilder().with_sql(sql).should_query_with_tsma('tsma2').get_qc())

        sql = 'select avg(c1), avg(c2) from meters where tbname = \'t1\' or tbname = \'t2\''
        ctxs.append(TSMAQCBuilder().with_sql(sql).should_query_with_tsma('tsma2').get_qc())

        sql = '''select avg(c1), avg(c2) from meters where tbname = 't1' and c1 is not NULL'''
        ctxs.append(TSMAQCBuilder().with_sql(sql).should_query_with_table('meters').get_qc())
        
        sql = 'select avg(c1+c2) from meters'
        ctxs.append(TSMAQCBuilder().with_sql(sql).should_query_with_table('meters').get_qc())

        sql = 'select avg(c1), avg(c2) from meters where ts >= "2018-09-17 9:25:00" and ts < "2018-09-17 10:00:00" limit 6'
        ctxs.append(TSMAQCBuilder().with_sql(sql).should_query_with_tsma('tsma1', '2018-09-17 9:25:00', '2018-09-17 9:29:59.999') \
                    .should_query_with_tsma('tsma2', '2018-09-17 9:30:00', '2018-09-17 9:59:59.999').get_qc())

        return ctxs

    def test_query_with_tsma_agg_group_by_tbname(self):
        ctxs: List[TSMAQueryContext] = []
        sql = 'select avg(c1) as a, avg(c2) as b, tbname from meters group by tbname order by tbname, a, b'
        ctxs.append(TSMAQCBuilder().with_sql(sql).should_query_with_tsma('tsma2').get_qc())

        sql = 'select avg(c1) as a, avg(c2) + 1 as b, tbname from meters where c1 > 10 group by tbname order by tbname, a, b'
        ctxs.append(TSMAQCBuilder().with_sql(sql).should_query_with_table('meters').get_qc())

        sql = 'select avg(c1) + avg(c2) as a, avg(c2) + 1 as b, tbname from meters where ts between "2018-09-17 09:00:00.200" and "2018-09-17 10:23:19.800" group by tbname order by tbname, a, b'
        ctxs.append(TSMAQCBuilder().with_sql(sql)\
                    .should_query_with_table('meters', '2018-09-17 09:00:00.200','2018-09-17 09:29:59:999') \
                        .should_query_with_tsma('tsma2', '2018-09-17 09:30:00','2018-09-17 09:59:59.999') \
                            .should_query_with_table('meters', '2018-09-17 10:00:00.000','2018-09-17 10:23:19.800').get_qc())

        sql = 'select avg(c1) + avg(c2) + 3 as a, substr(tbname, 1) as c from meters group by substr(tbname, 1) order by c, a'
        ctxs.append(TSMAQCBuilder().with_sql(sql).should_query_with_tsma('tsma2').get_qc())

        sql = 'select avg(c1) + avg(c2) as a, avg(c2) + 1 as b, substr(tbname, 1, 1) as c from meters where ts between "2018-09-17 09:00:00.200" and "2018-09-17 10:23:19.800" group by substr(tbname, 1, 1) order by c, a, b'
        ctxs.append(TSMAQCBuilder().with_sql(sql)\
                    .should_query_with_table('meters', '2018-09-17 09:00:00.200','2018-09-17 09:29:59:999') \
                        .should_query_with_tsma('tsma2', '2018-09-17 09:30:00','2018-09-17 09:59:59.999') \
                            .should_query_with_table('meters', '2018-09-17 10:00:00.000','2018-09-17 10:23:19.800').get_qc())

        sql = 'select avg(c1), tbname from meters group by tbname having avg(c1) > 0 order by tbname'
        ctxs.append(TSMAQCBuilder().with_sql(sql).should_query_with_tsma('tsma2').get_qc())
        sql = 'select avg(c1), tbname from meters group by tbname having avg(c1) > 0 and tbname = "t1"'
        ctxs.append(TSMAQCBuilder().with_sql(sql).should_query_with_tsma('tsma2').get_qc())

        sql = 'select avg(c1), tbname from meters group by tbname having avg(c1) > 0 and tbname = "t1" order by tbname'
        ctxs.append(TSMAQCBuilder().with_sql(sql).should_query_with_tsma('tsma2').get_qc())

        sql = 'select avg(c1) + 1, tbname from meters group by tbname having avg(c1) > 0 and tbname = "t1" order by tbname'
        ctxs.append(TSMAQCBuilder().with_sql(sql).should_query_with_tsma('tsma2').get_qc())
        sql = 'select avg(c1) + 1, tbname from meters group by tbname having avg(c1) > 0 and tbname like "t%" order by tbname'
        ctxs.append(TSMAQCBuilder().with_sql(sql).should_query_with_tsma('tsma2').get_qc())

        return ctxs

    def test_query_with_tsma_with_having(self):
        return []

    def test_ddl(self):
        self.test_create_tsma()
        self.test_drop_tsma()
        self.test_tb_ddl_with_created_tsma()

    def run(self):
        self.init_data()
        #time.sleep(999999)
        #self.test_ddl()
        self.test_query_with_tsma()
        #time.sleep(999999)

    def test_create_tsma(self):
        function_name = sys._getframe().f_code.co_name
        tdLog.debug(f'-----{function_name}------')
        self.test_create_tsma_on_stable()
        self.test_create_tsma_on_norm_table()
        self.test_create_tsma_on_child_table()
        self.test_create_recursive_tsma()
        ## self.test_drop_stable() ## drop stable and recreate a stable
        ## self.test_drop_ctable()
        self.test_drop_db()

    def test_drop_tsma(self):
        function_name = sys._getframe().f_code.co_name
        tdLog.debug(f'-----{function_name}------')
        self.create_tsma('tsma1', 'test', 'meters', ['avg(c1)', 'avg(c2)'], '5m')
        self.create_recursive_tsma('tsma1', 'tsma2', 'test', '15m', 'meters')

        tdSql.error('drop tsma tsma1', -2147482491) ## drop recursive tsma first
        tdSql.execute('drop tsma tsma2', queryTimes=1)
        tdSql.execute('drop tsma tsma1', queryTimes=1)
        tdSql.execute('drop database test', queryTimes=1)

        self.init_data()

    def test_drop_db(self):
        function_name = sys._getframe().f_code.co_name
        tdLog.debug(f'-----{function_name}------')
        tdSql.execute('create database nsdb precision "ns"', queryTimes=1)
        tdSql.execute('use nsdb', queryTimes=1)
        tdSql.execute('create table meters(ts timestamp, c1 int, c2 int) tags(t1 int, t2 int)', queryTimes=1)
        ## TODO insert data
        self.create_tsma('tsma1', 'nsdb', 'meters', ['avg(c1)', 'avg(c2)'], '5m')
        self.create_recursive_tsma('tsma1', 'tsma2', 'nsdb', '10m', 'meters')
        tdSql.query('select avg(c1) from meters', queryTimes=1)
        tdSql.execute('drop database nsdb', queryTimes=1)

    def test_tb_ddl_with_created_tsma(self):
        function_name = sys._getframe().f_code.co_name
        tdLog.debug(f'-----{function_name}------')
        tdSql.execute('create database nsdb precision "ns"', queryTimes=1)
        tdSql.execute('use nsdb', queryTimes=1)
        tdSql.execute('create table meters(ts timestamp, c1 int, c2 int) tags(t1 int, t2 int)', queryTimes=1)
        self.create_tsma('tsma1', 'nsdb', 'meters', ['avg(c1)', 'avg(c2)'], '5m')
        ## drop column, drop tag
        tdSql.error('alter table meters drop column c1', -2147482637)
        tdSql.error('alter table meters drop tag t1', -2147482637)
        tdSql.error('alter table meters drop tag t2', -2147482637) # Stream must be dropped first 
        tdSql.execute('drop tsma tsma1', queryTimes=1)

        ## add tag
        tdSql.execute('alter table meters add tag t3 int', queryTimes=1)
        tdSql.execute('alter table meters drop tag t3', queryTimes=1)
        tdSql.execute('drop database nsdb')

        ## TODO test drop stream

    def test_create_tsma_on_stable(self):
        function_name = sys._getframe().f_code.co_name
        tdLog.debug(f'-----{function_name}------')
        tdSql.execute('create database nsdb precision "ns"', queryTimes=1)
        tdSql.execute('use nsdb', queryTimes=1)
        tdSql.execute('create table meters(ts timestamp, c1 int, c2 int) tags(t1 int, t2 int)', queryTimes=1)
        self.create_tsma('tsma1', 'nsdb', 'meters', ['avg(c1)', 'avg(c2)'], '5m')
        tdSql.error('create tsma tsma2 on meters function(avg(c1), avg(c2)) interval(2h)', -2147471097) ## Invalid tsma interval, 1ms ~ 1h is allowed
        tdSql.error('create tsma tsma2 on meters function(avg(c1), avg(c2)) interval(3601s)', -2147471097)
        tdSql.error('create tsma tsma2 on meters function(avg(c1), avg(c2)) interval(3600001a)', -2147471097)
        tdSql.error('create tsma tsma2 on meters function(avg(c1), avg(c2)) interval(3600001000u)', -2147471097)
        tdSql.error('create tsma tsma2 on meters function(avg(c1), avg(c2)) interval(999999b)', -2147471097)
        tdSql.error('create tsma tsma2 on meters function(avg(c1), avg(c2)) interval(999u)', -2147471097)

        tdSql.execute('drop tsma tsma1', queryTimes=1)
        tdSql.execute('use test', queryTimes=1)
        tdSql.execute('create tsma tsma1 on nsdb.meters function(avg(c1), avg(c2)) interval(10m)', queryTimes=1)
        self.wait_for_tsma_calculation(['avg(c1)', 'avg(c2)'], 'nsdb', 'meters', '10m', 'tsma1')
        tdSql.execute('drop tsma nsdb.tsma1', queryTimes=1)

        tdSql.error('create tsma tsma1 on test.meters function(avg(c1), avg(c2)) interval(2h)', -2147471097)
        tdSql.execute('drop database nsdb')

    def test_create_tsma_on_norm_table(self):
        function_name = sys._getframe().f_code.co_name
        tdLog.debug(f'-----{function_name}------')

    def test_create_tsma_on_child_table(self):
        function_name = sys._getframe().f_code.co_name
        tdLog.debug(f'-----{function_name}------')
        tdSql.error('create tsma tsma1 on test.t1 function(avg(c1), avg(c2)) interval(1m)', -2147471098) ## Invalid table to create tsma, only stable or normal table allowed

    def test_create_recursive_tsma(self):
        function_name = sys._getframe().f_code.co_name
        tdLog.debug(f'-----{function_name}------')
        tdSql.execute('use test')
        self.create_tsma('tsma1', 'test', 'meters', ['avg(c1)', 'avg(c2)'], '5m')
        sql = 'create recursive tsma tsma2 on tsma1 interval(1m)'
        tdSql.error(sql, -2147471099) ## invalid tsma parameter
        sql = 'create recursive tsma tsma2 on tsma1 interval(7m)'
        tdSql.error(sql, -2147471099) ## invalid tsma parameter
        sql = 'create recursive tsma tsma2 on tsma1 interval(11m)'
        tdSql.error(sql, -2147471099) ## invalid tsma parameter
        self.create_recursive_tsma('tsma1', 'tsma2', 'test', '20m', 'meters')

        tdSql.execute('drop tsma tsma2', queryTimes=1)
        tdSql.execute('drop tsma tsma1', queryTimes=1)

    def stop(self):
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")

event = threading.Event()

tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
