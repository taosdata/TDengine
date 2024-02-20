from os import name
from random import randrange
from socket import TIPC_ADDR_NAMESEQ
import taos
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

class TSMAQueryContext:
    def __init__(self) -> None:
        self.sql = ''
        self.used_tsmas: List[UsedTsma] = []

    def __eq__(self, __value) -> bool:
        if isinstance(__value, self.__class__):
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

    def should_query_with_table(self, tb_name: str, ts_begin: str, ts_end: str) -> 'TSMAQCBuilder':
        used_tsma: UsedTsma = UsedTsma()
        used_tsma.name = tb_name
        used_tsma.time_range_start = self.to_timestamp(ts_begin)
        used_tsma.time_range_end = self.to_timestamp(ts_end)
        used_tsma.is_tsma_ = False
        self.qc_.used_tsmas.append(used_tsma)
        return self

    def should_query_with_tsma(self, tsma_name: str, ts_begin: str, ts_end: str) -> 'TSMAQCBuilder':
        used_tsma: UsedTsma = UsedTsma()
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

    def check_explain(self, sql: str, expect: TSMAQueryContext):
        query_ctx = self.get_tsma_query_ctx(sql)
        if not query_ctx == expect:
            tdLog.exit('check explain failed for sql: %s \nexpect: %s \nactual: %s' % (sql, str(expect), str(query_ctx)))

    def check_result(self, sql: str):
        tdSql.execute("alter local 'querySmaOptimize' '1'")
        tsma_res = tdSql.getResult(sql)

        tdSql.execute("alter local 'querySmaOptimize' '0'")
        no_tsma_res = tdSql.getResult(sql)

        if no_tsma_res is None or tsma_res is None:
            if no_tsma_res != tsma_res:
                tdLog.exit("comparing tsma res for: %s got different rows of result: with tsma: %s, with tsma: %s" % (sql, str(no_tsma_res), str(tsma_res)))
            else:
                return

        if len(no_tsma_res) != len(tsma_res):
            tdLog.exit("comparing tsma res for: %s got differnt rows of result: without tsma: %d, with tsma: %d" % (sql, len(no_tsma_res), len(tsma_res)))
        for row_no_tsma, row_tsma in zip(no_tsma_res, tsma_res):
            if row_no_tsma != row_tsma:
                tdLog.exit("comparing tsma res for: %s got different row data: no tsma row: %s, tsma row: %s \nno tsma res: %s \n  tsma res: %s" % (sql, str(row_no_tsma), str(row_tsma), str(no_tsma_res), str(tsma_res)))
        tdLog.info('result check succeed for sql: %s. \n   tsma-res: %s. \nno_tsma-res: %s' % (sql, str(tsma_res), str(no_tsma_res)))

    def check_sql(self, sql: str, expect: TSMAQueryContext):
        self.check_explain(sql, expect=expect)
        if expect.has_tsma():
            self.check_result(sql)

    def check_sqls(self, sqls: list[str], expects: list[TSMAQueryContext]):
        for sql, query_ctx in zip(sqls, expects):
            self.check_sql(sql, query_ctx)

class TSMATesterSQLGeneratorOptions:
    def __init__(self) -> None:
        pass

class TSMATestSQLGenerator:
    def __init__(self, opts: TSMATesterSQLGeneratorOptions):
        self.db_name_: str = ''
        self.tb_name_: str = ''
        self.ts_scan_range_: List[float] = [float(UsedTsma.TS_MIN), float(UsedTsma.TS_MAX)]
        self.agg_funcs_: List[str] = []
        self.tsmas_: List[TSMA] = [] ## currently created tsmas
        self.opts_: TSMATesterSQLGeneratorOptions = opts

        self.select_list_: List[str] = []
        self.where_list_: List[str] = []
        self.group_or_partition_by_list: List[str] = []
        self.interval: str = ''
    
    def get_random_type(self, funcs):
        rand: int = randrange(1, len(funcs))
        return funcs[rand-1]()
    
    def generate_one(self) -> str:
        pass

    def generate_timestamp(self, left: float = -1) -> str:
        pass

    def _generate_between(self):
        def generate(generator: TSMATestSQLGenerator):
            left = generator.generate_timestamp()
            return "BTEWEEN %s and %s" % (left, generator.generate_timestamp(left))
        return self.get_random_type([lambda: '', generate])

    def _generate_scan_range_operators(self):
        left = self._generate_scan_range_left()
        right = self._generate_scan_range_right(float(left.split(' ')[-1]))
        if len(left) == 0 and len(right) == 0:
            return ''
        sql = ' ts '
        if len(left) > 0:
            sql += '%s ' % (left)
        
        if len(right) > 0:
            if len(sql) > 0:
                sql += 'and ts '
            sql += '%s ' % (right)
        return sql

    def _generate_scan_range_left(self) -> str:
        def a(g: TSMATestSQLGenerator):
            return '>= %s' % (g.generate_timestamp())
        def b(g: TSMATestSQLGenerator):
            return '> %s' % (g.generate_timestamp())
        return self.get_random_type([lambda: '', a, b])

    def _generate_scan_range_right(self, left: float) -> str:
        def a(g:TSMATestSQLGenerator):
            return '< %s' % (self.generate_timestamp(left))
        def b(g:TSMATestSQLGenerator):
            return '<= %s' % (self.generate_timestamp(left))
        return self._generate_scan_range([lambda: '', a, b])

    ## generate ts scan ranges
    def _generate_scan_range(self) -> str:
        empty = lambda: ''
        def a(g:TSMATestSQLGenerator):
            return g._generate_between()
        def b(g:TSMATestSQLGenerator):
            return g._generate_scan_range_operators()
        def ts_range(g:TSMATestSQLGenerator):
            return g.get_random_type([a,b])
        return self.get_random_type([empty, ts_range])

    def _generate_where_conditions(self) -> str:
        pass

    ## generate func in tsmas(select list)
    def _generate_agg_func_for_select(self) -> str:
        pass

    ## generate group by tbname, or exprs containing tbnames
    def _generate_tbname_for_group_partition_by(self) -> str:
        pass

    ## generate group by tags, or exprs containing tags
    def _generate_tag_for_group_partition_by(self) -> str:
        pass

    ## interval, sliding, offset
    def _generate_interval(self) -> str:
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
            tdLog.debug('waiting for tsma %s to be useful with sql %s' % (tsma_name, sql))
            ctx: TSMAQueryContext = self.tsma_tester.get_tsma_query_ctx(sql)
            if ctx.has_tsma():
                if ctx.used_tsmas[0].name == tsma_name + UsedTsma.TSMA_RES_STB_POSTFIX:
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

    def create_recursive_tsma(self, base_tsma_name: str, new_tsma_name: str, db: str, interval: str, tb_name: str):
        tdSql.execute('use %s' % db, queryTimes=1)
        sql = 'CREATE RECURSIVE TSMA %s ON %s.%s INTERVAL(%s)' % (new_tsma_name, db, base_tsma_name, interval)
        tdSql.execute(sql, queryTimes=1)
        self.wait_for_tsma_calculation(['avg(c1)'], db, tb_name, interval, new_tsma_name)

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
        #self.create_recursive_tsma('tsma1', 'tsma3', 'test', '20m', 'meters')
        #self.create_recursive_tsma('tsma2', 'tsma4', 'test', '1h', 'meters')
        self.create_tsma('tsma5', 'test', 'norm_tb', ['avg(c1)', 'avg(c2)'], '10m')
        ## why need 10s, filling history not finished yet
        #ctx = TSMAQCBuilder().with_sql('select avg(c1) from meters').should_query_with_table('meters', UsedTsma.TS_MIN, UsedTsma.TS_MAX).get_qc()
        #self.tsma_tester.check_sql(ctx.sql, ctx)
        #time.sleep(5)
        #time.sleep(9999999)
        self.test_query_with_tsma_interval()
        self.test_query_with_tsma_agg()
        ## self.test_query_with_drop_tsma()
        ## self.test_query_with_add_tag()
        ## self.test_union()

    def test_query_with_tsma_interval(self):
        self.check(self.test_query_with_tsma_interval_no_partition)
        self.check(self.test_query_with_tsma_interval_partition_by_col)
        self.check(self.test_query_with_tsma_interval_partition_by_tbname)
        self.check(self.test_query_with_tsma_interval_partition_by_tag)
        self.check(self.test_query_with_tsma_interval_partition_by_hybrid)

    def test_query_with_tsma_interval_no_partition(self) -> List[TSMAQueryContext]:
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
        return ctxs

    def test_query_with_tsma_interval_partition_by_tbname(self):
        return []

    def test_query_with_tsma_interval_partition_by_tag(self):
        return []

    def test_query_with_tsma_interval_partition_by_col(self):
        return []

    def test_query_with_tsma_interval_partition_by_hybrid(self):
        return []

    def test_query_with_tsma_agg(self):
        self.check(self.test_query_with_tsma_agg_no_group_by)
        self.check(self.test_query_with_tsma_agg_group_by_hybrid)
        self.check(self.test_query_with_tsma_agg_group_by_tbname)
        self.check(self.test_query_with_tsma_agg_group_by_tag)

    def test_query_with_tsma_agg_no_group_by(self):
        ctxs: List[TSMAQueryContext] = []
        sql = 'select avg(c1), avg(c2) from meters'
        ctxs.append(TSMAQCBuilder().with_sql(sql).should_query_with_tsma('tsma2', UsedTsma.TS_MIN,UsedTsma.TS_MAX).get_qc())

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
        
        sql = 'select avg(c1) + avg(c2) from meters where tbname like "%t1%"'
        ctxs.append(TSMAQCBuilder().with_sql(sql) \
                    .should_query_with_tsma('tsma2', UsedTsma.TS_MIN,UsedTsma.TS_MAX).get_qc())

        sql = 'select avg(c1), avg(c2) from meters where c1 is not NULL'
        ctxs.append(TSMAQCBuilder().with_sql(sql) \
                    .should_query_with_table('meters', UsedTsma.TS_MIN,UsedTsma.TS_MAX).get_qc())

        sql = 'select avg(c1), avg(c2), spread(c4) from meters'
        ctxs.append(TSMAQCBuilder().with_sql(sql) \
                    .should_query_with_table('meters', UsedTsma.TS_MIN,UsedTsma.TS_MAX).get_qc())
        
        return ctxs

    def test_query_with_tsma_agg_group_by_tbname(self):
        return []

    def test_query_with_tsma_agg_group_by_tag(self):
        return []

    def test_query_with_tsma_agg_group_by_hybrid(self):
        return []

    def run(self):
        self.init_data()
        #time.sleep(999999)
        self.test_create_tsma()
        #self.test_drop_tsma()
        self.test_tb_ddl_with_created_tsma()
        self.test_query_with_tsma()
        #time.sleep(999999)
    
    def test_create_tsma(self):
        self.test_create_tsma_on_stable()
        self.test_create_tsma_on_norm_table()
        self.test_create_tsma_on_child_table()
        self.test_create_recursive_tsma()
        ## self.test_drop_stable()
        ## self.test_drop_ctable()
        ## self.test_drop_db()

    def test_tb_ddl_with_created_tsma(self):
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

        ## test_drop stream

    def test_create_tsma_on_stable(self):
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

        tdSql.execute('drop tsma tsma1')

        tdSql.error('create tsma tsma1 on test.meters function(avg(c1), avg(c2)) interval(2h)', -2147471097)
        tdSql.execute('drop database nsdb')

    def test_create_tsma_on_norm_table(self):
        pass

    def test_create_tsma_on_child_table(self):
        tdSql.error('create tsma tsma1 on test.t1 function(avg(c1), avg(c2)) interval(1m)', -2147471098) ## Invalid table to create tsma, only stable or normal table allowed

    def test_create_recursive_tsma(self):
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
