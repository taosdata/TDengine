from os import name
from random import randrange
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

class TSMAQueryContextBuilder:
    def __init__(self) -> None:
        self.ctx: TSMAQueryContext = TSMAQueryContext()

    def get_ctx(self) -> TSMAQueryContext:
        return self.ctx

    def with_sql(self, sql: str):
        self.ctx.sql = sql
        return self
    
    def to_timestamp(self, ts: str) -> float:
        if ts == UsedTsma.TS_MAX or ts == UsedTsma.TS_MIN:
            return float(ts)
        tdSql.query("select to_timestamp('%s', 'yyyy-mm-dd hh24-mi-ss.ms')" % (ts))
        res = tdSql.queryResult[0][0]
        return res.timestamp() * 1000

    def should_query_with_table(self, tb_name: str, ts_begin: str, ts_end: str) -> 'TSMAQueryContextBuilder':
        used_tsma: UsedTsma = UsedTsma()
        used_tsma.name = tb_name
        used_tsma.time_range_start = self.to_timestamp(ts_begin)
        used_tsma.time_range_end = self.to_timestamp(ts_end)
        used_tsma.is_tsma_ = False
        self.ctx.used_tsmas.append(used_tsma)
        return self

    def should_query_with_tsma(self, tsma_name: str, ts_begin: str, ts_end: str) -> 'TSMAQueryContextBuilder':
        used_tsma: UsedTsma = UsedTsma()
        used_tsma.name = tsma_name + UsedTsma.TSMA_RES_STB_POSTFIX
        used_tsma.time_range_start = self.to_timestamp(ts_begin)
        used_tsma.time_range_end = self.to_timestamp(ts_end)
        used_tsma.is_tsma_ = True
        self.ctx.used_tsmas.append(used_tsma)
        return self

class TSMATestContext:
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
                tdLog.exit("comparing tsma res for: %s got different rows of result: with tsma: %s, with tsma: %s" % (sql, no_tsma_res, tsma_res))

        if len(no_tsma_res) != len(tsma_res):
            tdLog.exit("comparing tsma res for: %s got differnt rows of result: without tsma: %d, with tsma: %d" % (sql, len(no_tsma_res), len(tsma_res)))
        for row_no_tsma, row_tsma in zip(no_tsma_res, tsma_res):
            if row_no_tsma != row_tsma:
                tdLog.exit("comparing tsma res for: %s got different row data: no tsma row: %s, tsma row: %s" % (sql, str(row_no_tsma), str(row_tsma)))
        tdLog.info('result check succeed for sql: %s. \n   tsma-res: %s. \nno_tsma-res: %s' % (sql, str(tsma_res), str(no_tsma_res)))

    def check_sql(self, sql: str, expect: TSMAQueryContext):
        self.check_explain(sql, expect=expect)
        if expect.has_tsma():
            self.check_result(sql)

    def check_sqls(self, sqls: list[str], expects: list[TSMAQueryContext]):
        for sql, query_ctx in zip(sqls, expects):
            self.check_sql(sql, query_ctx)

class TDTestCase:
    def __init__(self):
        self.vgroups    = 4
        self.ctbNum     = 10
        self.rowsPerTbl = 10000
        self.duraion = '1h'

    def init(self, conn, logSql, replicaVar=1):
        self.replicaVar = int(replicaVar)
        tdLog.debug(f"start to excute {__file__}")
        tdSql.init(conn.cursor(), False)
        self.test_ctx: TSMATestContext = TSMATestContext(tdSql)

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
        return

    def create_tsma(self, tsma_name: str, db: str, tb: str, func_list: list, interval: str):
        tdSql.execute('use %s' % db)
        sql = "CREATE TSMA %s ON %s.%s FUNCTION(%s) INTERVAL(%s)" % (tsma_name, db, tb, ','.join(func_list), interval)
        tdSql.execute(sql, queryTimes=1)
    
    def create_recursive_tsma(self, base_tsma_name: str, new_tsma_name: str, db: str, interval: str):
        tdSql.execute('use %s' % db, queryTimes=1)
        sql = 'CREATE RECURSIVE TSMA %s ON %s.%s INTERVAL(%s)' % (new_tsma_name, db, base_tsma_name, interval)
        tdSql.execute(sql, queryTimes=1)

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
            self.test_ctx.check_sql(ctx.sql, ctx)

    def test_query_with_tsma(self):
        self.init_data()
        self.create_tsma('tsma1', 'test', 'meters', ['avg(c1)', 'avg(c2)'], '5m')
        self.create_tsma('tsma2', 'test', 'meters', ['avg(c1)', 'avg(c2)'], '30m')
        self.create_recursive_tsma('tsma1', 'tsma3', 'test', '20m')
        self.create_recursive_tsma('tsma2', 'tsma4', 'test', '1h')
        ## why need 5s, calculation not finished yet.
        time.sleep(5)
        #time.sleep(9999999)
        self.test_query_with_tsma_interval()
        self.test_query_with_tsma_agg()

    def test_query_with_tsma_interval(self):
        self.check(self.test_query_with_tsma_interval_no_partition)
        self.check(self.test_query_with_tsma_interval_partition_by_col)
        self.check(self.test_query_with_tsma_interval_partition_by_tbname)
        self.check(self.test_query_with_tsma_interval_partition_by_tag)
        self.check(self.test_query_with_tsma_interval_partition_by_hybrid)

    def test_query_with_tsma_interval_no_partition(self) -> List[TSMAQueryContext]:
        ctxs: List[TSMAQueryContext] = []
        sql = 'select avg(c1), avg(c2) from meters interval(5m)'
        ctxs.append(TSMAQueryContextBuilder().with_sql(sql) \
                    .should_query_with_tsma('tsma1', UsedTsma.TS_MIN,UsedTsma.TS_MAX).get_ctx())

        sql = 'select avg(c1), avg(c2) from meters interval(10m)'
        ctxs.append(TSMAQueryContextBuilder().with_sql(sql) \
                    .should_query_with_tsma('tsma1', UsedTsma.TS_MIN,UsedTsma.TS_MAX).get_ctx())
        sql = 'select avg(c1), avg(c2) from meters interval(30m)'
        ctxs.append(TSMAQueryContextBuilder().with_sql(sql) \
                    .should_query_with_tsma('tsma2', UsedTsma.TS_MIN,UsedTsma.TS_MAX).get_ctx())
        sql = 'select avg(c1), avg(c2) from meters interval(60m)'
        ctxs.append(TSMAQueryContextBuilder().with_sql(sql) \
                    .should_query_with_tsma('tsma2', UsedTsma.TS_MIN,UsedTsma.TS_MAX).get_ctx())
        
        sql = "select avg(c1), avg(c2) from meters where ts >= '2018-09-17 09:00:00.009' and ts < '2018-09-17 10:23:19.665' interval(30m)"
        ctxs.append(TSMAQueryContextBuilder().with_sql(sql) \
                    .should_query_with_table('meters', '2018-09-17 09:00:00.009','2018-09-17 09:29:59.999') \
                        .should_query_with_tsma('tsma2', '2018-09-17 09:30:00','2018-09-17 09:59:59.999') \
                            .should_query_with_table('meters', '2018-09-17 10:00:00.000','2018-09-17 10:23:19.664').get_ctx())
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
        ctxs.append(TSMAQueryContextBuilder().with_sql(sql).should_query_with_tsma('tsma2', UsedTsma.TS_MIN,UsedTsma.TS_MAX).get_ctx())

        sql = 'select avg(c1), avg(c2) from meters where ts between "2018-09-17 09:00:00.000" and "2018-09-17 10:00:00.000"'
        ctxs.append(TSMAQueryContextBuilder().with_sql(sql) \
                    .should_query_with_tsma('tsma2', '2018-09-17 09:00:00','2018-09-17 09:59:59:999') \
                        .should_query_with_table("meters", '2018-09-17 10:00:00','2018-09-17 10:00:00').get_ctx())

        sql = 'select avg(c1), avg(c2) from meters where ts between "2018-09-17 09:00:00.200" and "2018-09-17 10:23:19.800"'
        ctxs.append(TSMAQueryContextBuilder().with_sql(sql) \
                    .should_query_with_table('meters', '2018-09-17 09:00:00.200','2018-09-17 09:29:59:999') \
                        .should_query_with_tsma('tsma2', '2018-09-17 09:30:00','2018-09-17 09:59:59.999') \
                            .should_query_with_table('meters', '2018-09-17 10:00:00.000','2018-09-17 10:23:19.800').get_ctx())

        sql = 'select avg(c1) + avg(c2), avg(c2) from meters where ts between "2018-09-17 09:00:00.200" and "2018-09-17 10:23:19.800"'
        ctxs.append(TSMAQueryContextBuilder().with_sql(sql) \
                    .should_query_with_table('meters', '2018-09-17 09:00:00.200','2018-09-17 09:29:59:999') \
                        .should_query_with_tsma('tsma2', '2018-09-17 09:30:00','2018-09-17 09:59:59.999') \
                            .should_query_with_table('meters', '2018-09-17 10:00:00.000','2018-09-17 10:23:19.800').get_ctx())

        sql = 'select avg(c1) + avg(c2), avg(c2) + 1 from meters where ts between "2018-09-17 09:00:00.200" and "2018-09-17 10:23:19.800"'
        ctxs.append(TSMAQueryContextBuilder().with_sql(sql) \
                    .should_query_with_table('meters', '2018-09-17 09:00:00.200','2018-09-17 09:29:59:999') \
                        .should_query_with_tsma('tsma2', '2018-09-17 09:30:00','2018-09-17 09:59:59.999') \
                            .should_query_with_table('meters', '2018-09-17 10:00:00.000','2018-09-17 10:23:19.800').get_ctx())
        
        sql = 'select avg(c1) + avg(c2) from meters where tbname like "%t1%"'
        ctxs.append(TSMAQueryContextBuilder().with_sql(sql) \
                    .should_query_with_tsma('tsma2', UsedTsma.TS_MIN,UsedTsma.TS_MAX).get_ctx())

        sql = 'select avg(c1), avg(c2) from meters where c1 is not NULL'
        ctxs.append(TSMAQueryContextBuilder().with_sql(sql) \
                    .should_query_with_table('meters', UsedTsma.TS_MIN,UsedTsma.TS_MAX).get_ctx())

        sql = 'select avg(c1), avg(c2), spread(c4) from meters'
        ctxs.append(TSMAQueryContextBuilder().with_sql(sql) \
                    .should_query_with_table('meters', UsedTsma.TS_MIN,UsedTsma.TS_MAX).get_ctx())
        
        return ctxs

    def test_query_with_tsma_agg_group_by_tbname(self):
        return []

    def test_query_with_tsma_agg_group_by_tag(self):
        return []

    def test_query_with_tsma_agg_group_by_hybrid(self):
        return []

    def run(self):
        self.test_query_with_tsma()
        #time.sleep(999999)

    def stop(self):
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")

event = threading.Event()

tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
