import taos
import sys
import time
import socket
import os
import threading
import math
from datetime import datetime

from util.log import *
from util.sql import *
from util.cases import *
from util.dnodes import *
from util.common import *
# from tmqCommon import *

COMPARE_DATA = 0
COMPARE_LEN = 1

class TDTestCase:
    def __init__(self):
        self.vgroups    = 4
        self.ctbNum     = 10
        self.rowsPerTbl = 10000
        self.duraion = '1h'

    def init(self, conn, logSql, replicaVar=1):
        self.replicaVar = int(replicaVar)
        tdLog.debug(f"start to excute {__file__}")
        tdSql.init(conn.cursor(), True)

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
                    sql += "(%d, %d, %d, %d,%d,%d,%d,true,'binary%d', 'nchar%d') "%(startTs + j*tsStep, j%1000, j%500, j%1000, j%5000, j%5400, j%128, j%10000, j%1000)
                else:
                    sql += "(%d, %d, NULL, %d,NULL,%d,%d,true,'binary%d', 'nchar%d') "%(startTs + j*tsStep, j%1000, j%500, j%1000, j%128, j%10000, j%1000)
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

    def prepareTestEnv(self):
        tdLog.printNoPrefix("======== prepare test env include database, stable, ctables, and insert data: ")
        paraDict = {'dbName':     'test',
                    'dropFlag':   1,
                    'vgroups':    2,
                    'stbName':    'meters',
                    'colPrefix':  'c',
                    'tagPrefix':  't',
                    'colSchema':   [{'type': 'INT', 'count':1},
                                    {'type': 'BIGINT', 'count':1},
                                    {'type': 'FLOAT', 'count':1},
                                    {'type': 'DOUBLE', 'count':1},
                                    {'type': 'smallint', 'count':1},
                                    {'type': 'tinyint', 'count':1},
                                    {'type': 'bool', 'count':1},
                                    {'type': 'binary', 'len':10, 'count':1},
                                    {'type': 'nchar', 'len':10, 'count':1}],
                    'tagSchema':   [{'type': 'INT', 'count':1},{'type': 'nchar', 'len':20, 'count':1},{'type': 'binary', 'len':20, 'count':1},{'type': 'BIGINT', 'count':1},{'type': 'smallint', 'count':1},{'type': 'DOUBLE', 'count':1}],
                    'ctbPrefix':  't',
                    'ctbStartIdx': 0,
                    'ctbNum':     100,
                    'rowsPerTbl': 10000,
                    'batchNum':   3000,
                    'startTs':    1537146000000,
                    'tsStep':     600000}

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

    def check_explain_res_has_row(self, plan_str_expect: str, rows):
        plan_found = False
        for row in rows:
            if str(row).find(plan_str_expect) >= 0:
                tdLog.debug("plan: [%s] found in: [%s]" % (plan_str_expect, str(row)))
                plan_found = True
                break
        if not plan_found:
            tdLog.exit("plan: %s not found in res: [%s]" % (plan_str_expect, str(rows)))

    def check_explain_res_no_row(self, plan_str_not_expect: str, res):
        for row in res:
            if str(row).find(plan_str_not_expect) >= 0:
                tdLog.exit('plan: [%s] found in: [%s]' % (plan_str_not_expect, str(row)))

    def test_sort_for_partition_hint(self):
        pass

    def add_order_by(self, sql: str, order_by: str, select_list: str = "*") -> str:
        return "select %s from (%s)t order by %s" % (select_list, sql, order_by)

    def add_hint(self, sql: str) -> str:
        return "select /*+ sort_for_group() */ %s" % sql[6:]
    
    def add_remove_partition_hint(self, sql: str) -> str:
        return "select /*+ remove_partition() */ %s" % sql[6:]

    def query_with_time(self, sql):
        start = datetime.now()
        tdSql.query(sql, queryTimes=1)
        return (datetime.now().timestamp() - start.timestamp()) * 1000

    def explain_sql(self, sql: str):
        sql = "explain verbose true " + sql
        tdSql.query(sql, queryTimes=1)
        return tdSql.queryResult

    def query_and_compare_res(self, sql1, sql2, compare_what: int = 0):
        dur = self.query_with_time(sql1)
        tdLog.debug("sql1 query with time: [%f]" % dur)
        res1 = tdSql.queryResult
        dur = self.query_with_time(sql2)
        tdLog.debug("sql2 query with time: [%f]" % dur)
        res2 = tdSql.queryResult
        if res1 is None or res2 is None:
            tdLog.exit("res1 or res2 is None")
        if compare_what <= COMPARE_LEN:
            if len(res1) != len(res2):
                tdLog.exit("query and copare failed cause different rows, sql1: [%s], rows: [%d], sql2: [%s], rows: [%d]" % (sql1, len(res1), sql2, len(res2)))
        if compare_what == COMPARE_DATA:
            for i in range(0, len(res1)):
                if res1[i] != res2[i]:
                    tdLog.exit("compare failed for row: [%d], sqls: [%s] res1: [%s], sql2 : [%s] res2: [%s]" % (i, sql1, res1[i], sql2, res2[i]))
        tdLog.debug("sql: [%s] and sql: [%s] have same results, rows: [%d]" % (sql1, sql2, len(res1)))

    def query_and_compare_first_rows(self, sql1, sql2):
        dur = self.query_with_time(sql1)
        tdLog.debug("sql1 query with time: [%f]" % dur)
        res1 = tdSql.queryResult
        dur = self.query_with_time(sql2)
        tdLog.debug("sql2 query with time: [%f]" % dur)
        res2 = tdSql.queryResult
        if res1 is None or res2 is None:
            tdLog.exit("res1 or res2 is None")
        for i in range(0, min(len(res1), len(res2))):
            if res1[i] != res2[i]:
                tdLog.exit("compare failed for row: [%d], sqls: [%s] res1: [%s], sql2 : [%s] res2: [%s]" % (i, sql1, res1[i], sql2, res2[i]))
        tdLog.debug("sql: [%s] and sql: [%s] have same results, rows: [%d]" % (sql1, sql2, min(len(res1), len(res2))))

    def prepare_and_query_and_compare(self, sqls: [], order_by: str, select_list: str = "*", compare_what: int = 0):
        for sql in sqls:
            sql_hint = self.add_order_by(self.add_hint(sql), order_by, select_list)
            sql = self.add_order_by(sql, order_by, select_list)
            self.check_explain_res_has_row("Sort", self.explain_sql(sql_hint))
            self.check_explain_res_has_row("Partition", self.explain_sql(sql))
            self.query_and_compare_res(sql, sql_hint, compare_what=compare_what)

    def check_explain(self, sql):
        sql_hint = self.add_hint(sql)
        explain_res = self.explain_sql(sql)
        #self.check_explain_res_has_row('SortMerge', explain_res)
        #self.check_explain_res_has_row("blocking=0", explain_res)
        explain_res = self.explain_sql(sql_hint)
        self.check_explain_res_has_row('Merge', explain_res)
        self.check_explain_res_has_row('blocking=0', explain_res)

    def test_pipelined_agg_plan_with_slimit(self):
        sql = 'select count(*), %s from meters partition by %s slimit 1'
        self.check_explain(sql % ('c1','c1'))
        self.check_explain(sql % ('c1,c2', 'c1,c2'))

        # should fail
        # self.check_explain(sql % ('t1', 'c1,t1'))
        # self.check_explain(sql % ('t1', 'c1,tbname'))

    def test_pipelined_agg_data_with_slimit(self):
        sql_template = 'select %s from meters partition by %s %s'

        sql_elems = [
                ['count(*), min(c1), c2', 'c2', 'slimit 10', 'c2'],
                ['count(*), c1, c2', 'c1, c2', 'slimit 100', 'c1,c2'],
                ['count(*), c2, c1', 'c1, c2', 'slimit 1000', 'c1,c2'],
                ['count(*), c4,c3', 'c3, c4', 'slimit 2000', 'c3,c4'],
                ['count(*), c8,c6', 'c8, c6', 'slimit 3000', 'c8,c6'],
                ['count(*), c1 +1 as a,c6', 'c1, c6', 'slimit 3000', 'a,c6'],
                ['count(*), c1 +1 as a,c6', 'c1+1, c6', 'slimit 3000', 'a, c6'],
                ]

        for ele in sql_elems:
            sql = sql_template % (ele[0], ele[1], ele[2])
            sql_hint = self.add_hint(sql)
            sql = self.add_order_by(sql, ele[3])
            sql_no_slimit = sql_template % (ele[0], ele[1], '')

            sql_no_slimit = self.add_order_by(sql_no_slimit, ele[3])
            self.query_and_compare_first_rows(sql_hint, sql_no_slimit)

    def test_remove_partition(self):
        sql = 'select c1, count(*) from meters partition by c1 slimit 10'
        explain_res = self.explain_sql(sql)
        self.check_explain_res_no_row("Partition", explain_res)
        self.check_explain_res_has_row("blocking=1", explain_res)

        sql = 'select c1, count(*) from meters partition by c1,c2 slimit 10'
        explain_res = self.explain_sql(sql)
        self.check_explain_res_no_row("Partition", explain_res)
        self.check_explain_res_has_row("blocking=1", explain_res)

    def run(self):
        self.prepareTestEnv()
        #time.sleep(99999999)
        self.test_pipelined_agg_plan_with_slimit()
        self.test_pipelined_agg_data_with_slimit()
        self.test_remove_partition()

    def stop(self):
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")

event = threading.Event()

tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
