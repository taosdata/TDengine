from sqlite3 import ProgrammingError
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
        self.cachemodel = 'both'
        self.cacheEnable = True
        #self.cacheEnable = False
        if not self.cacheEnable:
            self.cachemodel = 'none'

    def init(self, conn, logSql, replicaVar=1):
        self.replicaVar = int(replicaVar)
        tdLog.debug(f"start to excute {__file__}")
        tdSql.init(conn.cursor(), True)

    def create_database(self,tsql, dbName,dropFlag=1,vgroups=2,replica=1, duration:str='1d'):
        if dropFlag == 1:
            tsql.execute("drop database if exists %s"%(dbName))

        tsql.execute("create database if not exists %s vgroups %d replica %d duration %s  CACHEMODEL '%s'"%(dbName, vgroups, replica, duration, self.cachemodel))
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
                    sql += "(%d, %d, %d, %d,%d,%d,%d,true,'binary%d', 'nchar%d', %d) "%(startTs + j*tsStep, j%1000, j%500, j%1000, j%5000, j%5400, j%128, j%10000, j%1000, startTs+j*tsStep+1000)
                else:
                    sql += "(%d, %d, NULL, %d,NULL,%d,%d,true,'binary%d', 'nchar%d', %d) "%(startTs + j*tsStep, j%1000, j%500, j%1000, j%128, j%10000, j%1000, startTs + j*tsStep + 1000)
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
                                    {'type': 'nchar', 'len':10, 'count':1},
                                    {'type': 'timestamp', 'count':1}],
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

    def check_explain_res_has_row(self, plan_str_expect: str, rows, sql):
        if not self.cacheEnable:
            return

        plan_found = False
        for row in rows:
            if str(row).find(plan_str_expect) >= 0:
                tdLog.debug("plan: [%s] found in: [%s]" % (plan_str_expect, str(row)))
                plan_found = True
                break
        if not plan_found:
            tdLog.exit("plan: %s not found in res: [%s] in sql: %s" % (plan_str_expect, str(rows), sql))

    def check_explain_res_no_row(self, plan_str_not_expect: str, res, sql):
        for row in res:
            if str(row).find(plan_str_not_expect) >= 0:
                tdLog.exit('plan: [%s] found in: [%s] for sql: %s' % (plan_str_not_expect, str(row), sql))

    def explain_sql(self, sql: str):
        sql = "explain verbose true " + sql
        tdSql.query(sql, queryTimes=1)
        return tdSql.queryResult

    def explain_and_check_res(self, sqls, hasLastRowScanRes):
        for sql, has_last in zip(sqls, hasLastRowScanRes):
            res = self.explain_sql(sql)
            if has_last == 1:
                self.check_explain_res_has_row("Last Row Scan", res, sql)
            else:
                self.check_explain_res_no_row("Last Row Scan", res, sql)

    def format_sqls(self, sql_template, select_items):
        sqls = []
        for item in select_items:
            sqls.append(sql_template % item)
        return sqls

    def query_check_one(self, sql, res_expect):
        if res_expect is not None:
            tdSql.query(sql, queryTimes=1)
            tdSql.checkRows(1)
            for i in range(0, tdSql.queryCols):
                tdSql.checkData(0, i, res_expect[i])
                tdLog.info('%s check res col: %d succeed value: %s' % (sql, i, str(res_expect[i])))

    def query_check_sqls(self, sqls, has_last_row_scan_res, res_expect):
        for sql, has_last, res in zip(sqls, has_last_row_scan_res, res_expect):
            if has_last == 1:
                self.query_check_one(sql, res)

    def test_last_cache_scan(self):
        sql_template = 'select %s from meters'
        select_items = [
                "last(ts), ts", "last(ts), c1", "last(ts), c2", "last(ts), c3",\
                "last(ts), c4", "last(ts), tbname", "last(ts), t1", "last(ts), ts, ts"]
        has_last_row_scan_res = [1, 0, 0, 0, 0, 1, 1, 1]
        res_expect = [
                ["2018-11-25 19:30:00.000", "2018-11-25 19:30:00.000"],
                None, None, None, None, None, None,
                ["2018-11-25 19:30:00.000", "2018-11-25 19:30:00.000", "2018-11-25 19:30:00.000"]
                ]
        sqls = self.format_sqls(sql_template, select_items)
        self.explain_and_check_res(sqls, has_last_row_scan_res)
        self.query_check_sqls(sqls, has_last_row_scan_res, res_expect)

        select_items = ["last(c1),ts", "last(c1), c1", "last(c1), c2", "last(c1), c3",\
                "last(c1), c4", "last(c1), tbname", "last(c1), t1", "last(c1), ts, ts", "last(c1), c1, c1"]
        has_last_row_scan_res = [1, 1, 0, 0, 0, 1, 1, 1, 1]
        res_expect = [
                [999, "2018-11-25 19:30:00.000"],
                [999, 999], None, None, None, None, None,
                [999, "2018-11-25 19:30:00.000", "2018-11-25 19:30:00.000"],
                [999,999,999]
                ]
        sqls = self.format_sqls(sql_template, select_items)
        self.explain_and_check_res(sqls, has_last_row_scan_res)
        self.query_check_sqls(sqls, has_last_row_scan_res, res_expect)

        sql_template = 'select %s from t1'
        select_items = ["last(c4),ts", "last(c4), c1", "last(c4), c2", "last(c4), c3",\
                "last(c4), c4", "last(c4), tbname", "last(c4), t1"]
        has_last_row_scan_res = [1, 0, 0, 0, 1, 1, 1]
        res_expect = [
                [4999.000000000000000, "2018-11-25 19:30:00.000"],
                None,None,None,
                [4999.000000000000000, 4999.000000000000000]
                ]
        sqls = self.format_sqls(sql_template, select_items)
        self.explain_and_check_res(sqls, has_last_row_scan_res)
        self.query_check_sqls(sqls, has_last_row_scan_res, res_expect)

        sql_template = 'select %s from meters'
        select_items = ["last(c8), ts", "last(c8), c1", "last(c8), c8", "last(c8), tbname", \
                "last(c8), t1", "last(c8), c8, c8", "last(c8), ts, ts"]
        has_last_row_scan_res = [1, 0, 1, 1, 1, 1, 1]
        res_expect = [
                ["binary9999", "2018-11-25 19:30:00.000"],
                None,
                ["binary9999", "binary9999"],
                None, None,
                ["binary9999", "binary9999", "binary9999"],
                ["binary9999", "2018-11-25 19:30:00.000", "2018-11-25 19:30:00.000"]
                ]
        sqls = self.format_sqls(sql_template, select_items)
        self.explain_and_check_res(sqls, has_last_row_scan_res)
        self.query_check_sqls(sqls, has_last_row_scan_res, res_expect)

        # c2, c4 in last row of t5,t6,t7,t8,t9 will always be NULL
        sql_template = 'select %s from t5'
        select_items = ["last(c4), ts", "last(c4), c4", "last(c4), c4, c4", "last(c4), ts, ts"]
        has_last_row_scan_res = [1,1,1,1]

        sqls = self.format_sqls(sql_template, select_items)
        self.explain_and_check_res(sqls, has_last_row_scan_res)
        #for sql in sqls:
            #tdSql.query(sql, queryTimes=1)
            #tdSql.checkRows(0)

        sql_template = 'select %s from meters'
        select_items = [
                "last_row(ts), last(ts)",
                "last_row(c1), last(c1)",
                "last_row(c1), c1,c3, ts"
                ]
        has_last_row_scan_res = [1,1,1]
        sqls = self.format_sqls(sql_template, select_items)
        self.explain_and_check_res(sqls, has_last_row_scan_res)
        #res_expect = [None, None, [999, 999, 499, "2018-11-25 19:30:00.000"]]
        #self.query_check_sqls(sqls, has_last_row_scan_res, res_expect)

        select_items = ["last(c10), c10",
                        "last(c10), ts",
                        "last(c10), c10, ts",
                        "last(c10), c10, ts, c10,ts",
                        "last(c10), ts, c1"]
        has_last_row_scan_res = [1,1,1,1,0]
        sqls = self.format_sqls(sql_template, select_items)
        self.explain_and_check_res(sqls, has_last_row_scan_res)
        res_expect = [
                ["2018-11-25 19:30:01.000", "2018-11-25 19:30:01.000"],
                ["2018-11-25 19:30:01.000", "2018-11-25 19:30:00.000"],
                ["2018-11-25 19:30:01.000", "2018-11-25 19:30:01.000", "2018-11-25 19:30:00.000"],
                ["2018-11-25 19:30:01.000", "2018-11-25 19:30:01.000", "2018-11-25 19:30:00.000", "2018-11-25 19:30:01.000", "2018-11-25 19:30:00.000"]
                ]
        self.query_check_sqls(sqls, has_last_row_scan_res, res_expect)

        sql = "select last(c1), c1, c1+1, c1+2, ts from meters"
        res = self.explain_sql(sql)
        self.check_explain_res_has_row("Last Row Scan", res, sql)

        tdSql.query(sql)
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 999)
        tdSql.checkData(0, 1, 999)
        tdSql.checkData(0, 2, 1000)
        tdSql.checkData(0, 3, 1001)
        tdSql.checkData(0, 4, "2018-11-25 19:30:00.000")

        tdSql.query("select last(ts) from meters partition by tbname")
        tdSql.query("select last(ts) from meters partition by t1")
        sql_template = 'select %s from meters partition by tbname'
        select_items = ["ts, last(c10), c10, ts", "ts, ts, last(c10), c10, tbname", "last(c10), c10, ts"]
        has_last_row_scan_res = [1,1,1]
        sqls = self.format_sqls(sql_template, select_items)
        self.explain_and_check_res(sqls, has_last_row_scan_res)
        tdSql.query(sqls[0], queryTimes=1)
        tdSql.checkRows(10)
        tdSql.checkData(0,0, '2018-11-25 19:30:00.000')
        tdSql.checkData(0,1, '2018-11-25 19:30:01.000')
        tdSql.checkData(0,2, '2018-11-25 19:30:01.000')
        tdSql.checkData(0,3, '2018-11-25 19:30:00.000')

        tdSql.query(sqls[1], queryTimes=1)
        tdSql.checkRows(10)
        tdSql.checkData(0,0, '2018-11-25 19:30:00.000')
        tdSql.checkData(0,1, '2018-11-25 19:30:00.000')
        tdSql.checkData(0,2, '2018-11-25 19:30:01.000')
        tdSql.checkData(0,3, '2018-11-25 19:30:01.000')

        sql_template = 'select %s from meters partition by t1'
        select_items = ["ts, last(c10), c10, ts", "ts, ts, last(c10), c10, t1", "last(c10), c10, ts"]
        has_last_row_scan_res = [1,1,1]
        sqls = self.format_sqls(sql_template, select_items)
        self.explain_and_check_res(sqls, has_last_row_scan_res)
        tdSql.query(sqls[0], queryTimes=1)
        tdSql.checkRows(5)
        tdSql.checkData(0,0, '2018-11-25 19:30:00.000')
        tdSql.checkData(0,1, '2018-11-25 19:30:01.000')
        tdSql.checkData(0,2, '2018-11-25 19:30:01.000')
        tdSql.checkData(0,3, '2018-11-25 19:30:00.000')

        tdSql.query("select ts, last(c10), t1, t2 from meters partition by t1, t2")
        tdSql.checkRows(10)
        tdSql.checkData(0, 0, '2018-11-25 19:30:00.000')
        tdSql.checkData(0, 1, '2018-11-25 19:30:01.000')

    def test_cache_scan_with_drop_and_add_column(self):
        tdSql.query("select last(c10) from meters")
        tdSql.checkData(0, 0, '2018-11-25 19:30:01')
        p = subprocess.run(["taos", '-s', "alter table test.meters drop column c10; alter table test.meters add column c11 int"])
        p.check_returncode()
        tdSql.query("select last(c10) from meters", queryTimes=1)
        tdSql.checkData(0, 0, None)
        tdSql.query('select last(*) from meters', queryTimes=1)
        tdSql.checkData(0, 10, None)
        tdSql.query('select last(c10), c10, ts from meters', queryTimes=1)
        tdSql.checkData(0, 0, None)
        tdSql.checkData(0, 1, None)
        tdSql.checkData(0, 2, None)

    def test_cache_scan_with_drop_and_add_column2(self):
        tdSql.query("select last(c1) from meters")
        tdSql.checkData(0, 0, '999')
        p = subprocess.run(["taos", '-s', "alter table test.meters drop column c1; alter table test.meters add column c12 int"])
        p.check_returncode()
        tdSql.query_success_failed("select ts, last(c1), c1, ts, c1 from meters", queryTimes=10, expectErrInfo="Invalid column name: c1")
        tdSql.query('select last(c12), c12, ts from meters', queryTimes=1)
        tdSql.checkRows(0)
        #tdSql.checkCols(3)
        #tdSql.checkData(0, 0, None)
        #tdSql.checkData(0, 1, None)

    def test_cache_scan_with_drop_column(self):
        tdSql.query('select last(*) from meters')
        print(str(tdSql.queryResult))
        tdSql.checkCols(11)
        p = subprocess.run(["taos", '-s', "alter table test.meters drop column c9"])
        p.check_returncode()
        tdSql.query('select last(*) from meters')
        print(str(tdSql.queryResult))
        tdSql.checkCols(11)
        tdSql.checkData(0, 9, None)
    
    def test_cache_scan_last_row_with_drop_column(self):
        tdSql.query('select last_row(*) from meters')
        print(str(tdSql.queryResult))
        tdSql.checkCols(11)
        p = subprocess.run(["taos", '-s', "alter table test.meters drop column c10; alter table test.meters add column c11 int"])
        p.check_returncode()
        tdSql.query('select last_row(*) from meters')
        print(str(tdSql.queryResult))
        tdSql.checkCols(11)
        tdSql.checkData(0, 10, None)
        
    def test_cache_scan_last_row_with_drop_column2(self):
        tdSql.query('select last_row(c2) from meters')
        print(str(tdSql.queryResult))
        tdSql.checkCols(1)
        p = subprocess.run(["taos", '-s', "alter table test.meters drop column c2; alter table test.meters add column c1 int"])
        p.check_returncode()
        tdSql.query_success_failed("select ts, last(c2), c12, ts, c12 from meters", queryTimes=10, expectErrInfo="Invalid column name: c2")
        tdSql.query('select last(c1), c1, ts from meters', queryTimes=1)
        tdSql.checkRows(0)
        #tdSql.checkCols(3)
        #tdSql.checkData(0, 0, None)
        #tdSql.checkData(0, 1, None)

    def test_cache_scan_last_row_with_partition_by(self):
        tdSql.query('select last(c1) from meters partition by t1')
        print(str(tdSql.queryResult))
        #tdSql.checkCols(1)
        tdSql.checkRows(0)
        p = subprocess.run(["taos", '-s', "alter table test.meters drop column c1; alter table test.meters add column c2 int"])
        p.check_returncode()
        tdSql.query_success_failed('select last(c1) from meters partition by t1',  queryTimes=10, expectErrInfo="Invalid column name: c1")
        tdSql.query('select last(c2), c2, ts from meters', queryTimes=1)
        print(str(tdSql.queryResult))
        tdSql.checkRows(0)
        #tdSql.checkCols(3)
        #tdSql.checkData(0, 0, None)
        #tdSql.checkData(0, 1, None)


    def test_cache_scan_last_row_with_partition_by_tbname(self):
        tdSql.query('select last(c2) from meters partition by tbname')
        print(str(tdSql.queryResult))
        #tdSql.checkCols(1)
        tdSql.checkRows(0)
        p = subprocess.run(["taos", '-s', "alter table test.meters drop column c2; alter table test.meters add column c1 int"])
        p.check_returncode()
        tdSql.query_success_failed('select last_row(c2) from meters partition by tbname', queryTimes=10, expectErrInfo="Invalid column name: c2")
        tdSql.query('select last(c1), c1, ts from meters', queryTimes=1)
        print(str(tdSql.queryResult))
        tdSql.checkRows(0)
        #tdSql.checkCols(3)
        #tdSql.checkData(0, 0, None)
        #tdSql.checkData(0, 1, None)



    def run(self):
        self.prepareTestEnv()
        #time.sleep(99999999)
        self.test_last_cache_scan()
        #self.test_cache_scan_with_drop_and_add_column()
        self.test_cache_scan_with_drop_and_add_column2()
        #self.test_cache_scan_with_drop_column()
        #self.test_cache_scan_last_row_with_drop_column()
        self.test_cache_scan_last_row_with_drop_column2()
        self.test_cache_scan_last_row_with_partition_by()
        self.test_cache_scan_last_row_with_partition_by_tbname()

    def stop(self):
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")

event = threading.Event()

tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
