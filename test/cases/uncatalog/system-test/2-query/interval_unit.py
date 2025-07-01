import taos
import sys
import time
import socket
import os
import threading
import math

from util.log import *
from util.sql import *
from util.cases import *
from util.dnodes import *
from util.common import *
# from tmqCommon import *

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

    def create_database(self,tsql, dbName,dropFlag=1,vgroups=2,replica=1, duration:str='1d', precision: str='ms'):
        if dropFlag == 1:
            tsql.execute("drop database if exists %s"%(dbName), 1)

        tsql.execute("create database if not exists %s vgroups %d replica %d duration %s precision '%s'"%(dbName, vgroups, replica, duration, precision), 1)
        tdLog.debug("complete to create database %s"%(dbName))
        return

    def create_stable(self,tsql, paraDict):
        colString = tdCom.gen_column_type_str(colname_prefix=paraDict["colPrefix"], column_elm_list=paraDict["colSchema"])
        tagString = tdCom.gen_tag_type_str(tagname_prefix=paraDict["tagPrefix"], tag_elm_list=paraDict["tagSchema"])
        sqlString = f"create table if not exists %s.%s (%s) tags (%s)"%(paraDict["dbName"], paraDict["stbName"], colString, tagString)
        tdLog.debug("%s"%(sqlString))
        tsql.execute(sqlString, 1)
        return

    def create_ctable(self,tsql=None, dbName='dbx',stbName='stb',ctbPrefix='ctb',ctbNum=1,ctbStartIdx=0):
        for i in range(ctbNum):
            sqlString = "create table %s.%s%d using %s.%s tags(%d, 'tb%d', 'tb%d', %d, %d, %d)" % \
                    (dbName,ctbPrefix,i+ctbStartIdx,dbName,stbName,(i+ctbStartIdx) % 5,i+ctbStartIdx,i+ctbStartIdx,i+ctbStartIdx,i+ctbStartIdx,i+ctbStartIdx)
            tsql.execute(sqlString, 1)

        tdLog.debug("complete to create %d child tables by %s.%s" %(ctbNum, dbName, stbName))
        return

    def insert_data(self,tsql,dbName,ctbPrefix,ctbNum,rowsPerTbl,batchNum,startTs,tsStep):
        tdLog.debug("start to insert data ............")
        tsql.execute("use %s" %dbName, 1)
        pre_insert = "insert into "
        sql = pre_insert

        for i in range(ctbNum):
            rowsBatched = 0
            sql += " %s%d values "%(ctbPrefix,i)
            for j in range(rowsPerTbl):
                if (i < ctbNum/2):
                    sql += "(%d, %d, %d, %d,%d,%d,%d,true,'binary%d', 'nchar%d') "%(startTs + j*tsStep, j%10, j%10, j%10, j%10, j%10, j%10, j%10, j%10)
                else:
                    sql += "(%d, %d, NULL, %d,NULL,%d,%d,true,'binary%d', 'nchar%d') "%(startTs + j*tsStep, j%10, j%10, j%10, j%10, j%10, j%10)
                rowsBatched += 1
                if ((rowsBatched == batchNum) or (j == rowsPerTbl - 1)):
                    tsql.execute(sql, 1)
                    rowsBatched = 0
                    if j < rowsPerTbl - 1:
                        sql = "insert into %s%d values " %(ctbPrefix,i)
                    else:
                        sql = "insert into "
        if sql != pre_insert:
            tsql.execute(sql, 1)
        tdLog.debug("insert data ............ [OK]")
        return

    def prepare_db_for_interval_unit_test(self, dbname: str, precision: str = 'ms', startTs = 1537146000000):
        self.create_database(tdSql, dbname, precision=precision)
        paraDict = {'dbName':     dbname,
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
                                    {'type': 'nchar', 'len':10, 'count':1} ],
                    'tagSchema':   [{'type': 'INT', 'count':1},
                                    {'type': 'nchar', 'len':20, 'count':1},
                                    {'type': 'binary', 'len':20, 'count':1},
                                    {'type': 'BIGINT', 'count':1},
                                    {'type': 'smallint', 'count':1},
                                    {'type': 'DOUBLE', 'count':1}],
                    'ctbPrefix':  't',
                    'ctbStartIdx': 0,
                    'ctbNum':     100,
                    'rowsPerTbl': 1000,
                    'batchNum':   3000,
                    'startTs':    startTs,
                    'tsStep':     100}

        self.create_stable(tdSql, paraDict)

        self.create_ctable(tsql=tdSql, dbName=paraDict["dbName"], \
                stbName=paraDict["stbName"],ctbPrefix=paraDict["ctbPrefix"],\
                ctbNum=paraDict["ctbNum"],ctbStartIdx=paraDict["ctbStartIdx"])
        self.insert_data(tsql=tdSql, dbName=paraDict["dbName"],\
                ctbPrefix=paraDict["ctbPrefix"],ctbNum=paraDict["ctbNum"],\
                rowsPerTbl=paraDict["rowsPerTbl"],batchNum=paraDict["batchNum"],\
                startTs=paraDict["startTs"],tsStep=paraDict["tsStep"])

    def prepare_for_interval_unit_test(self):
        self.prepare_db_for_interval_unit_test('msdb', 'ms', startTs=1600000000000)
        self.prepare_db_for_interval_unit_test('usdb', 'us', startTs=1600000000000000)
        self.prepare_db_for_interval_unit_test('nsdb', 'ns', startTs=1600000000000000000)

    def check_explain_res_has_row(self, plan_str_expect: str, rows):
        plan_found = False
        for row in rows:
            if str(row).find(plan_str_expect) >= 0:
                tdLog.debug("plan: [%s] found in: [%s]" % (plan_str_expect, str(row)))
                plan_found = True
                break
        if not plan_found:
            tdLog.exit("plan: %s not found in res: [%s]" % (plan_str_expect, str(rows)))

    def explain_and_assert_res(self, sql, expect):
        tdSql.query(sql)
        res = tdSql.queryResult
        self.check_explain_res_has_row(expect, res)

    def test_interval_normal_cases(self):
        tdSql.execute('use msdb')
        sql_template = 'explain verbose true select count(*), min(c1) from meters interval(%s) sliding(%s)'

        fail_durations = ["'1 s'", "'11ns'", "'11ms'", "'11+2s'",
        "' 112s '", '"100s "', "' 112 s '", "'112a20s'", "'s'",
        '"1 s"', '"1"', '"100"', '"1y"', '1y', '"1s""', "'12s'12s'", "'12s\""]

        for dura in fail_durations:
            tdSql.error(sql_template % (dura, dura))

        msdb_success_durations = ['1s', '"1s"', '"1000000a"', "'100d'",
                             "'10m'", '"10h"', '1000']

        for dura in msdb_success_durations:
            tdSql.query(sql_template % (dura, dura), queryTimes=1)
            unit = ''
            if dura.isdigit():
                unit = 'a'
            self.check_explain_res_has_row('interval=' + dura.lstrip('\'"').rstrip('\'"') + unit, tdSql.queryResult)
            self.check_explain_res_has_row('sliding=' + dura.lstrip('\'"').rstrip('\'"') + unit, tdSql.queryResult)

        usdb_success_durations = ['"1s"', "'10s'", '"10d"', '"1000000u"', '1000000']
        for dura in usdb_success_durations:
            tdSql.execute("use usdb")
            tdSql.query(sql_template % (dura, dura), queryTimes=1)
            unit = ''
            if dura.isdigit():
                unit = 'u'
            self.check_explain_res_has_row('interval=' + dura.lstrip('\'"').rstrip('\'"') + unit, tdSql.queryResult)
            self.check_explain_res_has_row('sliding=' + dura.lstrip('\'"').rstrip('\'"') + unit, tdSql.queryResult)

        nsdb_success_durations = ['"1s"', "'10s'", '"10d"', '"1000000u"', '"1000000000b"', '1000000000']
        for dura in usdb_success_durations:
            tdSql.execute("use nsdb")
            tdSql.query(sql_template % (dura, dura), queryTimes=1)
            unit = ''
            if dura.isdigit():
                unit = 'b'
            self.check_explain_res_has_row('interval=' + dura.lstrip('\'"').rstrip('\'"') + unit, tdSql.queryResult)
            self.check_explain_res_has_row('sliding=' + dura.lstrip('\'"').rstrip('\'"') + unit, tdSql.queryResult)

    def test_interval_unit_feature(self):
        self.prepare_for_interval_unit_test()
        self.test_interval_normal_cases()

    def run(self):
        self.test_interval_unit_feature()

    def stop(self):
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")

event = threading.Event()

tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
