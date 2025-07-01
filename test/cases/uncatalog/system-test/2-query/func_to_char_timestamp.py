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
                if i % 3 == 0:
                    ts_format = 'NULL'
                else:
                    ts_format = "'yyyy-mm-dd hh24:mi:ss'"

                if (i < ctbNum/2):
                    sql += "(%d, %d, %d, %d,%d,%d,%d,true,'2023-11-01 10:10:%d', %s, 'nchar%d') "%(startTs + j*tsStep, j%10, j%10, j%10, j%10, j%10, j%10, j%10, ts_format, j%10)
                else:
                    sql += "(%d, %d, NULL, %d,NULL,%d,%d,true,NULL , %s, 'nchar%d') "%(startTs + j*tsStep, j%10, j%10, j%10, j%10, ts_format, j%10)
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
                    'colSchema':   [{'type': 'INT', 'count':1},{'type': 'BIGINT', 'count':1},{'type': 'FLOAT', 'count':1},{'type': 'DOUBLE', 'count':1},{'type': 'smallint', 'count':1},{'type': 'tinyint', 'count':1},{'type': 'bool', 'count':1},{'type': 'varchar', 'len':1024, 'count':2},{'type': 'nchar', 'len':10, 'count':1}],
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

    def convert_ts_and_check(self, ts_str: str, ts_format: str, expect_ts_char: str, expect_ts: str):
        tdSql.query("select to_timestamp('%s', '%s')" % (ts_str, ts_format), queryTimes=1)
        tdSql.checkData(0, 0, expect_ts)
        tdSql.query("select to_char(to_timestamp('%s', '%s'), '%s')" % (ts_str, ts_format, ts_format), queryTimes=1)
        tdSql.checkData(0, 0, expect_ts_char)

    def test_to_timestamp(self):
        self.convert_ts_and_check('2023-10-10 12:13:14.123', 'YYYY-MM-DD HH:MI:SS.MS', '2023-10-10 12:13:14.123', '2023-10-10 00:13:14.123000')
        self.convert_ts_and_check('2023-10-10 12:00:00.000AM', 'YYYY-DD-MM HH12:MI:SS.MSPM', '2023-10-10 12:00:00.000AM', '2023-10-10 00:00:00.000000')
        self.convert_ts_and_check('2023-01-01 12:10:10am', 'yyyy-mm-dd HH12:MI:SSAM', '2023-01-01 12:10:10AM', '2023-1-1 00:10:10.000000')
        self.convert_ts_and_check('23-1-01 9:10:10.123p.m.', 'yy-MM-dd HH12:MI:ss.msa.m.', '23-01-01 09:10:10.123p.m.', '2023-1-1 21:10:10.123000')
        self.convert_ts_and_check('23-1-01 9:10:10.123.000456.000000789p.m.', 'yy-MM-dd HH12:MI:ss.ms.us.nsa.m.', '23-01-01 09:10:10.123.123000.123000000p.m.', '2023-1-1 21:10:10.123000')
        self.convert_ts_and_check('  23   -1  - 01   \t 21:10:10 . 12 . \t 00045 . 00000078 \t', 'yy-MM-dd HH24:MI:SS.ms.us.ns', '23-01-01 21:10:10.120.120000.120000000', '2023-1-1 21:10:10.120000')
        self.convert_ts_and_check('  23  年 -1 月 - 01  日 \t 21:10:10 . 12 . \t 00045 . 00000078 \t+08', 'yy\"年\"-MM月-dd日 HH24:MI:SS.ms.us.ns TZH', '23年-01月-01日 21:10:10.120.120000.120000000 +08', '2023-1-1 21:10:10.120000')
        self.convert_ts_and_check('23-1-01 7:10:10.123p.m.6', 'yy-MM-dd HH:MI:ss.msa.m.TZH', '23-01-01 09:10:10.123p.m.+08', '2023-1-1 21:10:10.123000')

        self.convert_ts_and_check('2023-OCTober-19 10:10:10AM Thu', 'yyyy-month-dd hh24:mi:ssam dy', '2023-october  -19 10:10:10am thu', '2023-10-19 10:10:10')

        tdSql.error("select to_timestamp('210013/2', 'yyyyMM/dd')")
        tdSql.error("select to_timestamp('2100111111111/13/2', 'yyyyMM/dd')")

        tdSql.error("select to_timestamp('210a12/2', 'yyyyMM/dd')")

        tdSql.query("select to_timestamp(to_char(ts, 'yy-mon-dd hh24:mi:ss dy'), 'yy-mon-dd hh24:mi:ss dy') == ts from meters limit 10")
        tdSql.checkData(0, 0, 1)
        tdSql.checkData(1, 0, 1)
        tdSql.checkRows(10)

        tdSql.query("select to_char(ts, 'yy-mon-dd hh24:mi:ss.msa.m.TZH Day') from meters where to_timestamp(to_char(ts, 'yy-mon-dd hh24:mi:ss dy'), 'yy-mon-dd hh24:mi:ss dy') != ts")
        tdSql.checkRows(0)

        tdSql.query("select to_timestamp(c8, 'YYYY-MM-DD hh24:mi:ss') from meters")
        tdSql.query("select to_timestamp(c8, c9) from meters")

        format = "YYYY-MM-DD HH:MI:SS"
        for i in range(500):
            format = format + "1234567890"
        tdSql.query("select to_char(ts, '%s') from meters" % (format), queryTimes=1)
        time_str = '2023-11-11 10:10:10'
        for i in range(500):
            time_str  = time_str + "1234567890"
        tdSql.query("select to_timestamp('%s', '%s')" % (time_str, format))

    def run(self):
        self.prepareTestEnv()
        self.test_to_timestamp()
        self.test_ns_to_timestamp()

    def create_tables(self):
        tdSql.execute("create database if not exists test_us precision 'us'")
        tdSql.execute("create database if not exists test_ns precision 'ns'")
        tdSql.execute("use test_us")
        tdSql.execute(f"CREATE STABLE `meters_us` (`ts` TIMESTAMP, `ip_value` FLOAT, `ip_quality` INT, `ts2` timestamp) TAGS (`t1` INT)")
        tdSql.execute(f"CREATE TABLE `ctb1_us` USING `meters_us` (`t1`) TAGS (1)")
        tdSql.execute(f"CREATE TABLE `ctb2_us` USING `meters_us` (`t1`) TAGS (2)")
        tdSql.execute("use test_ns")
        tdSql.execute(f"CREATE STABLE `meters_ns` (`ts` TIMESTAMP, `ip_value` FLOAT, `ip_quality` INT, `ts2` timestamp) TAGS (`t1` INT)")
        tdSql.execute(f"CREATE TABLE `ctb1_ns` USING `meters_ns` (`t1`) TAGS (1)")
        tdSql.execute(f"CREATE TABLE `ctb2_ns` USING `meters_ns` (`t1`) TAGS (2)")

    def insert_ns_data(self):
        tdLog.debug("start to insert data ............")
        tdSql.execute(f"INSERT INTO `test_us`.`ctb1_us` VALUES ('2023-07-01 00:00:00.123456', 10.30000, 100, '2023-07-01 00:00:00.123456')")
        tdSql.execute(f"INSERT INTO `test_us`.`ctb2_us` VALUES ('2023-08-01 00:00:00.123456', 20.30000, 200, '2023-07-01 00:00:00.123456')")
        tdSql.execute(f"INSERT INTO `test_ns`.`ctb1_ns` VALUES ('2023-07-01 00:00:00.123456789', 10.30000, 100, '2023-07-01 00:00:00.123456000')")
        tdSql.execute(f"INSERT INTO `test_ns`.`ctb2_ns` VALUES ('2023-08-01 00:00:00.123456789', 20.30000, 200, '2023-08-01 00:00:00.123456789')")
        tdLog.debug("insert data ............ [OK]")

    def test_ns_to_timestamp(self):
        self.create_tables()
        self.insert_ns_data()
        tdSql.query("select to_timestamp('2023-08-1 10:10:10.123456789', 'yyyy-mm-dd hh:mi:ss.ns')", queryTimes=1)
        tdSql.checkData(0, 0, 1690855810123)
        tdSql.execute('use test_ns', queryTimes=1)
        tdSql.query("select to_timestamp('2023-08-1 10:10:10.123456789', 'yyyy-mm-dd hh:mi:ss.ns')", queryTimes=1)
        tdSql.checkData(0, 0, 1690855810123)
        tdSql.query("select to_char(ts2, 'yyyy-mm-dd hh:mi:ss.ns') from meters_ns", queryTimes=1)
        tdSql.checkData(0, 0, '2023-07-01 12:00:00.123456000')
        tdSql.checkData(1, 0, '2023-08-01 12:00:00.123456789')

        tdSql.query("select to_timestamp(to_char(ts2, 'yyyy-mm-dd hh:mi:ss.ns'), 'yyyy-mm-dd hh:mi:ss.ns') from meters_ns", queryTimes=1)
        tdSql.checkData(0, 0, 1688140800123456000)
        tdSql.checkData(1, 0, 1690819200123456789)


    def stop(self):
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")

event = threading.Event()

tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
