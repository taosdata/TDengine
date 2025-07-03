from new_test_framework.utils import tdLog, tdSql, tdCom

import time
import socket
import os
import threading
# from tmqCommon import *

class TestStateWindow:

    def setup_class(cls):
        cls.replicaVar = 1  # 设置默认副本数
        tdLog.debug(f"start to excute {__file__}")
        #tdSql.init(conn.cursor(), logSql)
        
        cls.vgroups    = 4
        cls.ctbNum     = 1
        cls.rowsPerTbl = 10
        cls.duraion = '1h'

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
                    sql += "(%d, %d, %d, %d,%d,%d,%d,true,'binary%d', 'nchar%d') "%(startTs + j*tsStep, j%10, 1, j%10, j%10, j%10, j%10, j%10, j%10)
                else:
                    sql += "(%d, %d, NULL, %d,NULL,%d,%d,true,'binary%d', 'nchar%d') "%(startTs + j*tsStep, j%10, j%10, j%10, j%10, j%10, j%10)
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
                    'colSchema':   [{'type': 'INT', 'count':1},{'type': 'BIGINT', 'count':1},{'type': 'FLOAT', 'count':1},{'type': 'DOUBLE', 'count':1},{'type': 'smallint', 'count':1},{'type': 'tinyint', 'count':1},{'type': 'bool', 'count':1},{'type': 'binary', 'len':10, 'count':1},{'type': 'nchar', 'len':10, 'count':1}],
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

    def prepare_original_data(self):
        tdSql.execute("insert into t0 values(now, 2,2,2,2,2,2,2,2,2)", queryTimes=1)
        tdSql.execute("insert into t0 values(now, 2,2,2,2,2,2,2,2,2)", queryTimes=1)
        tdSql.execute("insert into t0 values(now, 2,2,2,2,2,2,2,2,2)", queryTimes=1)
        tdSql.execute("insert into t0 values(now, 2,2,2,2,2,2,2,2,2)", queryTimes=1)
        tdSql.execute("insert into t0 values(now, 2,2,2,2,2,2,2,2,2)", queryTimes=1)
        tdSql.execute("insert into t0 values(now, 3,3,3,3,3,3,3,3,3)", queryTimes=1)
        tdSql.execute("flush database test", queryTimes=1)
        time.sleep(2)

    def check_crash_for_state_window1(self):
        tdSql.execute("drop database if exists test")
        self.prepareTestEnv()
        tdSql.execute("alter local 'queryPolicy' '3'")
        self.prepare_original_data()
        tdSql.execute("insert into t0 values(now, 4,4,4,4,4,4,4,4,4)", queryTimes=1)
        tdSql.execute("select bottom(c1, 1), c2 from t0 state_window(c2) order by ts", queryTimes=1)

    def check_crash_for_state_window2(self):
        tdSql.execute("drop database if exists test")
        self.prepareTestEnv()
        tdSql.execute("alter local 'queryPolicy' '3'")
        self.prepare_original_data()
        tdSql.execute("insert into t0 values(now, 4,NULL,4,4,4,4,4,4,4)", queryTimes=1)
        tdSql.execute("insert into t0 values(now, 4,4,4,4,4,4,4,4,4)", queryTimes=1)
        tdSql.execute("select bottom(c1, 1), c2 from t0 state_window(c2) order by ts", queryTimes=1)

    def check_crash_for_state_window3(self):
        tdSql.execute("drop database if exists test")
        self.prepareTestEnv()
        tdSql.execute("alter local 'queryPolicy' '3'")
        self.prepare_original_data()
        tdSql.execute("insert into t0 values(now, 4,NULL,4,4,4,4,4,4,4)", queryTimes=1)
        tdSql.execute("insert into t0 values(now, 4,5,4,4,4,4,4,4,4)", queryTimes=1)
        tdSql.execute("select bottom(c1, 1), c2 from t0 state_window(c2) order by ts", queryTimes=1)

    def check_crash_for_state_window4(self):
        tdSql.execute("drop database if exists test")
        self.prepareTestEnv()
        tdSql.execute("alter local 'queryPolicy' '3'")
        tdSql.execute("insert into t0 values(now, 2,2,2,2,2,2,2,2,2)", queryTimes=1)
        tdSql.execute("insert into t0 values(now, 2,2,2,2,2,2,2,2,2)", queryTimes=1)
        tdSql.execute("insert into t0 values(now, 2,2,2,2,2,2,2,2,2)", queryTimes=1)
        tdSql.execute("insert into t0 values(now, 2,2,2,2,2,2,2,2,2)", queryTimes=1)
        tdSql.execute("insert into t0 values(now, 2,2,2,2,2,2,2,2,2)", queryTimes=1)
        tdSql.execute("insert into t0 values(now, 3,3,3,3,3,3,3,3,3)", queryTimes=1)
        tdSql.execute("insert into t0 values(now, 3,NULL,3,3,3,3,3,3,3)", queryTimes=1)
        tdSql.execute("insert into t0 values(now, 3,NULL,3,3,3,3,3,3,3)", queryTimes=1)
        tdSql.execute("flush database test", queryTimes=1)
        time.sleep(2)
        tdSql.execute("insert into t0 values(now, 3,NULL,3,3,3,3,3,3,3)", queryTimes=1)
        tdSql.execute("select bottom(c1, 1), c2 from t0 state_window(c2) order by ts", queryTimes=1)

    def check_crash_for_state_window5(self):
        tdSql.execute("drop database if exists test")
        self.prepareTestEnv()
        tdSql.execute("alter local 'queryPolicy' '3'")
        tdSql.execute("insert into t0 values(now, 2,2,2,2,2,2,2,2,2)", queryTimes=1)
        tdSql.execute("insert into t0 values(now, 2,2,2,2,2,2,2,2,2)", queryTimes=1)
        tdSql.execute("insert into t0 values(now, 2,2,2,2,2,2,2,2,2)", queryTimes=1)
        tdSql.execute("insert into t0 values(now, 2,2,2,2,2,2,2,2,2)", queryTimes=1)
        tdSql.execute("insert into t0 values(now, 2,2,2,2,2,2,2,2,2)", queryTimes=1)
        tdSql.execute("insert into t0 values(now, 3,3,3,3,3,3,3,3,3)", queryTimes=1)
        tdSql.execute("insert into t0 values(now, 3,NULL,3,3,3,3,3,3,3)", queryTimes=1)
        tdSql.execute("insert into t0 values(now, 3,NULL,3,3,3,3,3,3,3)", queryTimes=1)
        tdSql.execute("flush database test", queryTimes=1)
        time.sleep(2)
        tdSql.execute("insert into t0 values(now, 3,NULL,3,3,3,3,3,3,3)", queryTimes=1)
        tdSql.execute("insert into t0 values(now, 3,3,3,3,3,3,3,3,3)", queryTimes=1)
        tdSql.execute("select bottom(c1, 1), c2 from t0 state_window(c2) order by ts", queryTimes=1)

    def check_crash_for_session_window(self):
        tdSql.execute("drop database if exists test")
        self.prepareTestEnv()
        tdSql.execute("alter local 'queryPolicy' '3'")
        tdSql.execute("insert into t0 values(now, 2,2,2,2,2,2,2,2,2)", queryTimes=1)
        tdSql.execute("insert into t0 values(now, 2,2,2,2,2,2,2,2,2)", queryTimes=1)
        tdSql.execute("insert into t0 values(now, 2,2,2,2,2,2,2,2,2)", queryTimes=1)
        tdSql.execute("insert into t0 values(now, 2,2,2,2,2,2,2,2,2)", queryTimes=1)
        tdSql.execute("insert into t0 values(now, 2,2,2,2,2,2,2,2,2)", queryTimes=1)
        tdSql.execute("insert into t0 values(now, 3,3,3,3,3,3,3,3,3)", queryTimes=1)
        tdSql.execute("insert into t0 values(now, 3,NULL,3,3,3,3,3,3,3)", queryTimes=1)
        tdSql.execute("insert into t0 values(now, 3,NULL,3,3,3,3,3,3,3)", queryTimes=1)
        tdSql.execute("flush database test", queryTimes=1)
        time.sleep(2)
        tdSql.execute("insert into t0 values(now, 3,NULL,3,3,3,3,3,3,3)", queryTimes=1)
        tdSql.query("select first(c2) from t0 session(ts, 1s) order by ts", queryTimes=1)

    def ts6079(self):
        ts = 1741757485230
        tdSql.execute("drop database if exists ts6079")
        tdSql.execute("create database ts6079 vgroups 2 replica 1")
        tdSql.execute("CREATE STABLE ts6079.`meters` (`ts` TIMESTAMP, `current` FLOAT, `voltage` INT, `phase` FLOAT) TAGS (`groupid` INT, `location` VARCHAR(24))")
        for tableIndex in range(10):
            tdSql.execute(f"CREATE TABLE ts6079.t{tableIndex} USING ts6079.meters TAGS ({tableIndex}, 'tb{tableIndex}')")
            for num in range(10):
                tdSql.execute(f"INSERT INTO ts6079.t{tableIndex} VALUES({ts + num}, {num * 1.0}, {215 + num}, 0.0)")

        tdSql.query("select _wstart ,first(ts),last(ts),count(*),to_char(ts, 'yyyymmdd') as ts from ts6079.meters partition by to_char(ts, 'yyyymmdd') as ts state_window(cast(current as varchar(2)));")
        tdSql.checkRows(10)
        tdSql.checkData(0, 3, 10)

    def test_state_window(self):
        """summary: xxx

        description: xxx

        Since: xxx

        Labels: xxx

        Jira: xxx

        Catalog:
            - xxx:xxx

        History:
            - xxx
            - xxx

        """

        self.ts6079()
        self.check_crash_for_session_window()
        self.check_crash_for_state_window1()
        self.check_crash_for_state_window2()
        self.check_crash_for_state_window3()
        self.check_crash_for_state_window4()
        self.check_crash_for_state_window5()

        #tdSql.close()
        tdLog.success(f"{__file__} successfully executed")

event = threading.Event()
