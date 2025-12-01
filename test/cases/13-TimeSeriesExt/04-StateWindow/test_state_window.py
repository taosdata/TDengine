from new_test_framework.utils import tdLog, tdSql, tdCom

import time
import socket
import os
import threading

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
        self.prepare_original_data()
        tdSql.execute("insert into t0 values(now, 4,4,4,4,4,4,4,4,4)", queryTimes=1)
        tdSql.execute("select bottom(c1, 1), c2 from t0 state_window(c2) order by ts", queryTimes=1)

    def check_crash_for_state_window2(self):
        tdSql.execute("drop database if exists test")
        self.prepareTestEnv()
        self.prepare_original_data()
        tdSql.execute("insert into t0 values(now, 4,NULL,4,4,4,4,4,4,4)", queryTimes=1)
        tdSql.execute("insert into t0 values(now, 4,4,4,4,4,4,4,4,4)", queryTimes=1)
        tdSql.execute("select bottom(c1, 1), c2 from t0 state_window(c2) order by ts", queryTimes=1)

    def check_crash_for_state_window3(self):
        tdSql.execute("drop database if exists test")
        self.prepareTestEnv()
        self.prepare_original_data()
        tdSql.execute("insert into t0 values(now, 4,NULL,4,4,4,4,4,4,4)", queryTimes=1)
        tdSql.execute("insert into t0 values(now, 4,5,4,4,4,4,4,4,4)", queryTimes=1)
        tdSql.execute("select bottom(c1, 1), c2 from t0 state_window(c2) order by ts", queryTimes=1)

    def check_crash_for_state_window4(self):
        tdSql.execute("drop database if exists test")
        self.prepareTestEnv()
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
    
    def test_state_window_start_with_null(self):
        """summary: test state window start with null

        description: when state window starts with null values, 
            the aggregation result should be correct, especially selecting the state column itself.

        Since: v3.3.8.2

        Labels: state window

        Jira: TD-38341

        Catalog:
            - Function:aggregation

        History:
            - 2025-10-22: Tony Zhang created

        """
        
        tdSql.execute("drop database if exists testdb")
        tdSql.execute("create database if not exists testdb keep 3650", show=True)
        tdSql.execute("use testdb")
        values = '''
                ('2025-09-01 10:00:00', null,    20),
                ('2025-09-01 10:00:01', 'a',     23.5),
                ('2025-09-01 10:00:02', 'a',     25.9),
                ('2025-09-01 10:00:03', null,    26),
                ('2025-09-01 10:00:04', 'a',     28),
                ('2025-09-01 10:00:05', null,    24.3),
                ('2025-09-01 10:00:06', null,    null),
                ('2025-09-01 10:00:07', 'b',     18),
                ('2025-09-01 10:00:08', 'b',     14.4),
                ('2025-09-01 10:00:09', 'a',     17.7),
                ('2025-09-01 10:00:10', 'a',     null),
                ('2025-09-01 10:00:11', null,    22.3),
                ('2025-09-01 10:00:12', 'b',     18.18),
                ('2025-09-01 10:00:13', 'b',     19.5),
                ('2025-09-01 10:00:14', null,    9.9)'''
        # normal table
        tdSql.execute("create table ntb (ts timestamp, s varchar(10), v double)", show=True)
        tdSql.execute(f"insert into ntb values {values}", show=True)

        tdSql.query("""
            select _wstart, _wduration, _wend, count(*), count(s), count(v),
            avg(v), first(v), cols(last(v), ts), cols(last_row(v), ts), s
            from ntb state_window(s, 2)
        """, show=True)
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, "2025-09-01 10:00:00.000")
        tdSql.checkData(0, 1, 4000)
        tdSql.checkData(0, 2, "2025-09-01 10:00:04.000")
        tdSql.checkData(0, 3, 5)
        tdSql.checkData(0, 4, 3)
        tdSql.checkData(0, 5, 5)
        tdSql.checkData(0, 6, 24.68)
        tdSql.checkData(0, 7, 20)
        tdSql.checkData(0, 8, "2025-09-01 10:00:04.000")
        tdSql.checkData(0, 9, "2025-09-01 10:00:04.000")
        tdSql.checkData(0, 10, "a")

        tdSql.checkData(1, 0, "2025-09-01 10:00:04.001")
        tdSql.checkData(1, 1, 3999)
        tdSql.checkData(1, 2, "2025-09-01 10:00:08.000")
        tdSql.checkData(1, 3, 4)
        tdSql.checkData(1, 4, 2)
        tdSql.checkData(1, 5, 3)
        tdSql.checkData(1, 6, 18.9)
        tdSql.checkData(1, 7, 24.3)
        tdSql.checkData(1, 8, "2025-09-01 10:00:08.000")
        tdSql.checkData(1, 9, "2025-09-01 10:00:08.000")
        tdSql.checkData(1, 10, "b")

        tdSql.checkData(2, 0, "2025-09-01 10:00:08.001")
        tdSql.checkData(2, 1, 1999)
        tdSql.checkData(2, 2, "2025-09-01 10:00:10.000")
        tdSql.checkData(2, 3, 2)
        tdSql.checkData(2, 4, 2)
        tdSql.checkData(2, 5, 1)
        tdSql.checkData(2, 6, 17.7)
        tdSql.checkData(2, 7, 17.7)
        tdSql.checkData(2, 8, "2025-09-01 10:00:09.000")
        tdSql.checkData(2, 9, "2025-09-01 10:00:10.000")
        tdSql.checkData(2, 10, "a")

        tdSql.checkData(3, 0, "2025-09-01 10:00:10.001")
        tdSql.checkData(3, 1, 2999)
        tdSql.checkData(3, 2, "2025-09-01 10:00:13.000")
        tdSql.checkData(3, 3, 3)
        tdSql.checkData(3, 4, 2)
        tdSql.checkData(3, 5, 3)
        tdSql.checkData(3, 6, 19.9933333333333)
        tdSql.checkData(3, 7, 22.3)
        tdSql.checkData(3, 8, "2025-09-01 10:00:13.000")
        tdSql.checkData(3, 9, "2025-09-01 10:00:13.000")
        tdSql.checkData(3, 10, "b")

event = threading.Event()
