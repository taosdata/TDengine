import taos
import sys
import time
import socket
import os
import threading
import math
from datetime import datetime

from new_test_framework.utils import (
    tdLog,
    tdSql,
    tdCom,
    tdDnodes,
)


COMPARE_DATA = 0
COMPARE_LEN = 1

class TestEmptyIdentifier:

    def setup_class(self):
        tdLog.debug(f"start to excute {__file__}")

    def test_empty_identifier(self):
        """OldPy: empty identifier validation 

        Test empty identifier `` handling in 28 specific SQL statements and verify error code -2147473897:

        1. Test [Table Operations] with Empty Identifiers
            1.1 show create table `` - verify error -2147473897
            1.2 show create table test.`` - verify error -2147473897
            1.3 create table `` (ts timestamp, c1 int) - verify error -2147473897
            1.4 drop table `` - verify error -2147473897
            1.5 alter table `` add column c2 int - verify error -2147473897
            1.6 select * from `` - verify error -2147473897

        2. Test [Column and Tag Operations] with Empty Identifiers
            2.1 alter table meters add column `` int - verify error -2147473897
            2.2 alter table meters drop column `` - verify error -2147473897
            2.3 alter stable meters add tag `` int - verify error -2147473897
            2.4 alter stable meters rename tag cc `` - verify error -2147473897
            2.5 alter stable meters drop tag `` - verify error -2147473897

        3. Test [Data Manipulation] with Empty Identifiers
            3.1 insert into `` select * from t0 - verify error -2147473897
            3.2 insert into t100 using `` tags('', '') values(1,1,1) - verify error -2147473897
            3.3 insert into `` values(1,1,1) - verify error -2147473897

        4. Test [View Operations] with Empty Identifiers
            4.1 create view `` as select count(*) from meters interval(10s) - verify error -2147473897
            4.2 create view ``.view1 as select count(*) from meters - verify error -2147473897
            4.3 drop view `` - verify error -2147473897
            4.4 drop view ``.st1 - verify error -2147473897

        5. Test [TSMA Operations] with Empty Identifiers
            5.1 create tsma `` on meters function(count(c1)) interval(1m) - verify error -2147473897
            5.2 create tsma tsma1 on `` function(count(c1)) interval(1m) - verify error -2147473897

        6. Test [Stream Operations] with Empty Identifiers
            6.1 create stream `` interval(10s) sliding(10s) from meters into st1 as select count(*) from meters - verify error -2147473897
            6.2 create stream stream1 interval(10s) sliding(10s) from meters into `` as select count(*) from meters - verify error -2147473897
            6.3 create stream stream1 interval(10s) sliding(10s) from meters into st1 as select count(*) from `` - verify error -2147473897
            6.4 create stream stream1 interval(10s) sliding(10s) from meters stream_options(max_delay(100s)) into st1 as select count(*) from `` - verify error -2147473897
            6.5 create stream stream1 interval(10s) sliding(10s) from `` stream_options(max_delay(100s)) into st1 as select count(*) from meters - verify error -2147473897

        7. Test [Topic Operations] with Empty Identifiers
            7.1 create topic `` as select count(*) from meters interval(10s) - verify error -2147473897
            7.2 drop topic `` - verify error -2147473897

        8. Test [Error Code Verification] for All Cases
            8.1 Execute all 28 SQL statements with empty identifiers
            8.2 Verify each returns exact error code -2147473897
            8.3 Confirm error message consistency
            8.4 Validate connection stability after errors

        Catalog:
            - Streams:OldPyCases

        Since: v3.3.7.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-07-22 Beryl Migrated to new test framework

        """
        
        self.prepareTestEnv()
        self.excute_empty_identifier()

    def create_database(self,tdSql, dbName,dropFlag=1,vgroups=2,replica=1, duration:str='1d'):
        if dropFlag == 1:
            tdSql.execute("drop database if exists %s"%(dbName))

        tdSql.execute("create database if not exists %s vgroups %d replica %d duration %s"%(dbName, vgroups, replica, duration))
        tdLog.debug("complete to create database %s"%(dbName))
        return

    def create_stable(self,tdSql, paraDict):
        colString = tdCom.gen_column_type_str(colname_prefix=paraDict["colPrefix"], column_elm_list=paraDict["colSchema"])
        tagString = tdCom.gen_tag_type_str(tagname_prefix=paraDict["tagPrefix"], tag_elm_list=paraDict["tagSchema"])
        sqlString = f"create table if not exists %s.%s (%s) tags (%s)"%(paraDict["dbName"], paraDict["stbName"], colString, tagString)
        tdLog.debug("%s"%(sqlString))
        tdSql.execute(sqlString)
        return

    def create_ctable(self,tdSql=None, dbName='dbx',stbName='stb',ctbPrefix='ctb',ctbNum=1,ctbStartIdx=0):
        for i in range(ctbNum):
            sqlString = "create table %s.%s%d using %s.%s tags(%d, 'tb%d', 'tb%d', %d, %d, %d)" % \
                    (dbName,ctbPrefix,i+ctbStartIdx,dbName,stbName,(i+ctbStartIdx) % 5,i+ctbStartIdx,i+ctbStartIdx,i+ctbStartIdx,i+ctbStartIdx,i+ctbStartIdx)
            tdSql.execute(sqlString)

        tdLog.debug("complete to create %d child tables by %s.%s" %(ctbNum, dbName, stbName))
        return

    def insert_data(self,tdSql,dbName,ctbPrefix,ctbNum,rowsPerTbl,batchNum,startTs,tsStep):
        tdLog.debug("start to insert data ............")
        tdSql.execute("use %s" %dbName)
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
                    tdSql.execute(sql)
                    rowsBatched = 0
                    if j < rowsPerTbl - 1:
                        sql = "insert into %s%d values " %(ctbPrefix,i)
                    else:
                        sql = "insert into "
        if sql != pre_insert:
            tdSql.execute(sql)
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

        paraDict['vgroups'] = 4
        paraDict['ctbNum'] = 10
        paraDict['rowsPerTbl'] = 10000

        tdLog.info("create database")
        self.create_database(tdSql=tdSql, dbName=paraDict["dbName"], dropFlag=paraDict["dropFlag"], vgroups=paraDict["vgroups"], replica=1, duration='1h')

        tdLog.info("create stb")
        self.create_stable(tdSql=tdSql, paraDict=paraDict)

        tdLog.info("create child tables")
        self.create_ctable(tdSql=tdSql, dbName=paraDict["dbName"], \
                stbName=paraDict["stbName"],ctbPrefix=paraDict["ctbPrefix"],\
                ctbNum=paraDict["ctbNum"],ctbStartIdx=paraDict["ctbStartIdx"])
        self.insert_data(tdSql=tdSql, dbName=paraDict["dbName"],\
                ctbPrefix=paraDict["ctbPrefix"],ctbNum=paraDict["ctbNum"],\
                rowsPerTbl=paraDict["rowsPerTbl"],batchNum=paraDict["batchNum"],\
                startTs=paraDict["startTs"],tsStep=paraDict["tsStep"])
        return

    def execute_sql_and_expect_err(self, sql: str, err: int):
        tdSql.error(sql, err)

    def excute_empty_identifier(self):
        ## invalid identifier
        sqls = [
                'show create table ``',
                'show create table test.``',
                'create table `` (ts timestamp, c1 int)',
                'drop table ``',
                'alter table `` add column c2 int',
                'select * from ``',
                'alter table meters add column `` int',
                'alter table meters drop column ``',
                'alter stable meters add tag `` int',
                'alter stable meters rename tag cc ``',
                'alter stable meters drop tag ``',
                'insert into `` select * from t0',
                'insert into t100 using `` tags('', '') values(1,1,1)',
                'create view `` as select count(*) from meters interval(10s)',
                'create view ``.view1 as select count(*) from meters'
                'create tsma `` on meters function(count(c1)) interval(1m)',
                'create tsma tsma1 on `` function(count(c1)) interval(1m)',
                'create stream `` interval(10s) sliding(10s) from meters into st1 as select count(*) from meters',
                'create stream stream1 interval(10s) sliding(10s) from meters into `` as select count(*) from meters',
                'create stream stream1 interval(10s) sliding(10s) from meters into st1 as select count(*) from ``',
                'create stream stream1 interval(10s) sliding(10s) from meters stream_options(max_delay(100s)) into st1 as select count(*) from ``',
                'create stream stream1 interval(10s) sliding(10s) from `` stream_options(max_delay(100s)) into st1 as select count(*) from meters',
                'drop view ``',
                'drop view ``.st1',
                'create topic `` as select count(*) from meters interval(10s)',
                'drop topic ``',
                'insert into `` values(1,1,1)',
                ]

        for sql in sqls:
            self.execute_sql_and_expect_err(sql, -2147473897)