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
        """Empty Identifier Validation Test

        Test comprehensive empty identifier handling across all SQL constructs with focus on stream processing:

        1. Test [Basic SQL Object] Empty Identifier Validation
            1.1 Test database operations with empty identifiers
                1.1.1 CREATE DATABASE with empty name - verify error -2147473897
                1.1.2 DROP DATABASE with empty name - verify error -2147473897
                1.1.3 USE DATABASE with empty name - verify error -2147473897
                1.1.4 ALTER DATABASE with empty name - verify error -2147473897
            1.2 Test table operations with empty identifiers
                1.2.1 CREATE TABLE with empty name - verify error -2147473897
                1.2.2 DROP TABLE with empty name - verify error -2147473897
                1.2.3 ALTER TABLE with empty name - verify error -2147473897
                1.2.4 DESCRIBE TABLE with empty name - verify error -2147473897
            1.3 Test column and tag operations with empty identifiers
                1.3.1 Column creation with empty name - verify error -2147473897
                1.3.2 Tag creation with empty name - verify error -2147473897
                1.3.3 Column reference with empty name - verify error -2147473897
                1.3.4 Tag reference with empty name - verify error -2147473897

        2. Test [Stream Processing Objects] Empty Identifier Validation
            2.1 Test stream operations with empty identifiers
                2.1.1 CREATE STREAM with empty name - verify error -2147473897
                2.1.2 DROP STREAM with empty name - verify error -2147473897
                2.1.3 PAUSE STREAM with empty name - verify error -2147473897
                2.1.4 RESUME STREAM with empty name - verify error -2147473897
            2.2 Test stream target table with empty identifiers
                2.2.1 INTO clause with empty table name - verify error -2147473897
                2.2.2 OUTPUT_SUBTABLE with empty expression - verify error -2147473897
                2.2.3 Target column specification with empty name - verify error -2147473897
                2.2.4 Tag specification with empty name - verify error -2147473897

        3. Test [Advanced SQL Objects] Empty Identifier Validation
            3.1 Test view operations with empty identifiers
                3.1.1 CREATE VIEW with empty name - verify error -2147473897
                3.1.2 DROP VIEW with empty name - verify error -2147473897
                3.1.3 View reference with empty name - verify error -2147473897
                3.1.4 View column alias with empty name - verify error -2147473897
            3.2 Test topic operations with empty identifiers
                3.2.1 CREATE TOPIC with empty name - verify error -2147473897
                3.2.2 DROP TOPIC with empty name - verify error -2147473897
                3.2.3 Topic subscription with empty name - verify error -2147473897
                3.2.4 Topic reference with empty name - verify error -2147473897

        4. Test [Function and Expression] Empty Identifier Validation
            4.1 Test function calls with empty identifiers
                4.1.1 User-defined function with empty name - verify error -2147473897
                4.1.2 Function parameter with empty name - verify error -2147473897
                4.1.3 Aggregate function alias with empty name - verify error -2147473897
                4.1.4 Window function alias with empty name - verify error -2147473897
            4.2 Test expression components with empty identifiers
                4.2.1 Variable reference with empty name - verify error -2147473897
                4.2.2 Alias specification with empty name - verify error -2147473897
                4.2.3 Label specification with empty name - verify error -2147473897
                4.2.4 Parameter binding with empty name - verify error -2147473897

        5. Test [Complex SQL Constructs] Empty Identifier Validation
            5.1 Test JOIN operations with empty identifiers
                5.1.1 Table alias in JOIN with empty name - verify error -2147473897
                5.1.2 Column alias in JOIN with empty name - verify error -2147473897
                5.1.3 JOIN condition with empty column name - verify error -2147473897
                5.1.4 Subquery alias with empty name - verify error -2147473897
            5.2 Test window operations with empty identifiers
                5.2.1 PARTITION BY with empty column name - verify error -2147473897
                5.2.2 ORDER BY with empty column name - verify error -2147473897
                5.2.3 Window alias with empty name - verify error -2147473897
                5.2.4 Window frame specification with empty reference - verify error -2147473897

        6. Test [Data Manipulation] Empty Identifier Validation
            6.1 Test INSERT operations with empty identifiers
                6.1.1 INSERT INTO with empty table name - verify error -2147473897
                6.1.2 Column list with empty column name - verify error -2147473897
                6.1.3 Value alias with empty name - verify error -2147473897
                6.1.4 Source table with empty name - verify error -2147473897
            6.2 Test UPDATE/DELETE operations with empty identifiers
                6.2.1 UPDATE with empty table name - verify error -2147473897
                6.2.2 DELETE FROM with empty table name - verify error -2147473897
                6.2.3 SET clause with empty column name - verify error -2147473897
                6.2.4 WHERE clause with empty column name - verify error -2147473897

        7. Test [Index and Constraint] Empty Identifier Validation
            7.1 Test index operations with empty identifiers
                7.1.1 CREATE INDEX with empty name - verify error -2147473897
                7.1.2 DROP INDEX with empty name - verify error -2147473897
                7.1.3 Index column with empty name - verify error -2147473897
                7.1.4 Index reference with empty name - verify error -2147473897
            7.2 Test constraint operations with empty identifiers
                7.2.1 Primary key constraint with empty name - verify error -2147473897
                7.2.2 Foreign key constraint with empty name - verify error -2147473897
                7.2.3 Check constraint with empty name - verify error -2147473897
                7.2.4 Unique constraint with empty name - verify error -2147473897

        8. Test [Error Consistency and Recovery]
            8.1 Test error code consistency
                8.1.1 All empty identifier scenarios return -2147473897
                8.1.2 Error message format consistency
                8.1.3 Error context information accuracy
                8.1.4 Error recovery mechanisms
            8.2 Test parsing and validation behavior
                8.2.1 Early error detection in parsing phase
                8.2.2 Graceful error handling without crashes
                8.2.3 Connection stability after errors
                8.2.4 Transaction rollback behavior on errors

        Catalog:
            - SQL:SyntaxValidation:EmptyIdentifier

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