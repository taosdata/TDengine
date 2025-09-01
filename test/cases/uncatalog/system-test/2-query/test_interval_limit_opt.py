import taos
import sys
import time
import socket
import os
import threading
import math

from new_test_framework.utils import tdLog, tdSql
from new_test_framework.utils.common import tdCom

class TestIntervalLimitOpt:

    def setup_class(cls):
        cls.replicaVar = 1  # 设置默认副本数
        tdLog.debug(f"start to excute {__file__}")
        #tdSql.init(conn.cursor(), logSql)
        
        cls.vgroups    = 4
        cls.ctbNum     = 10
        cls.rowsPerTbl = 10000
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
                    sql += "(%d, %d, %d, %d,%d,%d,%d,true,'binary%d', 'nchar%d') "%(startTs + j*tsStep, j%10, j%10, j%10, j%10, j%10, j%10, j%10, j%10)
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

    def check_first_rows(self, all_rows, limited_rows, offset: int = 0):
        for i in range(0, len(limited_rows) - 1):
            if limited_rows[i] != all_rows[i + offset]:
                tdLog.info("row: %d, row in all:    %s" % (i+offset+1, str(all_rows[i+offset])))
                tdLog.info("row: %d, row in limted: %s" % (i+1, str(limited_rows[i])))
                tdLog.exit("row data check failed")
        tdLog.info("all rows are the same as query without limit..")

    def query_and_check_with_slimit(self, sql: str, max_limit: int, step: int, offset: int = 0):
        self.query_and_check_with_limit(sql, max_limit, step, offset, ' slimit ')

    def query_and_check_with_limit(self, sql: str, max_limit: int, step: int, offset: int = 0, limit_str: str = ' limit '):
        for limit in range(0, max_limit, step):
            limited_sql = sql + limit_str + str(offset) + "," + str(limit)
            tdLog.info("query with sql: %s "  % (sql) + limit_str + " %d,%d" % (offset, limit))
            all_rows = tdSql.getResult(sql)
            limited_rows = tdSql.getResult(limited_sql)
            tdLog.info("all rows: %d, limited rows: %d" % (len(all_rows), len(limited_rows)))
            if limit_str == ' limit ':
                if limit + offset <= len(all_rows) and len(limited_rows) != limit:
                    tdLog.exit("limited sql has less rows than limit value which is not right, \
                            limit: %d, limited_rows: %d, all_rows: %d, offset: %d" % (limit, len(limited_rows), len(all_rows), offset))
                elif limit + offset > len(all_rows) and offset < len(all_rows) and offset + len(limited_rows) != len(all_rows):
                    tdLog.exit("limited sql has less rows than all_rows which is not right, \
                            limit: %d, limited_rows: %d, all_rows: %d, offset: %d" % (limit, len(limited_rows), len(all_rows), offset))
                elif offset >= len(all_rows) and len(limited_rows) != 0:
                    tdLog.exit("limited rows should be zero, \
                            limit: %d, limited_rows: %d, all_rows: %d, offset: %d" % (limit, len(limited_rows), len(all_rows), offset))

            self.check_first_rows(all_rows, limited_rows, offset)

    def check_interval_limit_asc(self, offset: int = 0):
        sqls = ["select _wstart, _wend, count(*), sum(c1), avg(c2), first(ts) from meters interval(1s) ",
                "select _wstart, _wend, count(*), sum(c1), avg(c2), first(ts) from meters interval(1m) ",
                "select _wstart, _wend, count(*), sum(c1), avg(c2), first(ts) from meters interval(1h) ",
                "select _wstart, _wend, count(*), sum(c1), avg(c2), first(ts) from meters interval(1d) ",
                "select _wstart, _wend, count(*), sum(c1), avg(c2), first(ts) from t1 interval(1s) ",
                "select _wstart, _wend, count(*), sum(c1), avg(c2), first(ts) from t1 interval(1m) ",
                "select _wstart, _wend, count(*), sum(c1), avg(c2), first(ts) from t1 interval(1h) ",
                "select _wstart, _wend, count(*), sum(c1), avg(c2), first(ts) from t1 interval(1d) "]
        for sql in sqls:
            self.query_and_check_with_limit(sql, 5000, 500, offset)

    def check_interval_limit_desc(self, offset: int = 0):
        sqls = ["select _wstart, _wend, count(*), sum(c1), avg(c2), last(ts) from meters interval(1s) ",
                "select _wstart, _wend, count(*), sum(c1), avg(c2), last(ts) from meters interval(1m) ",
                "select _wstart, _wend, count(*), sum(c1), avg(c2), last(ts) from meters interval(1h) ",
                "select _wstart, _wend, count(*), sum(c1), avg(c2), last(ts) from meters interval(1d) ",
                "select _wstart, _wend, count(*), sum(c1), avg(c2), last(ts) from t1 interval(1s) ",
                "select _wstart, _wend, count(*), sum(c1), avg(c2), last(ts) from t1 interval(1m) ",
                "select _wstart, _wend, count(*), sum(c1), avg(c2), last(ts) from t1 interval(1h) ",
                "select _wstart, _wend, count(*), sum(c1), avg(c2), last(ts) from t1 interval(1d) "]
        for sql in sqls:
            self.query_and_check_with_limit(sql, 5000, 500, offset)

    def check_interval_limit_offset(self):
        for offset in range(0, 1000, 500):
            self.check_interval_limit_asc(offset)
            self.check_interval_limit_desc(offset)

    def check_interval_partition_by_slimit_limit(self):
        sql = "select * from (select _wstart as a, _wend as b, count(*), sum(c1), last(c2), first(ts),c3 from meters " \
                "where ts >= '2018-09-17 09:00:00.000' and ts <= '2018-10-17 09:30:00.000' partition by c3 interval(1m) slimit 10 limit 2) order by c3 asc"
        tdSql.query(sql)
        tdSql.checkRows(20)
        tdSql.checkData(0, 4, 0)
        tdSql.checkData(1, 4, 0)
        tdSql.checkData(2, 4, 1)
        tdSql.checkData(3, 4, 1)
        tdSql.checkData(18, 4, 9)
        tdSql.checkData(19, 4, 9)

        sql = "select * from (select _wstart as a, _wend as b, count(*), sum(c1), last(c2), first(ts),c3 from meters " \
                "where ts >= '2018-09-17 09:00:00.000' and ts <= '2018-10-17 09:30:00.000' partition by c3 interval(1m) slimit 2,2 limit 2) order by c3 asc"
        tdSql.query(sql)
        tdSql.checkRows(4)
        tdSql.checkData(0, 4, 2)
        tdSql.checkData(1, 4, 2)
        tdSql.checkData(2, 4, 9)
        tdSql.checkData(3, 4, 9)
        
        sql = "SELECT _wstart, last(c1) FROM t6 INTERVAL(1w);"
        tdSql.query(sql)
        tdSql.checkRows(11)

    def check_partition_by_limit_no_agg(self):
        sql_template = 'select t1 from meters partition by t1 limit %d'

        for i in range(1, 5000, 1000):
            tdSql.query(sql_template % i)
            tdSql.checkRows(5 * i)

    def test_interval_limit_opt(self):
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

        self.prepareTestEnv()
        self.check_interval_limit_offset()
        self.check_interval_partition_by_slimit_limit()
        self.check_partition_by_limit_no_agg()

        #tdSql.close()
        tdLog.success(f"{__file__} successfully executed")

event = threading.Event()
