
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
        self.vgroups    = 2
        self.ctbNum     = 10
        self.rowsPerTbl = 10000

    def init(self, conn, logSql, replicaVar=1):
        self.replicaVar = int(replicaVar)
        tdLog.debug(f"start to excute {__file__}")
        tdSql.init(conn.cursor(), True)

    def create_database(self,tsql, dbName,dropFlag=1,vgroups=2,replica=1):
        if dropFlag == 1:
            tsql.execute("drop database if exists %s"%(dbName))

        tsql.execute("create database if not exists %s vgroups %d replica %d"%(dbName, vgroups, replica))
        tdLog.debug("complete to create database %s"%(dbName))
        return
    
    def create_stable(self,tsql, paraDict):        
        colString = tdCom.gen_column_type_str(colname_prefix=paraDict["colPrefix"], column_elm_list=paraDict["colSchema"])
        tagString = tdCom.gen_tag_type_str(tagname_prefix=paraDict["tagPrefix"], tag_elm_list=paraDict["tagSchema"])
        # tdLog.debug(colString)
        # tdLog.debug(tagString)        
        sqlString = f"create table if not exists %s.%s (%s) tags (%s)"%(paraDict["dbName"], paraDict["stbName"], colString, tagString)
        tdLog.debug("%s"%(sqlString))
        tsql.execute(sqlString)
        return
 
    def create_ctable(self,tsql=None, dbName='dbx',stbName='stb',ctbPrefix='ctb',ctbNum=1,ctbStartIdx=0):
        for i in range(ctbNum):
            sqlString = "create table %s.%s%d using %s.%s tags(%d, 'tb%d', 'tb%d', %d, %d, %d)"%(dbName,ctbPrefix,i+ctbStartIdx,dbName,stbName,i+ctbStartIdx,i+ctbStartIdx,i+ctbStartIdx,i+ctbStartIdx,i+ctbStartIdx,i+ctbStartIdx)
            tsql.execute(sqlString)        

        tdLog.debug("complete to create %d child tables by %s.%s" %(ctbNum, dbName, stbName))
        return
       
    def insert_data(self,tsql,dbName,ctbPrefix,ctbNum,rowsPerTbl,batchNum,startTs,tsStep):
        tdLog.debug("start to insert data ............")
        tsql.execute("use %s" %dbName)
        pre_insert = "insert into "
        sql = pre_insert

        # t = time.time()
        # startTs = int(round(t * 1000))
        #tdLog.debug("doing insert data into stable:%s rows:%d ..."%(stbName, allRows))
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
                    # print("===sql: %s"%(sql))
                    rowsBatched = 0
                    if j < rowsPerTbl - 1:
                        sql = "insert into %s%d values " %(ctbPrefix,i)
                    else:
                        sql = "insert into "
        #end sql
        if sql != pre_insert:
            #print("insert sql:%s"%sql)
            tsql.execute(sql)
            # print("===sql: %s"%(sql))
        tdLog.debug("insert data ............ [OK]")
        return

    def prepareTestEnv(self):
        tdLog.printNoPrefix("======== prepare test env include database, stable, ctables, and insert data: ")
        paraDict = {'dbName':     'lm2_db0',
                    'dropFlag':   1,
                    'vgroups':    2,
                    'stbName':    'lm2_stb0',
                    'colPrefix':  'c',
                    'tagPrefix':  't',
                    'colSchema':   [{'type': 'INT', 'count':1},{'type': 'BIGINT', 'count':1},{'type': 'FLOAT', 'count':1},{'type': 'DOUBLE', 'count':1},{'type': 'smallint', 'count':1},{'type': 'tinyint', 'count':1},{'type': 'bool', 'count':1},{'type': 'binary', 'len':10, 'count':1},{'type': 'nchar', 'len':10, 'count':1}],
                    'tagSchema':   [{'type': 'INT', 'count':1},{'type': 'nchar', 'len':20, 'count':1},{'type': 'binary', 'len':20, 'count':1},{'type': 'BIGINT', 'count':1},{'type': 'smallint', 'count':1},{'type': 'DOUBLE', 'count':1}],
                    'ctbPrefix':  'lm2_tb',
                    'ctbStartIdx': 0,
                    'ctbNum':     10,
                    'rowsPerTbl': 10000,
                    'batchNum':   3000,
                    'startTs':    1537146000000,
                    'tsStep':     600000}

        paraDict['vgroups'] = self.vgroups
        paraDict['ctbNum'] = self.ctbNum
        paraDict['rowsPerTbl'] = self.rowsPerTbl
        
        tdLog.info("create database")
        self.create_database(tsql=tdSql, dbName=paraDict["dbName"], dropFlag=paraDict["dropFlag"], vgroups=paraDict["vgroups"], replica=self.replicaVar)
        
        tdLog.info("create stb")
        self.create_stable(tsql=tdSql, paraDict=paraDict)
        
        tdLog.info("create child tables")
        self.create_ctable(tsql=tdSql, dbName=paraDict["dbName"],stbName=paraDict["stbName"],ctbPrefix=paraDict["ctbPrefix"],ctbNum=paraDict["ctbNum"],ctbStartIdx=paraDict["ctbStartIdx"])
        self.insert_data(tsql=tdSql, dbName=paraDict["dbName"],ctbPrefix=paraDict["ctbPrefix"],ctbNum=paraDict["ctbNum"],rowsPerTbl=paraDict["rowsPerTbl"],batchNum=paraDict["batchNum"],startTs=paraDict["startTs"],tsStep=paraDict["tsStep"])
        return

    def tmqCase1(self):
        tdLog.printNoPrefix("======== test case 1: ")
        paraDict = {'dbName':     'lm2_db0',
                    'dropFlag':   1,
                    'vgroups':    2,
                    'stbName':    'lm2_stb0',
                    'colPrefix':  'c',
                    'tagPrefix':  't',
                    'colSchema':   [{'type': 'INT', 'count':1},{'type': 'BIGINT', 'count':1},{'type': 'FLOAT', 'count':1},{'type': 'DOUBLE', 'count':1},{'type': 'smallint', 'count':1},{'type': 'tinyint', 'count':1},{'type': 'bool', 'count':1},{'type': 'binary', 'len':10, 'count':1},{'type': 'nchar', 'len':10, 'count':1}],
                    'tagSchema':   [{'type': 'INT', 'count':1},{'type': 'nchar', 'len':20, 'count':1},{'type': 'binary', 'len':20, 'count':1},{'type': 'BIGINT', 'count':1},{'type': 'smallint', 'count':1},{'type': 'DOUBLE', 'count':1}],
                    'ctbPrefix':  'lm2_tb',
                    'ctbStartIdx': 0,
                    'ctbNum':     10,
                    'rowsPerTbl': 10000,
                    'batchNum':   3000,
                    'startTs':    1537146000000,
                    'tsStep':     600000}
        
        val1 = 1
        val2 = paraDict["ctbNum"] - 1
        # select count(*), t1, t2, t3, t4, t5, t6 from $stb where t1 > $val1 and t1 < $val2 group by t1, t2, t3, t4, t5, t6 order by t1 asc limit 1 offset 0
        sqlStr = f"select count(*), t1, t2, t3, t4, t5, t6 from %s where t1 > %d and t1 < %d group by t1, t2, t3, t4, t5, t6 order by t1 asc limit 1 offset 0"%(paraDict["stbName"], val1, val2)
        # tdLog.info("====sql:%s"%(sqlStr))
        tdSql.query(sqlStr)
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, paraDict["rowsPerTbl"])
        tdSql.checkData(0, 1, 2)
        tdSql.checkData(0, 2, "tb2")
        tdSql.checkData(0, 3, "tb2")
        tdSql.checkData(0, 4, 2)
        tdSql.checkData(0, 5, 2)
                
        # select count(*), t3, t4 from $stb where t2 like '%' and t1 > 2 and t1 < 5 group by t3, t4 order by t3 desc limit 2 offset 0        
        sqlStr = f"select count(*), t3, t4 from %s where t2 like '%%' and t1 > 2 and t1 < 5 group by t3, t4 order by t3 desc limit 2 offset 0"%(paraDict["stbName"])
        # tdLog.info("====sql:%s"%(sqlStr))
        tdSql.query(sqlStr)
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, paraDict["rowsPerTbl"])
        tdSql.checkData(0, 1, "tb4")
        tdSql.checkData(0, 2, 4)
        
        tdSql.checkData(1, 1, "tb3")
        tdSql.checkData(1, 2, 3)
        
        # select count(*) from $stb where t2 like '%' and t1 > 2 and t1 < 5 group by t3, t4 order by t3 desc limit 1 offset 1
        sqlStr = f"select count(*) from %s where t2 like '%%' and t1 > 2 and t1 < 5 group by t3, t4 order by t3 desc limit 1 offset 1"%(paraDict["stbName"])
        # tdLog.info("====sql:%s"%(sqlStr))
        tdSql.query(sqlStr)
        tdSql.checkRows(1)
        
        ## TBASE-348
        tdSql.error(f"select count(*) from %s where t1 like 1"%(paraDict["stbName"]))
        
        ts0 = paraDict["startTs"]
        delta = paraDict["tsStep"]
        tsu = paraDict["rowsPerTbl"] * delta
        tsu = tsu - delta
        tsu = tsu + ts0
        tb = paraDict["ctbPrefix"] + '0'
        # select _wstart, max(c1) from $tb where ts >= $ts0 and ts <= $tsu interval(5m) fill(value, -1) limit 10 offset 1
        sqlStr = f"select _wstart, max(c1) from %s where ts >= %d and ts <= %d interval(5m) fill(value, -1) limit 10 offset 1"%(tb, ts0, tsu)
        # tdLog.info("====sql:%s"%(sqlStr))
        tdSql.query(sqlStr)
        tdSql.checkRows(10)
        tdSql.checkData(0, 0, "2018-09-17 09:05:00.000")
        tdSql.checkData(0, 1, -1)
        tdSql.checkData(1, 1, 1)
        tdSql.checkData(9, 0, "2018-09-17 09:50:00.000")
        tdSql.checkData(9, 1, 5)
        
        tb5 = paraDict["ctbPrefix"] + '5'
        sqlStr = f"select max(c1), min(c2) from %s where ts >= %d and ts <= %d interval(5m) fill(value, -1, -2) limit 10 offset 1"%(tb5, ts0, tsu)
        # tdLog.info("====sql:%s"%(sqlStr))
        tdSql.query(sqlStr)
        tdSql.checkRows(10)
        tdSql.checkData(0, 0, -1)
        tdSql.checkData(0, 1, -2)
        tdSql.checkData(1, 0, 1)
        tdSql.checkData(1, 1, -2)
        tdSql.checkData(9, 0, 5)
        tdSql.checkData(9, 1, -2)

        ### [TBASE-350]
        ## tb + interval + fill(value) + limit offset
        tb = paraDict["ctbPrefix"] + '0'
        limit = paraDict["rowsPerTbl"]
        offset = limit / 2
        sqlStr = f"select max(c1), min(c2), sum(c3), avg(c4), stddev(c5), spread(c6), first(c7), last(c8), first(c9) from %s where ts >= %d and ts <= %d interval(5m) fill(value, -1, -2 ,-3, -4 , -5, -6 ,-7 ,'-8', '-9') limit %d offset %d"%(tb, ts0, tsu, limit, offset)
        # tdLog.info("====sql:%s"%(sqlStr))
        tdSql.query(sqlStr)
        tdSql.checkRows(limit)
        tdSql.checkData(0, 1, 0)

        sqlStr = f"select max(c1) from lm2_tb0 where ts >= 1537146000000 and ts <= 1543145400000 interval(5m) fill(value, -1000) limit 8200"
        # tdLog.info("====sql:%s"%(sqlStr))
        tdSql.query(sqlStr)
        tdSql.checkRows(8200)

        sqlStr = f"select max(c1) from lm2_tb0 where ts >= 1537146000000 and ts <= 1543145400000 interval(5m) fill(value, -1000) limit 100000;"
        # tdLog.info("====sql:%s"%(sqlStr))
        tdSql.query(sqlStr)

        sqlStr = f"select max(c1) from lm2_tb0 where ts >= 1537146000000 and ts <= 1543145400000 interval(5m) fill(value, -1000) limit 10 offset 8190;"
        # tdLog.info("====sql:%s"%(sqlStr))
        tdSql.query(sqlStr)
        tdSql.checkRows(10)
        tdSql.checkData(0, 0, 5)
        tdSql.checkData(1, 0, -1000)
        tdSql.checkData(2, 0, 6)
        tdSql.checkData(3, 0, -1000)


        sqlStr = f"select max(c1) from lm2_tb0 where ts >= 1537146000000 and ts <= 1543145400000 interval(5m) fill(value, -1000) limit 10 offset 10001;"
        # tdLog.info("====sql:%s"%(sqlStr))
        tdSql.query(sqlStr)
        tdSql.checkRows(10)
        tdSql.checkData(0, 0, -1000)
        tdSql.checkData(1, 0, 1)
        tdSql.checkData(2, 0, -1000)
        tdSql.checkData(3, 0, 2)

        sqlStr = f"select max(c1) from lm2_tb0 where ts >= 1537146000000 and ts <= 1543145400000 interval(5m) fill(value, -1000) limit 10000 offset 10001;"
        # tdLog.info("====sql:%s"%(sqlStr))
        tdSql.query(sqlStr)
        tdSql.checkRows(9998)


        sqlStr = f"select max(c1) from lm2_tb0 where ts >= 1537146000000 and ts <= 1543145400000 interval(5m) fill(value, -1000) limit 100 offset 20001;"
        # tdLog.info("====sql:%s"%(sqlStr))
        tdSql.query(sqlStr)
        tdSql.checkRows(0)

        # tb + interval + fill(linear) + limit offset
        limit = paraDict["rowsPerTbl"]
        offset = limit / 2
        sqlStr = f"select _wstart,max(c1), min(c2), sum(c3), avg(c4), stddev(c5), spread(c6), first(c7), last(c8), first(c9) from %s where ts >= %d and ts <= %d interval(5m) fill(linear) limit %d offset %d"%(tb,ts0,tsu,limit, offset)
        # tdLog.info("====sql:%s"%(sqlStr))
        tdSql.query(sqlStr)
        tdSql.checkRows(limit)
        tdSql.checkData(0, 1, 0)
        tdSql.checkData(1, 1, 0)
        tdSql.checkData(1, 3, 0.500000000)
        tdSql.checkData(3, 5, 0.000000000)
        tdSql.checkData(4, 6, 0.000000000)
        tdSql.checkData(4, 7, 1)
        tdSql.checkData(5, 7, None)        
        tdSql.checkData(6, 8, "binary3")
        tdSql.checkData(7, 9, None)



        limit = paraDict["rowsPerTbl"]
        offset = limit / 2
        sqlStr = f"select max(c1), min(c2), sum(c3), avg(c4), stddev(c5), spread(c6), first(c7), last(c8), first(c9) from %s where ts >= %d and ts <= %d interval(5m) fill(prev) limit %d offset %d"%(tb,ts0,tsu,limit, offset)
        # tdLog.info("====sql:%s"%(sqlStr))
        tdSql.query(sqlStr)
        tdSql.checkRows(limit)


        limit = paraDict["rowsPerTbl"]
        offset = limit / 2 + 10
        sqlStr = f"select _wstart,max(c1), min(c2), sum(c3), avg(c4), stddev(c5), spread(c6), first(c7), last(c8), first(c9) from %s where ts >= %d and ts <= %d and c1 = 5 interval(5m) fill(value, -1, -2 ,-3, -4 , -5, -6 ,-7 ,'-8', '-9') limit %d offset %d"%(tb,ts0,tsu,limit, offset)
        # tdLog.info("====sql:%s"%(sqlStr))
        tdSql.query(sqlStr)
        tdSql.checkRows(limit)
        tdSql.checkData(0, 1, 5)
        tdSql.checkData(0, 2, 5)
        tdSql.checkData(0, 3, 5.000000000)
        tdSql.checkData(0, 4, 5.000000000)
        tdSql.checkData(0, 5, 0.000000000)
        tdSql.checkData(0, 7, 1)
        tdSql.checkData(0, 8, "binary5")
        tdSql.checkData(0, 9, "nchar5")
        tdSql.checkData(1, 8, "-8")
        tdSql.checkData(1, 9, "-9")


        limit = paraDict["rowsPerTbl"]
        offset = limit * 2 - 11
        sqlStr = f"select _wstart,max(c1), min(c2), sum(c3), avg(c4), stddev(c5), spread(c6), first(c7), last(c8), first(c9) from %s where ts >= %d and ts <= %d and c1 = 5 interval(5m) fill(value, -1, -2 ,-3, -4 , -5, -6 ,-7 ,'-8', '-9') limit %d offset %d"%(tb,ts0,tsu,limit, offset)
        # tdLog.info("====sql:%s"%(sqlStr))
        tdSql.query(sqlStr)
        tdSql.checkRows(10)
        tdSql.checkData(0, 1, -1)
        tdSql.checkData(0, 2, -2)
        tdSql.checkData(1, 1, 5)
        tdSql.checkData(1, 2, 5)
        tdSql.checkData(1, 3, 5.000000000)
        tdSql.checkData(1, 5, 0.000000000)
        tdSql.checkData(1, 6, 0.000000000)
        tdSql.checkData(1, 8, "binary5")
        tdSql.checkData(1, 9, "nchar5")

        ### [TBASE-350]
        ## stb + interval + fill + group by + limit offset
        sqlStr = f"select max(c1), min(c2), sum(c3), avg(c4), first(c7), last(c8), first(c9) from lm2_tb0 where ts >= 1537146000000 and ts <= 1543145400000 partition by t1 interval(5m) fill(value, -1, -2, -3, -4 ,-7 ,'-8', '-9') limit 2 offset 10"
        tdLog.info("====sql:%s"%(sqlStr))
        tdSql.query(sqlStr)
        tdSql.checkRows(2)

        limit = 5
        offset = paraDict["rowsPerTbl"] * 2
        offset = offset - 2
        sqlStr = f"select max(c1), min(c2), sum(c3), avg(c4), first(c7), last(c8), first(c9) from lm2_tb0 where ts >= 1537146000000 and ts <= 1543145400000  partition by t1 interval(5m) fill(value, -1, -2, -3, -4 ,-7 ,'-8', '-9') order by t1, max(c1) limit %d offset %d"%(limit, offset)
        # tdLog.info("====sql:%s"%(sqlStr))
        tdSql.query(sqlStr)
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 9)
        tdSql.checkData(0, 1, 9)
        tdSql.checkData(0, 2, 9.000000000)
        tdSql.checkData(0, 3, 9.000000000)
        tdSql.checkData(0, 4, 1)
        tdSql.checkData(0, 5, "binary9")
        tdSql.checkData(0, 6, "nchar9")

        #add one more test case
        sqlStr = f"select max(c1), last(c8) from lm2_db0.lm2_tb0 where ts >= 1537146000000 and ts <= 1543145400000 interval(5m) fill(linear) limit 10 offset 4089;"
              
        tdLog.printNoPrefix("======== test case 1 end ...... ")

    # 
    def checkVGroups(self):

        # db2
        tdSql.execute("create database db2 vgroups 2;")
        tdSql.execute("use db2;")
        tdSql.execute("create table st(ts timestamp, age int) tags(area int);")
        tdSql.execute("create table t1 using st tags(1);")
        tdSql.query("select distinct(tbname) from st limit 1 offset 100;")
        tdSql.checkRows(0)
        tdLog.info("check db2 vgroups 2 limit 1 offset 100 successfully!")


        # db1
        tdSql.execute("create database db1 vgroups 1;")
        tdSql.execute("use db1;")
        tdSql.execute("create table st(ts timestamp, age int) tags(area int);")
        tdSql.execute("create table t1 using st tags(1);")
        tdSql.query("select distinct(tbname) from st limit 1 offset 100;")
        tdSql.checkRows(0)
        tdLog.info("check db1 vgroups 1 limit 1 offset 100 successfully!")


    def run(self):
        # tdSql.prepare()
        self.prepareTestEnv()
        self.tmqCase1()

        # one vgroup diff more than one vgroup check
        self.checkVGroups()


    def stop(self):
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")

event = threading.Event()

tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
