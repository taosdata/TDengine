###################################################################
#           Copyright (c) 2016 by TAOS Technologies, Inc.
#                     All rights reserved.
#
#  This file is proprietary and confidential to TAOS Technologies.
#  No part of this file may be reproduced, stored, transmitted,
#  disclosed or used in any form or by any means other than as
#  expressly provided by the written permission from Jianhui Tao
#
###################################################################

# -*- coding: utf-8 -*-

import sys
import os
import threading as thd
import multiprocessing as mp
from numpy.lib.function_base import insert
import taos
from util.log import *
from util.cases import *
from util.sql import *
import numpy as np
import datetime as dt
import time
# constant define
WAITS = 5 # wait seconds

class TDTestCase:
    #
    # --------------- main frame -------------------
    #
    clientCfgDict = {'queryproxy': '1','debugFlag': 135}
    clientCfgDict["queryproxy"] = '2'
    clientCfgDict["debugFlag"] = 143

    updatecfgDict = {'clientCfg': {}}
    updatecfgDict = {'debugFlag': 143}
    updatecfgDict["clientCfg"]  = clientCfgDict
    def caseDescription(self):
        '''
        limit and offset keyword function test cases;
        case1: limit offset base function test
        case2: offset return valid
        '''
        return

    def getBuildPath(self):
        selfPath = os.path.dirname(os.path.realpath(__file__))

        if ("community" in selfPath):
            projPath = selfPath[:selfPath.find("community")]
        else:
            projPath = selfPath[:selfPath.find("tests")]

        for root, dirs, files in os.walk(projPath):
            if ("taosd" in files or "taosd.exe" in files):
                rootRealPath = os.path.dirname(os.path.realpath(root))
                if ("packaging" not in rootRealPath):
                    buildPath = root[:len(root)-len("/build/bin")]
                    break
        return buildPath

    # init
    def init(self, conn, logSql, replicaVar=1):
        self.replicaVar = int(replicaVar)
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor())
        # tdSql.prepare()
        # self.create_tables();
        self.ts = 1500000000000

    # stop
    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


    # --------------- case  -------------------

    def newcur(self,host,cfg):
        user = "root"
        password = "taosdata"
        port =6030
        con=taos.connect(host=host, user=user, password=password, config=cfg ,port=port)
        cur=con.cursor()
        print(cur)
        return cur

    # create tables
    def create_tables(self,host,dbname,stbname,count):
        buildPath = self.getBuildPath()
        config = buildPath+ "../sim/dnode1/cfg/"

        tsql=self.newcur(host,config)
        tsql.execute("use %s" %dbname)

        pre_create = "create table"
        sql = pre_create
        count=int(count)

        tdLog.debug("doing create one  stable %s and %d  child table in %s  ..." %(stbname, count ,dbname))
        # print(time.time())
        exeStartTime=time.time()
        # print(type(tcountStop),type(tcountStart))
        for i in range(0,count):
            sql += " %s_%d using %s tags(%d)"%(stbname,i,stbname,i+1)
            if i >0 and i%20000 == 0:
                # print(sql)
                tsql.execute(sql)
                sql = pre_create
        # print(time.time())
        # end sql
        if sql != pre_create:
            # print(sql)
            tsql.execute(sql)
        exeEndTime=time.time()
        spendTime=exeEndTime-exeStartTime
        speedCreate=count/spendTime
        # tdLog.debug("spent %.2fs to create 1 stable and %d table, create speed is %.2f table/s... [OK]"% (spendTime,count,speedCreate))
        return

    def mutiThread_create_tables(self,host,dbname,stbname,vgroups,threadNumbers,childcount):
        buildPath = self.getBuildPath()
        config = buildPath+ "../sim/dnode1/cfg/"

        tsql=self.newcur(host,config)
        tdLog.debug("create database %s"%dbname)
        tsql.execute("drop database if exists %s"%dbname)
        tsql.execute("create database %s vgroups %d"%(dbname,vgroups))
        tsql.execute("use %s" %dbname)
        count=int(childcount)
        threads = []
        for i in range(threadNumbers):
            tsql.execute("create stable %s%d(ts timestamp, c1 int, c2 binary(10)) tags(t1 int)"%(stbname,i))
            threads.append(thd.Thread(target=self.create_tables, args=(host, dbname, stbname+"%d"%i, count,)))
        start_time = time.time()
        for tr in threads:
            tr.start()
        for tr in threads:
            tr.join()
        end_time = time.time()
        spendTime=end_time-start_time
        speedCreate=threadNumbers*count/spendTime
        tdLog.debug("spent %.2fs to create %d stable and %d table, create speed is %.2f table/s... [OK]"% (spendTime,threadNumbers,threadNumbers*count,speedCreate))

        return

    # def create_tables(self,host,dbname,stbname,vgroups,tcountStart,tcountStop):


    # insert data
    def insert_data(self, host, dbname, stbname, chilCount, ts_start, rowCount):
        buildPath = self.getBuildPath()
        config = buildPath+ "../sim/dnode1/cfg/"
        tsql=self.newcur(host,config)
        tdLog.debug("ready to inser data")
        tsql.execute("use %s" %dbname)
        pre_insert = "insert into "
        sql = pre_insert
        chilCount=int(chilCount)
        allRows=chilCount*rowCount
        tdLog.debug("doing insert data into stable-index:%s rows:%d ..."%(stbname, allRows))
        exeStartTime=time.time()
        for i in range(0,chilCount):
            sql += " %s_%d values "%(stbname,i)
            for j in range(rowCount):
                sql += "(%d, %d, 'taos_%d') "%(ts_start + j*1000, j, j)
                if j >0 and j%4000 == 0:
                    # print(sql)
                    tsql.execute(sql)
                    sql = "insert into %s_%d values " %(stbname,i)
        # end sql
        if sql != pre_insert:
            # print(sql)
            print(len(sql))
            tsql.execute(sql)
        exeEndTime=time.time()
        spendTime=exeEndTime-exeStartTime
        speedInsert=allRows/spendTime
        tdLog.debug("spent %.2fs to INSERT  %d rows into %s , insert rate is  %.2f rows/s... [OK]"% (spendTime,allRows,stbname,speedInsert))
        # tdLog.debug("INSERT TABLE DATA ............ [OK]")
        return

    def mutiThread_insert_data(self, host, dbname, stbname, threadNumbers, chilCount, ts_start, childrowcount):
        buildPath = self.getBuildPath()
        config = buildPath+ "../sim/dnode1/cfg/"

        tsql=self.newcur(host,config)
        tdLog.debug("ready to inser data")

        tsql.execute("use %s" %dbname)
        chilCount=int(chilCount)
        threads = []
        for i in range(threadNumbers):
            # tsql.execute("create stable %s%d(ts timestamp, c1 int, c2 binary(10)) tags(t1 int)"%(stbname,i))
            threads.append(thd.Thread(target=self.insert_data, args=(host, dbname, stbname+"%d"%i, chilCount, ts_start, childrowcount,)))
        start_time = time.time()
        for tr in threads:
            tr.start()
        for tr in threads:
            tr.join()
        end_time = time.time()
        spendTime=end_time-start_time
        tableCounts=threadNumbers*chilCount
        stableRows=chilCount*childrowcount
        allRows=stableRows*threadNumbers
        speedInsert=allRows/spendTime

        for i in range(threadNumbers):
            tdSql.execute("use %s" %dbname)
            tdSql.query("select count(*) from %s%d"%(stbname,i))
            tdSql.checkData(0,0,stableRows)
        tdLog.debug("spent %.2fs to insert %d rows  into %d stable and %d table,  speed is %.2f table/s... [OK]"% (spendTime,allRows,threadNumbers,tableCounts,speedInsert))
        tdLog.debug("INSERT TABLE DATA ............ [OK]")

        return


    def taosBench(self,jsonFile):
        buildPath = self.getBuildPath()
        if (buildPath == ""):
            tdLog.exit("taosd not found!")
        else:
            tdLog.info("taosd found in %s" % buildPath)
        taosBenchbin = buildPath+ "/build/bin/taosBenchmark"
        os.system("%s -f %s -y " %(taosBenchbin,jsonFile))

        return
    def taosBenchCreate(self,host,dropdb,dbname,stbname,vgroups,processNumbers,count):

        # count=50000
        buildPath = self.getBuildPath()
        config = buildPath+ "../sim/dnode1/cfg/"
        tsql=self.newcur(host,config)

        # insert: create one  or mutiple tables per sql and insert multiple rows per sql
        tsql.execute("drop database if exists %s"%dbname)

        tsql.execute("create database %s vgroups %d"%(dbname,vgroups))
        print("db has been created")
        # tsql.getResult("select * from information_schema.ins_databases")
        # print(tdSql.queryResult)
        tsql.execute("use %s" %dbname)

        threads = []
        for i in range(processNumbers):
            jsonfile="1-insert/Vgroups%d%d.json"%(vgroups,i)
            os.system("cp -f 1-insert/manyVgroups.json   %s"%(jsonfile))
            os.system("sed -i 's/\"name\": \"db\",/\"name\": \"%s\",/g' %s"%(dbname,jsonfile))
            os.system("sed -i 's/\"drop\": \"no\",/\"drop\": \"%s\",/g' %s"%(dropdb,jsonfile))
            os.system("sed -i 's/\"host\": \"127.0.0.1\",/\"host\": \"%s\",/g' %s"%(host,jsonfile))
            os.system("sed -i 's/\"childtable_count\": 10000,/\"childtable_count\": %d,/g' %s "%(count,jsonfile))
            os.system("sed -i 's/\"name\": \"stb1\",/\"name\":  \"%s%d\",/g' %s "%(stbname,i,jsonfile))
            os.system("sed -i 's/\"childtable_prefix\": \"stb1_\",/\"childtable_prefix\": \"%s%d_\",/g' %s "%(stbname,i,jsonfile))
            threads.append(mp.Process(target=self.taosBench, args=("%s"%jsonfile,)))
        start_time = time.time()
        for tr in threads:
            tr.start()
        for tr in threads:
            tr.join()
        end_time = time.time()

        spendTime=end_time-start_time
        speedCreate=count/spendTime
        tdLog.debug("spent %.2fs to create 1 stable and %d table, create speed is %.2f table/s... [OK]"% (spendTime,count,speedCreate))
        return

    def checkData(self,dbname,stbname,stableCount,CtableCount,rowsPerSTable,):
        tdSql.execute("use %s"%dbname)
        tdSql.query("show stables")
        tdSql.checkRows(stableCount)
        tdSql.query("show tables")
        tdSql.checkRows(CtableCount)
        for i in range(stableCount):
            tdSql.query("select count(*) from %s%d"%(stbname,i))
            tdSql.checkData(0,0,rowsPerSTable)
        return


    # test case1 base
    def test_case1(self):
        #stableCount=threadNumbersCtb
        parameterDict = {'vgroups':        1,    \
                         'threadNumbersCtb': 5,  \
                         'threadNumbersIda': 5, \
                         'stableCount':   5,      \
                         'tablesPerStb':    50,  \
                         'rowsPerTable':    10,  \
                         'dbname':    'db',    \
                         'stbname':    'stb',   \
                         'host':  'localhost',    \
                         'startTs':    1640966400000}  # 2022-01-01 00:00:00.000

        tdLog.debug("-----create database and muti-thread create tables test------- ")
        #host,dbname,stbname,vgroups,threadNumbers,tcountStart,tcountStop
        #host, dbname, stbname, threadNumbers, chilCount, ts_start, childrowcount
        self.mutiThread_create_tables(
            host=parameterDict['host'],
            dbname=parameterDict['dbname'],
            stbname=parameterDict['stbname'],
            vgroups=parameterDict['vgroups'],
            threadNumbers=parameterDict['threadNumbersCtb'],
            childcount=parameterDict['tablesPerStb'])

        self.mutiThread_insert_data(
            host=parameterDict['host'],
            dbname=parameterDict['dbname'],
            stbname=parameterDict['stbname'],
            threadNumbers=parameterDict['threadNumbersIda'],
            chilCount=parameterDict['tablesPerStb'],
            ts_start=parameterDict['startTs'],
            childrowcount=parameterDict['rowsPerTable'])

        tableCount=parameterDict['threadNumbersCtb']*parameterDict['tablesPerStb']
        rowsPerStable=parameterDict['rowsPerTable']*parameterDict['tablesPerStb']

        self.checkData(dbname=parameterDict['dbname'],stbname=parameterDict['stbname'], stableCount=parameterDict['threadNumbersCtb'],CtableCount=tableCount,rowsPerSTable=rowsPerStable)

    def test_case3(self):
        #stableCount=threadNumbersCtb
        parameterDict = {'vgroups':        1,    \
                         'threadNumbersCtb': 8,  \
                         'stableCount':   5,      \
                         'tablesPerStb':    10,  \
                         'rowsPerTable':    100,  \
                         'dbname':    'db1',    \
                         'stbname':    'stb1',   \
                         'host':  'localhost',    \
                         'startTs':    1640966400000}  # 2022-01-01 00:00:00.000

        self.taosBenchCreate(
            parameterDict['host'],
            "no",
            parameterDict['dbname'],
            parameterDict['stbname'],
            parameterDict['vgroups'],
            parameterDict['threadNumbersCtb'],
            parameterDict['tablesPerStb'])
        tableCount=parameterDict['threadNumbersCtb']*parameterDict['tablesPerStb']
        rowsPerStable=parameterDict['rowsPerTable']*parameterDict['tablesPerStb']

        self.checkData(
            dbname=parameterDict['dbname'],
            stbname=parameterDict['stbname'],
            stableCount=parameterDict['threadNumbersCtb'],
            CtableCount=tableCount,
            rowsPerSTable=rowsPerStable)

        # self.taosBenchCreate("test209","no","db2", "stb2", 1, 8, 1*10000)

        # self.taosBenchCreate("chenhaoran02","no","db1", "stb1", 1, 8, 1*10000)

        # self.taosBenchCreate("db1", "stb1", 4, 5, 100*10000)
        # self.taosBenchCreate("db1", "stb1", 1, 5, 100*10000)

        return

    # run case
    def run(self):

        # create database and tables。
        self.test_case1()
        tdLog.debug(" LIMIT test_case1 ............ [OK]")

        # taosBenchmark：create database/table and insert data
        self.test_case3()
        tdLog.debug(" LIMIT test_case3 ............ [OK]")



        return
#
# add case with filename
#
tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
