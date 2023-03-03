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
selfPath = os.path.dirname(os.path.realpath(__file__))
utilPath="%s/../../pytest/"%selfPath
import threading
import multiprocessing as mp
from numpy.lib.function_base import insert
import taos
sys.path.append(utilPath)
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
        # tdSql.init(conn.cursor())
        # tdSql.prepare()
        # self.create_tables();
        self.ts = 1500000000000


    # run case
    def run(self):

        #  test base case
        self.test_case1()
        tdLog.debug(" LIMIT test_case1 ............ [OK]")

        # test advance case
        # self.test_case2()
        # tdLog.debug(" LIMIT test_case2 ............ [OK]")

    # stop
    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)

    # --------------- case  -------------------

    # create tables
    def create_tables(self,dbname,stbname,count):
        tdSql.execute("use %s" %dbname)
        tdSql.execute("create stable %s(ts timestamp, c1 int, c2 binary(10)) tags(t1 int)"%stbname)
        pre_create = "create table"
        sql = pre_create
        tdLog.debug("doing create one  stable %s and %d  child table in %s  ..." %(stbname, count ,dbname))
        # print(time.time())
        exeStartTime=time.time()
        for i in range(count):
            sql += " %s_%d using %s tags(%d)"%(stbname,i,stbname,i+1)
            if i >0 and i%3000 == 0:
                tdSql.execute(sql)
                sql = pre_create
        # print(time.time())
        # end sql
        if sql != pre_create:
            tdSql.execute(sql)
        exeEndTime=time.time()
        spendTime=exeEndTime-exeStartTime
        speedCreate=count/spendTime
        tdLog.debug("spent %.2fs to create 1 stable and %d table, create speed is %.2f table/s... [OK]"% (spendTime,count,speedCreate))
        return

    def newcur(self,host,cfg):
        user = "root"
        password = "taosdata"
        port =6030
        con=taos.connect(host=host, user=user, password=password, config=cfg ,port=port)
        cur=con.cursor()
        print(cur)
        return cur

    def new_create_tables(self,dbname,vgroups,stbname,tcountStart,tcountStop):
        host = "127.0.0.1"
        buildPath = self.getBuildPath()
        config = buildPath+ "../sim/dnode1/cfg/"

        tsql=self.newcur(host,config)
        tsql.execute("drop database  if exists %s" %(dbname))
        tsql.execute("create database if not exists  %s vgroups %d"%(dbname,vgroups))
        tsql.execute("use %s" %dbname)
        tsql.execute("create stable  %s(ts timestamp, c1 int, c2 binary(10)) tags(t1 int)"%stbname)

        pre_create = "create table"
        sql = pre_create
        tcountStop=int(tcountStop)
        tcountStart=int(tcountStart)
        count=tcountStop-tcountStart

        tdLog.debug("doing create one  stable %s and %d  child table in %s  ..." %(stbname, count ,dbname))
        # print(time.time())
        exeStartTime=time.time()
        # print(type(tcountStop),type(tcountStart))
        for i in range(tcountStart,tcountStop):
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



    # insert data
    def insert_data(self, dbname, stbname, ts_start, tcountStart,tcountStop,rowCount):
        tdSql.execute("use %s" %dbname)
        pre_insert = "insert into "
        sql = pre_insert
        tcount=tcountStop-tcountStart
        allRows=tcount*rowCount
        tdLog.debug("doing insert data into stable:%s rows:%d ..."%(stbname, allRows))
        exeStartTime=time.time()
        for i in range(tcountStart,tcountStop):
            sql += " %s_%d values "%(stbname,i)
            for j in range(rowCount):
                sql += "(%d, %d, 'taos_%d') "%(ts_start + j*1000, j, j)
                if j >0 and j%5000 == 0:
                    # print(sql)
                    tdSql.execute(sql)
                    sql = "insert into %s_%d values " %(stbname,i)
        # end sql
        if sql != pre_insert:
            # print(sql)
            tdSql.execute(sql)
        exeEndTime=time.time()
        spendTime=exeEndTime-exeStartTime
        speedInsert=allRows/spendTime
        # tdLog.debug("spent %.2fs to INSERT  %d rows , insert rate is  %.2f rows/s... [OK]"% (spendTime,allRows,speedInsert))

        tdLog.debug("INSERT TABLE DATA ............ [OK]")
        return


    # test case1 base
    def test_case1(self):
        tdLog.debug("-----create database and tables test------- ")
        # tdSql.execute("drop database if exists db1")
        # tdSql.execute("drop database if exists db4")
        # tdSql.execute("drop database if exists db6")
        # tdSql.execute("drop database if exists db8")
        # tdSql.execute("drop database if exists db12")
        # tdSql.execute("drop database if exists db16")

        #create database and tables;

        # tdSql.execute("create database db11 vgroups 1")
        # # self.create_tables("db1", "stb1", 30*10000)
        # tdSql.execute("use db1")
        # tdSql.execute("create stable stb1(ts timestamp, c1 int, c2 binary(10)) tags(t1 int)")

        # tdSql.execute("create database db12 vgroups 1")
        # # self.create_tables("db1", "stb1", 30*10000)
        # tdSql.execute("use db1")

        # t1 = threading.Thread(target=self.new_create_tables("db1", "stb1", 15*10000), args=(1,))
        # t2 = threading.Thread(target=self.new_create_tables("db1", "stb1", 15*10000), args=(2,))
        # t1 = mp.Process(target=self.new_create_tables, args=("db1", "stb1", 0,count/2,))
        # t2 = mp.Process(target=self.new_create_tables, args=("db1", "stb1", count/2,count,))

        count=500000
        vgroups=1
        threads = []
        threadNumbers=2
        for i in range(threadNumbers):
            threads.append(mp.Process(target=self.new_create_tables, args=("db1%d"%i, vgroups, "stb1", 0,count,)))
        start_time = time.time()
        for tr in threads:
            tr.start()
        for tr in threads:
            tr.join()
        end_time = time.time()
        spendTime=end_time-start_time
        speedCreate=count/spendTime
        tdLog.debug("spent %.2fs to create 1 stable and %d table, create speed is %.2f table/s... [OK]"% (spendTime,count,speedCreate))
        # self.new_create_tables("db1", "stb1", 15*10000)
        # self.new_create_tables("db1", "stb1", 15*10000)

        # tdSql.execute("create database db4 vgroups 4")
        # self.create_tables("db4", "stb4", 30*10000)

        # tdSql.execute("create database db6 vgroups 6")
        # self.create_tables("db6", "stb6", 30*10000)

        # tdSql.execute("create database db8 vgroups 8")
        # self.create_tables("db8", "stb8", 30*10000)

        # tdSql.execute("create database db12 vgroups 12")
        # self.create_tables("db12", "stb12", 30*10000)

        # tdSql.execute("create database db16 vgroups 16")
        # self.create_tables("db16", "stb16", 30*10000)
        return

    # test case2 base:insert data
    def test_case2(self):

        tdLog.debug("-----insert data test------- ")
        # drop database
        tdSql.execute("drop database if exists db1")
        tdSql.execute("drop database if exists db4")
        tdSql.execute("drop database if exists db6")
        tdSql.execute("drop database if exists db8")
        tdSql.execute("drop database if exists db12")
        tdSql.execute("drop database if exists db16")

        #create database and tables;

        tdSql.execute("create database db1 vgroups 1")
        self.create_tables("db1", "stb1", 1*100)
        self.insert_data("db1", "stb1", self.ts, 1*50,1*10000)


        tdSql.execute("create database db4 vgroups 4")
        self.create_tables("db4", "stb4", 1*100)
        self.insert_data("db4", "stb4", self.ts, 1*100,1*10000)

        tdSql.execute("create database db6 vgroups 6")
        self.create_tables("db6", "stb6", 1*100)
        self.insert_data("db6", "stb6", self.ts, 1*100,1*10000)

        tdSql.execute("create database db8 vgroups 8")
        self.create_tables("db8", "stb8", 1*100)
        self.insert_data("db8", "stb8", self.ts, 1*100,1*10000)

        tdSql.execute("create database db12 vgroups 12")
        self.create_tables("db12", "stb12", 1*100)
        self.insert_data("db12", "stb12", self.ts, 1*100,1*10000)

        tdSql.execute("create database db16 vgroups 16")
        self.create_tables("db16", "stb16", 1*100)
        self.insert_data("db16", "stb16", self.ts, 1*100,1*10000)

        return

#
# add case with filename
#
# tdCases.addWindows(__file__, TDTestCase())
# tdCases.addLinux(__file__, TDTestCase())
case=TDTestCase()
case.test_case1()
