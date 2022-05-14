###################################################################
#           Copyright (c) 2016 by TAOS Technologies, Inc.
#                     All rights reserved.
#
#  This file is proprietary and confidential to TAOS Technologies.
#  No part of this file may be reproduced, db_test.stored, transmitted,
#  disclosed or used in any form or by any means other than as
#  expressly provided by the written permission from Jianhui Tao
#
###################################################################

# -*- coding: utf-8 -*-

import sys
import os
import taos
from util.log import tdLog
from util.cases import tdCases
from util.sql import tdSql
import time
import random
import datetime
class TDTestCase:

    def init(self, conn, logSql):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor(), logSql)

    # def assertCheck(self, filename, queryResult, expectResult):
    #     self.filename = filename
    #     self.queryResult = queryResult
    #     self.expectResult = expectResult
    #     args0 = (filename, queryResult, expectResult)
    #     assert queryResult == expectResult, "Queryfile:%s ,result is %s != expect: %s" % args0

    def assertfileDataExport(self, filename, expectResult):
        self.filename = filename
        self.expectResult = expectResult
        with open("%s" % filename, 'r+') as f1:
            for line in f1.readlines():
                queryResultTaosc = line.strip().split(',')[0]
                # self.assertCheck(filename, queryResultTaosc, expectResult)

    def run(self):
        starttime = 1537146000000
        tdSql.prepare()
        tdSql.execute("drop database if exists db_json;")
        print("==============step1 tag format =======")
        tdLog.info("create database db_jsonB  ")
        tdSql.execute("create database db_jsonB")
        tdSql.execute("use db_jsonB")    
        # test Benchmark 
        tdSql.execute("create table if not exists  jsons1(ts timestamp,dataFloat float) tags(jtag json)")
        for numTables in range(500):
            json = "{\"loc1%d\":\"beijingandshanghaiandchangzhouandshijiazhuanganda%d\",\"loc2%d\":\"beijingandshanghaiandchangzhouandshijiazhuangandb%d\" ,\"loc3%d\":\"beijingandshanghaiandchangzhouandshijiazhuangandc%d\",\
            \"loc4%d\":\"beijingandshanghaiandchangzhouandshijiazhuangandd%d\",\"loc5%d\":\"beijingandshanghaiandchangzhouandshijiazhuangande%d\",\"loc6%d\":\"beijingandshanghaiandchangzhouandshijiazhuangandf%d\",\
            \"loc7%d\":\"beijingandshanghaiandchangzhouandshijiazhuangandg%d\"}"% (numTables,numTables,numTables,numTables,numTables,numTables,numTables,numTables,numTables,numTables,numTables,numTables,numTables,numTables)
            print(json)
            createTableSqls = "create table if not exists  jsons1_%d using  jsons1 tags('%s')" %(numTables,json)
            print(createTableSqls)
            tdLog.info(createTableSqls) 
            tdSql.execute(createTableSqls) 
            for numRecords in range(1,101):
                dataFloatSql=numRecords*10+numRecords*0.01
                insertDataSqls = "insert into  jsons1_%d values(%d+%ds, %d) " %(numTables,starttime,numRecords,dataFloatSql)
                tdLog.info(insertDataSqls)
                tdSql.execute(insertDataSqls)
        tdSql.execute("use db_jsonB")    
        now_time1 = datetime.datetime.now()
        tdSql.query("select * from  jsons1 where ts>1537145900000 and ts<1537156000000;")
        spendTimes1 = datetime.datetime.now() - now_time1
        print(spendTimes1)
        now_time2 = datetime.datetime.now()
        tdSql.query("select * from  jsons1 where ts>1537156000000;")
        spendTimes2 = datetime.datetime.now() - now_time2
        print(spendTimes2)

        tdSql.execute("drop database db_jsonB")
        
        testcaseFilename = os.path.split(__file__)[-1]
        os.system("rm -rf ./insert_res.txt")
        os.system("rm -rf tools/taosdemoAllTest/%s.sql" % testcaseFilename )     


    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
