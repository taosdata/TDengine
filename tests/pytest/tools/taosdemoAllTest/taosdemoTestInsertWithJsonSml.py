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
from util.log import *
from util.cases import *
from util.sql import *
from util.dnodes import *


class TDTestCase:
    def init(self, conn, logSql):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor(), logSql)
        
    def getBuildPath(self):
        selfPath = os.path.dirname(os.path.realpath(__file__))

        if ("community" in selfPath):
            projPath = selfPath[:selfPath.find("community")]
        else:
            projPath = selfPath[:selfPath.find("tests")]

        for root, dirs, files in os.walk(projPath):
            if ("taosd" in files):
                rootRealPath = os.path.dirname(os.path.realpath(root))
                if ("packaging" not in rootRealPath):
                    buildPath = root[:len(root)-len("/build/bin")]
                    break
        return buildPath
        
    def run(self):
        buildPath = self.getBuildPath()
        if (buildPath == ""):
            tdLog.exit("taosd not found!")
        else:
            tdLog.info("taosd found in %s" % buildPath)
        binPath = buildPath+ "/build/bin/"

        # insert: create one  or mutiple tables per sql and insert multiple rows per sql 
        # line_protocol——telnet and json
        os.system("%staosBenchmark -f tools/taosdemoAllTest/sml/insert-1s1tnt1r-sml.json -y " % binPath)
        tdSql.execute("use db")
        tdSql.query("select count (tbname) from stb0")
        tdSql.checkData(0, 0, 10)
        tdSql.query("select count (tbname) from stb1")
        tdSql.checkData(0, 0, 20)
        tdSql.query("select count(*) from stb0")
        tdSql.checkData(0, 0, 1000)
        tdSql.query("select count(*) from stb1")
        tdSql.checkData(0, 0, 4000)        


        # insert: create  mutiple tables per sql and insert one rows per sql . 
        os.system("%staosBenchmark -f tools/taosdemoAllTest/sml/insert-1s1tntmr-sml.json -y " % binPath)
        tdSql.execute("use db")
        tdSql.query("select count (tbname) from stb0")
        tdSql.checkData(0, 0, 10)
        tdSql.query("select count (tbname) from stb1")
        tdSql.checkData(0, 0, 15)
        tdSql.query("select count(*) from stb0")
        tdSql.checkData(0, 0, 1500) 
        tdSql.query("select count(*) from stb1")
        tdSql.checkData(0, 0, 3000) 

        # insert: using parament "insert_interval to controls spped  of insert. 
        # but We need to have accurate methods to control the speed, such as getting the speed value, checking the count and so on。
        os.system("%staosBenchmark -f tools/taosdemoAllTest/sml/insert-interval-speed-sml.json -y" % binPath)
        tdSql.execute("use db")
        # tdSql.query("select count (tbname) from stb0")
        tdSql.query("select tbname from  db.stb0")
        tdSql.checkRows(100 )
        # tdSql.query("select count(*) from stb00_0")
        # tdSql.checkData(0, 0, 20)   
        tdSql.query("select count(*) from stb0")
        tdSql.checkData(0, 0, 2000) 
        tdSql.query("show stables")
        tdSql.checkData(1, 4, 20)
        # tdSql.query("select count(*) from stb01_0")
        # tdSql.checkData(0, 0, 35)   
        tdSql.query("select count(*) from stb1")
        tdSql.checkData(0, 0, 700)           
        
        # spend 2min30s for 3 testcases.
        # insert: drop and child_table_exists combination test 
        # insert: sml can't support parament "childtable_offset and childtable_limit" \ drop=no or child_table_exists = yes

        # os.system("%staosBenchmark -f tools/taosdemoAllTest/sml/insert-nodbnodrop-sml.json -y" % binPath)
        # tdSql.error("show dbno.stables")
        os.system("%staosBenchmark -f tools/taosdemoAllTest/sml/insert-newdb-sml.json -y" % binPath)
        tdSql.execute("use db")
        tdSql.query("select count (tbname) from stb0")
        tdSql.checkData(0, 0, 5)
        tdSql.query("select count (tbname) from stb1")
        tdSql.checkData(0, 0, 6)
        tdSql.query("select count (tbname) from stb2")
        tdSql.checkData(0, 0, 7)
        tdSql.query("select count (tbname) from stb3")
        tdSql.checkData(0, 0, 8)        
        tdSql.query("select count (tbname) from stb4")
        tdSql.checkData(0, 0, 8)  
        os.system("%staosBenchmark -f tools/taosdemoAllTest/sml/insert-renewdb-sml.json -y" % binPath)
        tdSql.execute("use db")             
        tdSql.query("select count(*) from stb0")
        tdSql.checkData(0, 0, 50) 
        tdSql.query("select count(*) from stb1")
        tdSql.checkData(0, 0, 120) 
        tdSql.query("select count(*) from stb2")
        tdSql.checkData(0, 0, 140) 
        tdSql.query("select count(*) from stb3")
        tdSql.checkData(0, 0, 160)
        tdSql.query("select count(*) from stb4")
        tdSql.checkData(0, 0, 160)


        # insert:  let parament in json file  is illegal, it'll expect error.
        tdSql.execute("drop database if exists db") 
        os.system("%staosBenchmark -f tools/taosdemoAllTest/sml/insertColumnsAndTagNumLarge4096-sml.json -y " % binPath)
        tdSql.error("use db")
        tdSql.execute("drop database if exists db") 
        os.system("%staosBenchmark -f tools/taosdemoAllTest/sml/insertSigcolumnsNum4096-sml.json -y " % binPath)
        tdSql.error("select * from db.stb0")
        # tdSql.execute("drop database if exists db") 
        # os.system("%staosBenchmark -f tools/taosdemoAllTest/sml/insertColumnsAndTagNum4096-sml.json -y " % binPath)
        # tdSql.query("select count(*) from db.stb0")
        # tdSql.checkData(0, 0, 10000) 

        # there is no limit of 4096 columns,so cancels this case
        # tdSql.execute("drop database if exists db")
        # os.system("%staosBenchmark -f tools/taosdemoAllTest/sml/insertInterlaceRowsLarge1M-sml.json -y " % binPath)
        # tdSql.query("select count(*) from db.stb0")
        # tdSql.checkRows(0)

        tdSql.execute("drop database if exists db") 
        os.system("%staosBenchmark -f tools/taosdemoAllTest/sml/insertColumnsNum0-sml.json -y " % binPath)
        tdSql.execute("use db") 
        tdSql.query("show stables like 'stb0%' ")
        tdSql.checkData(0, 2, 11)
        tdSql.execute("drop database if exists db") 
        os.system("%staosBenchmark -f tools/taosdemoAllTest/sml/insertTagsNumLarge128-sml.json -y " % binPath)   
        tdSql.error("use db1") 
        tdSql.execute("drop database if exists db") 
        os.system("%staosBenchmark -f tools/taosdemoAllTest/sml/insertBinaryLenLarge16374AllcolLar49151-sml.json -y " % binPath)   
        tdSql.query("select count(*) from db.stb0") 
        tdSql.checkRows(1)
        tdSql.query("select count(*) from db.stb1") 
        tdSql.checkRows(1)
        tdSql.query("select count(*) from db.stb3") 
        tdSql.checkRows(1)
        tdSql.execute("drop database if exists db") 
        os.system("%staosBenchmark -f tools/taosdemoAllTest/sml/insertBinaryLenLarge16374AllcolLar49151-error-sml.json -y " % binPath)   
        tdSql.error("select * from db.stb4")
        tdSql.error("select * from db.stb2")
        tdSql.execute("drop database if exists db") 
        os.system("%staosBenchmark -f tools/taosdemoAllTest/sml/insertNumOfrecordPerReq0-sml.json -y " % binPath)   
        tdSql.error("select count(*) from db.stb0") 
        tdSql.execute("drop database if exists db") 
        os.system("%staosBenchmark -f tools/taosdemoAllTest/sml/insertNumOfrecordPerReqless0-sml.json -y " % binPath)   
        tdSql.error("use db") 
        tdSql.execute("drop database if exists db") 
        os.system("%staosBenchmark -f tools/taosdemoAllTest/sml/insertChildTab0-sml.json -y " % binPath)   
        tdSql.error("use db") 
        tdSql.execute("drop database if exists db") 
        os.system("%staosBenchmark -f tools/taosdemoAllTest/sml/insertChildTabLess0-sml.json -y " % binPath)   
        tdSql.error("use db") 
        tdSql.execute("drop database if exists blf") 

        # child table name is invalid reading,so 
        # os.system("%staosBenchmark -f tools/taosdemoAllTest/sml/insertTimestepMulRowsLargeint16-sml.json -y " % binPath)   
        # tdSql.execute("use blf") 
        # tdSql.query("select ts from blf.p_0_topics_7 limit 262800,1") 
        # tdSql.checkData(0, 0, "2020-03-31 12:00:00.000")
        # tdSql.query("select first(ts) from blf.p_0_topics_2")
        # tdSql.checkData(0, 0, "2019-10-01 00:00:00")
        # tdSql.query("select last(ts) from blf.p_0_topics_6 ")        
        # tdSql.checkData(0, 0, "2020-09-29 23:59:00")

        # it will be commented in ci because it spend too much time to insert data, but when you can excute it when you want to test this case.
        # os.system("%staosBenchmark -f tools/taosdemoAllTest/sml/insertMaxNumPerReq-sml.json -y " % binPath)
        # tdSql.execute("use db")
        # tdSql.query("select count(*) from stb0")
        # tdSql.checkData(0, 0, 5000000)
        # tdSql.query("select count(*) from stb1")
        # tdSql.checkData(0, 0, 5000000)
        # os.system("%staosBenchmark -f tools/taosdemoAllTest/sml/insertMaxNumPerReq-sml-telnet.json -y " % binPath)
        # tdSql.execute("use db")
        # tdSql.query("select count(*) from stb0")
        # tdSql.checkData(0, 0, 5000000)
        # tdSql.query("select count(*) from stb1")
        # tdSql.checkData(0, 0, 5000000)



        # insert: timestamp and step 
        os.system("%staosBenchmark -f tools/taosdemoAllTest/sml/insert-timestep-sml.json -y " % binPath)
        tdSql.execute("use db")
        tdSql.query("select count (tbname) from stb0")
        tdSql.checkData(0, 0, 10)
        tdSql.query("select count (tbname) from stb1")
        tdSql.checkData(0, 0, 20)
        # tdSql.query("select last(ts) from db.stb00_0")
        # tdSql.checkData(0, 0, "2020-10-01 00:00:00.019000")   
        tdSql.query("select count(*) from stb0")
        tdSql.checkData(0, 0, 200) 
        # tdSql.query("select last(ts) from db.stb01_0")
        # tdSql.checkData(0, 0, "2020-11-01 00:00:00.190000")   
        tdSql.query("select count(*) from stb1")
        tdSql.checkData(0, 0, 400) 

        # # insert:  disorder_ratio
        os.system("%staosBenchmark -f tools/taosdemoAllTest/sml/insert-disorder-sml.json 2>&1  -y " % binPath)
        tdSql.execute("use db")
        tdSql.query("select count (tbname) from stb0")
        tdSql.checkData(0, 0, 1)
        tdSql.query("select count (tbname) from stb1")
        tdSql.checkData(0, 0, 1)
        tdSql.query("select count(*) from stb0")
        tdSql.checkData(0, 0, 10) 
        tdSql.query("select count(*) from stb1")
        tdSql.checkData(0, 0, 10) 

        # insert:  doesn‘t currently supported sample json
        assert os.system("%staosBenchmark -f tools/taosdemoAllTest/sml/insert-sample-sml.json -y " % binPath) != 0 
        # tdSql.execute("use dbtest123")
        # tdSql.query("select c2 from stb0")
        # tdSql.checkData(0, 0, 2147483647)
        # tdSql.query("select * from stb1 where t1=-127")
        # tdSql.checkRows(20)
        # tdSql.query("select * from stb1 where t2=127")
        # tdSql.checkRows(10)
        # tdSql.query("select * from stb1 where t2=126")
        # tdSql.checkRows(10)

        # insert: test interlace parament 
        os.system("%staosBenchmark -f tools/taosdemoAllTest/sml/insert-interlace-row-sml.json -y " % binPath)
        tdSql.execute("use db")
        tdSql.query("select count (tbname) from stb0")
        tdSql.checkData(0, 0, 100)
        tdSql.query("select count (*) from stb0")
        tdSql.checkData(0, 0, 15000)        


        testcaseFilename = os.path.split(__file__)[-1]
        os.system("rm -rf ./insert_res.txt")
        os.system("rm -rf tools/taosdemoAllTest/%s.sql" % testcaseFilename )     
        
        
        
    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
