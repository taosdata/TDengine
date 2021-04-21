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
        os.system("%staosdemo -f tools/taosdemoAllTest/insert-1s1tnt1r.json -y " % binPath)
        tdSql.execute("use db")
        tdSql.query("select count (tbname) from stb0")
        tdSql.checkData(0, 0, 1000)
        tdSql.query("select count (tbname) from stb1")
        tdSql.checkData(0, 0, 1000)
        tdSql.query("select count(*) from stb00_0")
        tdSql.checkData(0, 0, 100)
        tdSql.query("select count(*) from stb0")
        tdSql.checkData(0, 0, 100000)
        tdSql.query("select count(*) from stb01_1")
        tdSql.checkData(0, 0, 200)
        tdSql.query("select count(*) from stb1")
        tdSql.checkData(0, 0, 200000)        


        # insert: create  mutiple tables per sql and insert one rows per sql . 
        os.system("%staosdemo -f tools/taosdemoAllTest/insert-1s1tntmr.json -y " % binPath)
        tdSql.execute("use db")
        tdSql.query("select count (tbname) from stb0")
        tdSql.checkData(0, 0, 10)
        tdSql.query("select count (tbname) from stb1")
        tdSql.checkData(0, 0, 20)
        tdSql.query("select count(*) from stb00_0")
        tdSql.checkData(0, 0, 10000)   
        tdSql.query("select count(*) from stb0")
        tdSql.checkData(0, 0, 100000) 
        tdSql.query("select count(*) from stb01_0")
        tdSql.checkData(0, 0, 20000)   
        tdSql.query("select count(*) from stb1")
        tdSql.checkData(0, 0, 400000) 

        # insert: using parament "insert_interval to controls spped  of insert. 
        # but We need to have accurate methods to control the speed, such as getting the speed value, checking the count and so on。
        os.system("%staosdemo -f tools/taosdemoAllTest/insert-interval-speed.json -y" % binPath)
        tdSql.execute("use db")
        tdSql.query("show stables")
        tdSql.checkData(0, 4, 100)
        tdSql.query("select count(*) from stb00_0")
        tdSql.checkData(0, 0, 20000)   
        tdSql.query("select count(*) from stb0")
        tdSql.checkData(0, 0, 2000000) 
        tdSql.query("show stables")
        tdSql.checkData(1, 4, 100)
        tdSql.query("select count(*) from stb01_0")
        tdSql.checkData(0, 0, 20000)   
        tdSql.query("select count(*) from stb1")
        tdSql.checkData(0, 0, 2000000)           
        
        # spend 2min30s for 3 testcases.
        # insert: drop and child_table_exists combination test
        # insert: using parament "childtable_offset and childtable_limit" to control  table'offset point and offset 
        os.system("%staosdemo -f tools/taosdemoAllTest/insert-nodbnodrop.json -y" % binPath)
        tdSql.error("show dbno.stables")
        os.system("%staosdemo -f tools/taosdemoAllTest/insert-newdb.json -y" % binPath)
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
        os.system("%staosdemo -f tools/taosdemoAllTest/insert-offset.json -y" % binPath)
        tdSql.execute("use db")             
        tdSql.query("select count(*) from stb0")
        tdSql.checkData(0, 0, 50) 
        tdSql.query("select count(*) from stb1")
        tdSql.checkData(0, 0, 240) 
        tdSql.query("select count(*) from stb2")
        tdSql.checkData(0, 0, 220) 
        tdSql.query("select count(*) from stb3")
        tdSql.checkData(0, 0, 180)
        tdSql.query("select count(*) from stb4")
        tdSql.checkData(0, 0, 160)
        os.system("%staosdemo -f tools/taosdemoAllTest/insert-newtable.json -y" % binPath)
        tdSql.execute("use db")             
        tdSql.query("select count(*) from stb0")
        tdSql.checkData(0, 0, 150) 
        tdSql.query("select count(*) from stb1")
        tdSql.checkData(0, 0, 360) 
        tdSql.query("select count(*) from stb2")
        tdSql.checkData(0, 0, 360) 
        tdSql.query("select count(*) from stb3")
        tdSql.checkData(0, 0, 340)
        tdSql.query("select count(*) from stb4")
        tdSql.checkData(0, 0, 400)
        os.system("%staosdemo -f tools/taosdemoAllTest/insert-renewdb.json -y" % binPath)
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


        # insert:  let parament in json file  is illegal ，i need know how to write  exception.
        tdSql.execute("drop database if exists db") 
        os.system("%staosdemo -f tools/taosdemoAllTest/insert-illegal-columns.json -y " % binPath)
        tdSql.error("use db")
        os.system("%staosdemo -f tools/taosdemoAllTest/insert-illegal-columns-lmax.json -y " % binPath)
        tdSql.error("select * from db.stb0")
        os.system("%staosdemo -f tools/taosdemoAllTest/insert-illegal-columns-count-0.json -y " % binPath)
        tdSql.execute("use db") 
        tdSql.query("select count(*) from db.stb0")
        tdSql.checkData(0, 0, 10000)
        tdSql.execute("drop database if exists db") 
        os.system("%staosdemo -f tools/taosdemoAllTest/insert-illegal-tags-count129.json -y " % binPath)   
        tdSql.error("use db1") 

        # insert: timestamp and step 
        os.system("%staosdemo -f tools/taosdemoAllTest/insert-timestep.json -y " % binPath)
        tdSql.execute("use db")
        tdSql.query("show stables")
        tdSql.query("select count (tbname) from stb0")
        tdSql.checkData(0, 0, 10)
        tdSql.query("select count (tbname) from stb1")
        tdSql.checkData(0, 0, 20)
        tdSql.query("select last(ts) from db.stb00_0")
        tdSql.checkData(0, 0, "2020-10-01 00:00:00.019000")   
        tdSql.query("select count(*) from stb0")
        tdSql.checkData(0, 0, 200) 
        tdSql.query("select last(ts) from db.stb01_0")
        tdSql.checkData(0, 0, "2020-11-01 00:00:00.190000")   
        tdSql.query("select count(*) from stb1")
        tdSql.checkData(0, 0, 400) 

        # # insert:  disorder_ratio
        os.system("%staosdemo -f tools/taosdemoAllTest/insert-disorder.json -g 2>&1  -y " % binPath)
        tdSql.execute("use db")
        tdSql.query("select count (tbname) from stb0")
        tdSql.checkData(0, 0, 1)
        tdSql.query("select count (tbname) from stb1")
        tdSql.checkData(0, 0, 1)
        tdSql.query("select count(*) from stb0")
        tdSql.checkData(0, 0, 10) 
        tdSql.query("select count(*) from stb1")
        tdSql.checkData(0, 0, 10) 

        # insert:  sample json
        os.system("%staosdemo -f tools/taosdemoAllTest/insert-sample.json -y " % binPath)
        tdSql.execute("use dbtest123")
        tdSql.query("select col2 from stb0")
        tdSql.checkData(0, 0, 2147483647)
        tdSql.query("select t1 from stb1")
        tdSql.checkData(0, 0, -127)
        tdSql.query("select t2 from stb1")
        tdSql.checkData(1, 0, 126)

        # insert: test interlace parament 
        os.system("%staosdemo -f tools/taosdemoAllTest/insert-interlace-row.json -y " % binPath)
        tdSql.execute("use db")
        tdSql.query("select count (tbname) from stb0")
        tdSql.checkData(0, 0, 100)
        tdSql.query("select count (*) from stb0")
        tdSql.checkData(0, 0, 15000)        


        os.system("rm -rf ./insert_res.txt")
        os.system("rm -rf tools/taosdemoAllTest/taosdemoTestInsertWithJson.py.sql")        
        
        
        
    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
