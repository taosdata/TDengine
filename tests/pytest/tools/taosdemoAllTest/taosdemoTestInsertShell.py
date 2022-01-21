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
        global cfgPath
        if ("community" in selfPath):
            projPath = selfPath[:selfPath.find("community")]
            cfgPath = projPath + "/community/sim/dnode1/cfg"
            
        else:
            projPath = selfPath[:selfPath.find("tests")]
            cfgPath = projPath + "/sim/dnode1/cfg"

        for root, dirs, files in os.walk(projPath):
            if ("taosd" in files):
                rootRealPath = os.path.dirname(os.path.realpath(root))
                if ("packaging" not in rootRealPath):
                    buildPath = root[:len(root)-len("/build/bin")]
                    break
        return buildPath

    # def checkGerData():

    def run(self):
        buildPath = self.getBuildPath()
        print("%s" % cfgPath )
        if (buildPath == ""):
            tdLog.exit("taosd not found!")
        else:
            tdLog.info("taosd found in %s" % buildPath)
        binPath = buildPath+ "/build/bin/"

        tdLog.info("create super table")
        # create super table 
        os.system("%staosBenchmark -u root  -c %s -h localhost -P 6030 -d db1  -a 1 -l 10 -b float,int,NCHAR\(15\) -w 4096 \
        -T 8 -i 10 -S 1000 -r 1000000 -t 10 -n 100 -M  -x -y -O 10 -R 100  -E  -m test.  " % (binPath,cfgPath))
        tdSql.execute("use db1")
        tdSql.query("describe meters;")
        tdSql.checkRows(13)
        tdSql.query("select count(*) from meters")
        tdSql.checkData(0, 0, 1000)
        tdSql.query("select count(tbname) from meters")
        tdSql.checkData(0, 0, 10)
        tdSql.query("select count(*) from `test.0`")
        tdSql.checkData(0, 0, 100)

  
        tdLog.info("create general table -N ")
        tdSql.execute("drop database db1;")
        # create general table -N 
        os.system("%staosBenchmark -u root  -c %s -h localhost -P 6030 -d db1  -a 1 -l 10 -b float,int,NCHAR\(15\) -w 4096 \
        -T 8 -i 10 -S 1000 -r 1000000 -t 10 -n 100 -M  -x -y -O 10 -R 100  -E  -m test. -N " % (binPath,cfgPath))
        tdSql.execute("use db1")
        tdSql.query("describe `test.0`;")
        tdSql.checkRows(11)
        tdSql.error("select count(*) from meters")
        tdSql.error("select count(tbname) from meters")
        tdSql.query("select count(*) from `test.0`")
        tdSql.checkData(0, 0, 100)

        tdLog.info("use diffrent interface stmt")
        tdSql.execute("drop database db1;")
        # use diffrent interface-stmt
        os.system("%staosBenchmark -u root  -c %s -h localhost -P 6030 -d db1  -a 1 -l 10 -b float,int,BINARY\(4000\) -w 40 \
        -T 8 -i 10 -S 1000 -r 1000000 -t 10 -n 100 -M  -x -y -O 10 -R 100  -E  -m test. -I stmt " % (binPath,cfgPath))
        tdSql.execute("use db1")
        tdSql.query("select count(*) from meters")
        tdSql.checkData(0, 0, 1000)
        tdSql.query("select count(tbname) from meters")
        tdSql.checkData(0, 0, 10)
        tdSql.query("select count(*) from `test.0`")
        tdSql.checkData(0, 0, 100)

        tdLog.info("use diffrent interface rest")
        tdSql.execute("drop database db1;")
        # use diffrent interface -rest
        os.system("%staosBenchmark -u root  -c %s -h localhost -P 6030 -d db1  -a 1 -l 10 -b float,int,NCHAR\(15\) -w 4097 \
        -T 8 -i 10 -S 1000 -r 1000000 -t 10 -n 100 -M  -x -y -O 10 -R 100  -E  -m test. -I rest " % (binPath,cfgPath))
        tdSql.execute("use db1")
        tdSql.query("select count(*) from meters")
        tdSql.checkData(0, 0, 1000)
        tdSql.query("select count(tbname) from meters")
        tdSql.checkData(0, 0, 10)
        tdSql.query("select count(*) from `test.0`")
        tdSql.checkData(0, 0, 100)

        tdLog.info("use diffrent interface sml")
        tdSql.execute("drop database db1;")
        # use diffrent interface-sml
        os.system("%staosBenchmark -u root  -c %s -h localhost -P 6030 -d db1  -a 1 -l 10 -b float,int,NCHAR\(15\) -w 1024 \
        -T 8 -i 10 -S 1000 -r 1000000 -t 10 -n 100 -M  -x -y -O 10 -R 100  -E  -m test. -I sml " % (binPath,cfgPath))
        tdSql.execute("use db1")
        tdSql.query("select count(*) from meters")
        tdSql.checkData(0, 0, 1000)
        tdSql.query("select count(tbname) from meters")
        tdSql.checkData(0, 0, 10)

        tdLog.info("all data type")
        tdSql.execute("drop database db1;")
        # all data type-taosc
        os.system("%staosBenchmark -u root  -c %s -h localhost -P 6030 -d db1  -a 1 -l 10 \
        -b INT,TIMESTAMP,BIGINT,FLOAT,DOUBLE,SMALLINT,TINYINT,BOOL,UINT,UBIGINT,UTINYINT,USMALLINT,BINARY\(15\),NCHAR\(15\) -w 4096 \
        -T 8 -i 10 -S 1000 -r 1000000 -t 10 -n 100 -M  -x -y -O 10 -R 100  -E  -m test. " % (binPath,cfgPath))
        tdSql.execute("use db1")
        tdSql.query("select count(*) from meters")
        tdSql.checkData(0, 0, 1000)
        tdSql.query("select count(tbname) from meters")
        tdSql.checkData(0, 0, 10)
        tdSql.query("select count(*) from `test.0`")
        tdSql.checkData(0, 0, 100)
        tdLog.info("all data type")
        tdSql.execute("drop database db1;")
        # all data type-stmt
        os.system("%staosBenchmark -u root  -c %s -h localhost -P 6030 -d db1  -a 1 -l 10 \
        -b INT,TIMESTAMP,BIGINT,FLOAT,DOUBLE,SMALLINT,TINYINT,BOOL,UINT,UBIGINT,UTINYINT,USMALLINT,BINARY\(15\),NCHAR\(15\) -w 4096 \
        -T 8 -i 10 -S 1000 -r 1000000 -t 10 -n 100 -M  -x -y -O 10 -R 100  -E  -m test. -I stmt " % (binPath,cfgPath))
        tdSql.execute("use db1")
        tdSql.query("select count(*) from meters")
        tdSql.checkData(0, 0, 1000)
        tdSql.query("select count(tbname) from meters")
        tdSql.checkData(0, 0, 10)
        tdSql.query("select count(*) from `test.0`")
        tdSql.checkData(0, 0, 100)

        # all data type-rest
        os.system("%staosBenchmark -u root  -c %s -h localhost -P 6030 -d db1  -a 1 -l 10 \
        -b INT,TIMESTAMP,BIGINT,FLOAT,DOUBLE,SMALLINT,TINYINT,BOOL,UINT,UBIGINT,UTINYINT,USMALLINT,BINARY\(15\),NCHAR\(15\) -w 4096 \
        -T 8 -i 10 -S 1000 -r 1000000 -t 10 -n 100 -M  -x -y -O 10 -R 100  -E  -m test. -I rest " % (binPath,cfgPath))
        tdSql.execute("use db1")
        tdSql.query("select count(*) from meters")
        tdSql.checkData(0, 0, 1000)
        tdSql.query("select count(tbname) from meters")
        tdSql.checkData(0, 0, 10)
        tdSql.query("select count(*) from `test.0`")
        tdSql.checkData(0, 0, 100)

        # # all data type-rest
        # os.system("%staosBenchmark -u root  -c %s -h localhost -P 6030 -d db1  -a 1 -l 10 \
        # -b INT,BIGINT,FLOAT,DOUBLE,SMALLINT,TINYINT,BOOL,UINT,UBIGINT,UTINYINT,USMALLINT,BINARY\(15\),NCHAR\(15\) -w 4096 \
        # -T 8 -i 10 -S 1000 -r 1000000 -t 10 -n 100 -M  -x -y -O 10 -R 100  -E  -m test. -I sml " % (binPath,cfgPath))
        # tdSql.execute("use db1")
        # tdSql.query("select count(*) from meters")
        # tdSql.checkData(0, 0, 1000)
        # tdSql.query("select count(tbname) from meters")
        # tdSql.checkData(0, 0, 10)
        # # tdSql.query("select count(*) from `test.0`")
        # # tdSql.checkData(0, 0, 100)

        tdLog.info("all data type and interlace rows")
        tdSql.execute("drop database db1;")
        # all data type 
        os.system("%staosBenchmark -u root  -c %s -h localhost -P 6030 -d db3  -a 1 -l 10\
        -b INT,TIMESTAMP,BIGINT,FLOAT,DOUBLE,SMALLINT,TINYINT,BOOL,UINT,UBIGINT,UTINYINT,USMALLINT,BINARY\(15\),NCHAR\(15\) -w 4096\
        -T 8 -i 10 -S 1000 -r 1000000 -t 10 -n 100 -B 1000  -M  -x -y -O 10 -R 100  -E  -m test.  " % (binPath,cfgPath))
        tdSql.execute("use db3")
        tdSql.query("select count(*) from meters")
        tdSql.checkData(0, 0, 1000)
        tdSql.query("select count(tbname) from meters")
        tdSql.checkData(0, 0, 10)
        tdSql.query("select count(*) from `test.0`")
        tdSql.checkData(0, 0, 100)

        tdLog.info("all data type and too much para")
        tdLog.info("%staosBenchmark -u root  -c %s -h localhost -P 6030 -d db1  -a 1 -l 10 -b float,int,NCHAR\(15\) -w 4096 \
        -T 8 -i 10 -S 1000 -r 1000000 -t 10 -n 100 -M  -x -y -O 10 -R 100  -E  -m test.taosdemo -u root  -c %s -h \
        localhost -P 6030 -d db1  -a 1 -l 100 -b float,int,NCHAR\(15\) -w 4096   -T 8 -i 10 -S 1000 -r 1000000 -t 10 -n 100 -M  -x -y -O 10 -R 100  -E  -m test. " % (binPath,cfgPath,cfgPath))
        tdSql.execute("drop database db3;")
        # repeate parameters 
        os.system("%staosBenchmark -u root  -c %s -h localhost -P 6030 -d db1  -a 1 -l 10 -b float,int,NCHAR\(15\) -w 4096 \
        -T 8 -i 10 -S 1000 -r 1000000 -t 10 -n 100 -M  -x -y -O 10 -R 100  -E  -m test.taosdemo -u root  -c %s -h \
        localhost -P 6030 -d db1  -a 1 -l 100 -b float,int,NCHAR\(15\) -w 4096   -T 8 -i 10 -S 1000 -r 1000000 -t 10 -n 100 -M  -x -y -O 10 -R 100  -E  -m test. " % (binPath,cfgPath,cfgPath))
        tdSql.execute("use db1")
        tdSql.query("select count(*) from meters")
        tdSql.checkData(0, 0, 1000)
        tdSql.query("select count(tbname) from meters")
        tdSql.checkData(0, 0, 10)
        tdSql.query("select count(*) from `test.0`")
        tdSql.checkData(0, 0, 100)

        # taosdemo error
        # too max length
        sql = "%staosBenchmark -u root -c %s -h localhost -P 6030 -d db1 -a 1 -l 10 -b float,int,NCHAR\(4096\) \
                -w 40 -T 8 -i 10 -S 1000 -r 1000000 -t 10 -n 100 -M -x -y -O 10 -R 100 -E -m test. -I taosc" % (binPath,cfgPath)
        tdLog.info("%s" % sql ) 
        assert os.system("%s" % sql ) != 0   
                 
        # error password  
        sql = "%staosBenchmark -u root -c %s -h localhost -P 6030 -p123 -d db1 -a 1 -l 10 -b float,int,NCHAR\(40\) \
        -w 40 -T 8 -i 10 -S 1000 -r 1000000 -t 10 -n 100 -M -x -y -O 10 -R 100 -E -m test. -I stmt" % (binPath,cfgPath)
        tdLog.info("%s" % sql ) 
        assert os.system("%s" % sql ) != 0       

        testcaseFilename = os.path.split(__file__)[-1]
        os.system("rm -rf ./insert_res*.txt*")
        os.system("rm -rf tools/taosdemoAllTest/%s.sql" % testcaseFilename )         
        

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
