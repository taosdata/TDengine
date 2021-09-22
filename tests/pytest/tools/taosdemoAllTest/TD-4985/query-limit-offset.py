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
import time
from util.log import *
from util.cases import *
from util.sql import *
from util.dnodes import *


class TDTestCase:
    def init(self, conn, logSql):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor(), logSql)

        now = time.time()

        print(int(round(now * 1000)))

        self.ts = int(round(now * 1000))

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
        # test case for https://jira.taosdata.com:18080/browse/TD-4985
        os.system("rm -rf tools/taosdemoAllTest/TD-4985/query-limit-offset.py.sql")
        os.system("%staosdemo -f tools/taosdemoAllTest/TD-4985/query-limit-offset.json -y " % binPath)
        tdSql.execute("use db")
        tdSql.query("select count (tbname) from stb0")
        tdSql.checkData(0, 0, 10000)

        for i in range(1000):
            tdSql.execute('''insert into stb00_9999 values(%d, %d, %d,'test99.%s')'''
                            % (self.ts + i, i, -10000+i, i))
            tdSql.execute('''insert into stb00_8888 values(%d, %d, %d,'test98.%s')'''
                            % (self.ts + i, i, -10000+i, i))
            tdSql.execute('''insert into stb00_7777 values(%d, %d, %d,'test97.%s')'''
                            % (self.ts + i, i, -10000+i, i))
            tdSql.execute('''insert into stb00_6666 values(%d, %d, %d,'test96.%s')'''
                            % (self.ts + i, i, -10000+i, i))
            tdSql.execute('''insert into stb00_5555 values(%d, %d, %d,'test95.%s')'''
                            % (self.ts + i, i, -10000+i, i))
            tdSql.execute('''insert into stb00_4444 values(%d, %d, %d,'test94.%s')'''
                            % (self.ts + i, i, -10000+i, i))
            tdSql.execute('''insert into stb00_3333 values(%d, %d, %d,'test93.%s')'''
                            % (self.ts + i, i, -10000+i, i))
            tdSql.execute('''insert into stb00_2222 values(%d, %d, %d,'test92.%s')'''
                            % (self.ts + i, i, -10000+i, i))
            tdSql.execute('''insert into stb00_1111 values(%d, %d, %d,'test91.%s')'''
                            % (self.ts + i, i, -10000+i, i))
            tdSql.execute('''insert into stb00_100 values(%d, %d, %d,'test90.%s')'''
                            % (self.ts + i, i, -10000+i, i))
        tdSql.query("select * from stb0 where c2 like 'test99%' ")
        tdSql.checkRows(1000)

        tdSql.query("select * from stb0 where  tbname like 'stb00_9999'  limit 10" )
        tdSql.checkData(0, 1, 0)
        tdSql.checkData(1, 1, 1)
        tdSql.checkData(2, 1, 2)
        tdSql.query("select * from stb0 where  tbname like 'stb00_9999'  limit 10 offset 5" )
        tdSql.checkData(0, 1, 5)
        tdSql.checkData(1, 1, 6)
        tdSql.checkData(2, 1, 7)
        tdSql.query("select *  from stb0 where c2 like 'test98%' ")
        tdSql.checkRows(1000)
        tdSql.query("select  *  from stb0 where tbname like 'stb00_8888' limit 10" )
        tdSql.checkData(0, 1, 0)
        tdSql.checkData(1, 1, 1)
        tdSql.checkData(2, 1, 2)
        tdSql.query("select *  from stb0 where tbname like 'stb00_8888' limit 10 offset 5" )
        tdSql.checkData(0, 1, 5)
        tdSql.checkData(1, 1, 6)
        tdSql.checkData(2, 1, 7)
        tdSql.query("select *  from stb0 where c2 like 'test97%' ")
        tdSql.checkRows(1000)
        tdSql.query("select  *  from stb0 where tbname like 'stb00_7777' limit 10" )
        tdSql.checkData(0, 1, 0)
        tdSql.checkData(1, 1, 1)
        tdSql.checkData(2, 1, 2)
        tdSql.query("select  *  from stb0 where tbname like 'stb00_7777' limit 10 offset 5" )
        tdSql.checkData(0, 1, 5)
        tdSql.checkData(1, 1, 6)
        tdSql.checkData(2, 1, 7)
        tdSql.query("select *  from stb0 where c2 like 'test96%' ")
        tdSql.checkRows(1000)
        tdSql.query("select  *  from stb0 where tbname like 'stb00_6666' limit 10" )
        tdSql.checkData(0, 1, 0)
        tdSql.checkData(1, 1, 1)
        tdSql.checkData(2, 1, 2)
        tdSql.query("select  *  from stb0 where tbname like 'stb00_6666' limit 10 offset 5" )
        tdSql.checkData(0, 1, 5)
        tdSql.checkData(1, 1, 6)
        tdSql.checkData(2, 1, 7)
        tdSql.query("select *  from stb0 where c2 like 'test95%' ")
        tdSql.checkRows(1000)
        tdSql.query("select  *  from stb0 where tbname like 'stb00_5555' limit 10" )
        tdSql.checkData(0, 1, 0)
        tdSql.checkData(1, 1, 1)
        tdSql.checkData(2, 1, 2)
        tdSql.query("select  *  from stb0 where tbname like 'stb00_5555' limit 10 offset 5" )
        tdSql.checkData(0, 1, 5)
        tdSql.checkData(1, 1, 6)
        tdSql.checkData(2, 1, 7)
        tdSql.query("select * from stb0 where c2 like 'test94%' ")
        tdSql.checkRows(1000)
        tdSql.query("select * from stb0 where tbname like 'stb00_4444' limit 10" )
        tdSql.checkData(0, 1, 0)
        tdSql.checkData(1, 1, 1)
        tdSql.checkData(2, 1, 2)
        tdSql.query("select * from stb0 where tbname like 'stb00_4444' limit 10 offset 5" )
        tdSql.checkData(0, 1, 5)
        tdSql.checkData(1, 1, 6)
        tdSql.checkData(2, 1, 7)
        tdSql.query("select *  from stb0 where c2 like 'test93%' ")
        tdSql.checkRows(1000)
        tdSql.query("select  *  from stb0 where tbname like 'stb00_3333' limit 100" )
        tdSql.checkData(0, 1, 0)
        tdSql.checkData(1, 1, 1)
        tdSql.checkData(2, 1, 2)
        tdSql.query("select *  from stb0 where tbname like 'stb00_3333' limit 100 offset 5" )
        tdSql.checkData(0, 1, 5)
        tdSql.checkData(1, 1, 6)
        tdSql.checkData(2, 1, 7)
        tdSql.query("select *  from stb0 where c2 like 'test92%' ")
        tdSql.checkRows(1000)
        tdSql.query("select  *  from stb0 where tbname like 'stb00_2222' limit 100" )
        tdSql.checkData(0, 1, 0)
        tdSql.checkData(1, 1, 1)
        tdSql.checkData(2, 1, 2)
        tdSql.query("select  *  from stb0 where tbname like 'stb00_2222' limit 100 offset 5" )
        tdSql.checkData(0, 1, 5)
        tdSql.checkData(1, 1, 6)
        tdSql.checkData(2, 1, 7)
        tdSql.query("select *  from stb0 where c2 like 'test91%' ")
        tdSql.checkRows(1000)
        tdSql.query("select  *  from stb0 where tbname like 'stb00_1111' limit 100" )
        tdSql.checkData(0, 1, 0)
        tdSql.checkData(1, 1, 1)
        tdSql.checkData(2, 1, 2)
        tdSql.query("select  *  from stb0 where tbname like 'stb00_1111' limit 100 offset 5" )
        tdSql.checkData(0, 1, 5)
        tdSql.checkData(1, 1, 6)
        tdSql.checkData(2, 1, 7)
        tdSql.query("select *  from stb0 where c2 like 'test90%' ")
        tdSql.checkRows(1000)
        tdSql.query("select  *  from stb0 where tbname like 'stb00_100' limit 100" )
        tdSql.checkData(0, 1, 0)
        tdSql.checkData(1, 1, 1)
        tdSql.checkData(2, 1, 2)
        tdSql.query("select  *  from stb0 where tbname like 'stb00_100' limit 100 offset 5" )
        tdSql.checkData(0, 1, 5)
        tdSql.checkData(1, 1, 6)
        tdSql.checkData(2, 1, 7)
        os.system("rm -rf tools/taosdemoAllTest/TD-4985/query-limit-offset.py.sql")

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
