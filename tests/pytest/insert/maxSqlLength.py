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
import datetime
from util.log import *
from util.cases import *
from util.sql import *
from util.dnodes import *
import string
import random


class TDTestCase:
    def init(self, conn, logSql):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor(), logSql)

        self.ts = 1537146000000

    def get_random_string(self, length):
        letters = string.ascii_lowercase
        result_str = ''.join(random.choice(letters) for i in range(length))
        return result_str

    def run(self):
        tdSql.prepare()

        tdSql.execute("create table tb(ts timestamp, name1 binary(1000), name2 binary(1000), name3 binary(1000))")
        
        sql = "insert into tb values"
        for i in range(21):
            value = self.get_random_string(1000)
            sql += "(%d, '%s', '%s', '%s')" % (self.ts + i, value, value, value)        
        tdSql.execute(sql)

        self.ts += 21
        for i in range(22):
            value = self.get_random_string(1000)
            sql += "(%d, '%s', '%s', '%s')" % (self.ts + i, value, value, value)        
        tdSql.error(sql)

        tdSql.query("select * from tb")
        tdSql.checkRows(21)
        
        tdDnodes.stop(1)             
        tdDnodes.setTestCluster(False)
        tdDnodes.setValgrind(False)
        tdDnodes.addSimExtraCfg("maxSQLLength", "1048576")        
        tdDnodes.deploy(1)                  
        tdDnodes.cfg(1, "maxSQLLength", "1048576")
        tdLog.sleep(20)
        tdDnodes.start(1)
        

        tdSql.prepare()    
        tdSql.execute("create table tb(ts timestamp, name1 binary(1000), name2 binary(1000), name3 binary(1000))")
        
        sql = "insert into tb values"
        for i in range(22):
            value = self.get_random_string(1000)
            sql += "(%d, '%s', '%s', '%s')" % (self.ts + i, value, value, value)        
        tdSql.execute(sql)
        
        tdSql.query("select * from tb")
        tdSql.checkRows(43)

        # self.ts += 43
        # for i in range(330):
        #     value = self.get_random_string(1000)
        #     sql += "(%d, '%s', '%s', '%s')" % (self.ts + i, value, value, value)        
        # tdSql.execute(sql)

        # tdSql.query("select * from tb")
        # tdSql.checkRows(379)
                
    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
