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
import threading
import random
import string
import time

from util.log import *
from util.cases import *
from util.sql import *
from util.autogen import *


#
# Test Main class
#

class TDTestCase:
    def init(self, conn, logSql, replicaVar=1):
        self.replicaVar = int(replicaVar)
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor(), logSql)
        self.autoGen = AutoGen()

    def query_test(self, stbname):
        sql = f"select count(*) from {stbname}"
        tdSql.execute(sql)
        sql = f"select * from {stbname} order by ts desc;"
        tdSql.execute(sql)
        sql = f"select * from (select * from {stbname} where c1=c2 or c3=c4 or c5=c6) order by ts desc;"
        tdSql.execute(sql)

        tdLog.info(" test query ok!")


    def test_db(self, dbname, stbname, childname, tag_cnt, column_cnt, child_cnt, insert_rows, binary_len, nchar_len):    
        self.autoGen.create_db(dbname)
        self.autoGen.create_stable(stbname, tag_cnt, column_cnt, binary_len, nchar_len)
        self.autoGen.create_child(stbname, childname, child_cnt)
        self.autoGen.insert_data(insert_rows)
        self.autoGen.insert_samets(insert_rows)
        self.query_test(stbname)

    def run(self):  
        dbname = "test"
        stbname = "st"
        childname = "d"
        child_cnt = 2
        insert_rows = 10
        tag_cnt    = 15
        column_cnt = 20
        binary_len = 10240
        nchar_len =  1025
        self.autoGen.set_batch_size(1)
        
        # normal
        self.test_db(dbname, stbname, childname, tag_cnt, column_cnt, child_cnt, insert_rows, binary_len, nchar_len)

        # max
        dbname = "test_max_col"
        child_cnt = 3
        insert_rows = 50
        tag_cnt = 128
        binary_len = 3
        nchar_len = 4
        column_cnt = 4096 - tag_cnt
        self.autoGen.set_batch_size(1)
        self.test_db(dbname, stbname, childname, tag_cnt, column_cnt, child_cnt, insert_rows, binary_len, nchar_len)

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)

tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
