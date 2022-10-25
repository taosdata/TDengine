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
from util.log import *
from util.cases import *
from util.sql import *


class TDTestCase:
    def init(self, conn, logSql):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor(), logSql)

    def run(self):
        tdSql.prepare()

        tdSql.query('select database()')
        tdSql.checkData(0, 0, "db")        
        
        tdSql.execute("alter database db comp 2")
        tdSql.query("select * from information_schema.ins_databases")
        tdSql.checkData(0, 14, 2)

        tdSql.execute("alter database db keep 365,365,365")
        tdSql.query("select * from information_schema.ins_databases")
        tdSql.checkData(0, 7, "365,365,365")
        
        tdSql.error("alter database db quorum 2")
        

        tdSql.execute("alter database db blocks 100")
        tdSql.query("select * from information_schema.ins_databases")
        tdSql.checkData(0, 9, 100)



    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())