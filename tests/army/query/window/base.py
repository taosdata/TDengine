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
from frame.log import *
from frame.cases import *
from frame.sql import *
from frame import etool


class TDTestCase:
    # init
    def init(self, conn, logSql, replicaVar=1):
        self.replicaVar = int(replicaVar)
        tdLog.debug(f"start to excute {__file__}")
        tdSql.init(conn.cursor(), True)
        # taosBenchmark run
        tdLog.info(f"insert data.")
        jfile = etool.curFile(__file__, "window.json")
        etool.benchMark(json=jfile)

    # run
    def run(self):
        # TD-31660
        sql = "select _wstart,_wend,count(voltage),tbname from db.stb partition by tbname event_window start with voltage >2 end with voltage > 15 slimit 5 limit 5"
        tdSql.query(sql)
        tdSql.checkRows(25)
        sql = "select _wstart,_wend,count(voltage),tbname from db.stb partition by tbname count_window(600) slimit 5 limit 5;"
        tdSql.query(sql)
        tdSql.checkRows(25)

    # stop
    def stop(self):
        tdSql.execute("drop database db;")
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")


tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
