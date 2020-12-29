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

# migrated from 'stream_on_sys.sim'
# -*- coding: utf-8 -*-
import sys
import time
import taos
from util.log import tdLog
from util.cases import tdCases
from util.sql import tdSql


class TDTestCase:
    def init(self, conn, logSql):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor(), logSql)


    def run(self):
        tdSql.execute("use log")

        tdSql.execute("create table cpustrm as select count(*), avg(cpu_taosd), max(cpu_taosd), min(cpu_taosd), avg(cpu_system), max(cpu_cores), min(cpu_cores), last(cpu_cores) from log.dn1 interval(4s)")
        tdSql.execute("create table memstrm as select count(*), avg(mem_taosd), max(mem_taosd), min(mem_taosd), avg(mem_system), first(mem_total), last(mem_total) from log.dn1 interval(4s)")
        tdSql.execute("create table diskstrm as select count(*), avg(disk_used), last(disk_used), avg(disk_total), first(disk_total) from log.dn1 interval(4s)")
        tdSql.execute("create table bandstrm as select count(*), avg(band_speed), last(band_speed) from log.dn1 interval(4s)")
        tdSql.execute("create table reqstrm as select count(*), avg(req_http), last(req_http), avg(req_select), last(req_select), avg(req_insert), last(req_insert) from log.dn1 interval(4s)")
        tdSql.execute("create table iostrm as select count(*), avg(io_read), last(io_read), avg(io_write), last(io_write) from log.dn1 interval(4s)")

        sqls = [
            "select * from cpustrm",
            "select * from memstrm",
            "select * from diskstrm",
            "select * from bandstrm",
            "select * from reqstrm",
            "select * from iostrm",
        ]
        for sql in sqls:
            (rows, _) = tdSql.waitedQuery(sql, 1, 120)
            if rows < 1:
                tdLog.exit("failed: sql:%s, expect at least one row" % sql)


    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())

