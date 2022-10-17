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


from util.log import *
from util.cases import *
from util.sql import *
import subprocess
from util.common import *


class TDTestCase:
    def init(self, conn, logSql):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor())
        self.dbname = 'db'
        self.ins_param_list = ['dnodes','mnodes','qnodes','cluster','functions','users','grants','topics','subscriptions','streams']
        self.perf_param = ['apps','connections','consumers','queries','transactions']
        self.perf_param_list = ['apps','connections','consumers','queries','trans']

    def ins_check(self):
        for param in self.ins_param_list:
            tdSql.query(f'show {param}')
            show_result = tdSql.queryResult
            tdSql.query(f'select * from information_schema.ins_{param}')
            select_result = tdSql.queryResult
            tdSql.checkEqual(show_result,select_result)

    def perf_check(self):
        for param in range(len(self.perf_param_list)):
            tdSql.query(f'show {self.perf_param[param]}')
            if len(tdSql.queryResult) != 0:
                show_result = tdSql.queryResult[0][0]
                tdSql.query(f'select * from performance_schema.perf_{self.perf_param_list[param]}')
                select_result = tdSql.queryResult[0][0]
                tdSql.checkEqual(show_result,select_result)
            else :
                continue
    def run(self):
        tdSql.prepare()
        self.ins_check()
        self.perf_check()


    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)

tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
