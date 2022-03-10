###################################################################
#           Copyright (c) 2020 by TAOS Technologies, Inc.
#                     All rights reserved.
#
#  This file is proprietary and confidential to TAOS Technologies.
#  No part of this file may be reproduced, stored, transmitted,
#  disclosed or used in any form or by any means other than as
#  expressly provided by the written permission from taosdata
#
###################################################################

# -*- coding: utf-8 -*-

import taos
import time
import requests

from util.log import *
from util.cases import *
from util.sql import *
from util.dnodes import *


class TDTestCase:
    def __init__(self):
        self.err_case = 0
        self.curret_case = 0
        self.url = "http://127.0.0.1:6041/rest/sql"
        self.header = {'Authorization': 'Basic cm9vdDp0YW9zZGF0YQ=='}

    def caseDescription(self):

        '''
        case1 <cpwu>: [TD-12163] alter table-schema using restful interface
        '''
        return

    def init(self, conn, logSql):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor(), logSql)

    def check_td12163(self):
        # basetime = int(round(time.time() * 1000))
        tdSql.prepare()
        tdSql.execute(f"create stable stb1(ts timestamp, c1 int) tags (tag1 int)")
        tdSql.execute(f"create table nt1 (nts timestamp, nc1 int)")

        add_column_stb = "alter table db.stb1 add column c2 float"
        drop_column_stb = "alter table db.stb1 drop column c2 "
        add_column_ntb = "alter table db.nt1 add column nc2 float"
        drop_column_ntb = "alter table db.nt1 drop column nc2 "

        conn_add_stb = requests.post(url=self.url, headers=self.header, data=add_column_stb)
        resp_code_stb_add = conn_add_stb.status_code
        resp_add_stb = conn_add_stb.json()
        try:
            assert resp_code_stb_add//200 == 1
            assert resp_add_stb["status"] == "succ"
            self.curret_case += 1
            tdLog.printNoPrefix("the case add column to stable successful")
        except:
            self.err_case += 1
            tdLog.printNoPrefix("the case add column to stable failed")


        conn_add_ntb = requests.post(url=self.url, headers=self.header, data=add_column_ntb)
        resp_code_ntb_add = conn_add_ntb.status_code
        resp_add_ntb = conn_add_ntb.json()
        try:
            assert resp_code_ntb_add//200 == 1
            assert resp_add_ntb["status"] == "succ"
            self.curret_case += 1
            tdLog.printNoPrefix("the case add column to normal table successful")
        except:
            self.err_case += 1
            tdLog.printNoPrefix("the case add column to normal table failed")

        conn_drop_stb =  requests.post(url=self.url, headers=self.header, data=drop_column_stb)
        resp_code_stb_drop = conn_drop_stb.status_code
        resp_drop_stb = conn_drop_stb.json()
        try:
            assert resp_code_stb_drop // 200 == 1
            assert resp_drop_stb["status"] == "succ"
            self.curret_case += 1
            tdLog.printNoPrefix("the case drop column to stable successful")
        except:
            self.err_case += 1
            tdLog.printNoPrefix("the case add column to stable failed")

        conn_drop_ntb =  requests.post(url=self.url, headers=self.header, data=drop_column_ntb)
        resp_code_ntb_drop = conn_drop_ntb.status_code
        resp_drop_ntb = conn_drop_ntb.json()
        try:
            assert resp_code_ntb_drop // 200 == 1
            assert resp_drop_ntb["status"] == "succ"
            self.curret_case += 1
            tdLog.printNoPrefix("the case drop column to stable successful")
        except:
            self.err_case += 1
            tdLog.printNoPrefix("the case add column to stable failed")

        pass

    def run(self):
        self.check_td12163()

        if self.err_case > 0:
            tdLog.exit(f"{self.err_case} case for TD-12163 run failed")
        else:
            tdLog.success("case for TD-12163 run passed")

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())