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

from frame import etool
from frame.etool import *
from frame.log import *
from frame.cases import *
from frame.sql import *
from frame.caseBase import *
from frame.common import *

class TDTestCase(TBase):
    updatecfgDict = {
        "keepColumnName": "1",
        "ttlChangeOnWrite": "1",
        "querySmaOptimize": "1",
        "slowLogScope": "none",
        "queryBufferSize": 10240
    }

    def run(self):
        tdLog.debug(f"start to excute {__file__}")
        tdSql.execute("create database ts_6333;")
        tdSql.execute("use ts_6333;")
        tdSql.execute("select database();")
        tdSql.execute("create stable prop_statistics_equity_stable("
                      "ts timestamp, "
                      "uid int, "
                      "balance_equity decimal(38,16), "
                      "daily_begin_balance decimal(38,16), "
                      "update_time timestamp) "
                      "tags"
                      "(trade_account varchar(20))")
        tdSql.execute("create table prop_1 using prop_statistics_equity_stable tags ('1000000000600');")
        tdSql.execute("create table prop_2 using prop_statistics_equity_stable tags ('1000000000601');")
        tdSql.query("select * from prop_statistics_equity_stable where trade_account = '1000000000601';")
        tdSql.checkRows(0);


        tdLog.success(f"{__file__} successfully executed")


tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
