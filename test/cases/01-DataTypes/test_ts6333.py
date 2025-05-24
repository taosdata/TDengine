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

from new_test_framework.utils import tdLog, tdSql

class TestTs6333:
    updatecfgDict = {
        "keepColumnName": "1",
        "ttlChangeOnWrite": "1",
        "querySmaOptimize": "1",
        "slowLogScope": "none",
        "queryBufferSize": 10240
    }

    def test_ts6333(self):
        """test decimal types filtering with tag conditions crash fix

        test decimal types filtering with tag conditions crash fix

        Since: v3.3.6.4

        Labels: decimal

        Jira: TS-6333

        History:
            - 2025-4-15 Jing Sima Created
            - 2025-5-08 Huo Hong Migrated to new test framework

        """
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

