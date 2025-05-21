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
from new_test_framework.utils import tdLog, tdSql, etool


class TestMlevelBasic:
    def insertData(self):
        tdLog.info(f"insert data.")
        # taosBenchmark run
        json = etool.curFile(__file__, "mlevel_basic.json")
        etool.benchMark(json=json)

        tdSql.execute(f"use {self.db}")
        # set insert data information
        self.childtable_count = 4
        self.insert_rows = 1000000
        self.timestamp_step = 1000

    def doAction(self):
        tdLog.info(f"do action.")
        self.flushDb()
        self.trimDb()
        self.compactDb()

    # run
    def test_mlevel_basic(self):
        """multi level storage

        insert data, fluch & trim database, check aggregate value.

        Since: v3.0.0.0

        History:
            - 2024-6-5 Alex Duan Created
            - 2025-5-13 Huo Hong Migrated to new test framework

        """
        tdLog.debug(f"start to excute {__file__}")

        # insert data
        self.insertData()

        # check insert data correct
        self.checkInsertCorrect()

        # save
        self.snapshotAgg()

        # do action
        self.doAction()

        # check save agg result correct
        self.checkAggCorrect()

        # check insert correct again
        self.checkInsertCorrect()

        tdLog.success(f"{__file__} successfully executed")

