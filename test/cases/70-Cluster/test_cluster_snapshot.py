# Copyright (c) 2016 TAOS Data, Inc. <jhtao@taosdata.com>
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This program is free software: you can use, redistribute, and/or modify
# it under the terms of the GNU Affero General Public License, version 3
# or later ("AGPL"), as published by the Free Software Foundation.
#
# This program is distributed in the hope that it will be useful, but WITHOUT
# ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
# FITNESS FOR A PARTICULAR PURPOSE.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program. If not, see <http://www.gnu.org/licenses/>.

# -*- coding: utf-8 -*-

from new_test_framework.utils import tdLog, tdSql, epath, sc, etool
import time
import random


class TestSnapshot:
    updatecfgDict = {
        "countAlwaysReturnValue" : "1",
        "lossyColumns"           : "float,double",
        "fPrecision"             : "0.000000001",
        "dPrecision"             : "0.00000000000000001",
        "ifAdtFse"               : "1",
        'slowLogScope'           : "insert"
    }

    def insertData(self):
        tdLog.info(f"insert data.")
        # taosBenchmark run
        jfile = etool.curFile(__file__, "snapshot.json")
        etool.benchMark(json=jfile, low_log=True)

        tdSql.execute(f"use {self.db}")
        # set insert data information
        self.childtable_count = 10
        self.insert_rows      = 100000
        self.timestamp_step   = 10000

        # create count check table
        sql = f"create table {self.db}.ta(ts timestamp, age int) tags(area int)"
        tdSql.execute(sql)

    def checkFloatDouble(self):
        sql = f"select * from {self.db}.{self.stb} where fc!=100"
        tdSql.query(sql)
        tdSql.checkRows(0)
        sql = f"select * from {self.db}.{self.stb} where dc!=200"
        tdSql.query(sql)
        tdSql.checkRows(0)
        sql = f"select avg(fc) from {self.db}.{self.stb}"
        tdSql.checkFirstValue(sql, 100)
        sql = f"select avg(dc) from {self.db}.{self.stb}"
        tdSql.checkFirstValue(sql, 200)

    def alterReplica3(self):
        sql = f"alter database {self.db} replica 3"
        tdSql.execute(sql, show=True)
        time.sleep(2)
        sc.dnodeStop(2)
        sc.dnodeStop(3)
        time.sleep(5)
        sc.dnodeStart(2)
        sc.dnodeStart(3)

        if self.waitTransactionZero() is False:
            tdLog.exit(f"{sql} transaction not finished")
            return False
        return True

    def doAction(self):
        tdLog.info(f"do action.")
        self.flushDb()

        # split vgroups
        self.splitVGroups()
        self.trimDb()
        self.checkAggCorrect()

        # after trimDb, one of replica is delayed, this replica can't be forcibly leader, 
        # balance vgroup leader will randomly choose this replica
        # so need wait a while
        time.sleep(10)

        # balance vgroups
        self.balanceVGroupLeader()
        
        if self.waitCompactsRetentionsZero() is False:
            tdLog.exit(f"compact or retentions not finished")
            return False

        # replica to 1
        self.alterReplica(1)
        self.checkAggCorrect()
        self.compactDb()

        if self.waitCompactsZero() is False:
            tdLog.exit(f"compact not finished")
            return False
        
        self.alterReplica3()

        # after alter replica, one of replica is delayed, this replica can't be forcely leader, 
        # balance vgroup leader will randomly choose this replica
        # so need wait a while
        time.sleep(10)

        for i in range(3):
            vgids = self.getVGroup(self.db)
            selid = random.choice(vgids)
            sql = f"balance vgroup leader on {selid}"
            tdLog.info(sql)
            tdSql.execute(sql, show=True)

            if self.waitTransactionZero():
                break  # Success, exit retry loop

            tdLog.info(f"Attempt {i + 1} failed, trying to kill transaction before retry...")
            sql = "show transactions;"
            rows = tdSql.query(sql)
            if rows > 0:
                tranId = tdSql.getData(0, 0)
                tdLog.info(f'kill transaction {tranId}')
                tdSql.execute(f'kill transaction {tranId}', queryTimes=1)
        else:  # This block executes if the loop completes without a `break`
            tdLog.exit("balance vgroup leader failed after 3 retries")
            return False

        # check count always return value
        sql = f"select count(*) from {self.db}.ta"
        tdSql.query(sql)
        tdSql.checkRows(1) # countAlwaysReturnValue is false

    # run
    def test_snapshot(self):
        """Cluster snapshot aggregation

        1. Create 3 dnode cluster environment
        2. taosBenchmark insert data with full data-type columns
        3. Check taosBenchmark insert data correct
        4. Save snapshot with snapshot aggregation like avg, min, max, sum, count
        5. Do cluster action: split vgroups, trim, balance vgroup leaders, alter replica, compact
        6. Check snapshot aggregation result correct
        7. Check taosBenchmark insert data correct again
        8. Check float double value correct again
        
        Since: v3.0.0.0

        Labels: common,ci,integration,functional

        Jira: None

        History:
            - 2025-10-23 Alex Duan Migrated from test/cases/uncatalog/army/cluster/test_snapshot.py

        """
        tdLog.debug(f"start to excute {__file__}")

        # insert data
        self.insertData()

        # check insert data correct
        self.checkInsertCorrect()

        # check float double value ok
        self.checkFloatDouble()

        # save
        self.snapshotAgg()

        # do action
        self.doAction()

        # check save agg result correct
        self.checkAggCorrect()

        # check insert correct again
        self.checkInsertCorrect()

        # check float double value ok
        self.checkFloatDouble()




        

