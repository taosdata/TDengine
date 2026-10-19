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
from new_test_framework.utils import tdLog, tdSql
import time
import platform


class TestKillBalanceLeader:

    def setup_class(cls):
        tdLog.debug(f"start to init {__file__}")


    def test_kill_balance_leader(self):
        """summary: xxx

        description: xxx

        Since: xxx

        Labels: common,ci,integration,functional
        Jira: xxx

        Catalog:
            - xxx:xxx

        History:
            - xxx
            - xxx
        """
        tdLog.debug(f"start to excute {__file__}")
        vgroups = 160
        if platform.system() == "Windows":
            vgroups = 1

        tdSql.execute(f'CREATE DATABASE db vgroups {vgroups} replica 3;')
        tdSql.execute('balance vgroup leader')

        sql ="show transactions;"
        if platform.system() == 'Windows':
            time.sleep(5)
        rows = tdSql.query(sql)

        if rows > 0:
            tranId = tdSql.getData(0, 0)
            tdLog.info('kill transaction %d'%tranId)
            tdSql.execute('kill transaction %d'%tranId, queryTimes=1 )

            if self.waitTransactionZero() is False:
                tdLog.exit(f"{sql} transaction not finished")
                return False
        else:
            tdLog.info(f"{sql} no balance transaction exist")
            return False



    def waitTransactionZero(self, seconds = 300, interval = 1):
        # wait end
        for i in range(seconds):
            sql ="show transactions;"
            rows = tdSql.query(sql)
            if rows == 0:
                tdLog.info("transaction count became zero.")
                return True
            #tdLog.info(f"i={i} wait ...")
            time.sleep(interval)
        
        return False 


