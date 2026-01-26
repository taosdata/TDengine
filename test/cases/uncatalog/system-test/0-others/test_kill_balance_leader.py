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
import time
import platform


class TestKillBalanceLeader:

    def setup_class(cls):
        tdLog.debug(f"start to init {__file__}")


    def test_kill_balance_leader(self):
        """summary: xxx

        description: xxx

        Since: xxx

        Labels: xxx

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

        tdLog.success(f"{__file__} ")

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


