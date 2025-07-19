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
import sys
import os

sys.path.append(os.path.dirname(os.path.realpath(__file__)))
from restore_basic import *
import threading

class TestKillRestoreDnode:
    # init
    def setup_class(cls):
        tdLog.debug("start to execute %s" % __file__)
        cls.basic = RestoreBasic()
        cls.basic.init(cls.replicaVar)
    
    # run
    def test_kill_restore_dnode(self):
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
        self.basic.restore_dnode_prepare(2)

        self.execute()

    # stop
        self.basic.stop()
        tdLog.success("%s successfully executed" % __file__)

    def execute(self):
        newTdSql=tdCom.newTdSql()
        t0 = threading.Thread(target=self.restoreDnodeThread, args=('', newTdSql))
        t0.start()

        time.sleep(2)
        sql ="show transactions;"
        tdLog.info(sql)
        rows = tdSql.query(sql)

        if rows > 0:
            self.basic.stop_dnode(2)

            tranId = tdSql.getData(0, 0)

            tdLog.info('show transaction %d'%tranId)
            rows=tdSql.query('show transaction %d'%tranId, queryTimes=1)
            if rows != 11:
                tdLog.exit(f"restore transaction detial error, rows={rows}")
                return False

            tdLog.info('kill transaction %d'%tranId)
            tdSql.execute('kill transaction %d'%tranId, queryTimes=1 )

            time.sleep(3)
            sql ="show transactions;"
            tdLog.info(sql)
            rows = tdSql.query(sql)
            if rows > 0:
                tdLog.info(f"{sql} transaction not finished")
                return False
            
            self.basic.restore_dnode_exec(2)
        else:
            tdLog.exit(f"{sql} no transaction exist")
            return False

    def restoreDnodeThread(self, p, newTdSql):
        time.sleep(1)
        count = 0
        while count < 100:
            newTdSql.query('show dnodes')
            if newTdSql.queryResult[1][4] == "ready":
               break
            count+=1

        sql = f"restore dnode 2"
        tdLog.info(sql)
        newTdSql.error(sql, expectErrInfo="Wrong transaction execution context")
        tdLog.info(f"{sql} finished")


