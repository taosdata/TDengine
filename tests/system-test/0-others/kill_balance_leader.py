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
from util.dnodes import *
from util.sql import *



class TDTestCase:
    def init(self, conn, logSql, replicaVar=1):
        tdLog.debug(f"start to init {__file__}")
        self.replicaVar = int(replicaVar)
        tdSql.init(conn.cursor(), logSql)

    def stop(self):
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")

    def run(self):
      tdLog.debug(f"start to excute {__file__}")

      tdSql.execute('CREATE DATABASE db vgroups 160 replica 3;')

      tdSql.execute('balance vgroup leader')

      sql ="show transactions;"
      rows = tdSql.query(sql)

      if rows > 0:
          tranId = tdSql.getData(0, 0)
          tdLog.info('kill transaction %d'%tranId)
          tdSql.execute('kill transaction %d'%tranId, queryTimes=1 )

      if self.waitTransactionZero() is False:
          tdLog.exit(f"{sql} transaction not finished")
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


tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())