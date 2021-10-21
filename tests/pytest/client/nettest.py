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

import taos
import subprocess

from util.log import tdLog
from util.cases import tdCases
from util.sql import tdSql

class TDTestCase:
    def init(self, conn, logSql):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor(), logSql)

    def run(self):
        try:
            str1 = 'taos -n speed -P 6030 -N 1000 -l 100000 -S tcp'
            result1 = subprocess.call(str1)
        except Exception as result1:
            if result1 == 1:
                tdLog.exit("the shell 'taos -n speed -P 6030 -N 1000 -l 100000 -S tcp' is wrong")

        try:
            str2 = 'taos -n speed -P 6030 -N 1000 -l 100000 -S udp'
            result2 = subprocess.call(str2)
        except Exception as result2:
            if result2 == 1:
                tdLog.exit("the shell 'taos -n speed -P 6030 -N 1000 -l 100000 -S udp' is wrong")
            
        try:
            str3 = 'taos -n fqdn'
            result3 = subprocess.call(str3)
        except Exception as result3:
            if result3 ==1:
                tdLog.exit('the shell"taos -n fqdn" is wrong')
                

    
    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)



tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
