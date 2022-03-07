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

#TODO: after TD-4518 and TD-4510 is resolved, add the exception test case for these situations

import sys
from util.log import *
from util.cases import *
from util.sql import *


class TDTestCase:
    def init(self, conn, logSql):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor(), logSql)

    def run(self):
        tdSql.prepare()

        tdSql.error('alter database keep db 0')
        tdSql.error('alter database keep db -10')
        tdSql.error('alter database keep db -2147483648')
        
        #this is the test case problem for keep overflow
        #the error is caught, but type is wrong.
        #TODO: can be solved in the future, but improvement is minimal
        tdSql.error('alter database keep db -2147483649')   
    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())