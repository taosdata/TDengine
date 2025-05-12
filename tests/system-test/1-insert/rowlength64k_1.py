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
from util.cases import tdCases
from .rowlength64k import *

class TDTestCase(TDTestCase):

                        
    def run(self):
        tdSql.prepare()
        
        startTime_all = time.time() 
        #self.run_1() 
        # self.run_2() 
        self.run_3() 
        #self.run_4() 
        
        endTime_all = time.time()
        print("total time %ds" % (endTime_all - startTime_all))  

        


    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
