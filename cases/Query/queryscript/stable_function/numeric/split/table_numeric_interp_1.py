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

from Query.queryscript.stable_function.numeric.table_numeric_interp import *

class TDTestQuery(TDTestQuery):
        
    def tags(self) :
	
        return ""

    def author(self) -> str:

        return "Guo Xiangyang"

    def desc(self) -> str:
        case_description = '''
        case1:# support INTERP function [hanshu = 'INTERP']
        case2:
        '''
        return case_description
                
    def run(self):
        startTime = time.time() 
        self.tdSql.query("alter local 'schedulePolicy' '2';") 
        
        self.data_create(self.db_1)
        
        self.right_case_1_range()    

        endTime = time.time()
        self.rm_sql(self.db_1)
        self.logger.info("total time %ds" % (endTime - startTime))


