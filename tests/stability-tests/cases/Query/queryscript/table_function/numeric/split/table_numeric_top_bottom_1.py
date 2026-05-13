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

from Query.queryscript.table_function.numeric.table_numeric_top_bottom import *

class TDTestQuery(TDTestQuery):
        
    def tags(self) :
	
        return ""

    def author(self) -> str:

        return "Guo Xiangyang"

    def desc(self) -> str:
        case_description = '''
        case1:# support all int type \ double type  [hanshu = 'TOP','BOTTOM']
        case2:
        '''
        return case_description
                
    def run(self):
        startTime = time.time() 
        
        # self.data_create(self.db)
          
        startTime1 = time.time()  
        self.data_create(self.db_1)      
        self.right_case_1()
        self.right_case_1_tbname()        
        self.right_case_1_interval()
        self.right_case_1_tbname_interval()
        self.rm_sql_1()
        endTime1 = time.time()       
        self.logger.info("total time1 %d s" % (endTime1 - startTime1))
    
        # startTime2 = time.time()
        # self.data_create(self.db_2)
        # self.right_case_2()
        # self.right_case_2_tbname()
        # self.right_case_2_interval()
        # self.right_case_2_tbname_interval()
        # self.rm_sql_2()
        # endTime2 = time.time()       
        # self.logger.info("total time2 %d s" % (endTime2 - startTime2))
        
        # startTime3 = time.time()
        # self.data_create(self.db_3)
        # self.right_case_3()
        # self.right_case_3_tbname()
        # self.right_case_3_interval()
        # self.right_case_3_tbname_interval()
        # self.rm_sql_3()
        # endTime3 = time.time()
        # self.logger.info("total time3 %ds" % (endTime3 - startTime3))     

        endTime = time.time()
        # self.rm_sql()
        self.logger.info("total time %ds" % (endTime - startTime))


