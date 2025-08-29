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
import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from test_nestedQuery import TestNestedquery as NestedQueryHelper

class TestNestedQueryStrCase:

    def test_nestedQuery_str(self):
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

        tdSql.prepare()
        
        startTime = time.time() 

        # self.function_before_26()       
               
        # self.math_nest(['UNIQUE'])
        # self.math_nest(['MODE']) 
        # self.math_nest(['SAMPLE'])
                
        # self.math_nest(['ABS','SQRT'])     
        # self.math_nest(['SIN','COS','TAN','ASIN','ACOS','ATAN'])        
        # self.math_nest(['POW','LOG']) 
        # self.math_nest(['FLOOR','CEIL','ROUND']) 
        # self.math_nest(['MAVG'])  
        # self.math_nest(['HYPERLOGLOG']) 
        # self.math_nest(['TAIL']) 
        # self.math_nest(['CSUM'])
        # self.math_nest(['statecount','stateduration'])
        # self.math_nest(['HISTOGRAM']) 
        
        nested_query_test = NestedQueryHelper()
        nested_query_test.case_init()
        nested_query_test.str_nest(['LTRIM','RTRIM','LOWER','UPPER']) 
        nested_query_test.str_nest(['LENGTH','CHAR_LENGTH']) 
        nested_query_test.str_nest(['SUBSTR'])   
        nested_query_test.str_nest(['CONCAT']) 
        nested_query_test.str_nest(['CONCAT_WS']) 
        # self.time_nest(['CAST']) #放到time里起来弄
        # self.time_nest(['CAST_1'])
        # self.time_nest(['CAST_2'])
        # self.time_nest(['CAST_3'])
        # self.time_nest(['CAST_4'])

        # self.time_nest(['NOW','TODAY']) 
        # self.time_nest(['TIMEZONE']) 
        # self.time_nest(['TIMETRUNCATE']) 
        # self.time_nest(['TO_ISO8601'])
        # self.time_nest(['TO_UNIXTIMESTAMP'])
        # self.time_nest(['ELAPSED'])
        # self.time_nest(['TIMEDIFF_1'])
        # self.time_nest(['TIMEDIFF_2'])

        endTime = time.time()
        print("total time %ds" % (endTime - startTime))

        #tdSql.close()
        tdLog.success("%s successfully executed" % __file__)
