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
from .nestedQuery import *

class TDTestCase(TDTestCase):

                        
    def run(self):
        tdSql.prepare()
        
        startTime = time.time() 

        self.function_before_26()       
               
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
        
        # self.str_nest(['LTRIM','RTRIM','LOWER','UPPER']) 
        # self.str_nest(['LENGTH','CHAR_LENGTH']) 
        # self.str_nest(['SUBSTR'])   
        # self.str_nest(['CONCAT']) 
        # self.str_nest(['CONCAT_WS']) 
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
        #self.time_nest(['TIMEDIFF_1'])
        #self.time_nest(['TIMEDIFF_2'])
        

        endTime = time.time()
        time.sleep(5)
        print("total time %ds" % (endTime - startTime))

        


    def stop(self):
        tdSql.close()
        time.sleep(5)
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
