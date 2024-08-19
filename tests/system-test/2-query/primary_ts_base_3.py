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
from .primary_ts_base import *
from faker import Faker
import random

class TDTestCase(TDTestCase):
                            
    def run(self):
        startTime = time.time() 
        self.dropandcreateDB_primary_key(self.database, 1 , 1 ,'yes','yes','no')

        # self.fun_pk_interp(self.database,'interp','') 
        # self.multiple_agg_groupby(self.database,1) 
        # self.fun_pk_diff(self.database,'diff','') 
        # self.fun_pk_twa(self.database,'derivative',',1s,0') 
        # self.fun_pk_twa(self.database,'derivative',',1s,1') 
        # self.fun_pk_unique(self.database,'unique','')  
        # self.fun_pk_last_init(self.database,'last','')  
        # self.fun_pk_last(self.database,'last','')  
        # self.fun_pk_last(self.database,'last_row','') 
        # self.fun_pk_first(self.database,'first','') 
        
        # self.query_pk_fun(self.database,'') 
        
        # self.touying_pk_1(self.database,1) 
        self.touying_pk_where(self.database,'') 
        # self.touying_pk_where(self.database,'tags') 
        # self.touying_pk_where(self.database,'distinct') 
        # self.count_pk(self.database,1) 
        
        endTime = time.time()
        print("total time %ds" % (endTime - startTime))

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
