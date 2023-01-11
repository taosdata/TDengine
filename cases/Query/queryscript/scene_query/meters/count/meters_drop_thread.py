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

from Query.queryscript.scene_query.meters.meters_bigdata_common import *

import threading

class TDTestQuery(TDTestQuery):

    def tags(self) :
	
        return ""

    def author(self) -> str:

        return "Guo Xiangyang"

    def desc(self) -> str:
        case_description = '''
        case1:# meters all query
        '''
        return case_description        
    
    def db_10w(self):        
        self.countdb_10w_table1w_row10(replica=1,func='drop')
        self.countdb_10w_table1w_row10(replica=3,func='drop')
        self.countdb_10w_table1w_row10(replica=1,func='drop')
        self.countdb_10w_table1w_row10(replica=3,func='drop')
        
    def db_20w(self):  
        self.countdb_20w_table1w_row20(replica=1,func='drop')
        self.countdb_20w_table1w_row20(replica=1,func='drop')
        self.countdb_20w_table1w_row20(replica=3,func='drop')
        self.countdb_20w_table1w_row20(replica=3,func='drop')
        
    def db_40w(self):  
        self.countdb_40w_table1w_row40(replica=1,func='drop')
        self.countdb_40w_table1w_row40(replica=3,func='drop')
        self.countdb_40w_table1w_row40(replica=1,func='drop')
        self.countdb_40w_table1w_row40(replica=3,func='drop')
            
    def db_80w(self):  
        self.countdb_80w_table1w_row80(replica=1,func='drop')
        self.countdb_80w_table1w_row80(replica=1,func='drop')
        self.countdb_80w_table1w_row80(replica=3,func='drop')
        self.countdb_80w_table1w_row80(replica=3,func='drop')
    
    def db_100w(self):        
        self.countdb_100w_table1w_row100(replica=1,func='drop')
        self.countdb_100w_table1w_row100(replica=3,func='drop')
        self.countdb_100w_table1w_row100(replica=1,func='drop')
        self.countdb_100w_table1w_row100(replica=3,func='drop')
        
    def db_200w(self):  
        self.countdb_200w_table1w_row200(replica=1,func='drop')
        self.countdb_200w_table1w_row200(replica=1,func='drop')
        self.countdb_200w_table1w_row200(replica=3,func='drop')
        self.countdb_200w_table1w_row200(replica=3,func='drop')
        
    def db_400w(self):  
        self.countdb_400w_table1w_row400(replica=1,func='drop')
        self.countdb_400w_table1w_row400(replica=3,func='drop')
        self.countdb_400w_table1w_row400(replica=1,func='drop')
        self.countdb_400w_table1w_row400(replica=3,func='drop')
            
    def db_800w(self):  
        self.countdb_800w_table1w_row800(replica=1,func='drop')
        self.countdb_800w_table1w_row800(replica=1,func='drop')
        self.countdb_800w_table1w_row800(replica=3,func='drop')
        self.countdb_800w_table1w_row800(replica=3,func='drop')
    
    def db_1000w(self):        
        self.countdb_1000w_table1w_row1000(replica=1,func='drop')
        self.countdb_1000w_table1w_row1000(replica=3,func='drop')
        self.countdb_1000w_table1w_row1000(replica=1,func='drop')
        self.countdb_1000w_table1w_row1000(replica=3,func='drop')
        
    def db_2000w(self):  
        self.countdb_2000w_table1w_row2000(replica=1,func='drop')
        self.countdb_2000w_table1w_row2000(replica=1,func='drop')
        self.countdb_2000w_table1w_row2000(replica=3,func='drop')
        self.countdb_2000w_table1w_row2000(replica=3,func='drop')
        
    def db_4000w(self):  
        self.countdb_4000w_table1w_row4000(replica=1,func='drop')
        self.countdb_4000w_table1w_row4000(replica=3,func='drop')
        self.countdb_4000w_table1w_row4000(replica=1,func='drop')
        self.countdb_4000w_table1w_row4000(replica=3,func='drop')
            
    def db_8000w(self):  
        self.countdb_8000w_table1w_row8000(replica=1,func='drop')
        self.countdb_8000w_table1w_row8000(replica=1,func='drop')
        self.countdb_8000w_table1w_row8000(replica=3,func='drop')
        self.countdb_8000w_table1w_row8000(replica=3,func='drop')
            
    def db_10000w(self):  
        self.countdb_10000w_table1w_row1w(replica=1,func='drop')
        self.countdb_10000w_table1w_row1w(replica=3,func='drop')
        self.countdb_10000w_table1w_row1w(replica=1,func='drop')
        self.countdb_10000w_table1w_row1w(replica=1,func='drop')
        self.countdb_10000w_table1w_row1w(replica=3,func='drop')
        self.countdb_10000w_table1w_row1w(replica=3,func='drop')
                                                                    
    def run(self):
        startTime = time.time() 
        
        while(1):
      
            t1 = threading.Thread(target=self.db_10w) 
            t2 = threading.Thread(target=self.db_20w) 
            t3 = threading.Thread(target=self.db_40w) 
            t4 = threading.Thread(target=self.db_80w) 
            t5 = threading.Thread(target=self.db_100w) 
            t6 = threading.Thread(target=self.db_200w) 
            t7 = threading.Thread(target=self.db_400w) 
            t8 = threading.Thread(target=self.db_800w) 
            t9 = threading.Thread(target=self.db_1000w)
            t10 = threading.Thread(target=self.db_2000w) 
            t11 = threading.Thread(target=self.db_4000w) 
            t12 = threading.Thread(target=self.db_8000w) 
            t13 = threading.Thread(target=self.db_10000w) 
            
            t1.start() 
            t2.start() 
            t3.start()  
            t4.start() 
            t5.start() 
            t6.start()
            t7.start() 
            t8.start() 
            t9.start()
            t10.start() 
            t11.start() 
            t12.start() 
            t13.start() 
            
            
            t1.join()
            t2.join()
            t3.join()
            t4.join()
            t5.join()
            t6.join()
            t7.join()
            t8.join()
            t9.join()
            t10.join()
            t11.join()
            t12.join()
            t13.join()
        
        endTime = time.time()
        
    
        self.logger.info("total time %ds" % (endTime - startTime))
    

