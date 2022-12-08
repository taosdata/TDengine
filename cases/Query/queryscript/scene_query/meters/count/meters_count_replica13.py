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
                                           
    def run(self):
        startTime = time.time() 
        
        self.countdb_10w_table1w_row10(replica=1,func='count')
        self.countdb_10w_table1w_row10(replica=3,func='count')
        self.countdb_10w_table1w_row10(replica=1,func='count')
        self.countdb_10w_table1w_row10(replica=3,func='count')
        self.countdb_20w_table1w_row20(replica=1,func='count')
        self.countdb_20w_table1w_row20(replica=1,func='count')
        self.countdb_20w_table1w_row20(replica=3,func='count')
        self.countdb_20w_table1w_row20(replica=3,func='count')
        
        self.countdb_40w_table1w_row40(replica=1,func='count')
        self.countdb_40w_table1w_row40(replica=3,func='count')
        self.countdb_40w_table1w_row40(replica=1,func='count')
        self.countdb_40w_table1w_row40(replica=3,func='count')
        self.countdb_80w_table1w_row80(replica=1,func='count')
        self.countdb_80w_table1w_row80(replica=1,func='count')
        self.countdb_80w_table1w_row80(replica=3,func='count')
        self.countdb_80w_table1w_row80(replica=3,func='count')
        
        self.countdb_100w_table1w_row100(replica=1,func='count')
        self.countdb_100w_table1w_row100(replica=3,func='count')
        self.countdb_100w_table1w_row100(replica=1,func='count')
        self.countdb_100w_table1w_row100(replica=3,func='count')
        self.countdb_200w_table1w_row200(replica=1,func='count')
        self.countdb_200w_table1w_row200(replica=1,func='count')
        self.countdb_200w_table1w_row200(replica=3,func='count')
        self.countdb_200w_table1w_row200(replica=3,func='count')
        
        self.countdb_400w_table1w_row400(replica=1,func='count')
        self.countdb_400w_table1w_row400(replica=3,func='count')
        self.countdb_400w_table1w_row400(replica=1,func='count')
        self.countdb_400w_table1w_row400(replica=3,func='count')
        self.countdb_800w_table1w_row800(replica=1,func='count')
        self.countdb_800w_table1w_row800(replica=1,func='count')
        self.countdb_800w_table1w_row800(replica=3,func='count')
        self.countdb_800w_table1w_row800(replica=3,func='count')
        
        self.countdb_1000w_table1w_row1000(replica=1,func='count')
        self.countdb_1000w_table1w_row1000(replica=3,func='count')
        self.countdb_1000w_table1w_row1000(replica=1,func='count')
        self.countdb_1000w_table1w_row1000(replica=3,func='count')
        
        self.countdb_2000w_table1w_row2000(replica=1,func='count')
        self.countdb_2000w_table1w_row2000(replica=1,func='count')
        self.countdb_2000w_table1w_row2000(replica=3,func='count')
        self.countdb_2000w_table1w_row2000(replica=3,func='count')
        
        self.countdb_4000w_table1w_row4000(replica=1,func='count')
        self.countdb_4000w_table1w_row4000(replica=3,func='count')
        self.countdb_4000w_table1w_row4000(replica=1,func='count')
        self.countdb_4000w_table1w_row4000(replica=3,func='count')
        
        self.countdb_8000w_table1w_row8000(replica=1,func='count')
        self.countdb_8000w_table1w_row8000(replica=1,func='count')
        self.countdb_8000w_table1w_row8000(replica=3,func='count')
        self.countdb_8000w_table1w_row8000(replica=3,func='count')
        
        self.countdb_10000w_table1w_row1w(replica=1,func='count')
        self.countdb_10000w_table1w_row1w(replica=3,func='count')
        self.countdb_10000w_table1w_row1w(replica=1,func='count')
        self.countdb_10000w_table1w_row1w(replica=1,func='count')
        self.countdb_10000w_table1w_row1w(replica=3,func='count')
        self.countdb_10000w_table1w_row1w(replica=3,func='count')
        
        
        endTime = time.time()
        
    
        self.logger.info("total time %ds" % (endTime - startTime))
    

