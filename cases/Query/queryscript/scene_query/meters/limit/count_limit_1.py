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

from Query.queryscript.scene_query.meters.limit.count_limit import *

class TDTestQuery(TDTestQuery):
    
    
    def tags(self) :
	
        return ""

    def author(self) -> str:

        return "Guo Xiangyang"

    def desc(self) -> str:
        case_description = '''
        case1:# meters count limit all query 
        '''
        return case_description        
    

    def run_sql(self,dbname,tables,per_table_num,dbnamejoin):
        
        num,num2 = random.randint(10,100),random.randint(10,100)
        #self.fun_count(dbname,num,num2,self.tables,self.per_table_num,self.dbnamejoin_local1,'count','count')
        self.fun_count_1_7(dbname,num,num2,self.tables,self.per_table_num,self.dbnamejoin_local1,'count','count')
        self.fun_count_8_15(dbname,num,num2,self.tables,self.per_table_num,self.dbnamejoin_local1,'count','count')
        #self.fun_count_16_22(dbname,num,num2,self.tables,self.per_table_num,self.dbnamejoin_local1,'count','count')

        self.tdSql.execute(" flush database %s;" %self.dbnamejoin_local1)

        #self.fun_count(dbname,num,num2,self.tables,self.per_table_num,self.dbnamejoin_local1,'count','count')
        #self.fun_count_1_7(dbname,num,num2,self.tables,self.per_table_num,self.dbnamejoin_local1,'count','count')
        #self.fun_count_8_15(dbname,num,num2,self.tables,self.per_table_num,self.dbnamejoin_local1,'count','count')
        self.fun_count_16_22(dbname,num,num2,self.tables,self.per_table_num,self.dbnamejoin_local1,'count','count')    
                                              
    def run(self):
        startTime = time.time() 
        
        self.tdCreateData.alter_local_slowlogthreshold()  #设置慢查询
        self.tdSql.query("alter local 'schedulePolicy' '%d';" %random.randint(1,3))
        
        self.benchmark_insert_stb(self.source_taosd_list,self.dbnamejoin_base1,'stb',self.tables,self.per_table_num,self.vgroups,self.replica) 
        self.base_sql_count(self.dbnamejoin_base1,self.tables,self.per_table_num)
        self.benchmark_insert_stb(self.source_taosd_list,self.dbnamejoin_local1,'stb',self.join_tables,self.join_per_table_num,self.join_vgroups,self.replica) 
        self.base_sql_count(self.dbnamejoin_local1,self.join_tables,self.join_per_table_num)
        
        self.run_sql(self.dbnamejoin_base1,self.tables,self.per_table_num,self.dbnamejoin_local1)   #前面base,解决不同容器的错误，后面用local_join
        
        self.drop_db_table(self.dbnamejoin_base1)  
        self.drop_db_table(self.dbnamejoin_local1)
        
        endTime = time.time()
        
    
        self.logger.info("total time %ds" % (endTime - startTime))
    

