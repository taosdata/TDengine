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

from Query.queryscript.scene_query.meters.meters_limit_common import *

class TDTestQuery(TDTestQuery):
    
    #basic_param
    dbname_pw = 'meters_base_pw'
    tables_pw = 8
    per_table_num_pw = 20000 #000
    vgroups = random.randint(1,8)
    
    dbnamejoin_pw = 'meters_join_pw'
    #比base表要大   
    join_tables_pw = 300
    join_per_table_num_pw = 500
    join_vgroups = random.randint(1,8)
        
    replica = random.choice(['1','3'])
    
    def tags(self) :
	
        return ""

    def author(self) -> str:

        return "Guo Xiangyang"

    def desc(self) -> str:
        case_description = '''
        case1:# meters count limit all query 
        '''
        return case_description        
    
    def run_limit_slimit_sql_pw(self,dbname,tables,per_table_num,dbnamejoin):
        self.tdSql.query("alter local 'schedulePolicy' '%d';" %random.randint(1,3))
        num,num2 = random.randint(10,100),random.randint(10,100)
        self.sql_base_pw(dbname,num,num2,tables,per_table_num,dbnamejoin)

        self.tdSql.execute(" flush database %s;" %dbname)
        self.tdSql.execute(" flush database %s;" %dbnamejoin)

        self.sql_base_pw(dbname,num,num2,tables,per_table_num,dbnamejoin)
        
    def sql_base_pw(self,dbname_pw,num,num2,tables_pw,per_table_num_pw,dbnamejoin_pw):
        self.tdSql.query("alter local 'schedulePolicy' '%d';" %random.randint(1,3))
        sql = "select count(*) from %s.meters" %dbname_pw
        self.sql_query_time_cost(sql)
        self.tdSql.checkData(0,0,tables_pw*per_table_num_pw)
        sql = "select count(*) from %s.meters" %dbnamejoin_pw
        self.sql_query_time_cost(sql)
        
        self.join_base(dbname_pw,num,num2,tables_pw,per_table_num_pw,dbnamejoin_pw,'*','*')   
        
    def create_db_joindb_pw(self,replica):
        #每个库的个性设置+数据创建+通用检查，支持单/3副本        
        self.benchmark_insert_stb(self.source_taosd_list,self.dbname_pw,'stb',self.tables_pw,self.per_table_num_pw,self.vgroups,self.replica)
        self.base_sql_count(self.dbname_pw,self.tables_pw,self.per_table_num_pw)

        self.benchmark_insert_stb(self.source_taosd_list,self.dbnamejoin_pw,'stb',self.join_tables_pw,self.join_per_table_num_pw,self.join_vgroups,self.replica) 
        self.base_sql_count(self.dbnamejoin_pw,self.join_tables_pw,self.join_per_table_num_pw)
        
        self.run_limit_slimit_sql_pw(self.dbname_pw,self.tables_pw,self.per_table_num_pw,self.dbnamejoin_pw)
                                                      
    def run(self):
        startTime = time.time()  
        
        self.tdSql.query("alter local 'schedulePolicy' '%d';" %random.randint(1,3))     

        self.tdCreateData.alter_local_slowlogthreshold()  #设置慢查询
        
        self.create_db_joindb_pw(self.replica) 
        
        #self.drop_db_table(self.dbnamejoin_pw)

        endTime = time.time()
        print("total time %ds" % (endTime - startTime))
        
    
        self.logger.info("total time %ds" % (endTime - startTime))
    

