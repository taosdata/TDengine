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
    
    dbnamejoin_local = 'co_join_unt'
    
    def tags(self) :
	
        return ""

    def author(self) -> str:

        return "Guo Xiangyang"

    def desc(self) -> str:
        case_description = '''
        case1:# meters count limit all query 
        '''
        return case_description        
    
    def fun_count(self,dbname,num,num2,tables,per_table_num,dbnamejoin,base_fun,replace_fun):
        
        self.logger.info("count query ---------1----------")       
        sql = "select count(*) from %s.meters limit %d" %(dbname,num)
        self.sql_limit_retun_1_slimit_return_error(sql,tables,per_table_num,base_fun,replace_fun)
        sql = "select count(*) from (%s)" %sql
        self.sql_data_limit_retun_1_slimit_return_error(sql,tables,per_table_num,base_fun,replace_fun)
        sql_union = "(%s) union (%s)" %(sql,sql)
        self.sql_data_limit_retun_1_slimit_return_error(sql_union,tables,per_table_num,base_fun,replace_fun)
        sql_union_all = "(%s) union all (%s)" %(sql,sql_union)
        self.sql_data_limit_retun_1_slimit_return_error(sql_union_all,tables,per_table_num,base_fun,replace_fun)
        
        sql_join = "select count(a.*) from %s.meters a,%s.meters b where a.ts = b.ts limit %d" %(dbname,dbnamejoin,num)
        self.sql_limit_retun_1_slimit_return_error(sql_join,tables,per_table_num,base_fun,replace_fun)
        sql_join = "select count(*) from (%s)" %sql_join
        self.sql_data_limit_retun_1_slimit_return_error(sql_join,tables,per_table_num,base_fun,replace_fun)
        sql_union = "(%s) union (%s)" %(sql_join,sql_join)
        self.sql_data_limit_retun_1_slimit_return_error(sql_union,tables,per_table_num,base_fun,replace_fun)
        sql_union_all = "(%s) union all (%s)" %(sql_join,sql_union)
        self.sql_data_limit_retun_1_slimit_return_error(sql_union_all,tables,per_table_num,base_fun,replace_fun)
        
        
        
        
        self.logger.info("count query ---------2----------")
        sql = "select count(*) from %s.meters where ts is not null limit %d" %(dbname,num)
        self.sql_limit_retun_1_slimit_return_error(sql,tables,per_table_num,base_fun,replace_fun)
        sql = "select count(*) from (%s)" %sql
        self.sql_data_limit_retun_1_slimit_return_error(sql,tables,per_table_num,base_fun,replace_fun)
        sql_union = "(%s) union (%s)" %(sql,sql)
        self.sql_data_limit_retun_1_slimit_return_error(sql_union,tables,per_table_num,base_fun,replace_fun)
        sql_union_all = "(%s) union all (%s)" %(sql,sql_union)
        self.sql_data_limit_retun_1_slimit_return_error(sql_union_all,tables,per_table_num,base_fun,replace_fun)
        
        sql_join = "select count(a.*) from %s.meters a,%s.meters b where a.ts is not null and  a.ts = b.ts limit %d" %(dbname,dbnamejoin,num)
        self.sql_limit_retun_1_slimit_return_error(sql_join,tables,per_table_num,base_fun,replace_fun)
        sql_join = "select count(*) from (%s)" %sql_join
        self.sql_data_limit_retun_1_slimit_return_error(sql_join,tables,per_table_num,base_fun,replace_fun)
        sql_union = "(%s) union (%s)" %(sql_join,sql_join)
        self.sql_data_limit_retun_1_slimit_return_error(sql_union,tables,per_table_num,base_fun,replace_fun)
        sql_union_all = "(%s) union all (%s)" %(sql_join,sql_union)
        self.sql_data_limit_retun_1_slimit_return_error(sql_union_all,tables,per_table_num,base_fun,replace_fun)
        
        
        
        self.logger.info("count query ---------3----------")
        sql = "select count(*) from %s.meters where ts is not null order by ts limit %d" %(dbname,num)
        self.sql_retun_error(sql,base_fun,replace_fun)
        sql = "select count(*) from (%s)" %sql
        self.sql_retun_error(sql,base_fun,replace_fun)
        sql_union = "(%s) union (%s)" %(sql,sql)
        self.sql_retun_error(sql_union,base_fun,replace_fun)
        sql_union_all = "(%s) union all (%s)" %(sql,sql_union)
        self.sql_retun_error(sql_union_all,base_fun,replace_fun)
        
        sql_join = "select count(a.*) from %s.meters a,%s.meters b where a.ts is not null and  a.ts = b.ts order by b.ts limit %d" %(dbname,dbnamejoin,num)
        self.sql_retun_error(sql_join,base_fun,replace_fun)
        sql_join = "select count(*) from (%s)" %sql_join
        self.sql_retun_error(sql_join,base_fun,replace_fun)
        sql_union = "(%s) union (%s)" %(sql_join,sql_join)
        self.sql_retun_error(sql_union,base_fun,replace_fun)
        sql_union_all = "(%s) union all (%s)" %(sql_join,sql_union)
        self.sql_retun_error(sql_union_all,base_fun,replace_fun)
        
        
        
        self.logger.info("count query ---------4----------")
        sql = "select count(*) from %s.meters where ts is not null order by ts desc limit %d" %(dbname,num)        
        self.sql_retun_error(sql,base_fun,replace_fun)
        sql = "select count(*) from (%s)" %sql
        self.sql_retun_error(sql,base_fun,replace_fun)
        sql_union = "(%s) union (%s)" %(sql,sql)
        self.sql_retun_error(sql_union,base_fun,replace_fun)
        sql_union_all = "(%s) union all (%s)" %(sql,sql_union)
        self.sql_retun_error(sql_union_all,base_fun,replace_fun)
        
        sql_join = "select count(a.*) from %s.meters a,%s.meters b where b.ts is not null and  a.ts = b.ts order by a.ts desc limit %d" %(dbname,dbnamejoin,num)        
        self.sql_retun_error(sql_join,base_fun,replace_fun)
        sql_join = "select count(*) from (%s)" %sql_join
        self.sql_retun_error(sql_join,base_fun,replace_fun)
        sql_union = "(%s) union (%s)" %(sql_join,sql_join)
        self.sql_retun_error(sql_union,base_fun,replace_fun)
        sql_union_all = "(%s) union all (%s)" %(sql_join,sql_union)
        self.sql_retun_error(sql_union_all,base_fun,replace_fun)
        
        
        
        self.logger.info("count query ---------5----------")
        sql = "select count(*) from %s.meters where ts is not null group by tbname limit %d" %(dbname,num)
        self.sql_limit_retun_tables_slimit_return_n(sql,num,tables,per_table_num,base_fun,replace_fun)        
        sql = "select count(*) from (%s)" %sql
        self.sql_data_limit_retun_tables_slimit_return_n(sql,num,tables,per_table_num,base_fun,replace_fun) 
        sql_union = "(%s) union (%s)" %(sql,sql)
        self.sql_data_limit_retun_tables_slimit_return_n(sql_union,num,tables,per_table_num,base_fun,replace_fun) 
        sql_union_all = "(%s) union all (%s)" %(sql,sql_union)
        self.sql_data_limit_retun_tables_slimit_return_n(sql_union_all,num,tables,per_table_num,base_fun,replace_fun) 
        
        sql_join = "select count(*) from %s.meters a,%s.meters b where a.ts is not null and a.ts = b.ts group by b.tbname limit %d" %(dbname,dbnamejoin,num)
        self.sql_limit_retun_tables_slimit_return_n(sql_join,num,tables,per_table_num,base_fun,replace_fun)        
        sql_join = "select count(*) from (%s)" %sql_join
        self.sql_data_limit_retun_tables_slimit_return_n(sql_join,num,tables,per_table_num,base_fun,replace_fun) 
        sql_union = "(%s) union (%s)" %(sql_join,sql_join)
        self.sql_data_limit_retun_tables_slimit_return_n(sql_union,num,tables,per_table_num,base_fun,replace_fun) 
        sql_union_all = "(%s) union all (%s)" %(sql_join,sql_union)
        self.sql_data_limit_retun_tables_slimit_return_n(sql_union_all,num,tables,per_table_num,base_fun,replace_fun) 
        
        
        
        
        self.logger.info("count query ---------6----------")
        sql = "select count(*) from %s.meters where ts is not null partition by tbname limit %d" %(dbname,num)
        self.sql_limit_retun_tables_slimit_return_n(sql,num,tables,per_table_num,base_fun,replace_fun) 
        sql = "select count(*) from (%s)" %sql
        self.sql_data_limit_retun_tables_slimit_return_n(sql,num,tables,per_table_num,base_fun,replace_fun) 
        sql_union = "(%s) union (%s)" %(sql,sql)
        self.sql_data_limit_retun_tables_slimit_return_n(sql_union,num,tables,per_table_num,base_fun,replace_fun) 
        sql_union_all = "(%s) union all (%s)" %(sql,sql_union)
        self.sql_data_limit_retun_tables_slimit_return_n(sql_union_all,num,tables,per_table_num,base_fun,replace_fun) 
        
        sql_join = "select count(*) from %s.meters a,%s.meters b where a.ts is not null and a.ts = b.ts partition by b.tbname limit %d" %(dbname,dbnamejoin,num)
        self.sql_limit_retun_tables_slimit_return_n(sql_join,num,tables,per_table_num,base_fun,replace_fun) 
        sql_join = "select count(*) from (%s)" %sql_join
        self.sql_data_limit_retun_tables_slimit_return_n(sql_join,num,tables,per_table_num,base_fun,replace_fun) 
        sql_union = "(%s) union (%s)" %(sql_join,sql_join)
        self.sql_data_limit_retun_tables_slimit_return_n(sql_union,num,tables,per_table_num,base_fun,replace_fun) 
        sql_union_all = "(%s) union all (%s)" %(sql_join,sql_union)
        self.sql_data_limit_retun_tables_slimit_return_n(sql_union_all,num,tables,per_table_num,base_fun,replace_fun) 
        
        
        
        self.logger.info("count query ---------7----------")
        sql = "select count(*) cc from %s.meters where ts is not null group by tbname order by cc limit %d" %(dbname,num)
        self.sql_limit_retun_n_slimit_return_tables(sql,num,tables,per_table_num,base_fun,replace_fun)
        sql = "select count(*) from (%s)" %sql
        self.sql_data_limit_retun_n_slimit_return_tables(sql,num,tables,per_table_num,base_fun,replace_fun) 
        sql_union = "(%s) union (%s)" %(sql,sql)
        self.sql_data_limit_retun_n_slimit_return_tables(sql_union,num,tables,per_table_num,base_fun,replace_fun) 
        sql_union_all = "(%s) union all (%s)" %(sql,sql_union)
        self.sql_data_limit_retun_n_slimit_return_tables(sql_union_all,num,tables,per_table_num,base_fun,replace_fun) 
        
        sql_join  = "select count(*) cc from %s.meters a,%s.meters b where a.ts is not null and a.ts = b.ts group by b.tbname order by cc limit %d" %(dbname,dbnamejoin,num)
        self.sql_limit_retun_n_slimit_return_tables(sql_join,num,tables,per_table_num,base_fun,replace_fun)
        sql_join = "select count(*) from (%s)" %sql_join
        self.sql_data_limit_retun_n_slimit_return_tables(sql_join,num,tables,per_table_num,base_fun,replace_fun) 
        sql_union = "(%s) union (%s)" %(sql_join,sql_join)
        self.sql_data_limit_retun_n_slimit_return_tables(sql_union,num,tables,per_table_num,base_fun,replace_fun) 
        sql_union_all = "(%s) union all (%s)" %(sql_join,sql_union)
        self.sql_data_limit_retun_n_slimit_return_tables(sql_union_all,num,tables,per_table_num,base_fun,replace_fun) 
        
        
        
        self.logger.info("count query ---------8----------")
        sql = "select count(*) cc from %s.meters where ts is not null partition by tbname order by cc limit %d" %(dbname,num)
        self.sql_limit_retun_n_slimit_return_tables(sql,num,tables,per_table_num,base_fun,replace_fun)
        sql = "select count(*) from (%s)" %sql
        self.sql_data_limit_retun_n_slimit_return_tables(sql,num,tables,per_table_num,base_fun,replace_fun) 
        sql_union = "(%s) union (%s)" %(sql,sql)
        self.sql_data_limit_retun_n_slimit_return_tables(sql_union,num,tables,per_table_num,base_fun,replace_fun) 
        sql_union_all = "(%s) union all (%s)" %(sql,sql_union)
        self.sql_data_limit_retun_n_slimit_return_tables(sql_union_all,num,tables,per_table_num,base_fun,replace_fun) 
        
        sql_join = "select count(*) cc from %s.meters a,%s.meters b where a.ts is not null and a.ts = b.ts partition by b.tbname order by cc limit %d" %(dbname,dbnamejoin,num)
        self.sql_limit_retun_n_slimit_return_tables(sql_join,num,tables,per_table_num,base_fun,replace_fun)
        sql_join = "select count(*) from (%s)" %sql_join
        self.sql_data_limit_retun_n_slimit_return_tables(sql_join,num,tables,per_table_num,base_fun,replace_fun) 
        sql_union = "(%s) union (%s)" %(sql_join,sql_join)
        self.sql_data_limit_retun_n_slimit_return_tables(sql_union,num,tables,per_table_num,base_fun,replace_fun) 
        sql_union_all = "(%s) union all (%s)" %(sql_join,sql_union)
        self.sql_data_limit_retun_n_slimit_return_tables(sql_union_all,num,tables,per_table_num,base_fun,replace_fun) 
        
        
        
        self.logger.info("count query ---------9----------")
        sql = "select count(*) cc from %s.meters where ts is not null interval(1a) limit %d" %(dbname,num)
        self.sql_limit_retun_n_slimit_return_error(sql,num,tables,per_table_num,base_fun,replace_fun)
        sql = "select count(*) from (%s)" %sql
        self.sql_data_limit_retun_n_slimit_return_error(sql,num,tables,per_table_num,base_fun,replace_fun)
        sql_union = "(%s) union (%s)" %(sql,sql)
        self.sql_data_limit_retun_n_slimit_return_error(sql_union,num,tables,per_table_num,base_fun,replace_fun) 
        sql_union_all = "(%s) union all (%s)" %(sql,sql_union)
        self.sql_data_limit_retun_n_slimit_return_error(sql_union_all,num,tables,per_table_num,base_fun,replace_fun) 
        
        sql_join  = "select count(*) cc from %s.meters a,%s.meters b where a.ts is not null and a.ts = b.ts interval(1a) limit %d" %(dbname,dbnamejoin,num)
        self.sql_limit_retun_n_slimit_return_error(sql_join,num,tables,per_table_num,base_fun,replace_fun)
        sql_join = "select count(*) from (%s)" %sql_join
        self.sql_data_limit_retun_n_slimit_return_error(sql_join,num,tables,per_table_num,base_fun,replace_fun)
        sql_union = "(%s) union (%s)" %(sql_join,sql_join)
        self.sql_data_limit_retun_n_slimit_return_error(sql_union,num,tables,per_table_num,base_fun,replace_fun) 
        sql_union_all = "(%s) union all (%s)" %(sql_join,sql_union)
        self.sql_data_limit_retun_n_slimit_return_error(sql_union_all,num,tables,per_table_num,base_fun,replace_fun) 
        
        
        
        self.logger.info("count query ---------10----------")
        sql = "select count(*) cc from %s.meters where ts is not null interval(1a) order by cc asc limit %d" %(dbname,num)
        self.sql_limit_retun_n_slimit_return_error(sql,num,tables,per_table_num,base_fun,replace_fun)
        sql = "select count(*) from (%s)" %sql
        self.sql_data_limit_retun_n_slimit_return_error(sql,num,tables,per_table_num,base_fun,replace_fun)
        sql_union = "(%s) union (%s)" %(sql,sql)
        self.sql_data_limit_retun_n_slimit_return_error(sql_union,num,tables,per_table_num,base_fun,replace_fun) 
        sql_union_all = "(%s) union all (%s)" %(sql,sql_union)
        self.sql_data_limit_retun_n_slimit_return_error(sql_union_all,num,tables,per_table_num,base_fun,replace_fun) 
        
        sql_join = "select count(*) cc from %s.meters a,%s.meters b where a.ts is not null and a.ts = b.ts interval(1a) order by cc asc limit %d" %(dbname,dbnamejoin,num)
        self.sql_limit_retun_n_slimit_return_error(sql_join,num,tables,per_table_num,base_fun,replace_fun)
        sql_join = "select count(*) from (%s)" %sql_join
        self.sql_data_limit_retun_n_slimit_return_error(sql_join,num,tables,per_table_num,base_fun,replace_fun)
        sql_union = "(%s) union (%s)" %(sql_join,sql_join)
        self.sql_data_limit_retun_n_slimit_return_error(sql_union,num,tables,per_table_num,base_fun,replace_fun) 
        sql_union_all = "(%s) union all (%s)" %(sql_join,sql_union)
        self.sql_data_limit_retun_n_slimit_return_error(sql_union_all,num,tables,per_table_num,base_fun,replace_fun) 
        
        
        
        self.logger.info("count query ---------11----------")
        sql = "select count(*) cc from %s.meters where ts is not null interval(1a) order by cc desc limit %d" %(dbname,num)
        self.sql_limit_retun_n_slimit_return_error(sql,num,tables,per_table_num,base_fun,replace_fun)
        sql = "select count(*) from (%s)" %sql
        self.sql_data_limit_retun_n_slimit_return_error(sql,num,tables,per_table_num,base_fun,replace_fun)
        sql_union = "(%s) union (%s)" %(sql,sql)
        self.sql_data_limit_retun_n_slimit_return_error(sql_union,num,tables,per_table_num,base_fun,replace_fun) 
        sql_union_all = "(%s) union all (%s)" %(sql,sql_union)
        self.sql_data_limit_retun_n_slimit_return_error(sql_union_all,num,tables,per_table_num,base_fun,replace_fun) 
        
        sql_join = "select count(*) cc from %s.meters a,%s.meters b where a.ts is not null and a.ts = b.ts interval(1a) order by cc desc limit %d" %(dbname,dbnamejoin,num)
        self.sql_limit_retun_n_slimit_return_error(sql_join,num,tables,per_table_num,base_fun,replace_fun)
        sql_join = "select count(*) from (%s)" %sql_join
        self.sql_data_limit_retun_n_slimit_return_error(sql_join,num,tables,per_table_num,base_fun,replace_fun)
        sql_union = "(%s) union (%s)" %(sql_join,sql_join)
        self.sql_data_limit_retun_n_slimit_return_error(sql_union,num,tables,per_table_num,base_fun,replace_fun) 
        sql_union_all = "(%s) union all (%s)" %(sql_join,sql_union)
        self.sql_data_limit_retun_n_slimit_return_error(sql_union_all,num,tables,per_table_num,base_fun,replace_fun) 
        
        
        
        self.logger.info("count query ---------12----------")
        sql = "select tbname,count(*) cc from %s.meters where ts is not null interval(1a) group by tbname limit %d" %(dbname,num)
        self.sql_retun_error(sql,base_fun,replace_fun)
        sql = "select count(*) from (%s)" %sql
        self.sql_retun_error(sql,base_fun,replace_fun)
        sql_union = "(%s) union (%s)" %(sql,sql)
        self.sql_retun_error(sql_union,base_fun,replace_fun)
        sql_union_all = "(%s) union all (%s)" %(sql,sql_union)
        self.sql_retun_error(sql_union_all,base_fun,replace_fun)
        
        sql_join = "select a.tbname,count(*) cc from %s.meters a,%s.meters b where a.ts is not null and a.ts = b.ts interval(1a) group by b.tbname limit %d" %(dbname,dbnamejoin,num)
        self.sql_retun_error(sql_join,base_fun,replace_fun)
        sql_join = "select count(*) from (%s)" %sql_join
        self.sql_retun_error(sql_join,base_fun,replace_fun)
        sql_union = "(%s) union (%s)" %(sql_join,sql_join)
        self.sql_retun_error(sql_union,base_fun,replace_fun)
        sql_union_all = "(%s) union all (%s)" %(sql_join,sql_union)
        self.sql_retun_error(sql_union_all,base_fun,replace_fun)
        
        
        
        self.logger.info("count query ---------13----------")
        sql = "select tbname,count(*) cc from %s.meters where ts is not null interval(1a) partition by tbname limit %d" %(dbname,num)
        self.sql_retun_error(sql,base_fun,replace_fun)
        sql = "select count(*) from (%s)" %sql
        self.sql_retun_error(sql,base_fun,replace_fun)
        sql_union = "(%s) union (%s)" %(sql,sql)
        self.sql_retun_error(sql_union,base_fun,replace_fun)
        sql_union_all = "(%s) union all (%s)" %(sql,sql_union)
        self.sql_retun_error(sql_union_all,base_fun,replace_fun)
        
        sql_join = "select a.tbname,count(*) cc from %s.meters a,%s.meters b where a.ts is not null and a.ts = b.ts interval(1a) partition by b.tbname limit %d" %(dbname,dbnamejoin,num)
        self.sql_retun_error(sql_join,base_fun,replace_fun)
        sql_join = "select count(*) from (%s)" %sql_join
        self.sql_retun_error(sql_join,base_fun,replace_fun)
        sql_union = "(%s) union (%s)" %(sql_join,sql_join)
        self.sql_retun_error(sql_union,base_fun,replace_fun)
        sql_union_all = "(%s) union all (%s)" %(sql_join,sql_union)
        self.sql_retun_error(sql_union_all,base_fun,replace_fun)
        
        
        
        self.logger.info("count query ---------14----------")
        sql = "select tbname,count(*) cc from %s.meters where ts is not null partition by tbname interval(1a) limit %d" %(dbname,num)
        self.sql_limit_retun_tables_times_n_slimit_return_per_table_num_times_n(sql,num,tables,per_table_num,base_fun,replace_fun)
        sql = "select count(*) from (%s)" %sql
        self.sql_data_limit_retun_tables_times_n_slimit_return_per_table_num_times_n(sql,num,tables,per_table_num,base_fun,replace_fun)
        sql_union = "(%s) union (%s)" %(sql,sql)
        self.sql_data_limit_retun_tables_times_n_slimit_return_per_table_num_times_n(sql_union,num,tables,per_table_num,base_fun,replace_fun)
        sql_union_all = "(%s) union all (%s)" %(sql,sql_union)
        self.sql_data_limit_retun_tables_times_n_slimit_return_per_table_num_times_n(sql_union_all,num,tables,per_table_num,base_fun,replace_fun)
        
        sql_join = "select a.tbname,count(*) cc from %s.meters a,%s.meters b where a.ts is not null and a.ts = b.ts partition by a.tbname interval(1a) limit %d" %(dbname,dbnamejoin,num)
        self.sql_limit_retun_tables_times_n_slimit_return_per_table_num_times_n(sql_join,num,tables,per_table_num,base_fun,replace_fun)
        sql_join = "select count(*) from (%s)" %sql_join
        self.sql_data_limit_retun_tables_times_n_slimit_return_per_table_num_times_n(sql_join,num,tables,per_table_num,base_fun,replace_fun)
        sql_union = "(%s) union (%s)" %(sql_join,sql_join)
        self.sql_data_limit_retun_tables_times_n_slimit_return_per_table_num_times_n(sql_union,num,tables,per_table_num,base_fun,replace_fun)
        sql_union_all = "(%s) union all (%s)" %(sql_join,sql_union)
        self.sql_data_limit_retun_tables_times_n_slimit_return_per_table_num_times_n(sql_union_all,num,tables,per_table_num,base_fun,replace_fun)
        
        
        
        self.logger.info("count query ---------15----------")
        sql = "select tbname,count(*) cc from %s.meters where ts is not null partition by tbname interval(1a) order by cc asc limit %d" %(dbname,num)
        self.sql_limit_retun_n_slimit_return_per_table_num_times_tables(sql,num,tables,per_table_num,base_fun,replace_fun)
        sql = "select count(*) from (%s)" %sql
        self.sql_data_limit_retun_n_slimit_return_per_table_num_times_tables(sql,num,tables,per_table_num,base_fun,replace_fun)
        sql_union = "(%s) union (%s)" %(sql,sql)
        self.sql_data_limit_retun_n_slimit_return_per_table_num_times_tables(sql_union,num,tables,per_table_num,base_fun,replace_fun)
        sql_union_all = "(%s) union all (%s)" %(sql,sql_union)
        self.sql_data_limit_retun_n_slimit_return_per_table_num_times_tables(sql_union_all,num,tables,per_table_num,base_fun,replace_fun)
        
        sql_join = "select a.tbname,count(*) cc from %s.meters a,%s.meters b where a.ts is not null and a.ts = b.ts partition by a.tbname interval(1a) order by cc asc limit %d" %(dbname,dbnamejoin,num)
        self.sql_limit_retun_n_slimit_return_per_table_num_times_tables(sql_join,num,tables,per_table_num,base_fun,replace_fun)
        sql_join = "select count(*) from (%s)" %sql_join
        self.sql_data_limit_retun_n_slimit_return_per_table_num_times_tables(sql_join,num,tables,per_table_num,base_fun,replace_fun)
        sql_union = "(%s) union (%s)" %(sql_join,sql_join)
        self.sql_data_limit_retun_n_slimit_return_per_table_num_times_tables(sql_union,num,tables,per_table_num,base_fun,replace_fun)
        sql_union_all = "(%s) union all (%s)" %(sql_join,sql_union)
        self.sql_data_limit_retun_n_slimit_return_per_table_num_times_tables(sql_union_all,num,tables,per_table_num,base_fun,replace_fun)
        
        
        
        self.logger.info("count query ---------16----------")
        sql = "select tbname,count(*) cc from %s.meters where ts is not null partition by tbname interval(1a) order by cc desc limit %d" %(dbname,num)
        self.sql_limit_retun_n_slimit_return_per_table_num_times_tables(sql,num,tables,per_table_num,base_fun,replace_fun)
        sql = "select count(*) from (%s)" %sql
        self.sql_data_limit_retun_n_slimit_return_per_table_num_times_tables(sql,num,tables,per_table_num,base_fun,replace_fun)
        sql_union = "(%s) union (%s)" %(sql,sql)
        self.sql_data_limit_retun_n_slimit_return_per_table_num_times_tables(sql_union,num,tables,per_table_num,base_fun,replace_fun)
        sql_union_all = "(%s) union all (%s)" %(sql,sql_union)
        self.sql_data_limit_retun_n_slimit_return_per_table_num_times_tables(sql_union_all,num,tables,per_table_num,base_fun,replace_fun)
        
        sql_join = "select a.tbname,count(*) cc from %s.meters a,%s.meters b where a.ts is not null and a.ts = b.ts partition by a.tbname interval(1a) order by cc desc limit %d" %(dbname,dbnamejoin,num)
        self.sql_limit_retun_n_slimit_return_per_table_num_times_tables(sql_join,num,tables,per_table_num,base_fun,replace_fun)
        sql_join = "select count(*) from (%s)" %sql_join
        self.sql_data_limit_retun_n_slimit_return_per_table_num_times_tables(sql_join,num,tables,per_table_num,base_fun,replace_fun)
        sql_union = "(%s) union (%s)" %(sql_join,sql_join)
        self.sql_data_limit_retun_n_slimit_return_per_table_num_times_tables(sql_union,num,tables,per_table_num,base_fun,replace_fun)
        sql_union_all = "(%s) union all (%s)" %(sql_join,sql_union)
        self.sql_data_limit_retun_n_slimit_return_per_table_num_times_tables(sql_union_all,num,tables,per_table_num,base_fun,replace_fun)
        
        
        
        self.logger.info("count query ---------17----------")
        sql = "select tbname,count(*) cc from %s.meters where ts is not null partition by tbname interval(1a) slimit %d" %(dbname,num)
        self.sql_limit_not_test_slimitkeep_return_per_table_num_times_n(sql,num,tables,per_table_num,base_fun,replace_fun)
        sql = "select count(*) from (%s)" %sql
        self.sql_data_limit_not_test_slimitkeep_return_per_table_num_times_n(sql,num,tables,per_table_num,base_fun,replace_fun)
        sql_union = "(%s) union (%s)" %(sql,sql)
        self.sql_data_limit_not_test_slimitkeep_return_per_table_num_times_n(sql_union,num,tables,per_table_num,base_fun,replace_fun)
        sql_union_all = "(%s) union all (%s)" %(sql,sql_union)
        self.sql_data_limit_not_test_slimitkeep_return_per_table_num_times_n(sql_union_all,num,tables,per_table_num,base_fun,replace_fun)
        
        sql_join = "select a.tbname,count(*) cc from %s.meters a,%s.meters b where a.ts is not null and a.ts = b.ts partition by a.tbname interval(1a) slimit %d" %(dbname,dbnamejoin,num)
        self.sql_limit_not_test_slimitkeep_return_per_table_num_times_n(sql_join,num,tables,per_table_num,base_fun,replace_fun)
        sql_join = "select count(*) from (%s)" %sql_join
        self.sql_data_limit_not_test_slimitkeep_return_per_table_num_times_n(sql_join,num,tables,per_table_num,base_fun,replace_fun)
        sql_union = "(%s) union (%s)" %(sql_join,sql_join)
        self.sql_data_limit_not_test_slimitkeep_return_per_table_num_times_n(sql_union,num,tables,per_table_num,base_fun,replace_fun)
        sql_union_all = "(%s) union all (%s)" %(sql_join,sql_union)
        self.sql_data_limit_not_test_slimitkeep_return_per_table_num_times_n(sql_union_all,num,tables,per_table_num,base_fun,replace_fun)
        
        
        
        self.logger.info("count query ---------18----------")
        sql = "select tbname,count(*) cc from %s.meters where ts is not null partition by tbname interval(1a) order by cc asc slimit %d" %(dbname,num)
        self.sql_limit_not_test_slimitkeep_return_per_table_num_times_tables(sql,num,tables,per_table_num,base_fun,replace_fun)
        sql = "select count(*) from (%s)" %sql
        self.sql_data_limit_not_test_slimitkeep_return_per_table_num_times_tables(sql,num,tables,per_table_num,base_fun,replace_fun)
        sql_union = "(%s) union (%s)" %(sql,sql)
        self.sql_data_limit_not_test_slimitkeep_return_per_table_num_times_tables(sql_union,num,tables,per_table_num,base_fun,replace_fun)
        sql_union_all = "(%s) union all (%s)" %(sql,sql_union)
        self.sql_data_limit_not_test_slimitkeep_return_per_table_num_times_tables(sql_union_all,num,tables,per_table_num,base_fun,replace_fun)
        
        sql_join = "select a.tbname,count(*) cc from %s.meters a,%s.meters b where a.ts is not null and a.ts = b.ts partition by a.tbname interval(1a) order by cc asc slimit %d" %(dbname,dbnamejoin,num)
        self.sql_limit_not_test_slimitkeep_return_per_table_num_times_tables(sql_join,num,tables,per_table_num,base_fun,replace_fun)
        sql_join = "select count(*) from (%s)" %sql_join
        self.sql_data_limit_not_test_slimitkeep_return_per_table_num_times_tables(sql_join,num,tables,per_table_num,base_fun,replace_fun)
        sql_union = "(%s) union (%s)" %(sql_join,sql_join)
        self.sql_data_limit_not_test_slimitkeep_return_per_table_num_times_tables(sql_union,num,tables,per_table_num,base_fun,replace_fun)
        sql_union_all = "(%s) union all (%s)" %(sql_join,sql_union)
        self.sql_data_limit_not_test_slimitkeep_return_per_table_num_times_tables(sql_union_all,num,tables,per_table_num,base_fun,replace_fun)
        
        
        self.logger.info("count query ---------19----------")
        sql = "select tbname,count(*) cc from %s.meters where ts is not null partition by tbname interval(1a) order by cc desc slimit %d" %(dbname,num)
        self.sql_limit_not_test_slimitkeep_return_per_table_num_times_tables(sql,num,tables,per_table_num,base_fun,replace_fun)
        sql = "select count(*) from (%s)" %sql
        self.sql_data_limit_not_test_slimitkeep_return_per_table_num_times_tables(sql,num,tables,per_table_num,base_fun,replace_fun)
        sql_union = "(%s) union (%s)" %(sql,sql)
        self.sql_data_limit_not_test_slimitkeep_return_per_table_num_times_tables(sql_union,num,tables,per_table_num,base_fun,replace_fun)
        sql_union_all = "(%s) union all (%s)" %(sql,sql_union)
        self.sql_data_limit_not_test_slimitkeep_return_per_table_num_times_tables(sql_union_all,num,tables,per_table_num,base_fun,replace_fun)
        
        sql_join = "select a.tbname,count(*) cc from %s.meters a,%s.meters b where a.ts is not null and a.ts = b.ts partition by a.tbname interval(1a) order by cc desc slimit %d" %(dbname,dbnamejoin,num)
        self.sql_limit_not_test_slimitkeep_return_per_table_num_times_tables(sql_join,num,tables,per_table_num,base_fun,replace_fun)
        sql_join = "select count(*) from (%s)" %sql_join
        self.sql_data_limit_not_test_slimitkeep_return_per_table_num_times_tables(sql_join,num,tables,per_table_num,base_fun,replace_fun)
        sql_union = "(%s) union (%s)" %(sql_join,sql_join)
        self.sql_data_limit_not_test_slimitkeep_return_per_table_num_times_tables(sql_union,num,tables,per_table_num,base_fun,replace_fun)
        sql_union_all = "(%s) union all (%s)" %(sql_join,sql_union)
        self.sql_data_limit_not_test_slimitkeep_return_per_table_num_times_tables(sql_union_all,num,tables,per_table_num,base_fun,replace_fun)
        
        
        
        self.logger.info("count query ---------20----------")
        sql = "select tbname,count(*) cc from %s.meters where ts is not null partition by tbname interval(1a) slimit %d limit %d" %(dbname,num,num2)
        self.sql_limit_retun_n_times_n2_slimitkeep_return_n_times_n2(sql,num,num2,tables,per_table_num,base_fun,replace_fun)
        sql = "select count(*) from (%s)" %sql
        self.sql_data_limit_retun_n_times_n2_slimitkeep_return_n_times_n2(sql,num,num2,tables,per_table_num,base_fun,replace_fun)
        sql_union = "(%s) union (%s)" %(sql,sql)
        self.sql_data_limit_retun_n_times_n2_slimitkeep_return_n_times_n2(sql_union,num,num2,tables,per_table_num,base_fun,replace_fun)
        sql_union_all = "(%s) union all (%s)" %(sql,sql_union)
        self.sql_data_limit_retun_n_times_n2_slimitkeep_return_n_times_n2(sql_union_all,num,num2,tables,per_table_num,base_fun,replace_fun)
        
        sql_join = "select a.tbname,count(*) cc from %s.meters a,%s.meters b where a.ts is not null and a.ts = b.ts partition by a.tbname interval(1a) slimit %d limit %d" %(dbname,dbnamejoin,num,num2)
        self.sql_limit_retun_n_times_n2_slimitkeep_return_n_times_n2(sql_join,num,num2,tables,per_table_num,base_fun,replace_fun)
        sql_join = "select count(*) from (%s)" %sql_join
        self.sql_data_limit_retun_n_times_n2_slimitkeep_return_n_times_n2(sql_join,num,num2,tables,per_table_num,base_fun,replace_fun)
        sql_union = "(%s) union (%s)" %(sql_join,sql_join)
        self.sql_data_limit_retun_n_times_n2_slimitkeep_return_n_times_n2(sql_union,num,num2,tables,per_table_num,base_fun,replace_fun)
        sql_union_all = "(%s) union all (%s)" %(sql_join,sql_union)
        self.sql_data_limit_retun_n_times_n2_slimitkeep_return_n_times_n2(sql_union_all,num,num2,tables,per_table_num,base_fun,replace_fun)
        
        
        self.logger.info("count query ---------21----------")
        sql = "select tbname,count(*) cc from %s.meters where ts is not null partition by tbname interval(1a) order by cc asc slimit %d limit %d" %(dbname,num,num2)
        self.sql_limit_times_slimitkeep_return_n2(sql,num,num2,tables,per_table_num,base_fun,replace_fun)
        sql = "select count(*) from (%s)" %sql
        self.sql_data_limit_times_slimitkeep_return_n2(sql,num,num2,tables,per_table_num,base_fun,replace_fun)
        sql_union = "(%s) union (%s)" %(sql,sql)
        self.sql_data_limit_times_slimitkeep_return_n2(sql_union,num,num2,tables,per_table_num,base_fun,replace_fun)
        sql_union_all = "(%s) union all (%s)" %(sql,sql_union)
        self.sql_data_limit_times_slimitkeep_return_n2(sql_union_all,num,num2,tables,per_table_num,base_fun,replace_fun)       
        
        sql_join = "select a.tbname,count(*) cc from %s.meters a,%s.meters b where a.ts is not null and a.ts = b.ts partition by a.tbname interval(1a) order by cc asc slimit %d limit %d" %(dbname,dbnamejoin,num,num2)
        self.sql_limit_times_slimitkeep_return_n2(sql_join,num,num2,tables,per_table_num,base_fun,replace_fun)
        sql_join = "select count(*) from (%s)" %sql_join
        self.sql_data_limit_times_slimitkeep_return_n2(sql_join,num,num2,tables,per_table_num,base_fun,replace_fun)
        sql_union = "(%s) union (%s)" %(sql_join,sql_join)
        self.sql_data_limit_times_slimitkeep_return_n2(sql_union,num,num2,tables,per_table_num,base_fun,replace_fun)
        sql_union_all = "(%s) union all (%s)" %(sql_join,sql_union)
        self.sql_data_limit_times_slimitkeep_return_n2(sql_union_all,num,num2,tables,per_table_num,base_fun,replace_fun)
        
        
        self.logger.info("count query ---------22----------")
        sql = "select tbname,count(*) cc from %s.meters where ts is not null partition by tbname interval(1a) order by cc desc slimit %d limit %d" %(dbname,num,num2)
        self.sql_limit_times_slimitkeep_return_n2(sql,num,num2,tables,per_table_num,base_fun,replace_fun)
        sql = "select count(*) from (%s)" %sql
        self.sql_data_limit_times_slimitkeep_return_n2(sql,num,num2,tables,per_table_num,base_fun,replace_fun)
        sql_union = "(%s) union (%s)" %(sql,sql)
        self.sql_data_limit_times_slimitkeep_return_n2(sql_union,num,num2,tables,per_table_num,base_fun,replace_fun)
        sql_union_all = "(%s) union all (%s)" %(sql,sql_union)
        self.sql_data_limit_times_slimitkeep_return_n2(sql_union_all,num,num2,tables,per_table_num,base_fun,replace_fun)
        
        sql_join = "select a.tbname,count(*) cc from %s.meters a,%s.meters b where a.ts is not null and a.ts = b.ts partition by a.tbname interval(1a) order by cc desc slimit %d limit %d" %(dbname,dbnamejoin,num,num2)
        self.sql_limit_times_slimitkeep_return_n2(sql_join,num,num2,tables,per_table_num,base_fun,replace_fun)
        sql_join = "select count(*) from (%s)" %sql_join
        self.sql_data_limit_times_slimitkeep_return_n2(sql_join,num,num2,tables,per_table_num,base_fun,replace_fun)
        sql_union = "(%s) union (%s)" %(sql_join,sql_join)
        self.sql_data_limit_times_slimitkeep_return_n2(sql_union,num,num2,tables,per_table_num,base_fun,replace_fun)
        sql_union_all = "(%s) union all (%s)" %(sql_join,sql_union)
        self.sql_data_limit_times_slimitkeep_return_n2(sql_union_all,num,num2,tables,per_table_num,base_fun,replace_fun)
    
    def run_sql(self,dbname,tables,per_table_num,dbnamejoin):
        
        num,num2 = random.randint(10,100),random.randint(10,100)
        self.fun_count(dbname,num,num2,self.tables,self.per_table_num,self.dbnamejoin_local,'count','count')

        self.tdSql.execute(" flush database %s;" %self.dbnamejoin_local)

        self.fun_count(dbname,num,num2,self.tables,self.per_table_num,self.dbnamejoin_local,'count','count')     
                                              
    def run(self):
        startTime = time.time() 
        
        self.tdCreateData.alter_local_slowlogthreshold()  #设置慢查询
        self.tdSql.query("alter local 'schedulePolicy' '%d';" %random.randint(1,3))
        
        self.benchmark_insert_stb(self.source_taosd_list,self.dbname_other_local,'stb',self.tables,self.per_table_num,self.vgroups,self.replica) 
        self.base_sql_count(self.dbname_other_local,self.tables,self.per_table_num)
        self.benchmark_insert_stb(self.source_taosd_list,self.dbnamejoin_local,'stb',self.join_tables,self.join_per_table_num,self.join_vgroups,self.replica) 
        self.base_sql_count(self.dbnamejoin_local,self.join_tables,self.join_per_table_num)
        
        #self.run_sql(self.dbname,self.tables,self.per_table_num,self.dbnamejoin_local)   #前面用base的，后面用local的
        self.run_sql(self.dbname_other_local,self.tables,self.per_table_num,self.dbnamejoin_local)   #前面用base_local的,解决不同容器的错误，后面用local的
        
        self.drop_db_table(self.dbname_other_local)  #共用时可以删除
        self.drop_db_table(self.dbnamejoin_local)
        
        endTime = time.time()
        
    
        self.logger.info("total time %ds" % (endTime - startTime))
    

