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
#from .nestedQueryInterval import *
from .nestedQuery import *
from faker import Faker
import random

class TDTestCase(TDTestCase):
    
    def data_check_tbname(self,sql,groupby='Y',partitionby='Y',base_fun='',replace_fun='',base_column='',replace_column=''):
        
        if groupby=='Y':
            sql = sql.replace(f'{base_fun}',f'{replace_fun}').replace(f'{base_column}',f'{replace_column}')
            tdSql.query(sql)
            tdSql.checkRows(12)
            tdSql.checkData(0, 1, 100)
            tdSql.checkData(1, 1, 200)
            tdSql.checkData(2, 1, 00)
            tdSql.checkData(3, 1, 0)
            tdSql.checkData(4, 1, 00)
            tdSql.checkData(5, 1, 0)
            tdSql.checkData(6, 1, 0)
            tdSql.checkData(7, 1, 00)
            tdSql.checkData(8, 1, 00)
            tdSql.checkData(9, 1, 0)
            tdSql.checkData(10, 1, 00)
            tdSql.checkData(11, 1, 0)
        elif groupby=='UNIONALL':
            sql = sql.replace(f'{base_fun}',f'{replace_fun}').replace(f'{base_column}',f'{replace_column}')
            tdSql.query(sql)
            tdSql.checkRows(24)
            tdSql.checkData(0, 1, 100)
            tdSql.checkData(1, 1, 100)
            tdSql.checkData(2, 1, 200)
            tdSql.checkData(3, 1, 200)
            tdSql.checkData(4, 1, 00)
            tdSql.checkData(5, 1, 0)
            tdSql.checkData(6, 1, 0)
            tdSql.checkData(7, 1, 00)
            tdSql.checkData(8, 1, 00)
            tdSql.checkData(9, 1, 0)
            tdSql.checkData(10, 1, 00)
            tdSql.checkData(11, 1, 0)
        elif groupby=='UNIONALLNULL':
            sql = sql.replace(f'{base_fun}',f'{replace_fun}').replace(f'{base_column}',f'{replace_column}')
            tdSql.query(sql)
            tdSql.checkRows(24)
            tdSql.checkData(0, 1, 0)
            tdSql.checkData(1, 1, 00)
            tdSql.checkData(2, 1, 00)
            tdSql.checkData(3, 1, 0)
            tdSql.checkData(4, 1, 00)
            tdSql.checkData(5, 1, 0)
            tdSql.checkData(6, 1, 0)
            tdSql.checkData(7, 1, 00)
            tdSql.checkData(8, 1, 00)
            tdSql.checkData(9, 1, 0)
            tdSql.checkData(10, 1, 00)
            tdSql.checkData(11, 1, 0)
        elif groupby=='NULL':
            sql = sql.replace(f'{base_fun}',f'{replace_fun}').replace(f'{base_column}',f'{replace_column}')
            tdSql.query(sql)
            tdSql.checkRows(12)
            tdSql.checkData(0, 1, 0)
            tdSql.checkData(1, 1, 0)
            tdSql.checkData(2, 1, 00)
            tdSql.checkData(3, 1, 0)
            tdSql.checkData(4, 1, 00)
            tdSql.checkData(5, 1, 0)
            tdSql.checkData(6, 1, 0)
            tdSql.checkData(7, 1, 00)
            tdSql.checkData(8, 1, 00)
            tdSql.checkData(9, 1, 0)
            tdSql.checkData(10, 1, 00)
            tdSql.checkData(11, 1, 0)
        elif groupby=='HAVING=0':
            sql = sql.replace(f'{base_fun}',f'{replace_fun}').replace(f'{base_column}',f'{replace_column}')
            tdSql.query(sql)
            tdSql.checkRows(10)
            tdSql.checkData(0, 1, 0)
            tdSql.checkData(1, 1, 0)
            tdSql.checkData(2, 1, 00)
            tdSql.checkData(3, 1, 0)
            tdSql.checkData(4, 1, 00)
            tdSql.checkData(5, 1, 0)
            tdSql.checkData(6, 1, 0)
            tdSql.checkData(7, 1, 00)
            tdSql.checkData(8, 1, 00)
            tdSql.checkData(9, 1, 0)
        elif groupby=='HAVING>0':
            sql = sql.replace(f'{base_fun}',f'{replace_fun}').replace(f'{base_column}',f'{replace_column}')
            tdSql.query(sql)
            tdSql.checkRows(2)
            tdSql.checkData(0, 1, 100)
            tdSql.checkData(1, 1, 200)
        elif groupby=='HAVING>04':
            sql = sql.replace(f'{base_fun}',f'{replace_fun}').replace(f'{base_column}',f'{replace_column}')
            tdSql.query(sql)
            tdSql.checkRows(4)
            tdSql.checkData(0, 1, 100)
            tdSql.checkData(1, 1, 100)
            tdSql.checkData(2, 1, 200)
            tdSql.checkData(3, 1, 200)
        elif groupby=='HAVINGCOLUMN=0':
            sql = sql.replace(f'{base_fun}',f'{replace_fun}').replace(f'{base_column}',f'{replace_column}')
            tdSql.query(sql)
            tdSql.checkRows(2)
            tdSql.checkData(0, 1, 0)
            tdSql.checkData(1, 1, 0)
        elif groupby=='NOTSUPPORT':
            sql = sql.replace(f'{base_fun}',f'{replace_fun}').replace(f'{base_column}',f'{replace_column}')
            tdSql.query(sql)
            tdSql.checkRows(2)
            tdSql.checkData(0, 1, 0)
            tdSql.checkData(1, 1, 0)
        elif groupby=='AGG0':
            sql = sql.replace(f'{base_fun}',f'{replace_fun}').replace(f'{base_column}',f'{replace_column}')
            tdSql.query(sql)
            tdSql.checkRows(0)
        elif groupby=='AGG2':
            sql = sql.replace(f'{base_fun}',f'{replace_fun}').replace(f'{base_column}',f'{replace_column}')
            tdSql.query(sql)
            tdSql.checkRows(2)
        elif groupby=='AGG2_NULL':
            sql = sql.replace(f'{base_fun}',f'{replace_fun}').replace(f'{base_column}',f'{replace_column}')
            tdSql.query(sql)
            tdSql.checkRows(2)
            tdSql.checkData(0, 1, 'None')
            tdSql.checkData(1, 1, 'None')
        elif groupby=='AGG4':
            sql = sql.replace(f'{base_fun}',f'{replace_fun}').replace(f'{base_column}',f'{replace_column}')
            tdSql.query(sql)
            tdSql.checkRows(4)
        elif groupby=='AGG4_NULL':
            sql = sql.replace(f'{base_fun}',f'{replace_fun}').replace(f'{base_column}',f'{replace_column}')
            tdSql.query(sql)
            tdSql.checkRows(4)
            tdSql.checkData(0, 1, 'None')
            tdSql.checkData(1, 1, 'None')
        elif groupby=='AGG10':
            sql = sql.replace(f'{base_fun}',f'{replace_fun}').replace(f'{base_column}',f'{replace_column}')
            tdSql.query(sql)
            tdSql.checkRows(10)
        elif groupby=='AGG12':
            sql = sql.replace(f'{base_fun}',f'{replace_fun}').replace(f'{base_column}',f'{replace_column}')
            tdSql.query(sql)
            tdSql.checkRows(12)
        elif groupby=='AGG20':
            sql = sql.replace(f'{base_fun}',f'{replace_fun}').replace(f'{base_column}',f'{replace_column}')
            tdSql.query(sql)
            tdSql.checkRows(20)
        elif groupby=='AGG24':
            sql = sql.replace(f'{base_fun}',f'{replace_fun}').replace(f'{base_column}',f'{replace_column}')
            tdSql.query(sql)
            tdSql.checkRows(24)
        else:
            sql = sql.replace(f'{base_fun}',f'{replace_fun}').replace(f'{base_column}',f'{replace_column}')
            tdSql.query(sql)
            tdSql.checkRows(12)
            
            
        if partitionby=='Y':
            sql = sql.replace(f'{base_fun}',f'{replace_fun}').replace(f'{base_column}',f'{replace_column}')
            sql = sql.replace('group','partition')
            tdSql.query(sql)
            tdSql.checkRows(12)
            tdSql.checkData(0, 1, 100)
            tdSql.checkData(1, 1, 200)
            tdSql.checkData(2, 1, 00)
            tdSql.checkData(3, 1, 0)
            tdSql.checkData(4, 1, 00)
            tdSql.checkData(5, 1, 0)
            tdSql.checkData(6, 1, 0)
            tdSql.checkData(7, 1, 00)
            tdSql.checkData(8, 1, 00)
            tdSql.checkData(9, 1, 0)
            tdSql.checkData(10, 1, 00)
            tdSql.checkData(11, 1, 0)
        elif groupby=='UNIONALL':
            sql = sql.replace(f'{base_fun}',f'{replace_fun}').replace(f'{base_column}',f'{replace_column}')
            sql = sql.replace('group','partition')
            tdSql.query(sql)
            tdSql.checkRows(24)
            tdSql.checkData(0, 1, 100)
            tdSql.checkData(1, 1, 100)
            tdSql.checkData(2, 1, 200)
            tdSql.checkData(3, 1, 200)
            tdSql.checkData(4, 1, 00)
            tdSql.checkData(5, 1, 0)
            tdSql.checkData(6, 1, 0)
            tdSql.checkData(7, 1, 00)
            tdSql.checkData(8, 1, 00)
            tdSql.checkData(9, 1, 0)
            tdSql.checkData(10, 1, 00)
            tdSql.checkData(11, 1, 0)
        elif groupby=='UNIONALLNULL':
            sql = sql.replace(f'{base_fun}',f'{replace_fun}').replace(f'{base_column}',f'{replace_column}')
            sql = sql.replace('group','partition')
            tdSql.query(sql)
            tdSql.checkRows(24)
            tdSql.checkData(0, 1, 00)
            tdSql.checkData(1, 1, 00)
            tdSql.checkData(2, 1, 00)
            tdSql.checkData(3, 1, 0)
            tdSql.checkData(4, 1, 00)
            tdSql.checkData(5, 1, 0)
            tdSql.checkData(6, 1, 0)
            tdSql.checkData(7, 1, 00)
            tdSql.checkData(8, 1, 00)
            tdSql.checkData(9, 1, 0)
            tdSql.checkData(10, 1, 00)
            tdSql.checkData(11, 1, 0)
        elif partitionby=='NULL':
            sql = sql.replace(f'{base_fun}',f'{replace_fun}').replace(f'{base_column}',f'{replace_column}')
            sql = sql.replace('group','partition')
            tdSql.query(sql)
            tdSql.checkRows(12)
            tdSql.checkData(0, 1, 0)
            tdSql.checkData(1, 1, 0)
            tdSql.checkData(2, 1, 00)
            tdSql.checkData(3, 1, 0)
            tdSql.checkData(4, 1, 00)
            tdSql.checkData(5, 1, 0)
            tdSql.checkData(6, 1, 0)
            tdSql.checkData(7, 1, 00)
            tdSql.checkData(8, 1, 00)
            tdSql.checkData(9, 1, 0)
            tdSql.checkData(10, 1, 00)
            tdSql.checkData(11, 1, 0)
        elif partitionby=='HAVING=0':
            sql = sql.replace(f'{base_fun}',f'{replace_fun}').replace(f'{base_column}',f'{replace_column}')
            sql = sql.replace('group','partition')
            tdSql.query(sql)
            tdSql.checkRows(10)
            tdSql.checkData(0, 1, 0)
            tdSql.checkData(1, 1, 0)
            tdSql.checkData(2, 1, 00)
            tdSql.checkData(3, 1, 0)
            tdSql.checkData(4, 1, 00)
            tdSql.checkData(5, 1, 0)
            tdSql.checkData(6, 1, 0)
            tdSql.checkData(7, 1, 00)
            tdSql.checkData(8, 1, 00)
            tdSql.checkData(9, 1, 0)
        elif partitionby=='HAVING>0':
            sql = sql.replace(f'{base_fun}',f'{replace_fun}').replace(f'{base_column}',f'{replace_column}')
            sql = sql.replace('group','partition')
            tdSql.query(sql)
            tdSql.checkRows(2)
            tdSql.checkData(0, 1, 100)
            tdSql.checkData(1, 1, 200)
        elif partitionby=='HAVING>04':
            sql = sql.replace(f'{base_fun}',f'{replace_fun}').replace(f'{base_column}',f'{replace_column}')
            tdSql.query(sql)
            tdSql.checkRows(4)
            tdSql.checkData(0, 1, 100)
            tdSql.checkData(1, 1, 100)
            tdSql.checkData(2, 1, 200)
            tdSql.checkData(3, 1, 200)
        elif partitionby=='HAVINGCOLUMN=0':
            sql = sql.replace(f'{base_fun}',f'{replace_fun}').replace(f'{base_column}',f'{replace_column}')
            sql = sql.replace('group','partition')
            tdSql.query(sql)
            tdSql.checkRows(2)
            tdSql.checkData(0, 1, 0)
            tdSql.checkData(1, 1, 0)
        elif partitionby=='NOTSUPPORT':
            sql = sql.replace(f'{base_fun}',f'{replace_fun}').replace(f'{base_column}',f'{replace_column}')
            sql = sql.replace('group','partition')
            tdSql.query(sql)
            tdSql.checkRows(2)
            tdSql.checkData(0, 1, 0)
            tdSql.checkData(1, 1, 0)
        elif partitionby=='AGG0':
            sql = sql.replace(f'{base_fun}',f'{replace_fun}').replace(f'{base_column}',f'{replace_column}')
            sql = sql.replace('group','partition')
            tdSql.query(sql)
            tdSql.checkRows(0)
        elif partitionby=='AGG2':
            sql = sql.replace(f'{base_fun}',f'{replace_fun}').replace(f'{base_column}',f'{replace_column}')
            sql = sql.replace('group','partition')
            tdSql.query(sql)
            tdSql.checkRows(2)
        elif partitionby=='AGG2_NULL':
            sql = sql.replace(f'{base_fun}',f'{replace_fun}').replace(f'{base_column}',f'{replace_column}')
            tdSql.query(sql)
            tdSql.checkRows(2)
            tdSql.checkData(0, 1, 'None')
            tdSql.checkData(1, 1, 'None')
        elif partitionby=='AGG4':
            sql = sql.replace(f'{base_fun}',f'{replace_fun}').replace(f'{base_column}',f'{replace_column}')
            sql = sql.replace('group','partition')
            tdSql.query(sql)
            tdSql.checkRows(4)
        elif partitionby=='AGG4_NULL':
            sql = sql.replace(f'{base_fun}',f'{replace_fun}').replace(f'{base_column}',f'{replace_column}')
            tdSql.query(sql)
            tdSql.checkRows(4)
            tdSql.checkData(0, 1, 'None')
            tdSql.checkData(1, 1, 'None')
        elif partitionby=='AGG10':
            sql = sql.replace(f'{base_fun}',f'{replace_fun}').replace(f'{base_column}',f'{replace_column}')
            sql = sql.replace('group','partition')
            tdSql.query(sql)
            tdSql.checkRows(10)
        elif partitionby=='AGG12':
            sql = sql.replace(f'{base_fun}',f'{replace_fun}').replace(f'{base_column}',f'{replace_column}')
            sql = sql.replace('group','partition')
            tdSql.query(sql)
            tdSql.checkRows(12)
        elif partitionby=='AGG20':
            sql = sql.replace(f'{base_fun}',f'{replace_fun}').replace(f'{base_column}',f'{replace_column}')
            sql = sql.replace('group','partition')
            tdSql.query(sql)
            tdSql.checkRows(20)
        elif partitionby=='AGG24':
            sql = sql.replace(f'{base_fun}',f'{replace_fun}').replace(f'{base_column}',f'{replace_column}')
            sql = sql.replace('group','partition')
            tdSql.query(sql)
            tdSql.checkRows(24)
        else:
            sql = sql.replace(f'{base_fun}',f'{replace_fun}').replace(f'{base_column}',f'{replace_column}')
            sql = sql.replace('group','partition')
            tdSql.query(sql)
            tdSql.checkRows(12)
  

    def tbname_count(self, dbname="nested",base_fun="AGG",replace_fun="COUNT",base_column="COLUMN",replace_column="q_int",execute="Y"):

        # stable count(*)
        sql = f"select tbname,AGG(COLUMN) from {dbname}.stable_1 group by tbname order by tbname "
        self.data_check_tbname(sql,'Y','Y',f'{base_fun}',f'{replace_fun}',f'{base_column}',f'{replace_column}')         
        
        sql1 = f"select * from ({sql})"
        self.data_check_tbname(sql1,'Y','Y',f'{base_fun}',f'{replace_fun}',f'{base_column}',f'{replace_column}')  
        
        sql2 = f"select * from ({sql}) order by tbname "
        self.data_check_tbname(sql2,'Y','Y',f'{base_fun}',f'{replace_fun}',f'{base_column}',f'{replace_column}')         
        
        #where null \ not null
        sql = f"select tbname,AGG(COLUMN) from {dbname}.stable_1 where ts is not null group by tbname order by tbname "
        self.data_check_tbname(sql,'Y','Y',f'{base_fun}',f'{replace_fun}',f'{base_column}',f'{replace_column}')        
        
        sql1 = f"select * from ({sql})"
        self.data_check_tbname(sql1,'Y','Y',f'{base_fun}',f'{replace_fun}',f'{base_column}',f'{replace_column}') 
        
        sql2 = f"select * from ({sql}) order by tbname "
        self.data_check_tbname(sql2,'Y','Y',f'{base_fun}',f'{replace_fun}',f'{base_column}',f'{replace_column}') 
        
        sql = f"select tbname,AGG(COLUMN) from {dbname}.stable_1 where ts is null group by tbname order by tbname "
        self.data_check_tbname(sql,'NULL','NULL',f'{base_fun}',f'{replace_fun}',f'{base_column}',f'{replace_column}')        
        
        sql1 = f"select * from ({sql})"
        self.data_check_tbname(sql1,'NULL','NULL',f'{base_fun}',f'{replace_fun}',f'{base_column}',f'{replace_column}') 
        
        sql2 = f"select * from ({sql}) order by tbname "
        self.data_check_tbname(sql2,'NULL','NULL',f'{base_fun}',f'{replace_fun}',f'{base_column}',f'{replace_column}') 
        
        #union
        sql = f"select tbname tb,AGG(COLUMN) from {dbname}.stable_1 group by tbname order by tbname "
        sql = f"({sql}) union ({sql}) order by tb"
        self.data_check_tbname(sql,'Y','Y',f'{base_fun}',f'{replace_fun}',f'{base_column}',f'{replace_column}')         
        
        sql1 = f"select * from ({sql})"
        self.data_check_tbname(sql1,'Y','Y',f'{base_fun}',f'{replace_fun}',f'{base_column}',f'{replace_column}')  
        
        sql2 = f"select * from ({sql}) order by tb "
        self.data_check_tbname(sql2,'Y','Y',f'{base_fun}',f'{replace_fun}',f'{base_column}',f'{replace_column}')  
        
        #union all
        sql = f"select tbname tb,AGG(COLUMN) from {dbname}.stable_1 group by tbname order by tbname "
        sql = f"({sql}) union all ({sql}) order by tb"
        self.data_check_tbname(sql,'UNIONALL','UNIONALL',f'{base_fun}',f'{replace_fun}',f'{base_column}',f'{replace_column}')         
        
        sql1 = f"select * from ({sql})"
        self.data_check_tbname(sql1,'UNIONALL','UNIONALL',f'{base_fun}',f'{replace_fun}',f'{base_column}',f'{replace_column}')  
        
        sql2 = f"select * from ({sql}) order by tb "
        self.data_check_tbname(sql2,'UNIONALL','UNIONALL',f'{base_fun}',f'{replace_fun}',f'{base_column}',f'{replace_column}')  
        
        
        #having <=0
        sql = f"select tbname,AGG(COLUMN) from {dbname}.stable_1 group by tbname having count(*) = 0 order by tbname "
        self.data_check_tbname(sql,'HAVING=0','HAVING=0',f'{base_fun}',f'{replace_fun}',f'{base_column}',f'{replace_column}')          
        
        sql1 = f"select * from ({sql})"
        self.data_check_tbname(sql1,'HAVING=0','HAVING=0',f'{base_fun}',f'{replace_fun}',f'{base_column}',f'{replace_column}')   
        
        sql2 = f"select * from ({sql}) order by tbname "
        self.data_check_tbname(sql2,'HAVING=0','HAVING=0',f'{base_fun}',f'{replace_fun}',f'{base_column}',f'{replace_column}')   
        
        sql = f"select tbname,AGG(COLUMN) from {dbname}.stable_1 group by tbname having count(*) <= 0 order by tbname "
        self.data_check_tbname(sql,'HAVING=0','HAVING=0',f'{base_fun}',f'{replace_fun}',f'{base_column}',f'{replace_column}')          
        
        sql1 = f"select * from ({sql})"
        self.data_check_tbname(sql1,'HAVING=0','HAVING=0',f'{base_fun}',f'{replace_fun}',f'{base_column}',f'{replace_column}')  
        
        sql2 = f"select * from ({sql}) order by tbname "
        self.data_check_tbname(sql2,'HAVING=0','HAVING=0',f'{base_fun}',f'{replace_fun}',f'{base_column}',f'{replace_column}')   
        
        
        sql = f"select tbname,AGG(COLUMN) from {dbname}.stable_1 where ts is null group by tbname having count(*) <= 0 order by tbname "
        self.data_check_tbname(sql,'NULL','NULL',f'{base_fun}',f'{replace_fun}',f'{base_column}',f'{replace_column}')          
        
        sql1 = f"select * from ({sql})"
        self.data_check_tbname(sql1,'NULL','NULL',f'{base_fun}',f'{replace_fun}',f'{base_column}',f'{replace_column}')  
        
        sql2 = f"select * from ({sql}) order by tbname "
        self.data_check_tbname(sql2,'NULL','NULL',f'{base_fun}',f'{replace_fun}',f'{base_column}',f'{replace_column}')   
        
        #count + having <=0 
        sql = f"select tbname,AGG(COLUMN) from {dbname}.stable_1 group by tbname having AGG(COLUMN) = 0 order by tbname "
        self.data_check_tbname(sql,'HAVING=0','HAVING=0',f'{base_fun}',f'{replace_fun}',f'{base_column}',f'{replace_column}')          
        
        sql1 = f"select * from ({sql})"
        self.data_check_tbname(sql1,'HAVING=0','HAVING=0',f'{base_fun}',f'{replace_fun}',f'{base_column}',f'{replace_column}')   
        
        sql2 = f"select * from ({sql}) order by tbname "
        self.data_check_tbname(sql2,'HAVING=0','HAVING=0',f'{base_fun}',f'{replace_fun}',f'{base_column}',f'{replace_column}') 
        
        
        sql = f"select tbname,AGG(COLUMN) from {dbname}.stable_1 group by tbname having AGG(COLUMN) <= 0 order by tbname "
        self.data_check_tbname(sql,'HAVING=0','HAVING=0',f'{base_fun}',f'{replace_fun}',f'{base_column}',f'{replace_column}')          
        
        sql1 = f"select * from ({sql})"
        self.data_check_tbname(sql1,'HAVING=0','HAVING=0',f'{base_fun}',f'{replace_fun}',f'{base_column}',f'{replace_column}')   
        
        sql2 = f"select * from ({sql}) order by tbname "
        self.data_check_tbname(sql2,'HAVING=0','HAVING=0',f'{base_fun}',f'{replace_fun}',f'{base_column}',f'{replace_column}') 
        
        
        sql = f"select tbname,AGG(COLUMN) from {dbname}.stable_1 where ts is null group by tbname having AGG(COLUMN) = 0 order by tbname "
        self.data_check_tbname(sql,'NULL','NULL',f'{base_fun}',f'{replace_fun}',f'{base_column}',f'{replace_column}')          
        
        sql1 = f"select * from ({sql})"
        self.data_check_tbname(sql1,'NULL','NULL',f'{base_fun}',f'{replace_fun}',f'{base_column}',f'{replace_column}')  
        
        sql2 = f"select * from ({sql}) order by tbname "
        self.data_check_tbname(sql2,'NULL','NULL',f'{base_fun}',f'{replace_fun}',f'{base_column}',f'{replace_column}')  
        
        #having >0
        sql = f"select tbname,AGG(COLUMN) from {dbname}.stable_1 group by tbname having count(*) != 0 order by tbname "
        self.data_check_tbname(sql,'HAVING>0','HAVING>0',f'{base_fun}',f'{replace_fun}',f'{base_column}',f'{replace_column}')          
        
        sql1 = f"select * from ({sql})"
        self.data_check_tbname(sql1,'HAVING>0','HAVING>0',f'{base_fun}',f'{replace_fun}',f'{base_column}',f'{replace_column}')  
        
        sql2 = f"select * from ({sql}) order by tbname "
        self.data_check_tbname(sql2,'HAVING>0','HAVING>0',f'{base_fun}',f'{replace_fun}',f'{base_column}',f'{replace_column}')   
        
        sql = f"select tbname,AGG(COLUMN) from {dbname}.stable_1 group by tbname having count(*) > 0 order by tbname "
        self.data_check_tbname(sql,'HAVING>0','HAVING>0',f'{base_fun}',f'{replace_fun}',f'{base_column}',f'{replace_column}')          
        
        sql1 = f"select * from ({sql})"
        self.data_check_tbname(sql1,'HAVING>0','HAVING>0',f'{base_fun}',f'{replace_fun}',f'{base_column}',f'{replace_column}')   
        
        sql2 = f"select * from ({sql}) order by tbname "
        self.data_check_tbname(sql2,'HAVING>0','HAVING>0',f'{base_fun}',f'{replace_fun}',f'{base_column}',f'{replace_column}')   
        
        
        sql = f"select tbname,AGG(COLUMN) from {dbname}.stable_1 where ts is null group by tbname having count(*) > 0 order by tbname "
        self.data_check_tbname(sql,'AGG0','AGG0',f'{base_fun}',f'{replace_fun}',f'{base_column}',f'{replace_column}')          
        
        sql1 = f"select * from ({sql})"
        self.data_check_tbname(sql1,'AGG0','AGG0',f'{base_fun}',f'{replace_fun}',f'{base_column}',f'{replace_column}')   
        
        sql2 = f"select * from ({sql}) order by tbname "
        self.data_check_tbname(sql2,'AGG0','AGG0',f'{base_fun}',f'{replace_fun}',f'{base_column}',f'{replace_column}')  
        
        #count + having >0 
        sql = f"select tbname,AGG(COLUMN) from {dbname}.stable_1 group by tbname having AGG(COLUMN) != 0 order by tbname "
        self.data_check_tbname(sql,'HAVING>0','HAVING>0',f'{base_fun}',f'{replace_fun}',f'{base_column}',f'{replace_column}')          
        
        sql1 = f"select * from ({sql})"
        self.data_check_tbname(sql1,'HAVING>0','HAVING>0',f'{base_fun}',f'{replace_fun}',f'{base_column}',f'{replace_column}')  
        
        sql2 = f"select * from ({sql}) order by tbname "
        self.data_check_tbname(sql2,'HAVING>0','HAVING>0',f'{base_fun}',f'{replace_fun}',f'{base_column}',f'{replace_column}')   
        
        sql = f"select tbname,AGG(COLUMN) from {dbname}.stable_1 group by tbname having AGG(COLUMN) > 0 order by tbname "
        self.data_check_tbname(sql,'HAVING>0','HAVING>0',f'{base_fun}',f'{replace_fun}',f'{base_column}',f'{replace_column}')          
        
        sql1 = f"select * from ({sql})"
        self.data_check_tbname(sql1,'HAVING>0','HAVING>0',f'{base_fun}',f'{replace_fun}',f'{base_column}',f'{replace_column}')   
        
        sql2 = f"select * from ({sql}) order by tbname "
        self.data_check_tbname(sql2,'HAVING>0','HAVING>0',f'{base_fun}',f'{replace_fun}',f'{base_column}',f'{replace_column}')   
        
        sql = f"select tbname,AGG(COLUMN) from {dbname}.stable_1 where ts is null group by tbname having AGG(COLUMN) != 0 order by tbname "
        self.data_check_tbname(sql,'AGG0','AGG0',f'{base_fun}',f'{replace_fun}',f'{base_column}',f'{replace_column}')          
        
        sql1 = f"select * from ({sql})"
        self.data_check_tbname(sql1,'AGG0','AGG0',f'{base_fun}',f'{replace_fun}',f'{base_column}',f'{replace_column}')   
        
        sql2 = f"select * from ({sql}) order by tbname "
        self.data_check_tbname(sql2,'AGG0','AGG0',f'{base_fun}',f'{replace_fun}',f'{base_column}',f'{replace_column}')  
        
        #order by count(*) 
        sql = f"select tbname,AGG(COLUMN) from {dbname}.stable_1 group by tbname order by tbname,count(*) "
        self.data_check_tbname(sql,'Y','Y',f'{base_fun}',f'{replace_fun}',f'{base_column}',f'{replace_column}')        
        
        sql1 = f"select * from ({sql})"
        self.data_check_tbname(sql1,'Y','Y',f'{base_fun}',f'{replace_fun}',f'{base_column}',f'{replace_column}')  
        
        sql2 = f"select * from ({sql}) order by tbname "
        self.data_check_tbname(sql2,'Y','Y',f'{base_fun}',f'{replace_fun}',f'{base_column}',f'{replace_column}')  
        
        sql = f"select tbname,AGG(COLUMN) from {dbname}.stable_1 where ts is null group by tbname order by tbname,count(*) "
        self.data_check_tbname(sql,'NULL','NULL',f'{base_fun}',f'{replace_fun}',f'{base_column}',f'{replace_column}')        
        
        sql1 = f"select * from ({sql})"
        self.data_check_tbname(sql1,'NULL','NULL',f'{base_fun}',f'{replace_fun}',f'{base_column}',f'{replace_column}')  
        
        sql2 = f"select * from ({sql}) order by tbname "
        self.data_check_tbname(sql2,'NULL','NULL',f'{base_fun}',f'{replace_fun}',f'{base_column}',f'{replace_column}')  

    def tbname_count_null(self, dbname="nested",base_fun="AGG",replace_fun="COUNT",base_column="COLUMN",replace_column="q_int",execute="Y"):

        # stable count(*)
        sql = f"select tbname,AGG(COLUMN) from {dbname}.stable_1 group by tbname order by tbname "
        self.data_check_tbname(sql,'NULL','NULL',f'{base_fun}',f'{replace_fun}',f'{base_column}',f'{replace_column}')         
        
        sql1 = f"select * from ({sql})"
        self.data_check_tbname(sql1,'NULL','NULL',f'{base_fun}',f'{replace_fun}',f'{base_column}',f'{replace_column}')  
        
        sql2 = f"select * from ({sql}) order by tbname "
        self.data_check_tbname(sql2,'NULL','NULL',f'{base_fun}',f'{replace_fun}',f'{base_column}',f'{replace_column}')  
        
        #where null \ not null
        sql = f"select tbname,AGG(COLUMN) from {dbname}.stable_1 where ts is not null group by tbname order by tbname "
        self.data_check_tbname(sql,'NULL','NULL',f'{base_fun}',f'{replace_fun}',f'{base_column}',f'{replace_column}')        
        
        sql1 = f"select * from ({sql})"
        self.data_check_tbname(sql1,'NULL','NULL',f'{base_fun}',f'{replace_fun}',f'{base_column}',f'{replace_column}') 
        
        sql2 = f"select * from ({sql}) order by tbname "
        self.data_check_tbname(sql2,'NULL','NULL',f'{base_fun}',f'{replace_fun}',f'{base_column}',f'{replace_column}') 
        
        sql = f"select tbname,AGG(COLUMN) from {dbname}.stable_1 where ts is null group by tbname order by tbname "
        self.data_check_tbname(sql,'NULL','NULL',f'{base_fun}',f'{replace_fun}',f'{base_column}',f'{replace_column}')        
        
        sql1 = f"select * from ({sql})"
        self.data_check_tbname(sql1,'NULL','NULL',f'{base_fun}',f'{replace_fun}',f'{base_column}',f'{replace_column}') 
        
        sql2 = f"select * from ({sql}) order by tbname "
        self.data_check_tbname(sql2,'NULL','NULL',f'{base_fun}',f'{replace_fun}',f'{base_column}',f'{replace_column}') 
        
        #union
        sql = f"select tbname tb,AGG(COLUMN) from {dbname}.stable_1 group by tbname order by tbname "
        sql = f"({sql}) union ({sql}) order by tb"
        self.data_check_tbname(sql,'NULL','NULL',f'{base_fun}',f'{replace_fun}',f'{base_column}',f'{replace_column}')         
        
        sql1 = f"select * from ({sql})"
        self.data_check_tbname(sql1,'NULL','NULL',f'{base_fun}',f'{replace_fun}',f'{base_column}',f'{replace_column}')  
        
        sql2 = f"select * from ({sql}) order by tb "
        self.data_check_tbname(sql2,'NULL','NULL',f'{base_fun}',f'{replace_fun}',f'{base_column}',f'{replace_column}')  
        
        #union all
        sql = f"select tbname tb,AGG(COLUMN) from {dbname}.stable_1 group by tbname order by tbname "
        sql = f"({sql}) union all ({sql}) order by tb"
        self.data_check_tbname(sql,'UNIONALLNULL','UNIONALLNULL',f'{base_fun}',f'{replace_fun}',f'{base_column}',f'{replace_column}')         
        
        sql1 = f"select * from ({sql})"
        self.data_check_tbname(sql1,'UNIONALLNULL','UNIONALLNULL',f'{base_fun}',f'{replace_fun}',f'{base_column}',f'{replace_column}')  
        
        sql2 = f"select * from ({sql}) order by tb "
        self.data_check_tbname(sql2,'UNIONALLNULL','UNIONALLNULL',f'{base_fun}',f'{replace_fun}',f'{base_column}',f'{replace_column}')  
        
        #having <=0
        sql = f"select tbname,AGG(COLUMN) from {dbname}.stable_1 group by tbname having count(*) = 0 order by tbname "
        self.data_check_tbname(sql,'HAVING=0','HAVING=0',f'{base_fun}',f'{replace_fun}',f'{base_column}',f'{replace_column}')          
        
        sql1 = f"select * from ({sql})"
        self.data_check_tbname(sql1,'HAVING=0','HAVING=0',f'{base_fun}',f'{replace_fun}',f'{base_column}',f'{replace_column}')   
        
        sql2 = f"select * from ({sql}) order by tbname "
        self.data_check_tbname(sql2,'HAVING=0','HAVING=0',f'{base_fun}',f'{replace_fun}',f'{base_column}',f'{replace_column}')   
        
        sql = f"select tbname,AGG(COLUMN) from {dbname}.stable_1 group by tbname having count(*) <= 0 order by tbname "
        self.data_check_tbname(sql,'HAVING=0','HAVING=0',f'{base_fun}',f'{replace_fun}',f'{base_column}',f'{replace_column}')          
        
        sql1 = f"select * from ({sql})"
        self.data_check_tbname(sql1,'HAVING=0','HAVING=0',f'{base_fun}',f'{replace_fun}',f'{base_column}',f'{replace_column}')  
        
        sql2 = f"select * from ({sql}) order by tbname "
        self.data_check_tbname(sql2,'HAVING=0','HAVING=0',f'{base_fun}',f'{replace_fun}',f'{base_column}',f'{replace_column}')   
        
        
        sql = f"select tbname,AGG(COLUMN) from {dbname}.stable_1 where ts is null group by tbname having count(*) <= 0 order by tbname "
        self.data_check_tbname(sql,'NULL','NULL',f'{base_fun}',f'{replace_fun}',f'{base_column}',f'{replace_column}')          
        
        sql1 = f"select * from ({sql})"
        self.data_check_tbname(sql1,'NULL','NULL',f'{base_fun}',f'{replace_fun}',f'{base_column}',f'{replace_column}')  
        
        sql2 = f"select * from ({sql}) order by tbname "
        self.data_check_tbname(sql2,'NULL','NULL',f'{base_fun}',f'{replace_fun}',f'{base_column}',f'{replace_column}')   
        
        #having >0
        sql = f"select tbname,AGG(COLUMN) from {dbname}.stable_1 group by tbname having count(*) != 0 order by tbname "
        self.data_check_tbname(sql,'HAVINGCOLUMN=0','HAVINGCOLUMN=0',f'{base_fun}',f'{replace_fun}',f'{base_column}',f'{replace_column}')          
        
        sql1 = f"select * from ({sql})"
        self.data_check_tbname(sql1,'HAVINGCOLUMN=0','HAVINGCOLUMN=0',f'{base_fun}',f'{replace_fun}',f'{base_column}',f'{replace_column}')  
        
        sql2 = f"select * from ({sql}) order by tbname "
        self.data_check_tbname(sql2,'HAVINGCOLUMN=0','HAVINGCOLUMN=0',f'{base_fun}',f'{replace_fun}',f'{base_column}',f'{replace_column}')   
        
        sql = f"select tbname,AGG(COLUMN) from {dbname}.stable_1 group by tbname having count(*) > 0 order by tbname "
        self.data_check_tbname(sql,'HAVINGCOLUMN=0','HAVINGCOLUMN=0',f'{base_fun}',f'{replace_fun}',f'{base_column}',f'{replace_column}')          
        
        sql1 = f"select * from ({sql})"
        self.data_check_tbname(sql1,'HAVINGCOLUMN=0','HAVINGCOLUMN=0',f'{base_fun}',f'{replace_fun}',f'{base_column}',f'{replace_column}')   
        
        sql2 = f"select * from ({sql}) order by tbname "
        self.data_check_tbname(sql2,'HAVINGCOLUMN=0','HAVINGCOLUMN=0',f'{base_fun}',f'{replace_fun}',f'{base_column}',f'{replace_column}')   
        
        
        sql = f"select tbname,AGG(COLUMN) from {dbname}.stable_1 where ts is null group by tbname having count(*) != 0 order by tbname "
        self.data_check_tbname(sql,'AGG0','AGG0',f'{base_fun}',f'{replace_fun}',f'{base_column}',f'{replace_column}')          
        
        sql1 = f"select * from ({sql})"
        self.data_check_tbname(sql1,'AGG0','AGG0',f'{base_fun}',f'{replace_fun}',f'{base_column}',f'{replace_column}')  
        
        sql2 = f"select * from ({sql}) order by tbname "
        self.data_check_tbname(sql2,'AGG0','AGG0',f'{base_fun}',f'{replace_fun}',f'{base_column}',f'{replace_column}') 
        
        #order by count(*) 
        sql = f"select tbname,AGG(COLUMN) from {dbname}.stable_1 group by tbname order by tbname,count(*) "
        self.data_check_tbname(sql,'NULL','NULL',f'{base_fun}',f'{replace_fun}',f'{base_column}',f'{replace_column}')        
        
        sql1 = f"select * from ({sql})"
        self.data_check_tbname(sql1,'NULL','NULL',f'{base_fun}',f'{replace_fun}',f'{base_column}',f'{replace_column}')  
        
        sql2 = f"select * from ({sql}) order by tbname "
        self.data_check_tbname(sql2,'NULL','NULL',f'{base_fun}',f'{replace_fun}',f'{base_column}',f'{replace_column}')  
        
        sql = f"select tbname,AGG(COLUMN) from {dbname}.stable_1 where ts is null group by tbname order by tbname,count(*) "
        self.data_check_tbname(sql,'NULL','NULL',f'{base_fun}',f'{replace_fun}',f'{base_column}',f'{replace_column}')        
        
        sql1 = f"select * from ({sql})"
        self.data_check_tbname(sql1,'NULL','NULL',f'{base_fun}',f'{replace_fun}',f'{base_column}',f'{replace_column}')  
        
        sql2 = f"select * from ({sql}) order by tbname "
        self.data_check_tbname(sql2,'NULL','NULL',f'{base_fun}',f'{replace_fun}',f'{base_column}',f'{replace_column}')  
        
    def tbname_count_all(self):  
        #random data        
        fake = Faker('zh_CN')
        random_data_big = fake.random_int(min=-9223372036854775807, max=9223372036854775807, step=10)
        random_data_str = fake.pystr() 
        random_data_float = fake.pyfloat()
        
        #random cal
        calculations = ['+','-','*','/']
        calculation = random.sample(calculations,1)
        calculation = str(calculation).replace("[","").replace("]","").replace("'","").replace(", ","")
        
        #random data column
        data_columns = ['q_int','q_bigint','q_smallint','q_tinyint','q_float','q_double']
        data_null_columns = ['q_int_null','q_bigint_null','q_smallint_null','q_tinyint_null','q_float_null','q_double_null']
        
        data_column = random.sample(data_columns,1)
        data_column = str(data_column).replace("[","").replace("]","").replace("'","").replace(", ","")
        
        data_null_column = random.sample(data_null_columns,1)
        data_null_column = str(data_null_column).replace("[","").replace("]","").replace("'","").replace(", ","")
        
        data_all_columns = data_columns + data_null_columns
        data_all_column = random.sample(data_all_columns,1)
        data_all_column = str(data_all_column).replace("[","").replace("]","").replace("'","").replace(", ","")
        
        #random data tag
        data_tags = ['t_int','t_bigint','t_smallint','t_tinyint','t_float','t_double']
        data_null_tags = ['t_int_null','t_bigint_null','t_smallint_null','t_tinyint_null','t_float_null','t_double_null']
        
        data_tag = random.sample(data_tags,1)
        data_tag = str(data_tag).replace("[","").replace("]","").replace("'","").replace(", ","")
        
        data_null_tag = random.sample(data_null_tags,1)
        data_null_tag = str(data_null_tag).replace("[","").replace("]","").replace("'","").replace(", ","")
        
        data_all_tags = data_tags + data_null_tags
        data_all_tag = random.sample(data_all_tags,1)
        data_all_tag = str(data_all_tag).replace("[","").replace("]","").replace("'","").replace(", ","")
        
        #random ts column
        ts_columns = ['ts','_c0','_C0','_rowts']
        ts_column = random.sample(ts_columns,1)
        ts_column = str(ts_column).replace("[","").replace("]","").replace("'","").replace(", ","")
        #random other column
        other_columns = ['q_bool','q_binary','q_nchar','q_ts','loc','t_bool','t_binary','t_nchar','t_ts']
        other_column = random.sample(other_columns,1)
        other_column = str(other_column).replace("[","").replace("]","").replace("'","").replace(", ","")
        
        #random other null column and tag
        other_null_columns = ['q_bool_null','q_binary_null','q_nchar_null','q_ts_null','t_bool_null','t_binary_null','t_nchar_null','t_ts_null']
        other_null_column = random.sample(other_null_columns,1)
        other_null_column = str(other_null_column).replace("[","").replace("]","").replace("'","").replace(", ","")
        
        #random all column
        all_columns = data_all_columns + data_all_tags + ts_columns + other_columns + other_null_columns
        all_column = random.sample(all_columns,1)
        all_column = str(all_column).replace("[","").replace("]","").replace("'","").replace(", ","")
        
        
        self.tbname_count_null(replace_fun='count',replace_column=f'{data_null_column}') 
        self.tbname_count_null(replace_fun='count',replace_column=f'{data_null_tag}') 
        self.tbname_count_null(replace_fun='count',replace_column=f'{other_null_column}') 
        
        self.tbname_count(replace_fun='count',replace_column='*') 
        self.tbname_count(replace_fun='count',replace_column=f'{data_column}') 
        self.tbname_count(replace_fun='count',replace_column=f'{ts_column}') 
        
        self.tbname_count(replace_fun='count',replace_column=f'{other_column}') 
        self.tbname_count(replace_fun='count',replace_column=f'{data_tag}') 
        
        self.tbname_count(replace_fun='count',replace_column=f'{random_data_big}') 
        self.tbname_count(replace_fun='count',replace_column=f'{random_data_float}') 
        self.tbname_count(replace_fun='count',replace_column=f'\'{random_data_str}\'') 
        self.tbname_count(replace_fun='count',replace_column=f'{random_data_big} {calculation} (abs({data_column})+1)') 
        self.tbname_count(replace_fun='count',replace_column=f'{random_data_float} {calculation} (abs({data_column})+1)')  

    def tbname_agg(self, dbname="nested",base_fun="AGG",replace_fun="",base_column="COLUMN",replace_column="q_int",execute="Y"):

        # stable count(*)
        sql = f"select tbname,AGG(COLUMN) from {dbname}.stable_1 group by tbname order by tbname "
        self.data_check_tbname(sql,'AGG2','AGG2',f'{base_fun}',f'{replace_fun}',f'{base_column}',f'{replace_column}')       
        
        sql1 = f"select * from ({sql})"
        self.data_check_tbname(sql1,'AGG2','AGG2',f'{base_fun}',f'{replace_fun}',f'{base_column}',f'{replace_column}')    
        
        sql2 = f"select * from ({sql}) order by tbname "
        self.data_check_tbname(sql2,'AGG2','AGG2',f'{base_fun}',f'{replace_fun}',f'{base_column}',f'{replace_column}')   
        
        #where null \ not null
        sql = f"select tbname,AGG(COLUMN) from {dbname}.stable_1 where ts is not null group by tbname order by tbname "
        self.data_check_tbname(sql,'AGG2','AGG2',f'{base_fun}',f'{replace_fun}',f'{base_column}',f'{replace_column}')        
        
        sql1 = f"select * from ({sql})"
        self.data_check_tbname(sql1,'AGG2','AGG2',f'{base_fun}',f'{replace_fun}',f'{base_column}',f'{replace_column}') 
        
        sql2 = f"select * from ({sql}) order by tbname "
        self.data_check_tbname(sql2,'AGG2','AGG2',f'{base_fun}',f'{replace_fun}',f'{base_column}',f'{replace_column}') 
        
        sql = f"select tbname,AGG(COLUMN) from {dbname}.stable_1 where ts is null group by tbname order by tbname "
        self.data_check_tbname(sql,'AGG0','AGG0',f'{base_fun}',f'{replace_fun}',f'{base_column}',f'{replace_column}')        
        
        sql1 = f"select * from ({sql})"
        self.data_check_tbname(sql1,'AGG0','AGG0',f'{base_fun}',f'{replace_fun}',f'{base_column}',f'{replace_column}') 
        
        sql2 = f"select * from ({sql}) order by tbname "
        self.data_check_tbname(sql2,'AGG0','AGG0',f'{base_fun}',f'{replace_fun}',f'{base_column}',f'{replace_column}') 
        
        
        #union
        sql = f"select tbname tb,AGG(COLUMN) from {dbname}.stable_1 group by tbname order by tbname "
        sql = f"({sql}) union ({sql}) order by tb"
        self.data_check_tbname(sql,'AGG2','AGG2',f'{base_fun}',f'{replace_fun}',f'{base_column}',f'{replace_column}')         
        
        sql1 = f"select * from ({sql})"
        self.data_check_tbname(sql1,'AGG2','AGG2',f'{base_fun}',f'{replace_fun}',f'{base_column}',f'{replace_column}')  
        
        sql2 = f"select * from ({sql}) order by tb "
        self.data_check_tbname(sql2,'AGG2','AGG2',f'{base_fun}',f'{replace_fun}',f'{base_column}',f'{replace_column}')
        
        #union all
        sql = f"select tbname tb,AGG(COLUMN) from {dbname}.stable_1 group by tbname order by tbname "
        sql = f"({sql}) union all ({sql}) order by tb"
        self.data_check_tbname(sql,'AGG4','AGG4',f'{base_fun}',f'{replace_fun}',f'{base_column}',f'{replace_column}')         
        
        sql1 = f"select * from ({sql})"
        self.data_check_tbname(sql1,'AGG4','AGG4',f'{base_fun}',f'{replace_fun}',f'{base_column}',f'{replace_column}')  
        
        sql2 = f"select * from ({sql}) order by tb "
        self.data_check_tbname(sql2,'AGG4','AGG4',f'{base_fun}',f'{replace_fun}',f'{base_column}',f'{replace_column}')
        
        if execute=="Y":
            sql = f"select tbname,AGG(COLUMN) from {dbname}.stable_1 group by tbname having count(*) = 0 order by tbname "
            self.data_check_tbname(sql,'AGG10','AGG10',f'{base_fun}',f'{replace_fun}',f'{base_column}',f'{replace_column}')            
            
            sql1 = f"select * from ({sql})"
            self.data_check_tbname(sql1,'AGG10','AGG10',f'{base_fun}',f'{replace_fun}',f'{base_column}',f'{replace_column}')  
            
            sql2 = f"select * from ({sql}) order by tbname "
            self.data_check_tbname(sql2,'AGG10','AGG10',f'{base_fun}',f'{replace_fun}',f'{base_column}',f'{replace_column}')  
            
            sql = f"select tbname,AGG(COLUMN) from {dbname}.stable_1 group by tbname having count(*) <= 0 order by tbname "
            self.data_check_tbname(sql,'AGG10','AGG10',f'{base_fun}',f'{replace_fun}',f'{base_column}',f'{replace_column}')           
            
            sql1 = f"select * from ({sql})"
            self.data_check_tbname(sql1,'AGG10','AGG10',f'{base_fun}',f'{replace_fun}',f'{base_column}',f'{replace_column}')   
            
            sql2 = f"select * from ({sql}) order by tbname "
            self.data_check_tbname(sql2,'AGG10','AGG10',f'{base_fun}',f'{replace_fun}',f'{base_column}',f'{replace_column}')   
            
            
            sql = f"select tbname,AGG(COLUMN) from {dbname}.stable_1 group by tbname having count(*) != 0 order by tbname "
            self.data_check_tbname(sql,'AGG2','AGG2',f'{base_fun}',f'{replace_fun}',f'{base_column}',f'{replace_column}')        
            
            sql1 = f"select * from ({sql})"
            self.data_check_tbname(sql1,'AGG2','AGG2',f'{base_fun}',f'{replace_fun}',f'{base_column}',f'{replace_column}')
            
            sql2 = f"select * from ({sql}) order by tbname "
            self.data_check_tbname(sql2,'AGG2','AGG2',f'{base_fun}',f'{replace_fun}',f'{base_column}',f'{replace_column}')
            
            sql = f"select tbname,AGG(COLUMN) from {dbname}.stable_1 group by tbname having count(*) > 0 order by tbname "
            self.data_check_tbname(sql,'AGG2','AGG2',f'{base_fun}',f'{replace_fun}',f'{base_column}',f'{replace_column}')        
            
            sql1 = f"select * from ({sql})"
            self.data_check_tbname(sql1,'AGG2','AGG2',f'{base_fun}',f'{replace_fun}',f'{base_column}',f'{replace_column}') 
            
            sql2 = f"select * from ({sql}) order by tbname "
            self.data_check_tbname(sql2,'AGG2','AGG2',f'{base_fun}',f'{replace_fun}',f'{base_column}',f'{replace_column}')
            
            
            sql = f"select tbname,AGG(COLUMN) from {dbname}.stable_1 group by tbname order by tbname,count(*) "
            self.data_check_tbname(sql,'AGG12','AGG12',f'{base_fun}',f'{replace_fun}',f'{base_column}',f'{replace_column}')       
            
            sql1 = f"select * from ({sql})"
            self.data_check_tbname(sql1,'AGG12','AGG12',f'{base_fun}',f'{replace_fun}',f'{base_column}',f'{replace_column}')  
            
            sql2 = f"select * from ({sql}) order by tbname "
            self.data_check_tbname(sql2,'AGG12','AGG12',f'{base_fun}',f'{replace_fun}',f'{base_column}',f'{replace_column}')  
        
        # Mixed scene
        if execute=="Y":
            sql = f"select tbname,AGG(COLUMN) from {dbname}.stable_1 group by tbname having count(*) = 0 order by tbname "
            self.data_check_tbname(sql,'AGG10','AGG10',f'{base_fun}',f'{replace_fun}',f'{base_column}',f'{replace_column}')            
            
            sql1 = f"select * from ({sql})"
            self.data_check_tbname(sql1,'AGG10','AGG10',f'{base_fun}',f'{replace_fun}',f'{base_column}',f'{replace_column}')  
            
            sql2 = f"select * from ({sql}) order by tbname "
            self.data_check_tbname(sql2,'AGG10','AGG10',f'{base_fun}',f'{replace_fun}',f'{base_column}',f'{replace_column}')  
            
            
            sql = f"select tbname,AGG(COLUMN) from {dbname}.stable_1 group by tbname having count(*) <= 0 order by tbname "
            self.data_check_tbname(sql,'AGG10','AGG10',f'{base_fun}',f'{replace_fun}',f'{base_column}',f'{replace_column}')           
            
            sql1 = f"select * from ({sql})"
            self.data_check_tbname(sql1,'AGG10','AGG10',f'{base_fun}',f'{replace_fun}',f'{base_column}',f'{replace_column}')   
            
            sql2 = f"select * from ({sql}) order by tbname "
            self.data_check_tbname(sql2,'AGG10','AGG10',f'{base_fun}',f'{replace_fun}',f'{base_column}',f'{replace_column}')   
            
            #union
            sql = f"select tbname tb,AGG(COLUMN) from {dbname}.stable_1 group by tbname having count(*) = 0 order by tbname "
            sql = f"({sql}) union ({sql}) order by tb"
            self.data_check_tbname(sql,'AGG10','AGG10',f'{base_fun}',f'{replace_fun}',f'{base_column}',f'{replace_column}')         
        
            sql1 = f"select * from ({sql})"
            self.data_check_tbname(sql1,'AGG10','AGG10',f'{base_fun}',f'{replace_fun}',f'{base_column}',f'{replace_column}')  
        
            sql2 = f"select * from ({sql}) order by tb "
            self.data_check_tbname(sql2,'AGG10','AGG10',f'{base_fun}',f'{replace_fun}',f'{base_column}',f'{replace_column}')
                   
            #union all 
            sql = f"select tbname tb,AGG(COLUMN) from {dbname}.stable_1 group by tbname having count(*) <= 0 order by tbname "
            sql = f"({sql}) union all ({sql}) order by tb"
            self.data_check_tbname(sql,'AGG20','AGG20',f'{base_fun}',f'{replace_fun}',f'{base_column}',f'{replace_column}')         
        
            sql1 = f"select * from ({sql})"
            self.data_check_tbname(sql1,'AGG20','AGG20',f'{base_fun}',f'{replace_fun}',f'{base_column}',f'{replace_column}')  
        
            sql2 = f"select * from ({sql}) order by tb "
            self.data_check_tbname(sql2,'AGG20','AGG20',f'{base_fun}',f'{replace_fun}',f'{base_column}',f'{replace_column}')
        
            
            
            sql = f"select tbname,AGG(COLUMN) from {dbname}.stable_1 group by tbname having count(*) != 0 order by tbname "
            self.data_check_tbname(sql,'AGG2','AGG2',f'{base_fun}',f'{replace_fun}',f'{base_column}',f'{replace_column}')        
            
            sql1 = f"select * from ({sql})"
            self.data_check_tbname(sql1,'AGG2','AGG2',f'{base_fun}',f'{replace_fun}',f'{base_column}',f'{replace_column}')
            
            sql2 = f"select * from ({sql}) order by tbname "
            self.data_check_tbname(sql2,'AGG2','AGG2',f'{base_fun}',f'{replace_fun}',f'{base_column}',f'{replace_column}')
            
            
            sql = f"select tbname,AGG(COLUMN) from {dbname}.stable_1 group by tbname having count(*) > 0 order by tbname "
            self.data_check_tbname(sql,'AGG2','AGG2',f'{base_fun}',f'{replace_fun}',f'{base_column}',f'{replace_column}')        
            
            sql1 = f"select * from ({sql})"
            self.data_check_tbname(sql1,'AGG2','AGG2',f'{base_fun}',f'{replace_fun}',f'{base_column}',f'{replace_column}') 
            
            sql2 = f"select * from ({sql}) order by tbname "
            self.data_check_tbname(sql2,'AGG2','AGG2',f'{base_fun}',f'{replace_fun}',f'{base_column}',f'{replace_column}')
            
            #union
            sql = f"select tbname tb,AGG(COLUMN) from {dbname}.stable_1 group by tbname having count(*) != 0 order by tbname "
            sql = f"({sql}) union ({sql}) order by tb"
            self.data_check_tbname(sql,'AGG2','AGG2',f'{base_fun}',f'{replace_fun}',f'{base_column}',f'{replace_column}')         
        
            sql1 = f"select * from ({sql})"
            self.data_check_tbname(sql1,'AGG2','AGG2',f'{base_fun}',f'{replace_fun}',f'{base_column}',f'{replace_column}')  
        
            sql2 = f"select * from ({sql}) order by tb "
            self.data_check_tbname(sql2,'AGG2','AGG2',f'{base_fun}',f'{replace_fun}',f'{base_column}',f'{replace_column}')
                   
            #union all 
            sql = f"select tbname tb,AGG(COLUMN) from {dbname}.stable_1 group by tbname having count(*) > 0 order by tbname "
            sql = f"({sql}) union all ({sql}) order by tb"
            self.data_check_tbname(sql,'AGG4','AGG4',f'{base_fun}',f'{replace_fun}',f'{base_column}',f'{replace_column}')         
        
            sql1 = f"select * from ({sql})"
            self.data_check_tbname(sql1,'AGG4','AGG4',f'{base_fun}',f'{replace_fun}',f'{base_column}',f'{replace_column}')  
        
            sql2 = f"select * from ({sql}) order by tb "
            self.data_check_tbname(sql2,'AGG4','AGG4',f'{base_fun}',f'{replace_fun}',f'{base_column}',f'{replace_column}')
        
            
                        
            sql = f"select tbname,AGG(COLUMN) from {dbname}.stable_1 group by tbname order by tbname,AGG(COLUMN) "
            self.data_check_tbname(sql,'AGG2','AGG2',f'{base_fun}',f'{replace_fun}',f'{base_column}',f'{replace_column}')       
            
            sql1 = f"select * from ({sql})"
            self.data_check_tbname(sql1,'AGG2','AGG2',f'{base_fun}',f'{replace_fun}',f'{base_column}',f'{replace_column}')  
            
            sql2 = f"select * from ({sql}) order by tbname "
            self.data_check_tbname(sql2,'AGG2','AGG2',f'{base_fun}',f'{replace_fun}',f'{base_column}',f'{replace_column}')
            
            #union
            sql = f"select tbname tb,AGG(COLUMN) from {dbname}.stable_1 group by tbname order by tbname,AGG(COLUMN) "
            sql = f"({sql}) union ({sql}) order by tb"
            self.data_check_tbname(sql,'AGG2','AGG2',f'{base_fun}',f'{replace_fun}',f'{base_column}',f'{replace_column}')         
        
            sql1 = f"select * from ({sql})"
            self.data_check_tbname(sql1,'AGG2','AGG2',f'{base_fun}',f'{replace_fun}',f'{base_column}',f'{replace_column}')  
        
            sql2 = f"select * from ({sql}) order by tb "
            self.data_check_tbname(sql2,'AGG2','AGG2',f'{base_fun}',f'{replace_fun}',f'{base_column}',f'{replace_column}')
                   
            #union all 
            sql = f"select tbname tb,AGG(COLUMN) from {dbname}.stable_1 group by tbname order by tbname,AGG(COLUMN) "
            sql = f"({sql}) union all ({sql}) order by tb"
            self.data_check_tbname(sql,'AGG4','AGG4',f'{base_fun}',f'{replace_fun}',f'{base_column}',f'{replace_column}')         
        
            sql1 = f"select * from ({sql})"
            self.data_check_tbname(sql1,'AGG4','AGG4',f'{base_fun}',f'{replace_fun}',f'{base_column}',f'{replace_column}')  
        
            sql2 = f"select * from ({sql}) order by tb "
            self.data_check_tbname(sql2,'AGG4','AGG4',f'{base_fun}',f'{replace_fun}',f'{base_column}',f'{replace_column}')
            
            
            
            sql = f"select tbname,AGG(COLUMN),count(*) from {dbname}.stable_1 group by tbname order by tbname,AGG(COLUMN) "
            self.data_check_tbname(sql,'AGG12','AGG12',f'{base_fun}',f'{replace_fun}',f'{base_column}',f'{replace_column}')       
            
            sql1 = f"select * from ({sql})"
            self.data_check_tbname(sql1,'AGG12','AGG12',f'{base_fun}',f'{replace_fun}',f'{base_column}',f'{replace_column}')  
            
            sql2 = f"select * from ({sql}) order by tbname "
            self.data_check_tbname(sql2,'AGG12','AGG12',f'{base_fun}',f'{replace_fun}',f'{base_column}',f'{replace_column}')
            
            #union
            sql = f"select tbname tb,AGG(COLUMN),count(*) from {dbname}.stable_1 group by tbname order by tbname,AGG(COLUMN)"
            sql = f"({sql}) union ({sql}) order by tb"
            self.data_check_tbname(sql,'AGG12','AGG12',f'{base_fun}',f'{replace_fun}',f'{base_column}',f'{replace_column}')         
        
            sql1 = f"select * from ({sql})"
            self.data_check_tbname(sql1,'AGG12','AGG12',f'{base_fun}',f'{replace_fun}',f'{base_column}',f'{replace_column}')  
        
            sql2 = f"select * from ({sql}) order by tb "
            self.data_check_tbname(sql2,'AGG12','AGG12',f'{base_fun}',f'{replace_fun}',f'{base_column}',f'{replace_column}')
                   
            #union all 
            sql = f"select tbname tb ,AGG(COLUMN),count(*) from {dbname}.stable_1 group by tbname order by tbname,AGG(COLUMN)"
            sql = f"({sql}) union all ({sql}) order by tb"
            self.data_check_tbname(sql,'AGG24','AGG24',f'{base_fun}',f'{replace_fun}',f'{base_column}',f'{replace_column}')         
        
            sql1 = f"select * from ({sql})"
            self.data_check_tbname(sql1,'AGG24','AGG24',f'{base_fun}',f'{replace_fun}',f'{base_column}',f'{replace_column}')  
        
            sql2 = f"select * from ({sql}) order by tb "
            self.data_check_tbname(sql2,'AGG24','AGG24',f'{base_fun}',f'{replace_fun}',f'{base_column}',f'{replace_column}')
            
            
            
            sql = f"select tbname,AGG(COLUMN) from {dbname}.stable_1 group by tbname order by tbname,AGG(COLUMN),count(*) "
            self.data_check_tbname(sql,'AGG12','AGG12',f'{base_fun}',f'{replace_fun}',f'{base_column}',f'{replace_column}')       
            
            sql1 = f"select * from ({sql})"
            self.data_check_tbname(sql1,'AGG12','AGG12',f'{base_fun}',f'{replace_fun}',f'{base_column}',f'{replace_column}')  
            
            sql2 = f"select * from ({sql}) order by tbname "
            self.data_check_tbname(sql2,'AGG12','AGG12',f'{base_fun}',f'{replace_fun}',f'{base_column}',f'{replace_column}')

            #union
            sql = f"select tbname tb,AGG(COLUMN) from {dbname}.stable_1 group by tbname order by tbname,AGG(COLUMN),count(*)"
            sql = f"({sql}) union ({sql}) order by tb"
            self.data_check_tbname(sql,'AGG12','AGG12',f'{base_fun}',f'{replace_fun}',f'{base_column}',f'{replace_column}')         
        
            sql1 = f"select * from ({sql})"
            self.data_check_tbname(sql1,'AGG12','AGG12',f'{base_fun}',f'{replace_fun}',f'{base_column}',f'{replace_column}')  
        
            sql2 = f"select * from ({sql}) order by tb "
            self.data_check_tbname(sql2,'AGG12','AGG12',f'{base_fun}',f'{replace_fun}',f'{base_column}',f'{replace_column}')
                   
            #union all 
            sql = f"select tbname tb,AGG(COLUMN) from {dbname}.stable_1 group by tbname order by tbname,AGG(COLUMN),count(*)"
            sql = f"({sql}) union all ({sql}) order by tb"
            self.data_check_tbname(sql,'AGG24','AGG24',f'{base_fun}',f'{replace_fun}',f'{base_column}',f'{replace_column}')         
        
            sql1 = f"select * from ({sql})"
            self.data_check_tbname(sql1,'AGG24','AGG24',f'{base_fun}',f'{replace_fun}',f'{base_column}',f'{replace_column}')  
        
            sql2 = f"select * from ({sql}) order by tb "
            self.data_check_tbname(sql2,'AGG24','AGG24',f'{base_fun}',f'{replace_fun}',f'{base_column}',f'{replace_column}')
            
            

    def tbname_agg_all(self):  
        #random data        
        fake = Faker('zh_CN')
        random_data_big = fake.random_int(min=-9223372036854775807, max=9223372036854775807, step=10)
        random_data_str = fake.pystr() 
        random_data_float = fake.pyfloat()
        random_data_start = fake.random_int(min=0, max=100, step=1)
        random_data_end = fake.random_int(min=0, max=100, step=1)
        
        #random cal
        calculations = ['+','-','*','/']
        calculation = random.sample(calculations,1)
        calculation = str(calculation).replace("[","").replace("]","").replace("'","").replace(", ","")
        
        #random data column
        data_columns = ['q_int','q_bigint','q_smallint','q_tinyint','q_float','q_double','t_int','t_bigint','t_smallint','t_tinyint','t_float','t_double']
        data_null_columns = ['q_int_null','q_bigint_null','q_smallint_null','q_tinyint_null','q_float_null','q_double_null','t_int','t_bigint','t_smallint','t_tinyint','t_float','t_double']
        data_columns = data_columns + data_null_columns
        data_column = random.sample(data_null_columns,1)
        data_column = str(data_column).replace("[","").replace("]","").replace("'","").replace(", ","")
        #random ts column
        ts_columns = ['ts','_c0','_C0','_rowts']
        ts_column = random.sample(ts_columns,1)
        ts_column = str(ts_column).replace("[","").replace("]","").replace("'","").replace(", ","")
        #random ts unit column
        ts_units = ['1a','1s','1m','1h','1d','1w']
        ts_unit = random.sample(ts_units,1)
        ts_unit = str(ts_unit).replace("[","").replace("]","").replace("'","").replace(", ","")
        #random str column
        str_columns = ['q_int','q_bigint','q_smallint','q_tinyint','q_float','q_double','t_int','t_bigint','t_smallint','t_tinyint','t_float','t_double']
        str_column = random.sample(str_columns,1)
        str_column = str(str_column).replace("[","").replace("]","").replace("'","").replace(", ","")
        #random other column
        other_columns = ['q_bool','q_binary','q_nchar','q_ts','loc','t_bool','t_binary','t_nchar','t_ts']
        other_column = random.sample(other_columns,1)
        other_column = str(other_column).replace("[","").replace("]","").replace("'","").replace(", ","")
        #random all column
        all_columns = data_columns + ts_columns + str_columns + other_columns
        all_column = random.sample(all_columns,1)
        all_column = str(all_column).replace("[","").replace("]","").replace("'","").replace(", ","")
        
        self.tbname_agg(replace_fun='sum',replace_column=f'{data_column}') 
        self.tbname_agg(replace_fun='sum',replace_column=f'{random_data_big}') 
        self.tbname_agg(replace_fun='sum',replace_column=f'{random_data_float}') 
        self.tbname_agg(replace_fun='sum',replace_column=f'{random_data_big} {calculation} {data_column}') 
        self.tbname_agg(replace_fun='sum',replace_column=f'{random_data_float} {calculation} {data_column}')        
        
        self.tbname_agg(replace_fun='avg',replace_column=f'{data_column}') 
        self.tbname_agg(replace_fun='avg',replace_column=f'{random_data_big}') 
        self.tbname_agg(replace_fun='avg',replace_column=f'{random_data_float}') 
        self.tbname_agg(replace_fun='avg',replace_column=f'{random_data_big} {calculation} {data_column}') 
        self.tbname_agg(replace_fun='avg',replace_column=f'{random_data_float} {calculation} {data_column}')        

        self.tbname_agg(replace_fun='spread',replace_column=f'{data_column}')         
        self.tbname_agg(replace_fun='spread',replace_column=f'{random_data_big}') 
        self.tbname_agg(replace_fun='spread',replace_column=f'{random_data_float}') 
        self.tbname_agg(replace_fun='spread',replace_column=f'{random_data_big} {calculation} {data_column}') 
        self.tbname_agg(replace_fun='spread',replace_column=f'{random_data_float} {calculation} {data_column}') 
        
        self.tbname_agg(replace_fun='stddev',replace_column=f'{data_column}')         
        self.tbname_agg(replace_fun='stddev',replace_column=f'{random_data_big}') 
        self.tbname_agg(replace_fun='stddev',replace_column=f'{random_data_float}') 
        self.tbname_agg(replace_fun='stddev',replace_column=f'{random_data_big} {calculation} {data_column}') 
        self.tbname_agg(replace_fun='stddev',replace_column=f'{random_data_float} {calculation} {data_column}') 
                
        self.tbname_agg(replace_fun='elapsed',replace_column=f'{ts_column}') 
        self.tbname_agg(replace_fun='elapsed',replace_column=f'{ts_column},{ts_unit}')   
        
        self.tbname_agg(replace_fun='hyperloglog',replace_column=f'{all_column}')
        self.tbname_agg(replace_fun='hyperloglog',replace_column=f'{random_data_big}') 
        self.tbname_agg(replace_fun='hyperloglog',replace_column=f'{random_data_float}') 
        self.tbname_agg(replace_fun='hyperloglog',replace_column=f'\'{random_data_str}\'') 
        self.tbname_agg(replace_fun='hyperloglog',replace_column=f'{random_data_big} {calculation} {data_column}') 
        self.tbname_agg(replace_fun='hyperloglog',replace_column=f'{random_data_float} {calculation} {data_column}') 
        
        self.tbname_agg(replace_fun='apercentile',replace_column=f'{data_column},{random_data_start}')
        self.tbname_agg(replace_fun='apercentile',replace_column=f'{data_column},{random_data_start},"default"')
        self.tbname_agg(replace_fun='apercentile',replace_column=f'{data_column},{random_data_start},"t-digest"')
        # not support
        # self.tbname_agg(replace_fun='percentile',replace_column=f'{data_column},{random_data_start}')
        # self.tbname_agg(replace_fun='percentile',replace_column=f'{data_column},{random_data_end}')
        # self.tbname_agg(replace_fun='percentile',replace_column=f'{data_column},{random_data_big}')

        self.tbname_agg(replace_fun='leastsquares',replace_column=f'{data_column},{random_data_start},{random_data_end}')
        self.tbname_agg(replace_fun='leastsquares',replace_column=f'{data_column},{random_data_start},{random_data_big}')
                
        self.tbname_agg(replace_fun='histogram',replace_column=f'{data_column},"user_input","[-100000000000,100000000000]",0',execute='N')
        self.tbname_agg(replace_fun='histogram',replace_column=f'{data_column},"user_input","[-100000000000,100000000000]",1',execute='N')
        histogram_logbin = '{"start":1.0, "factor": 2.0, "count": 1, "infinity": false}'
        histogram_linear_bin = '{"start":1.0, "width": 2.0, "count": 1, "infinity": false}'
        self.tbname_agg(replace_fun='histogram',replace_column=f'{data_column},"log_bin",\'{histogram_logbin}\',1',execute='N')
        self.tbname_agg(replace_fun='histogram',replace_column=f'{data_column},"linear_bin",\'{histogram_linear_bin}\',1',execute='N')



    def tag_count(self, dbname="nested",base_fun="AGG",replace_fun="COUNT",base_column="COLUMN",replace_column="q_int",execute="Y"):

        # stable count(*)
        sql = f"select loc,AGG(COLUMN) from {dbname}.stable_1 group by loc order by loc "
        self.data_check_tbname(sql,'Y','Y',f'{base_fun}',f'{replace_fun}',f'{base_column}',f'{replace_column}')         
        
        sql1 = f"select * from ({sql})"
        self.data_check_tbname(sql1,'Y','Y',f'{base_fun}',f'{replace_fun}',f'{base_column}',f'{replace_column}')  
        
        sql2 = f"select * from ({sql}) order by loc "
        self.data_check_tbname(sql2,'Y','Y',f'{base_fun}',f'{replace_fun}',f'{base_column}',f'{replace_column}')         
        
        #where null \ not null
        sql = f"select loc,AGG(COLUMN) from {dbname}.stable_1 where ts is not null group by loc order by loc "
        self.data_check_tbname(sql,'Y','Y',f'{base_fun}',f'{replace_fun}',f'{base_column}',f'{replace_column}')        
        
        sql1 = f"select * from ({sql})"
        self.data_check_tbname(sql1,'Y','Y',f'{base_fun}',f'{replace_fun}',f'{base_column}',f'{replace_column}') 
        
        sql2 = f"select * from ({sql}) order by loc "
        self.data_check_tbname(sql2,'Y','Y',f'{base_fun}',f'{replace_fun}',f'{base_column}',f'{replace_column}') 
        
        sql = f"select loc,AGG(COLUMN) from {dbname}.stable_1 where ts is null group by loc order by loc "
        self.data_check_tbname(sql,'NULL','NULL',f'{base_fun}',f'{replace_fun}',f'{base_column}',f'{replace_column}')        
        
        sql1 = f"select * from ({sql})"
        self.data_check_tbname(sql1,'NULL','NULL',f'{base_fun}',f'{replace_fun}',f'{base_column}',f'{replace_column}') 
        
        sql2 = f"select * from ({sql}) order by loc "
        self.data_check_tbname(sql2,'NULL','NULL',f'{base_fun}',f'{replace_fun}',f'{base_column}',f'{replace_column}') 
        
        #union
        sql = f"select loc tb,AGG(COLUMN) from {dbname}.stable_1 group by loc order by loc "
        sql = f"({sql}) union ({sql}) order by tb"
        self.data_check_tbname(sql,'Y','Y',f'{base_fun}',f'{replace_fun}',f'{base_column}',f'{replace_column}')         
        
        sql1 = f"select * from ({sql})"
        self.data_check_tbname(sql1,'Y','Y',f'{base_fun}',f'{replace_fun}',f'{base_column}',f'{replace_column}')  
        
        sql2 = f"select * from ({sql}) order by tb "
        self.data_check_tbname(sql2,'Y','Y',f'{base_fun}',f'{replace_fun}',f'{base_column}',f'{replace_column}')  
        
        #union all
        sql = f"select loc tb,AGG(COLUMN) from {dbname}.stable_1 group by loc order by loc "
        sql = f"({sql}) union all ({sql}) order by tb"
        self.data_check_tbname(sql,'UNIONALL','UNIONALL',f'{base_fun}',f'{replace_fun}',f'{base_column}',f'{replace_column}')         
        
        sql1 = f"select * from ({sql})"
        self.data_check_tbname(sql1,'UNIONALL','UNIONALL',f'{base_fun}',f'{replace_fun}',f'{base_column}',f'{replace_column}')  
        
        sql2 = f"select * from ({sql}) order by tb "
        self.data_check_tbname(sql2,'UNIONALL','UNIONALL',f'{base_fun}',f'{replace_fun}',f'{base_column}',f'{replace_column}')  
        
        
        #having <=0
        sql = f"select loc,AGG(COLUMN) from {dbname}.stable_1 group by loc having count(*) = 0 order by loc "
        self.data_check_tbname(sql,'HAVING=0','HAVING=0',f'{base_fun}',f'{replace_fun}',f'{base_column}',f'{replace_column}')          
        
        sql1 = f"select * from ({sql})"
        self.data_check_tbname(sql1,'HAVING=0','HAVING=0',f'{base_fun}',f'{replace_fun}',f'{base_column}',f'{replace_column}')   
        
        sql2 = f"select * from ({sql}) order by loc "
        self.data_check_tbname(sql2,'HAVING=0','HAVING=0',f'{base_fun}',f'{replace_fun}',f'{base_column}',f'{replace_column}')   
        
        sql = f"select loc,AGG(COLUMN) from {dbname}.stable_1 group by loc having count(*) <= 0 order by loc "
        self.data_check_tbname(sql,'HAVING=0','HAVING=0',f'{base_fun}',f'{replace_fun}',f'{base_column}',f'{replace_column}')          
        
        sql1 = f"select * from ({sql})"
        self.data_check_tbname(sql1,'HAVING=0','HAVING=0',f'{base_fun}',f'{replace_fun}',f'{base_column}',f'{replace_column}')  
        
        sql2 = f"select * from ({sql}) order by loc "
        self.data_check_tbname(sql2,'HAVING=0','HAVING=0',f'{base_fun}',f'{replace_fun}',f'{base_column}',f'{replace_column}')   
        
        
        sql = f"select loc,AGG(COLUMN) from {dbname}.stable_1 where ts is null group by loc having count(*) <= 0 order by loc "
        self.data_check_tbname(sql,'NULL','NULL',f'{base_fun}',f'{replace_fun}',f'{base_column}',f'{replace_column}')          
        
        sql1 = f"select * from ({sql})"
        self.data_check_tbname(sql1,'NULL','NULL',f'{base_fun}',f'{replace_fun}',f'{base_column}',f'{replace_column}')  
        
        sql2 = f"select * from ({sql}) order by loc "
        self.data_check_tbname(sql2,'NULL','NULL',f'{base_fun}',f'{replace_fun}',f'{base_column}',f'{replace_column}')   
        
        #count + having <=0 
        sql = f"select loc,AGG(COLUMN) from {dbname}.stable_1 group by loc having AGG(COLUMN) = 0 order by loc "
        self.data_check_tbname(sql,'HAVING=0','HAVING=0',f'{base_fun}',f'{replace_fun}',f'{base_column}',f'{replace_column}')          
        
        sql1 = f"select * from ({sql})"
        self.data_check_tbname(sql1,'HAVING=0','HAVING=0',f'{base_fun}',f'{replace_fun}',f'{base_column}',f'{replace_column}')   
        
        sql2 = f"select * from ({sql}) order by loc "
        self.data_check_tbname(sql2,'HAVING=0','HAVING=0',f'{base_fun}',f'{replace_fun}',f'{base_column}',f'{replace_column}') 
        
        
        sql = f"select loc,AGG(COLUMN) from {dbname}.stable_1 group by loc having AGG(COLUMN) <= 0 order by loc "
        self.data_check_tbname(sql,'HAVING=0','HAVING=0',f'{base_fun}',f'{replace_fun}',f'{base_column}',f'{replace_column}')          
        
        sql1 = f"select * from ({sql})"
        self.data_check_tbname(sql1,'HAVING=0','HAVING=0',f'{base_fun}',f'{replace_fun}',f'{base_column}',f'{replace_column}')   
        
        sql2 = f"select * from ({sql}) order by loc "
        self.data_check_tbname(sql2,'HAVING=0','HAVING=0',f'{base_fun}',f'{replace_fun}',f'{base_column}',f'{replace_column}') 
        
        
        sql = f"select loc,AGG(COLUMN) from {dbname}.stable_1 where ts is null group by loc having AGG(COLUMN) = 0 order by loc "
        self.data_check_tbname(sql,'NULL','NULL',f'{base_fun}',f'{replace_fun}',f'{base_column}',f'{replace_column}')          
        
        sql1 = f"select * from ({sql})"
        self.data_check_tbname(sql1,'NULL','NULL',f'{base_fun}',f'{replace_fun}',f'{base_column}',f'{replace_column}')  
        
        sql2 = f"select * from ({sql}) order by loc "
        self.data_check_tbname(sql2,'NULL','NULL',f'{base_fun}',f'{replace_fun}',f'{base_column}',f'{replace_column}')  
        
        #having >0
        sql = f"select loc,AGG(COLUMN) from {dbname}.stable_1 group by loc having count(*) != 0 order by loc "
        self.data_check_tbname(sql,'HAVING>0','HAVING>0',f'{base_fun}',f'{replace_fun}',f'{base_column}',f'{replace_column}')          
        
        sql1 = f"select * from ({sql})"
        self.data_check_tbname(sql1,'HAVING>0','HAVING>0',f'{base_fun}',f'{replace_fun}',f'{base_column}',f'{replace_column}')  
        
        sql2 = f"select * from ({sql}) order by loc "
        self.data_check_tbname(sql2,'HAVING>0','HAVING>0',f'{base_fun}',f'{replace_fun}',f'{base_column}',f'{replace_column}')   
        
        sql = f"select loc,AGG(COLUMN) from {dbname}.stable_1 group by loc having count(*) > 0 order by loc "
        self.data_check_tbname(sql,'HAVING>0','HAVING>0',f'{base_fun}',f'{replace_fun}',f'{base_column}',f'{replace_column}')          
        
        sql1 = f"select * from ({sql})"
        self.data_check_tbname(sql1,'HAVING>0','HAVING>0',f'{base_fun}',f'{replace_fun}',f'{base_column}',f'{replace_column}')   
        
        sql2 = f"select * from ({sql}) order by loc "
        self.data_check_tbname(sql2,'HAVING>0','HAVING>0',f'{base_fun}',f'{replace_fun}',f'{base_column}',f'{replace_column}')   
        
        
        sql = f"select loc,AGG(COLUMN) from {dbname}.stable_1 where ts is null group by loc having count(*) > 0 order by loc "
        self.data_check_tbname(sql,'AGG0','AGG0',f'{base_fun}',f'{replace_fun}',f'{base_column}',f'{replace_column}')          
        
        sql1 = f"select * from ({sql})"
        self.data_check_tbname(sql1,'AGG0','AGG0',f'{base_fun}',f'{replace_fun}',f'{base_column}',f'{replace_column}')   
        
        sql2 = f"select * from ({sql}) order by loc "
        self.data_check_tbname(sql2,'AGG0','AGG0',f'{base_fun}',f'{replace_fun}',f'{base_column}',f'{replace_column}')  
        
        #count + having >0 
        sql = f"select loc,AGG(COLUMN) from {dbname}.stable_1 group by loc having AGG(COLUMN) != 0 order by loc "
        self.data_check_tbname(sql,'HAVING>0','HAVING>0',f'{base_fun}',f'{replace_fun}',f'{base_column}',f'{replace_column}')          
        
        sql1 = f"select * from ({sql})"
        self.data_check_tbname(sql1,'HAVING>0','HAVING>0',f'{base_fun}',f'{replace_fun}',f'{base_column}',f'{replace_column}')  
        
        sql2 = f"select * from ({sql}) order by loc "
        self.data_check_tbname(sql2,'HAVING>0','HAVING>0',f'{base_fun}',f'{replace_fun}',f'{base_column}',f'{replace_column}')   
        
        sql = f"select loc,AGG(COLUMN) from {dbname}.stable_1 group by loc having AGG(COLUMN) > 0 order by loc "
        self.data_check_tbname(sql,'HAVING>0','HAVING>0',f'{base_fun}',f'{replace_fun}',f'{base_column}',f'{replace_column}')          
        
        sql1 = f"select * from ({sql})"
        self.data_check_tbname(sql1,'HAVING>0','HAVING>0',f'{base_fun}',f'{replace_fun}',f'{base_column}',f'{replace_column}')   
        
        sql2 = f"select * from ({sql}) order by loc "
        self.data_check_tbname(sql2,'HAVING>0','HAVING>0',f'{base_fun}',f'{replace_fun}',f'{base_column}',f'{replace_column}')   
        
        sql = f"select loc,AGG(COLUMN) from {dbname}.stable_1 where ts is null group by loc having AGG(COLUMN) != 0 order by loc "
        self.data_check_tbname(sql,'AGG0','AGG0',f'{base_fun}',f'{replace_fun}',f'{base_column}',f'{replace_column}')          
        
        sql1 = f"select * from ({sql})"
        self.data_check_tbname(sql1,'AGG0','AGG0',f'{base_fun}',f'{replace_fun}',f'{base_column}',f'{replace_column}')   
        
        sql2 = f"select * from ({sql}) order by loc "
        self.data_check_tbname(sql2,'AGG0','AGG0',f'{base_fun}',f'{replace_fun}',f'{base_column}',f'{replace_column}')  
        
        #order by count(*) 
        sql = f"select loc,AGG(COLUMN) from {dbname}.stable_1 group by loc order by loc,count(*) "
        self.data_check_tbname(sql,'Y','Y',f'{base_fun}',f'{replace_fun}',f'{base_column}',f'{replace_column}')        
        
        sql1 = f"select * from ({sql})"
        self.data_check_tbname(sql1,'Y','Y',f'{base_fun}',f'{replace_fun}',f'{base_column}',f'{replace_column}')  
        
        sql2 = f"select * from ({sql}) order by loc "
        self.data_check_tbname(sql2,'Y','Y',f'{base_fun}',f'{replace_fun}',f'{base_column}',f'{replace_column}')  
        
        sql = f"select loc,AGG(COLUMN) from {dbname}.stable_1 where ts is null group by loc order by loc,count(*) "
        self.data_check_tbname(sql,'NULL','NULL',f'{base_fun}',f'{replace_fun}',f'{base_column}',f'{replace_column}')        
        
        sql1 = f"select * from ({sql})"
        self.data_check_tbname(sql1,'NULL','NULL',f'{base_fun}',f'{replace_fun}',f'{base_column}',f'{replace_column}')  
        
        sql2 = f"select * from ({sql}) order by loc "
        self.data_check_tbname(sql2,'NULL','NULL',f'{base_fun}',f'{replace_fun}',f'{base_column}',f'{replace_column}')  

    def tag_count_null(self, dbname="nested",base_fun="AGG",replace_fun="COUNT",base_column="COLUMN",replace_column="q_int",execute="Y"):

        # stable count(*)
        sql = f"select loc,AGG(COLUMN) from {dbname}.stable_1 group by loc order by loc "
        self.data_check_tbname(sql,'NULL','NULL',f'{base_fun}',f'{replace_fun}',f'{base_column}',f'{replace_column}')         
        
        sql1 = f"select * from ({sql})"
        self.data_check_tbname(sql1,'NULL','NULL',f'{base_fun}',f'{replace_fun}',f'{base_column}',f'{replace_column}')  
        
        sql2 = f"select * from ({sql}) order by loc "
        self.data_check_tbname(sql2,'NULL','NULL',f'{base_fun}',f'{replace_fun}',f'{base_column}',f'{replace_column}')  
        
        #where null \ not null
        sql = f"select loc,AGG(COLUMN) from {dbname}.stable_1 where ts is not null group by loc order by loc "
        self.data_check_tbname(sql,'NULL','NULL',f'{base_fun}',f'{replace_fun}',f'{base_column}',f'{replace_column}')        
        
        sql1 = f"select * from ({sql})"
        self.data_check_tbname(sql1,'NULL','NULL',f'{base_fun}',f'{replace_fun}',f'{base_column}',f'{replace_column}') 
        
        sql2 = f"select * from ({sql}) order by loc "
        self.data_check_tbname(sql2,'NULL','NULL',f'{base_fun}',f'{replace_fun}',f'{base_column}',f'{replace_column}') 
        
        sql = f"select loc,AGG(COLUMN) from {dbname}.stable_1 where ts is null group by loc order by loc "
        self.data_check_tbname(sql,'NULL','NULL',f'{base_fun}',f'{replace_fun}',f'{base_column}',f'{replace_column}')        
        
        sql1 = f"select * from ({sql})"
        self.data_check_tbname(sql1,'NULL','NULL',f'{base_fun}',f'{replace_fun}',f'{base_column}',f'{replace_column}') 
        
        sql2 = f"select * from ({sql}) order by loc "
        self.data_check_tbname(sql2,'NULL','NULL',f'{base_fun}',f'{replace_fun}',f'{base_column}',f'{replace_column}') 
        
        #union
        sql = f"select loc tb,AGG(COLUMN) from {dbname}.stable_1 group by loc order by loc "
        sql = f"({sql}) union ({sql}) order by tb"
        self.data_check_tbname(sql,'NULL','NULL',f'{base_fun}',f'{replace_fun}',f'{base_column}',f'{replace_column}')         
        
        sql1 = f"select * from ({sql})"
        self.data_check_tbname(sql1,'NULL','NULL',f'{base_fun}',f'{replace_fun}',f'{base_column}',f'{replace_column}')  
        
        sql2 = f"select * from ({sql}) order by tb "
        self.data_check_tbname(sql2,'NULL','NULL',f'{base_fun}',f'{replace_fun}',f'{base_column}',f'{replace_column}')  
        
        #union all
        sql = f"select loc tb,AGG(COLUMN) from {dbname}.stable_1 group by loc order by loc "
        sql = f"({sql}) union all ({sql}) order by tb"
        self.data_check_tbname(sql,'UNIONALLNULL','UNIONALLNULL',f'{base_fun}',f'{replace_fun}',f'{base_column}',f'{replace_column}')         
        
        sql1 = f"select * from ({sql})"
        self.data_check_tbname(sql1,'UNIONALLNULL','UNIONALLNULL',f'{base_fun}',f'{replace_fun}',f'{base_column}',f'{replace_column}')  
        
        sql2 = f"select * from ({sql}) order by tb "
        self.data_check_tbname(sql2,'UNIONALLNULL','UNIONALLNULL',f'{base_fun}',f'{replace_fun}',f'{base_column}',f'{replace_column}')  
        
        #having <=0
        sql = f"select loc,AGG(COLUMN) from {dbname}.stable_1 group by loc having count(*) = 0 order by loc "
        self.data_check_tbname(sql,'HAVING=0','HAVING=0',f'{base_fun}',f'{replace_fun}',f'{base_column}',f'{replace_column}')          
        
        sql1 = f"select * from ({sql})"
        self.data_check_tbname(sql1,'HAVING=0','HAVING=0',f'{base_fun}',f'{replace_fun}',f'{base_column}',f'{replace_column}')   
        
        sql2 = f"select * from ({sql}) order by loc "
        self.data_check_tbname(sql2,'HAVING=0','HAVING=0',f'{base_fun}',f'{replace_fun}',f'{base_column}',f'{replace_column}')   
        
        sql = f"select loc,AGG(COLUMN) from {dbname}.stable_1 group by loc having count(*) <= 0 order by loc "
        self.data_check_tbname(sql,'HAVING=0','HAVING=0',f'{base_fun}',f'{replace_fun}',f'{base_column}',f'{replace_column}')          
        
        sql1 = f"select * from ({sql})"
        self.data_check_tbname(sql1,'HAVING=0','HAVING=0',f'{base_fun}',f'{replace_fun}',f'{base_column}',f'{replace_column}')  
        
        sql2 = f"select * from ({sql}) order by loc "
        self.data_check_tbname(sql2,'HAVING=0','HAVING=0',f'{base_fun}',f'{replace_fun}',f'{base_column}',f'{replace_column}')   
        
        
        sql = f"select loc,AGG(COLUMN) from {dbname}.stable_1 where ts is null group by loc having count(*) <= 0 order by loc "
        self.data_check_tbname(sql,'NULL','NULL',f'{base_fun}',f'{replace_fun}',f'{base_column}',f'{replace_column}')          
        
        sql1 = f"select * from ({sql})"
        self.data_check_tbname(sql1,'NULL','NULL',f'{base_fun}',f'{replace_fun}',f'{base_column}',f'{replace_column}')  
        
        sql2 = f"select * from ({sql}) order by loc "
        self.data_check_tbname(sql2,'NULL','NULL',f'{base_fun}',f'{replace_fun}',f'{base_column}',f'{replace_column}')   
        
        #having >0
        sql = f"select loc,AGG(COLUMN) from {dbname}.stable_1 group by loc having count(*) != 0 order by loc "
        self.data_check_tbname(sql,'HAVINGCOLUMN=0','HAVINGCOLUMN=0',f'{base_fun}',f'{replace_fun}',f'{base_column}',f'{replace_column}')          
        
        sql1 = f"select * from ({sql})"
        self.data_check_tbname(sql1,'HAVINGCOLUMN=0','HAVINGCOLUMN=0',f'{base_fun}',f'{replace_fun}',f'{base_column}',f'{replace_column}')  
        
        sql2 = f"select * from ({sql}) order by loc "
        self.data_check_tbname(sql2,'HAVINGCOLUMN=0','HAVINGCOLUMN=0',f'{base_fun}',f'{replace_fun}',f'{base_column}',f'{replace_column}')   
        
        sql = f"select loc,AGG(COLUMN) from {dbname}.stable_1 group by loc having count(*) > 0 order by loc "
        self.data_check_tbname(sql,'HAVINGCOLUMN=0','HAVINGCOLUMN=0',f'{base_fun}',f'{replace_fun}',f'{base_column}',f'{replace_column}')          
        
        sql1 = f"select * from ({sql})"
        self.data_check_tbname(sql1,'HAVINGCOLUMN=0','HAVINGCOLUMN=0',f'{base_fun}',f'{replace_fun}',f'{base_column}',f'{replace_column}')   
        
        sql2 = f"select * from ({sql}) order by loc "
        self.data_check_tbname(sql2,'HAVINGCOLUMN=0','HAVINGCOLUMN=0',f'{base_fun}',f'{replace_fun}',f'{base_column}',f'{replace_column}')   
        
        
        sql = f"select loc,AGG(COLUMN) from {dbname}.stable_1 where ts is null group by loc having count(*) != 0 order by loc "
        self.data_check_tbname(sql,'AGG0','AGG0',f'{base_fun}',f'{replace_fun}',f'{base_column}',f'{replace_column}')          
        
        sql1 = f"select * from ({sql})"
        self.data_check_tbname(sql1,'AGG0','AGG0',f'{base_fun}',f'{replace_fun}',f'{base_column}',f'{replace_column}')  
        
        sql2 = f"select * from ({sql}) order by loc "
        self.data_check_tbname(sql2,'AGG0','AGG0',f'{base_fun}',f'{replace_fun}',f'{base_column}',f'{replace_column}') 
        
        #order by count(*) 
        sql = f"select loc,AGG(COLUMN) from {dbname}.stable_1 group by loc order by loc,count(*) "
        self.data_check_tbname(sql,'NULL','NULL',f'{base_fun}',f'{replace_fun}',f'{base_column}',f'{replace_column}')        
        
        sql1 = f"select * from ({sql})"
        self.data_check_tbname(sql1,'NULL','NULL',f'{base_fun}',f'{replace_fun}',f'{base_column}',f'{replace_column}')  
        
        sql2 = f"select * from ({sql}) order by loc "
        self.data_check_tbname(sql2,'NULL','NULL',f'{base_fun}',f'{replace_fun}',f'{base_column}',f'{replace_column}')  
        
        sql = f"select loc,AGG(COLUMN) from {dbname}.stable_1 where ts is null group by loc order by loc,count(*) "
        self.data_check_tbname(sql,'NULL','NULL',f'{base_fun}',f'{replace_fun}',f'{base_column}',f'{replace_column}')        
        
        sql1 = f"select * from ({sql})"
        self.data_check_tbname(sql1,'NULL','NULL',f'{base_fun}',f'{replace_fun}',f'{base_column}',f'{replace_column}')  
        
        sql2 = f"select * from ({sql}) order by loc "
        self.data_check_tbname(sql2,'NULL','NULL',f'{base_fun}',f'{replace_fun}',f'{base_column}',f'{replace_column}')  
        
    def tag_count_all(self):  
        #random data        
        fake = Faker('zh_CN')
        random_data_big = fake.random_int(min=-9223372036854775807, max=9223372036854775807, step=10)
        random_data_str = fake.pystr() 
        random_data_float = fake.pyfloat()
        
        #random cal
        calculations = ['+','-','*','/']
        calculation = random.sample(calculations,1)
        calculation = str(calculation).replace("[","").replace("]","").replace("'","").replace(", ","")
        
        #random data column
        data_columns = ['q_int','q_bigint','q_smallint','q_tinyint','q_float','q_double']
        data_null_columns = ['q_int_null','q_bigint_null','q_smallint_null','q_tinyint_null','q_float_null','q_double_null']
        
        data_column = random.sample(data_columns,1)
        data_column = str(data_column).replace("[","").replace("]","").replace("'","").replace(", ","")
        
        data_null_column = random.sample(data_null_columns,1)
        data_null_column = str(data_null_column).replace("[","").replace("]","").replace("'","").replace(", ","")
        
        data_all_columns = data_columns + data_null_columns
        data_all_column = random.sample(data_all_columns,1)
        data_all_column = str(data_all_column).replace("[","").replace("]","").replace("'","").replace(", ","")
        
        #random data tag
        data_tags = ['t_int','t_bigint','t_smallint','t_tinyint','t_float','t_double']
        data_null_tags = ['t_int_null','t_bigint_null','t_smallint_null','t_tinyint_null','t_float_null','t_double_null']
        
        data_tag = random.sample(data_tags,1)
        data_tag = str(data_tag).replace("[","").replace("]","").replace("'","").replace(", ","")
        
        data_null_tag = random.sample(data_null_tags,1)
        data_null_tag = str(data_null_tag).replace("[","").replace("]","").replace("'","").replace(", ","")
        
        data_all_tags = data_tags + data_null_tags
        data_all_tag = random.sample(data_all_tags,1)
        data_all_tag = str(data_all_tag).replace("[","").replace("]","").replace("'","").replace(", ","")
        
        #random ts column
        ts_columns = ['ts','_c0','_C0','_rowts']
        ts_column = random.sample(ts_columns,1)
        ts_column = str(ts_column).replace("[","").replace("]","").replace("'","").replace(", ","")
        #random other column
        other_columns = ['q_bool','q_binary','q_nchar','q_ts','loc','t_bool','t_binary','t_nchar','t_ts']
        other_column = random.sample(other_columns,1)
        other_column = str(other_column).replace("[","").replace("]","").replace("'","").replace(", ","")
        
        #random other null column and tag
        other_null_columns = ['q_bool_null','q_binary_null','q_nchar_null','q_ts_null','t_bool_null','t_binary_null','t_nchar_null','t_ts_null']
        other_null_column = random.sample(other_null_columns,1)
        other_null_column = str(other_null_column).replace("[","").replace("]","").replace("'","").replace(", ","")
        
        #random all column
        all_columns = data_all_columns + data_all_tags + ts_columns + other_columns + other_null_columns
        all_column = random.sample(all_columns,1)
        all_column = str(all_column).replace("[","").replace("]","").replace("'","").replace(", ","")
        
        
        self.tag_count_null(replace_fun='count',replace_column=f'{data_null_column}') 
        self.tag_count_null(replace_fun='count',replace_column=f'{data_null_tag}') 
        self.tag_count_null(replace_fun='count',replace_column=f'{other_null_column}') 
        
        self.tag_count(replace_fun='count',replace_column='*') 
        self.tag_count(replace_fun='count',replace_column=f'{data_column}') 
        self.tag_count(replace_fun='count',replace_column=f'{ts_column}') 
        
        self.tag_count(replace_fun='count',replace_column=f'{other_column}') 
        self.tag_count(replace_fun='count',replace_column=f'{data_tag}') 
        
        self.tag_count(replace_fun='count',replace_column=f'{random_data_big}') 
        self.tag_count(replace_fun='count',replace_column=f'{random_data_float}') 
        self.tag_count(replace_fun='count',replace_column=f'\'{random_data_str}\'') 
        self.tag_count(replace_fun='count',replace_column=f'{random_data_big} {calculation} (abs({data_column})+1)') 
        self.tag_count(replace_fun='count',replace_column=f'{random_data_float} {calculation} (abs({data_column})+1)')  
                
        
    
    def modify_tables(self):
        fake = Faker('zh_CN')
        tdSql.execute('delete from stable_1_3;')
        tdSql.execute('delete from stable_1_4;')
        tdSql.execute('''create table stable_1_5 using stable_1 tags('stable_1_5', '%d' , '%d', '%d' , '%d' , 1 , 'binary6.%s' , 'nchar6.%s' , '%f', '%f' ,'%d') ;'''
                      %(fake.random_int(min=-2147483647, max=2147483647, step=1), fake.random_int(min=-9223372036854775807, max=9223372036854775807, step=1),
                        fake.random_int(min=-32767, max=32767, step=1) , fake.random_int(min=-127, max=127, step=1) ,
                        fake.pystr() ,fake.pystr() ,fake.pyfloat(),fake.pyfloat(),fake.random_int(min=-2147483647, max=2147483647, step=1)))
        tdSql.execute('''create table stable_1_6 using stable_1 tags('stable_1_6', '%d' , '%d', '%d' , '%d' , 1 , 'binary6.%s' , 'nchar6.%s' , '%f', '%f' ,'%d') ;'''
                      %(fake.random_int(min=-2147483647, max=2147483647, step=1), fake.random_int(min=-9223372036854775807, max=9223372036854775807, step=1),
                        fake.random_int(min=-32767, max=32767, step=1) , fake.random_int(min=-127, max=127, step=1) ,
                        fake.pystr() ,fake.pystr() ,fake.pyfloat(),fake.pyfloat(),fake.random_int(min=-2147483647, max=2147483647, step=1)))
        tdSql.execute('''create table stable_1_7 using stable_1 tags('stable_1_7', '%d' , '%d', '%d' , '%d' , 1 , 'binary6.%s' , 'nchar6.%s' , '%f', '%f' ,'%d') ;'''
                      %(fake.random_int(min=-2147483647, max=2147483647, step=1), fake.random_int(min=-9223372036854775807, max=9223372036854775807, step=1),
                        fake.random_int(min=-32767, max=32767, step=1) , fake.random_int(min=-127, max=127, step=1) ,
                        fake.pystr() ,fake.pystr() ,fake.pyfloat(),fake.pyfloat(),fake.random_int(min=-2147483647, max=2147483647, step=1)))
        tdSql.execute('''create table stable_1_8 using stable_1 tags('stable_1_8', '%d' , '%d', '%d' , '%d' , 1 , 'binary6.%s' , 'nchar6.%s' , '%f', '%f' ,'%d') ;'''
                      %(fake.random_int(min=-2147483647, max=2147483647, step=1), fake.random_int(min=-9223372036854775807, max=9223372036854775807, step=1),
                        fake.random_int(min=-32767, max=32767, step=1) , fake.random_int(min=-127, max=127, step=1) ,
                        fake.pystr() ,fake.pystr() ,fake.pyfloat(),fake.pyfloat(),fake.random_int(min=-2147483647, max=2147483647, step=1)))
        tdSql.execute('''create table stable_1_9 using stable_1 tags('stable_1_9', '%d' , '%d', '%d' , '%d' , 1 , 'binary6.%s' , 'nchar6.%s' , '%f', '%f' ,'%d') ;'''
                      %(fake.random_int(min=-2147483647, max=2147483647, step=1), fake.random_int(min=-9223372036854775807, max=9223372036854775807, step=1),
                        fake.random_int(min=-32767, max=32767, step=1) , fake.random_int(min=-127, max=127, step=1) ,
                        fake.pystr() ,fake.pystr() ,fake.pyfloat(),fake.pyfloat(),fake.random_int(min=-2147483647, max=2147483647, step=1)))
        tdSql.execute('''create table stable_1_90 using stable_1 tags('stable_1_90', '%d' , '%d', '%d' , '%d' , 1 , 'binary6.%s' , 'nchar6.%s' , '%f', '%f' ,'%d') ;'''
                      %(fake.random_int(min=-2147483647, max=2147483647, step=1), fake.random_int(min=-9223372036854775807, max=9223372036854775807, step=1),
                        fake.random_int(min=-32767, max=32767, step=1) , fake.random_int(min=-127, max=127, step=1) ,
                        fake.pystr() ,fake.pystr() ,fake.pyfloat(),fake.pyfloat(),fake.random_int(min=-2147483647, max=2147483647, step=1)))
        tdSql.execute('''create table stable_1_91 using stable_1 tags('stable_1_91', '%d' , '%d', '%d' , '%d' , 1 , 'binary6.%s' , 'nchar6.%s' , '%f', '%f' ,'%d') ;'''
                      %(fake.random_int(min=-2147483647, max=2147483647, step=1), fake.random_int(min=-9223372036854775807, max=9223372036854775807, step=1),
                        fake.random_int(min=-32767, max=32767, step=1) , fake.random_int(min=-127, max=127, step=1) ,
                        fake.pystr() ,fake.pystr() ,fake.pyfloat(),fake.pyfloat(),fake.random_int(min=-2147483647, max=2147483647, step=1)))
        tdSql.execute('''create table stable_1_92 using stable_1 tags('stable_1_92', '%d' , '%d', '%d' , '%d' , 1 , 'binary6.%s' , 'nchar6.%s' , '%f', '%f' ,'%d') ;'''
                      %(fake.random_int(min=-2147483647, max=2147483647, step=1), fake.random_int(min=-9223372036854775807, max=9223372036854775807, step=1),
                        fake.random_int(min=-32767, max=32767, step=1) , fake.random_int(min=-127, max=127, step=1) ,
                        fake.pystr() ,fake.pystr() ,fake.pyfloat(),fake.pyfloat(),fake.random_int(min=-2147483647, max=2147483647, step=1)))
                
        tdSql.execute('alter stable stable_1 add tag t_int_null INT;')
        tdSql.execute('alter stable stable_1 add tag t_bigint_null BIGINT;')
        tdSql.execute('alter stable stable_1 add tag t_smallint_null SMALLINT;')
        tdSql.execute('alter stable stable_1 add tag t_tinyint_null TINYINT;')
        tdSql.execute('alter stable stable_1 add tag t_bool_null BOOL;')
        tdSql.execute('alter stable stable_1 add tag t_binary_null VARCHAR(100);')
        tdSql.execute('alter stable stable_1 add tag t_nchar_null NCHAR(100);')
        tdSql.execute('alter stable stable_1 add tag t_float_null FLOAT;')
        tdSql.execute('alter stable stable_1 add tag t_double_null DOUBLE;')
        tdSql.execute('alter stable stable_1 add tag t_ts_null TIMESTAMP;')
               
        tdSql.execute('alter stable stable_1 drop column q_nchar8;')
        tdSql.execute('alter stable stable_1 drop column q_binary8;')
        tdSql.execute('alter stable stable_1 drop column q_nchar7;')
        tdSql.execute('alter stable stable_1 drop column q_binary7;')
        tdSql.execute('alter stable stable_1 drop column q_nchar6;')
        tdSql.execute('alter stable stable_1 drop column q_binary6;')
        tdSql.execute('alter stable stable_1 drop column q_nchar5;')
        tdSql.execute('alter stable stable_1 drop column q_binary5;')
        tdSql.execute('alter stable stable_1 drop column q_nchar4;')
        tdSql.execute('alter stable stable_1 drop column q_binary4;')
                        
    def run(self):
        tdSql.prepare()
        
        startTime = time.time() 

        # self.create_tables()
        # self.insert_data()     
         
        self.dropandcreateDB_random("nested", 1)
        self.modify_tables()
               
        for i in range(1):
            self.tbname_count_all() 
            self.tag_count_all()  
            self.tbname_agg_all()    
          

        endTime = time.time()
        print("total time %ds" % (endTime - startTime))

        


    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
