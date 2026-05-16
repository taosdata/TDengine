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

import os
import time
from Query.queryutil.createdata import *
from taostest import TDCase
from faker import Faker
import subprocess
from taostest import TDCase
from taostest.util.remote import Remote
from taostest.util.common import TDCom
from taostest.components import TaosD
import threading
import multiprocessing

class TDTestQuery(TDCase):
    
    db = 'dbnew'
    
    def init(self):
        super(TDTestQuery, self).init()
        self.tdCom = TDCom(self.tdSql)
        self.remote: Remote = Remote(self.logger)
        self.taosd = TaosD(self.remote)
        
        self.firstEP = []       
        self.source_taosd_list = []
        print(self.env_setting["settings"])
        for env_setting in self.env_setting["settings"]:
            if env_setting["name"].lower() == "taosd":
                self.taosd_setting = env_setting
                self.firstEP.append(self.taosd_setting['spec']['config']['firstEP'])
        self.taosd_num = len(self.firstEP)
        for i in range(self.taosd_num):
            self.source_taosd_list.append(self.firstEP[i].split(':'))
            print(self.source_taosd_list)
        self.target_taosd = self.firstEP[-1].split(':')
        self.taosBenchmark_fqdn = self.get_fqdn('taosBenchmark')
            
    def tags(self) -> str:
         
        return ""
    
    def author(self) -> str:
         
        return "Guo Xiangyang"

    def desc(self) -> str:
        case_description = '''
        case1<xyguo>:
        ''' 
        return case_description
    
    def sql_check1(self):
        self.tdSql.execute("alter database %s cachemodel 'both' ;" %self.db)
        self.tdSql.execute("reset query cache;")
        
        sql = "select tbname,last_row(c0),ts from %s.stb0 ;"%self.db
        self.tdSql.query(sql)
        print(self.tdSql.query_cols)
        print(self.tdSql.getData(0,0))
        print(self.tdSql.getData(0,1))
        print(self.tdSql.getData(0,2))
        
        self.tdSql.execute("alter database %s cachemodel 'none' ;" %self.db )
        self.tdSql.execute("reset query cache;")
        
        self.tdSql.query(sql)
        print(self.tdSql.getData(0,0))
        print(self.tdSql.getData(0,1))
        print(self.tdSql.getData(0,2))
    
    def sql_cachemodel_both_none_check(self,sql):
        self.tdSql.execute("alter database %s cachemodel 'last_row' ;" %self.db)
        self.tdSql.execute("reset query cache;")
        
        #sql = "select tbname,last_row(c0),ts from %s.stb0 ;"%self.db
        self.tdSql.query(sql)
        col = self.tdSql.query_cols
        for i in range(col):
            self.tdSql.execute("alter database %s cachemodel 'both' ;" %self.db)
            self.tdSql.execute("reset query cache;")
            self.tdSql.query(sql)
            value_both = self.tdSql.getData(0,i)
        
            self.tdSql.execute("alter database %s cachemodel 'none' ;" %self.db )
            self.tdSql.execute("reset query cache;")  
            self.tdSql.query(sql)        
            value_none = self.tdSql.getData(0,i)
        
            self.tdSql.checkEqual(value_both,value_none)
        
                                
    def run(self)-> bool:
        startTime = time.time() 
        
        self.sql_cachemodel_both_none_check("select last(*) from %s.stb0 ;"%self.db)
        self.sql_cachemodel_both_none_check("select last(ts) from %s.stb0 ;"%self.db)
        self.sql_cachemodel_both_none_check("select last(c0) from %s.stb0 ;"%self.db)
        self.sql_cachemodel_both_none_check("select last(c1) from %s.stb0 ;"%self.db)
        self.sql_cachemodel_both_none_check("select last(c2) from %s.stb0 ;"%self.db)
        self.sql_cachemodel_both_none_check("select last(c3) from %s.stb0 ;"%self.db)
        self.sql_cachemodel_both_none_check("select last(c4) from %s.stb0 ;"%self.db)
        self.sql_cachemodel_both_none_check("select last(c5) from %s.stb0 ;"%self.db)
        self.sql_cachemodel_both_none_check("select last(t0) from %s.stb0 ;"%self.db)
        self.sql_cachemodel_both_none_check("select last(t1) from %s.stb0 ;"%self.db)
        self.sql_cachemodel_both_none_check("select last(tbname) from %s.stb0 ;"%self.db)
        
        self.sql_cachemodel_both_none_check("select last_row(*) from %s.stb0 ;"%self.db)
        self.sql_cachemodel_both_none_check("select last_row(ts) from %s.stb0 ;"%self.db)
        self.sql_cachemodel_both_none_check("select last_row(c0) from %s.stb0 ;"%self.db)
        self.sql_cachemodel_both_none_check("select last_row(c1) from %s.stb0 ;"%self.db)
        self.sql_cachemodel_both_none_check("select last_row(c2) from %s.stb0 ;"%self.db)
        self.sql_cachemodel_both_none_check("select last_row(c3) from %s.stb0 ;"%self.db)
        self.sql_cachemodel_both_none_check("select last_row(c4) from %s.stb0 ;"%self.db)
        self.sql_cachemodel_both_none_check("select last_row(c5) from %s.stb0 ;"%self.db)
        self.sql_cachemodel_both_none_check("select last_row(t0) from %s.stb0 ;"%self.db)
        self.sql_cachemodel_both_none_check("select last_row(t1) from %s.stb0 ;"%self.db)
        self.sql_cachemodel_both_none_check("select last_row(tbname) from %s.stb0 ;"%self.db)
        
        self.sql_cachemodel_both_none_check("select tbname,last_row(c0),ts from %s.stb0 ;"%self.db)
        
        endTime = time.time()
        
        self.logger.info("total time %ds" % (endTime - startTime))
