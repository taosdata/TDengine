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

    def benchmark_query(self):
        taosBenchmark_fqdn = self.get_fqdn('taosBenchmark')
        for source in range(len(self.source_taosd_list)):
            file = '/root/asan/json/query_1time_1thread_last.json'
            self.remote.cmd(taosBenchmark_fqdn[0], f'taosBenchmark -f {file}')
            
    def tags(self) -> str:
         
        return ""
    
    def author(self) -> str:
         
        return "Guo Xiangyang"

    def desc(self) -> str:
        case_description = '''
        case1<xyguo>:
        ''' 
        return case_description
                                
    def run(self)-> bool:
        startTime = time.time() 
        
        for i in range(1000000000):
            startTime1 = time.time() 
            self.logger.info("  ================i=  %d ====================" % i)
            self.benchmark_query()  
            endTime1 = time.time()
            self.logger.info("total time %ds" % (endTime1 - startTime1))
        
        endTime = time.time()
        
        self.logger.info("total time %ds" % (endTime - startTime))
