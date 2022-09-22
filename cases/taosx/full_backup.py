###################################################################
#           Copyright (c) 2020 by TAOS Technologies, Inc.
#                     All rights reserved.
#
#  This file is proprietary and confidential to TAOS Technologies.
#  No part of this file may be reproduced, stored, transmitted,
#  disclosed or used in any form or by any means other than as
#  expressly provided by the written permission from Jianhui Tao
#
###################################################################

# -*- coding: utf-8 -*-


import json
import os
from taostest import TDCase, T
import taos
from taostest.util.remote import Remote
from taostest.performance.perfor_basic import InsertFile
from taostest.util.common import TDCom
class FullBackup(TDCase):
    def init(self):
        self.tdCom = TDCom(self.tdSql)
        self.remote: Remote = Remote(self.logger)
        self.firstEP = []
        self.source_taosd_list = []
        for env_setting in self.env_setting["settings"]:
            if env_setting["name"].lower() == "taosd":
                self.taosd_setting = env_setting
                self.firstEP.append(self.taosd_setting['spec']['config']['firstEP'])
            if env_setting["name"].lower() == 'taosx':
                self.taosx_setting = env_setting
        self.taosd_num = len(self.firstEP)
        for i in range(self.taosd_num-1):
            self.source_taosd_list.append(self.firstEP[i].split(':'))
        self.target_taosd = self.firstEP[-1].split(':')
        self.test_root = os.environ['TEST_ROOT']
        #param for taosBenchmark
        self.dbname = ['db1']
        self.stbname = ['stb1']
        self.tbname_m = ['d']
        self.tb_num = 1000
        self.row_num = 10000
        #param for taosx
        self.timeout = '1s'
        self.target_dbname = 'target'
    def get_json(self,json_path,host,port,dbname,stbname,tbname_m):
        dict = {}
        with open(json_path,'rb') as file:
            params = json.load(file)
            params['host'] = host
            params['port'] = port
            params['databases'][0]['dbinfo']['name'] = dbname
            params['databases'][0]['super_tables'][0]['name'] = stbname
            params['databases'][0]['super_tables'][0]['childtable_count'] = self.tb_num
            params['databases'][0]['super_tables'][0]['insert_rows'] = self.row_num
            params['databases'][0]['super_tables'][0]['childtable_prefix'] = tbname_m
            dict = params
        file.close()
        return dict
    def write_json(self,json_path,dict):
        with open(json_path,'w') as r:
            json.dump(dict,r)
        r.close()
    def data_insert(self):
        taosBenchmark_fqdn = self.get_fqdn('taosBenchmark')
        for source in range(len(self.source_taosd_list)):
            host = self.source_taosd_list[source][0]
            port = self.source_taosd_list[source][1]
            self.write_json(f'{self.test_root}/cases/taosx/basic.json',self.get_json(f'{self.test_root}/cases/taosx/basic.json',host,int(port),self.dbname[source],self.stbname[source],self.tbname_m[source]))
            self.remote.put(taosBenchmark_fqdn[0],f'{self.test_root}/cases/taosx/basic.json','/tmp/')
            self.remote.cmd(taosBenchmark_fqdn[0],f'taosBenchmark -f /tmp/basic.json')
    
    def run(self):
        self.data_insert()
        pass

    def cleanup(self):
        pass

    def desc(self):
        case_description = """
            export test of taosx <jiacy>
            """
        return case_description

    def author(self):
        return "Jiacy"

    def tags(self):
        return T.Taosx.Import