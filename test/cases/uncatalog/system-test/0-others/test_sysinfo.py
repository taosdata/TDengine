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


from new_test_framework.utils import tdLog, tdSql, tdDnodes
import subprocess
import time
import os

class TestSysinfo:

    def setup_class(cls):
        tdLog.debug("start to execute %s" % __file__)
        cls.dbname = 'db'
        cls.delaytime = 3
    
    def get_database_info(self):
        tdSql.query('select database()')
        tdSql.checkData(0,0,None)
        tdSql.execute(f'create database if not exists {self.dbname}')
        tdSql.execute(f'use {self.dbname}')
        tdSql.query('select database()')
        tdSql.checkData(0,0,self.dbname)
        tdSql.execute(f'drop database {self.dbname}')

    def check_version(self):
        taos_list = ['server','client']
        for i in taos_list:
            tdSql.query(f'select {i}_version()')
            version_c_file = os.path.join(os.path.dirname(os.getcwd()), 'source', 'util', 'src', 'version.c')
            tdLog.info(f"version_c_file: {version_c_file}")
            version_info = str(subprocess.run(f'cat {version_c_file} |grep "char td_version"', shell=True,capture_output=True).stdout.decode('utf8')).split('"')[1]
            tdSql.checkData(0,0,version_info)

    def get_server_status(self):
        time.sleep(self.delaytime)
        tdSql.query('select server_status()')
        tdSql.checkData(0,0,1)
        #!for bug
        tdDnodes.stoptaosd(1)
        time.sleep(self.delaytime * 5)
        tdSql.error('select server_status()')

    def test_sysinfo(self):
        """summary: xxx

        description: xxx

        Since: xxx

        Labels: xxx

        Jira: xxx

        Catalog:
            - xxx:xxx

        History:
            - xxx
            - xxx
        """
        self.get_database_info()
        self.check_version()
        self.get_server_status()
        tdLog.success("%s successfully executed" % __file__)

