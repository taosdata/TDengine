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


from util.log import *
from util.cases import *
from util.sql import *
import subprocess
from util.common import *


class TDTestCase:
    def init(self, conn, logSql, replicaVar=1):
        self.replicaVar = int(replicaVar)
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor())
        self.dbname = 'db'
        self.delaytime = 3
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
            version_info = str(subprocess.run('cat ../../source/util/src/version.c |grep "char version"', shell=True,capture_output=True).stdout.decode('utf8')).split('"')[1]
            tdSql.checkData(0,0,version_info)

    def get_server_status(self):
        sleep(self.delaytime)
        tdSql.query('select server_status()')
        tdSql.checkData(0,0,1)
        #!for bug
        tdDnodes.stoptaosd(1)
        sleep(self.delaytime * 5)
        tdSql.error('select server_status()')

    def run(self):
        self.get_database_info()
        self.check_version()
        self.get_server_status()
    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)

tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
