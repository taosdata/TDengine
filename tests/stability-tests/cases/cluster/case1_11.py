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
import random
import threading
import time

from taostest import ClusterCase

class Case11(ClusterCase):

    def init(self):
        super().init()
        self.db_name = "testdb"
        self.replicas = 2

    def check_result(self):
        self.logger.info("check result ...")
        pass

    def cleanup(self):
        pass

    def run(self):
        # drop database
        sql = "drop database if exists %s" % (self.db_name)
        self.logger.info(sql)
        self.tdSql.execute(sql)
        # create database
        sql = "create database %s replica %d" % (self.db_name, self.replicas)
        self.logger.info(sql)
        create_result = True
        try:
            self.tdSql.execute(sql)
        except:
            create_result = False
        if create_result == True:
            self.set_error_msg("create database with replica 2 success, failure expected")
            return False;
        # drop database
        sql = "drop database if exists %s" % (self.db_name)
        self.logger.info(sql)
        self.tdSql.execute(sql)
        # create database
        sql = "create database %s replica 3" % (self.db_name)
        self.logger.info(sql)
        self.tdSql.execute(sql)
        # alter database
        sql = "alter database %s replica %d" % (self.db_name, self.replicas)
        self.logger.info(sql)
        create_result = True
        try:
            self.tdSql.execute(sql)
        except:
            create_result = False
        if create_result == True:
            self.set_error_msg("alter database with replica 2 success, failure expected")
            return False;
        self.check_result()
        return True

    def cleanup(self):
        pass

    def author(self):
        '''
        abstract about author
        '''
        return "fztang"

    def tags(self):
        '''
        set tags
        '''
        return "cluster",

    def desc(self) -> str:
        case_description = '''
            [test]<fztang> test case for ... ;
        '''
        return case_description
