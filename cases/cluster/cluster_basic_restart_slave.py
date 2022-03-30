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

import taos
from queryutil.createdata import *
from queryutil.where import *
from taostest import TDCase


class ClusterTestBasic(TDCase):

    def init(self):
        self.ts = 1420041600000  # 2015-01-01 00:00:00  this is begin time for first record
        self.db_name = "testdb"
        self.table_name = "stb"
        self.table_num = 10
        self.conf = self.get_component_by_name(
            "taosd")[0]

    def write_thread(self):
        client_0 = self.tdSql.get_connection(self.conf)
        # clean
        sql = "drop database if exists %s" % (self.db_name)
        self.logger.info(sql)
        client_0.execute(sql)
        # create database
        sql = "create database %s replica 2" % (self.db_name)
        self.logger.info(sql)
        client_0.execute(sql)
        # use database
        sql = "use %s" % (self.db_name)
        self.logger.info(sql)
        client_0.execute(sql)
        # create table
        sql = "create table %s (ts timestamp, c1 int) tags(t1 int)" % (self.table_name)
        self.logger.info(sql)
        client_0.execute(sql)
        i = 0
        while i < self.table_num:
            tb = f"tb_{i}"
            # create table
            sql = "create table %s using %s tags( %d )" % (tb, self.table_name, i)
            self.logger.info(sql)
            client_0.execute(sql)
            i = i + 1
        client_0.close()
        self.logger.info("write thread exit")

    def restart_dnode_thread(self):
        pass

    def check_result(self):
        pass

    def cleanup(self):
        pass

    def run(self):
        # insert into database thread
        mthread=threading.Thread(target=self.write_thread,args=())
        # restart slave dnode thread
        dthread=threading.Thread(target=self.restart_dnode_thread,args=())
        # start thread
        mthread.start()
        dthread.start()
        # wait thread
        dthread.join()
        mthread.join()
        # check result
        self.check_result()

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
