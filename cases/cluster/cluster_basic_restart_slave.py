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
        self.row_num = 500000
        self.replicas = 3
        self.status = True
        self.conf = self.get_component_by_name(
            "taosd")[0]

    def write_thread(self):
        '''
        thread to write data
        '''
        client_0 = self.tdSql.get_connection(self.conf)
        # clean
        sql = "drop database if exists %s" % (self.db_name)
        self.logger.info(sql)
        client_0.execute(sql)
        # create database
        sql = "create database %s replica %d" % (self.db_name, self.replicas)
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
            j = 0
            while j < self.row_num:
                ts = self.ts + j
                k = 0
                value_statement = ""
                while k < 50:
                    value_statement = f"{value_statement} ({ts}+{k}a, {j})"
                    k = k + 1
                j = j + 50
                sql = f"insert into {tb} values {value_statement}"
                # self.logger.info(sql)
                client_0.execute(sql)
        client_0.close()
        self.logger.info("write thread exit")

    def wait_dnode_status(self, endpoint, status):
        '''
        query dnode status periodically until timeout or status match
        @endpoint
        @status
        '''
        client_0 = self.tdSql.get_connection(self.conf)
        sql = "use %s" % (self.db_name)
        client_0.execute(sql)
        sql = "show dnodes"
        '''
        [(1, 'dnode_1:6030', 1, 24, 'ready', 'any', datetime.datetime(2022, 3, 30, 14, 38, 50, 264000), ''),
         (2, 'dnode_2:6030', 2, 24, 'ready', 'any', datetime.datetime(2022, 3, 30, 14, 39, 11, 665000), ''),
         (3, 'dnode_3:6030', 1, 24, 'offline', 'any', datetime.datetime(2022, 3, 30, 14, 39, 23, 60000), 'status msg timeout')]
        '''
        status_match = False
        max_retry_time = 10
        retry_time = 0
        while status_match == False and retry_time < max_retry_time:
            i = 0
            time.sleep(2)
            result = client_0.query(sql)
            data = result.fetch_all()
            self.logger.info("%d show dnodes: %s", retry_time, str(data))
            while i < len(data):
                if data[i][1] == endpoint and data[i][4] == status:
                    status_match = True
                    break
                i = i + 1
            retry_time = retry_time + 1
        return status_match

    def show_vgroups(self):
        '''
        show vgroups
        '''
        client_0 = self.tdSql.get_connection(self.conf)
        sql = "use %s" % (self.db_name)
        client_0.execute(sql)
        sql = "show vgroups"
        '''
        [(6, 5, 'ready', 3, 4, 'master', 3, 'slave', 2, 'slave', 0)]
        '''
        result = client_0.query(sql)
        data = result.fetch_all()
        self.logger.info("show vgroups: %s", str(data))

    def restart_dnode_thread(self):
        '''
        thread to restart dnode
        '''
        time.sleep(5)
        # stop dnode_3
        self.logger.info("stop dnode dnode_3:6030")
        self.envMgr.stopDnode("dnode_3:6030")
        time.sleep(5)
        ret = self.wait_dnode_status("dnode_3:6030", "offline")
        if ret == False:
            self.logger.error("dnode status not match")
            self.error_msg = "dnode status not match"
            self.status = False
        self.show_vgroups()
        self.logger.info("start dnode dnode_3:6030")
        # start dnode_3
        self.envMgr.startDnode("dnode_3:6030")
        time.sleep(5)
        ret = self.wait_dnode_status("dnode_3:6030", "ready")
        if ret == False:
            self.logger.error("dnode status not match")
            self.error_msg = "dnode status not match"
            self.status = False
        self.show_vgroups()
        self.logger.info("stop dnode dnode_3:6030")
        # stop dnode_3
        self.envMgr.stopDnode("dnode_3:6030")
        time.sleep(5)
        ret = self.wait_dnode_status("dnode_3:6030", "offline")
        if ret == False:
            self.logger.error("dnode status not match")
            self.error_msg = "dnode status not match"
            self.status = False
        self.show_vgroups()
        self.logger.info("start dnode dnode_3:6030")
        # start dnode_3
        self.envMgr.startDnode("dnode_3:6030")
        ret = self.wait_dnode_status("dnode_3:6030", "ready")
        if ret == False:
            self.logger.error("dnode status not match")
            self.error_msg = "dnode status not match"
            self.status = False
        self.show_vgroups()
        self.logger.info("restart dnode thread exit")

    def check_result(self):
        client_0 = self.tdSql.get_connection(self.conf)
        # use database
        sql = "use %s" % (self.db_name)
        self.logger.info(sql)
        client_0.execute(sql)
        # query
        sql = "select count(*) from %s"  % (self.table_name)
        self.logger.info(sql)
        i = 0
        while i < 10:
            result = client_0.query(sql)
            data = result.fetch_all()
            self.logger.info("result: %s", str(data[0][0]))
            if data[0][0] == 5000000:
                break
            i = i + 1
            time.sleep(2)
        # total row = self.table_num * self.row_num = 5000000
        if data[0][0] != 5000000:
            self.logger.error("row not match")
            self.error_msg = "row not match"
            self.status = False
            return
        # check data
        '''c1 = 0
        i = 0;
        while i < 10000:
            sql = "select count(*) from %s where c1 = %d"  % (self.table_name, c1)
            # self.logger.info(sql)
            result = client_0.query(sql)
            data = result.fetch_all()
            # self.logger.info("result: %s", str(data[0][0]))
            if data[0][0] != 500:
                self.logger.error("row not match c1 = %d", c1)
                self.error_msg = f"row not match c1 = {c1}"
                self.status = False
            c1 = c1 + 50
            i = i + 1'''

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
        return self.status

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
