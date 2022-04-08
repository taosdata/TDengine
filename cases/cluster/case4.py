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

class Case4(ClusterCase):

    def init(self):
        super().init()
        self.db_name = "testdb"
        self.replicas = 3
        self.thread_num = 2
        self.stable_name = "stb"
        self.table_name = "tb"
        self.table_num = 10
        self.row_num = 500000  # row number per table
        self.max_restart_interval = [5, 10]
        self.restart_times = 5
        self.slave_nodes = self.get_slaves()
        i = random.randint(0, len(self.slave_nodes) - 1)
        self.slave_node = self.slave_nodes[i]
        self.logger.info("slave: %s", self.slave_node)
        self.master_nodes = self.get_masters()
        i = random.randint(0, len(self.master_nodes) - 1)
        self.master_node = self.master_nodes[i]
        self.logger.info("master: %s", self.master_node)

    def check_result_db(self, db, stb):
        client_0 = self.get_spec_conn(self.master_node)
        # use database
        sql = "use %s" % (db)
        self.logger.info(sql)
        client_0.execute(sql)
        # query
        sql = "select count(*) from %s"  % (stb)
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
            self._status = False
            return
        # check data
        '''c1 = 0
        i = 0;
        while i < 10000:
            sql = "select count(*) from %s where c1 = %d"  % (self.stb, c1)
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

    def check_result(self):
        i = 0
        while i < self.thread_num:
            db = f"{self.db_name}_{i}"
            stb = f"{self.stable_name}_{i}"
            self.check_result_db(db, stb)
            i = i + 1

    def cleanup(self):
        pass

    def run(self):
        # insert into database thread
        mthreads = []
        i = 0
        while i < self.thread_num:
            db = f"{self.db_name}_{i}"
            stb = f"{self.stable_name}_{i}"
            tb = f"{self.table_name}_{i}"
            mthread=threading.Thread(target=self.insert_into_table,args=(db, stb, tb, self.table_num, self.row_num, self.replicas, self.slave_node))
            mthreads.append(mthread)
            mthread.start()
            i = i + 1
        # restart master dnode thread
        dthread=threading.Thread(target=self.repeatedly_restart_dnode,args=(self.master_node, self.max_restart_interval, self.restart_times, self.slave_node))
        # start thread
        dthread.start()
        # wait thread
        dthread.join()
        i = 0
        while i < self.thread_num:
            mthread = mthreads[i]
            mthread.join()
            i = i + 1
        self.logger.info("checking result ...")
        # check result
        self.check_result()
        return self._status

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
