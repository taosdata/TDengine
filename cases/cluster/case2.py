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

class Case2(ClusterCase):

    def init(self):
        super().init()
        self.db_name = "testdb"
        self.replicas = 3
        self.stable_name = "stb"
        self.table_num = 10
        self.row_num = 500000  # row number per table
        self.max_restart_interval = 5
        self.restart_times = 3

    def check_result(self):
        client_0 = self.tdSql.get_connection(self._conf)
        # use database
        sql = "use %s" % (self.db_name)
        self.logger.info(sql)
        client_0.execute(sql)
        # query
        sql = "select count(*) from %s"  % (self.stable_name)
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
            sql = "select count(*) from %s where c1 = %d"  % (self.stable_name, c1)
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
        mthread=threading.Thread(target=self.insert_into_table,args=(self.db_name, self.stable_name, self.table_num, self.row_num, self.replicas))
        # restart slave dnode thread
        dthread=threading.Thread(target=self.repeatedly_restart_dnode,args=("dnode_3:6030", self.max_restart_interval, self.restart_times))
        # start thread
        mthread.start()
        dthread.start()
        # wait thread
        dthread.join()
        mthread.join()
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
