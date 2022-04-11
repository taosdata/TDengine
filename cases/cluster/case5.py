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
from taostest.util.sql import TDSql
from taostest import ClusterCase

class MyTDSQL(TDSql):
    def __init__(self, logger, run_log_dir, set_error_msg):
        super().__init__(logger, run_log_dir, set_error_msg)
        self._conn =  self.get_connection(None ,"dnode_2:6030")

class Case5(ClusterCase):

    def init(self):
        super().init()
        self.max_restart_interval = [5, 10]
        self.restart_times = 5
        self.slave_nodes = self.get_slaves()
        self.slave_nodes.remove('dnode_2:6030')
        i = random.randint(0, len(self.slave_nodes) - 1)
        self.slave_node = self.slave_nodes[i]
        self.logger.info("slave: %s", self.slave_node)
        self.master_nodes = self.get_masters()
        i = random.randint(0, len(self.master_nodes) - 1)
        self.master_node = self.master_nodes[i]
        self.logger.info("master: %s", self.master_node)
        # set alter schema params
        self.params = {"_ts" : 1420041600000 , "_ts_step":1 ,"_row_nums":2 ,"_col_nums":12 ,  "tables_of_per_stable":2 ,"_tags_nums" : 10 , "_replica" :3 }
        self.db_nums = 100 
        self.stable_nums = 100
        self.table_nums = 100
        self.time_sleep = 0
        self.tdSql = MyTDSQL(logger = self.logger, run_log_dir = self.run_log_dir, set_error_msg=self.set_error_msg)

    

    def cleanup(self):
        pass

    def run(self):
       
        # alter schema task for all 
        taskthread = threading.Thread(target=self.basic_alter_shema_task,args=( self.db_nums ,  self.stable_nums ,self.db_nums , self.time_sleep, self.params ))
        taskthread.start()

        # restart master dnode thread
        dthread=threading.Thread(target=self.repeatedly_restart_dnode,args=(self.master_node, self.max_restart_interval, self.restart_times, self.slave_node))
        # start thread
        dthread.start()
        # wait thread
        dthread.join()
        taskthread.join()
       

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
