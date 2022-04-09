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
       
        self.max_restart_interval = [5, 10]
        self.restart_times = 5
        self.master_nodes = self.get_masters()
        i = random.randint(0, len(self.master_nodes) - 1)
        self.master_node = self.master_nodes[i]
        self.logger.info("master: %s", self.master_node)
        self.slave_nodes = self.get_slaves()
        i = random.randint(0, len(self.slave_nodes) - 1)
        self.slave_node = self.slave_nodes[i]
        self.logger.info("slave: %s", self.slave_node)
        
        # set alter schema params
        self.params = {"_ts" : 1420041600000 , "_ts_step":1 ,"_row_nums":2 ,"_col_nums":16 , "_stables_nums" : 2 , "_table_nums":2 ,
        "tables_of_per_stable":2 ,"_tags_nums" : 10 , "_replica" :3 ,"_db_nums": 100 ,"_alter_times":100 , "_dbs":0 , "_used_dbs":[] , "_tags":0,
        "_used_tags":[] , "_stablenames":0 ,"_used_stables":[] }

    def cleanup(self):
        pass

    def run(self):
        # start alter schema task (self, db_nums , stable_nums,table_nums , time_sleep)

        taskthread = threading.Thread(target=self.basic_alter_shema_task,args=( 2 ,  2 ,2 , 0, self.params ))
        taskthread.start()
        # wait thread
        
        # restart slave dnode thread
        dthread=threading.Thread(target=self.repeatedly_restart_dnode,args=(self.slave_node, self.max_restart_interval, self.restart_times, self.master_node))
        # start thread
        dthread.start()
        # wait thread
        taskthread.join()
        dthread.join()
       
        
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
            [test]<wenzhouwww> test case for cluster about 1.3 alter schema task ... ;
        '''
        return case_description
