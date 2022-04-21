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
from http import client
import random
import threading
import time
import taos
from taostest import ClusterCase


class Case15(ClusterCase):

    def init(self):
        super().init()
        self.db_name = "testdb"
        self.replicas = 3
        self.db_nums = 10
        self.thread_num = 10
        self.stable_name = "stb"
        self.table_name = "tb"
        self.table_num = 100
        self.row_num = 500000  # row number per table
        self.max_restart_interval = [1, 3]
        self.restart_times = 20
        self.master_nodes = self.get_masters()
        i = random.randint(0, len(self.master_nodes) - 1)
        self.master_node = self.master_nodes[i]
        self.logger.info("master: %s", self.master_node)
        self.slave_nodes = self.get_slaves()
        i = random.randint(0, len(self.slave_nodes) - 1)
        self.slave_node = self.slave_nodes[i]
        self.logger.info("slave: %s", self.slave_node)
        self.dbnames = []
        self.tasks = []

    def prepare_datas(self,db_nums, query_endpoint):

        self.tasks = []
        for i in range(db_nums):
            db_name = "pre_db_%s"%i
            self.dbnames.append(db_name)
            stable_name = "stb"
            table_name = "sub_tb"
            table_nums = self.table_num
            row_nums = self.row_num
            replicas = self.replicas
            endpoint = query_endpoint
            time.sleep(1)
            self.logger.info("database {} is inserting datas ".format(db_name) )
            task = threading.Thread(target=self.insert_into_table,args=(db_name , stable_name, table_name, table_nums, row_nums, replicas, endpoint))
            task.start()
            self.tasks.append(task)
        
    
    def check_datas(self,query_endpoint):
        client = self.tdSql.get_connection(None , query_endpoint)
        for dbname in self.dbnames:
            query_sql = "select count(*) from {}.stb".format(dbname)
            result = client.query(query_sql)
            self.logger.info(query_sql)
            query_data = result.fetch_all()
            if query_data[0][0] ==self.row_num*self.table_num:
                self.logger.info(" database {} expect {} rows , real {} rows ,check pass ".format(dbname ,self.row_num*self.table_num ,query_data[0][0] ))
            else:
                self.logger.error(" database {} expect {} rows , real {} rows ,check failed ".format(dbname ,self.row_num*self.table_num ,query_data[0][0] ))

    def get_dnodes_list(self, endpoint):
        if not endpoint is None:
            client_0 = self.tdSql.get_connection(None, endpoint)
        else:
            client_0 = self.tdSql.get_connection(self._conf)
        result = client_0.query("show dnodes ")
        dnodes_data = result.fetch_all()
        dnodes_list = []
        for dnode_data in dnodes_data:
            dnode_name =dnode_data[1]
            dnodes_list.append(dnode_name)
        return dnodes_list

    def repeatedly_restart_task(self,restart_times, interval, query_endpoint):

        for _ in range(restart_times):
            dnodes_list = self.get_dnodes_list(query_endpoint)
            dnodes_list.remove(query_endpoint) # query dnode should connection steadly
            endpoint = random.sample(dnodes_list, 1 )[0]
            self.repeatedly_restart_dnode(endpoint , interval ,1,query_endpoint)


    def cleanup(self):
        pass

    def run(self):
        
        # prepare basic data for all dnodes have datas 
        self.prepare_datas(self.db_nums, self.master_node)

        # loop restart dnodes per 3 seconds
        repeatedly_restart_task=threading.Thread(target=self.repeatedly_restart_task,args=(self.restart_times,3, self.master_node))
        repeatedly_restart_task.start()
        
        for task in self.tasks:
            task.join()
        
        # check datas
        self.check_datas(self.master_node)

            
    def cleanup(self):
        pass

    def author(self):
        '''
        abstract about author
        '''
        return "wenzhowww"

    def tags(self):
        '''
        set tags
        '''
        return "cluster",

    def desc(self) -> str:
        case_description = '''
            [test]<wenzhouwww> test case for cluster about 1.15  repeatly restart taosd of random dnodes ;
        '''
        return case_description
