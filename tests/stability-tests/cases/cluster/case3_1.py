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
import os
import sys
from taostest import ClusterCase


class Case3_1(ClusterCase):

    def init(self):
        super().init()
        self.db_name = "testdb"
        self.replicas = 3
        self.db_nums = 10
        self.thread_num = 10
        self.stable_name = "stb"
        self.table_name = "tb"
        self.table_num = 100
        self.row_num = 50000000  # row number per table
        self.max_restart_interval = [10, 20]
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
        self.cases = []
        self.env_file = "cluster/cluster_5_replica.yaml"
        self.query_tasks = []
        self.query_results = []

    def install_test_frame_dnode_1(self):

        self.envMgr._remote.cmd("dnode_1", [
                                "pip3 install --extra-index-url http://192.168.1.131:8080/simple --trusted-host 192.168.1.131  taostest"])
        self.envMgr._remote.cmd("dnode_1", ["export TEST_ROOT={}".format(os.environ["TEST_ROOT"])])

    def get_query_cases(self):

        # run_script_dir = os.path.abspath(os.path.join(os.environ["TEST_ROOT"],"cases/Query/query_all.sh"))
        # with open(run_script_dir , "r") as f:
        #     lines = f.readlines()
            
        #     for line in lines:
        #         case = line.split("--case=")[-1].replace("--keep","").replace("\n","")
        #         run = "taostest --use {}  --case ".format(self.env_file) + case + "--keep"
        #         print(run)
        #         self.cases.append(run)
        # f.close()

        can_repeat_use_list = ["Query/queryscript/table_query/table_query.py --keep" ,
                            "Query/queryscript/table_query/table_query_null.py --keep",
                            "Query/queryscript/table_query/table_query_null.py --keep",
                            "Query/queryscript/table_query/table_query_union.py --keep",
                            "Query/queryscript/stable_query/stable_query.py --keep" ,
                            "--case=Query/queryscript/stable_query/stable_query_null.py --keep",
                            "Query/queryscript/stable_query/stable_query_union.py --keep"]

        for case_run in can_repeat_use_list:
            run = "taostest --use {}  --case ".format(self.env_file) + case_run 
            self.cases.append(run)
        # print(self.cases)
        
    def start_case(self , case_cmd):
    
        result = self.envMgr._remote.cmd2("dnode_1", ["export TEST_ROOT={}".format(os.environ["TEST_ROOT"]),case_cmd])
        self.query_results.append(result)
        return result

    def start_all_query_cases(self):

        for run_case in self.cases:
            print(run_case)
            task = threading.Thread(target=self.start_case, args=([run_case]))
            task.start()
            self.query_tasks.append(task)
    

    def check_all_query_tasks(self):
        
        check_status = True
        for task in self.query_tasks:
            task.join() 
        
        for result in self.query_results:
            if result.ok ==True:
                pass
            else:
                check_status=False
                sys.exit(1)
        
        return check_status
        
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

    def get_dnodes_list(self, endpoint):
        if not endpoint is None:
            client_0 = self.tdSql.get_connection(None, endpoint)
        else:
            client_0 = self.tdSql.get_connection(self._conf)
        result = client_0.query("show dnodes ")
        dnodes_data = result.fetch_all()
        dnodes_list = []
        for dnode_data in dnodes_data:
            dnode_name = dnode_data[1]
            dnodes_list.append(dnode_name)
        return dnodes_list

    def repeatedly_restart_task(self, restart_times, interval, query_endpoint):

        for _ in range(restart_times):
            dnodes_list = self.get_dnodes_list(query_endpoint)
            # query dnode should connection steadly
            dnodes_list.remove(query_endpoint)
            endpoint = random.sample(dnodes_list, 1)[0]
            self.repeatedly_restart_dnode(
                endpoint, interval, 1, query_endpoint)

    def cleanup(self):
        pass
    

    def run(self):

        # prepare test_frame_work for run case at dnode_1 ,default dnode_1 has not deploy
        self.install_test_frame_dnode_1()

        # start query cases
        self.get_query_cases()
        self.start_all_query_cases()
        time.sleep(100)
        # prepare basic data for all dnodes have datas

        mthreads = []
        i = 0
        while i < self.thread_num:
            db = f"{self.db_name}_{i}"
            stb = f"{self.stable_name}_{i}"
            tb = f"{self.table_name}_{i}"
            mthread=threading.Thread(target=self.insert_into_table,args=(db, stb, tb, self.table_num, self.row_num, self.replicas, self.master_node))
            mthreads.append(mthread)
            mthread.start()
            i = i + 1

        # loop restart dnodes per 3 seconds
        repeatedly_restart_task = threading.Thread(
            target=self.repeatedly_restart_task, args=(self.restart_times, 3, self.master_node))
        repeatedly_restart_task.start()
        i = 0
        while i < self.thread_num:
            mthread = mthreads[i]
            mthread.join()
            i = i + 1
        self.logger.info("checking result ...")
        # check result

        self.check_result()
        check_status = self.check_all_query_tasks()
        if check_status:
            self.logger.info(" all query task work pass and exit ")
        else:
            self.logger.info(" some query task work failed and exit ")
        return self._status

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
            [test]<wenzhouwww> test case for cluster about 3.1 , restart taosd and basic query task ,restart taosd and basic query task and restart taosd of slave mnode ;
        '''
        return case_description
