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


class Case16(ClusterCase):

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
        self.dbname_index = 0
        self.tasks = []
        self.dbnames = []

    def prepare_database(self, query_endpoint):
    
        
        db_name = "pre_db_%s"%self.dbname_index
        self.db_names.append(db_name)
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
        task.join()
        self.dbname_index +=1

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
            dnode_name = dnode_data[1]
            dnodes_list.append(dnode_name)
        return dnodes_list

    def insert_task(self, db_name, stable_name, table_name, table_num, row_num, replicas, restart_times, endpoint: str = None):
        '''
        thread to write data
        @db_name:
        @stable_name:
        @table_num:
        @row_num: row number per table
        @replicas:
        '''
        if not endpoint is None:
            client_0 = self.tdSql.get_connection(None, endpoint)
        else:
            client_0 = self.tdSql.get_connection(self._conf)
        # clean
        sql = "drop database if exists %s" % (db_name)
        self.logger.info(sql)
        client_0.execute(sql)
        # create database
        sql = "create database %s replica %d" % (db_name, replicas)
        self.logger.info(sql)
        client_0.execute(sql)
        # use database
        sql = "use %s" % (db_name)
        self.logger.info(sql)
        client_0.execute(sql)
        # create table
        sql = "create stable %s (ts timestamp, c1 int) tags(t1 int)" % (
            stable_name)
        self.logger.info(sql)
        client_0.execute(sql)
        i = 0
        while i < table_num:
            tb = f"{table_name}_{i}"
            # create table
            sql = "create table %s using %s tags( %d )" % (tb, stable_name, i)
            self.logger.info(sql)
            client_0.execute(sql)
            i = i + 1
            j = 0
            while j < row_num:
                ts = self.ts + j
                k = 0
                n = 50
                if row_num - j < n:
                    n = row_num - j
                value_statement = ""
                while k < n:
                    value_statement = f"{value_statement} ({ts}+{k}a, {j})"
                    k = k + 1
                j = j + n
                sql = f"insert into {tb} values {value_statement}"
                # self.logger.info(sql)
                client_0.execute(sql)

            if i % int(table_num/restart_times) == 0:

                self.prepare_database(endpoint)  # prepare an new database

                dnodes_lists = self.get_dnodes_list(endpoint)
                dnodes_lists.remove(endpoint)

                # random get an endpoint for all
                dnode = random.sample(dnodes_lists, 1)[0]
                self.repeatedly_restart_dnode(
                    endpoint=dnode, interval=1, times=1, query_endpoint=endpoint)

        client_0.close()
        self.logger.info("write thread exit")

    def cleanup(self):
        pass

    def run(self):

        self.insert_task(self.db_name, self.stable_name, self.table_name, self.table_num, self.row_num, self.replicas, self.restart_times, self.master_node)

        self.check_datas(self.master_node)

        # check insert_task done 
        query_sql = "select count(*) from {}.{}".format(self.db_name , self.stable_name)
        result = client.query(query_sql)
        self.logger.info(query_sql)
        query_data = result.fetch_all()
        if query_data[0][0] ==self.row_num*self.table_num:
            self.logger.info(" database {} expect {} rows , real {} rows ,check pass ".format(self.db_name ,self.row_num*self.table_num ,query_data[0][0] ))
        else:
            self.logger.error(" database {} expect {} rows , real {} rows ,check failed ".format(self.db_name ,self.row_num*self.table_num ,query_data[0][0] ))


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
            [test]<wenzhouwww> test case for cluster about 1.16  repeatly restart taosd of random dnodes ;
        '''
        return case_description
