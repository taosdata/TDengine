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


class Case12(ClusterCase):

    def init(self):
        super().init()
        self.db_name = "testdb"
        self.replicas = 3
        self.thread_num = 10
        self.stable_name = "stb"
        self.table_name = "tb"
        self.table_num = 10000
        self.row_num = 50000  # row number per table
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

    def get_replica_num(self ,db_name):
        client_0 = self.tdSql.get_connection(None, self.slave_node)
        result = client_0.query("show databases ")
        database_data = result.fetch_all()
        index_flag = -1
        for index , db in enumerate(database_data):
            if db[0] == db_name:
                index_flag ==index
        replica_num = database_data[index_flag][4]
        return replica_num

    def check_replica_sync(self,db_name ,check_interval):
        client_0 = self.tdSql.get_connection(None, self.slave_node)
        client_0.execute("use {}".format(db_name))
        while True:
            result = client_0.query(" show {}.vgroups ".format(db_name))
            vgroups = result.fetch_all()
            vgroups_status = []
            for row in vgroups:
                row_elem = []
                for index , elem in enumerate(row):
                    if index <=3 or index ==len(row)-1 :
                        continue
                    else:
                        if (index -3)%2 ==1:
                            continue
                        else:
                            row_elem.append(elem)
                vgroups_status.append(row_elem)
            
            # check sync done
            sync_done = True
            for status_row in vgroups_status:
                for status in status_row:
                    if status not in ['master', 'slave']:
                        sync_done = False
            time.sleep(check_interval)
            self.logger.info("current cluster status is: {}".format(vgroups_status))
            if  sync_done:
                self.logger.info(" {} replica sync  execute done ".format(db_name))
                break
    
    def alter_database_replica(self, db_name, stable_name, table_name, table_num, row_num, replicas, endpoint: str = None):
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
        sql = "create stable %s (ts timestamp, c1 int) tags(t1 int)" % (stable_name)
        self.logger.info(sql)
        client_0.execute(sql)
        i = 0
        flag = 1
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
            if i>100 and i%100 ==0: 

                if i %200==0:
                    alter_sql = "alter database {} replica 3".format(db_name)
                    self.logger.info(alter_sql)
                    self.check_replica_sync(db_name, 1)
                else:
                    alter_sql = "alter database {} replica 2".format(db_name)
                    self.logger.info(alter_sql)
                    self.check_replica_sync(db_name, 1)

        client_0.close()
        self.logger.info("write thread exit")


    def cleanup(self):
        pass

    def run(self):
        self.alter_database_replica( self.db_name, self.stable_name, self.table_name, self.table_num, self.row_num, self.replicas, None)
    

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
            [test]<wenzhouwww> test case for cluster about 1.12  and 1.13 always alter database replica from 3 to 1  ... ;
        '''
        return case_description
