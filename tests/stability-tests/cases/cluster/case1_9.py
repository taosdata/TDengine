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

class Case8(ClusterCase):

    def init(self):
        super().init()
        self.db_name = "testdb"
        self.replicas = 3
        self.thread_num = 10
        self.stable_name = "stb"
        self.table_name = "tb"
        self.table_num = 10
        self.row_num = 500000  # row number per table
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

    def drop_dnode(self, end_point:str ,interval_check_status,conn_end_point):

        def get_drop_status(dnodes_data):
            status_done = False
            end_points = []
            for dnode in dnodes_data:
                end_points.append(dnode[1])

            try:
                end_point_index = end_points.index(end_point)
                if dnodes_data[end_point_index][4]=="dropping":
                    self.logger.info(" {} is dropping ".format(end_point))
                else:
                    self.logger.info(" {}  current status is {}  ".format(end_point ,dnodes_data[end_point_index][4] ))
                status_done = False
            except ValueError as e :
                self.logger.info(" {} has dropping done   ".format(end_point ))
                status_done = True
            return status_done

        host = end_point.split(":")[0]
        cfgPath = self.envMgr.getCfg(end_point, "taosd", {"config_dir": ""})[
            "config_dir"]    

        # check drop dnode success util drop done! 
        client_local = self.tdSql.get_connection(None, conn_end_point)
        client_local.execute("drop dnode '{}' ;".format(end_point))
       
        while  True :
            time.sleep(interval_check_status)
            result = client_local.query("show dnodes")
            dnodes_data = result.fetch_all()
            status_done = get_drop_status(dnodes_data)
            if status_done:
                break
        # directly add dnode which has been dropping , it not work 
        client_local.execute("create dnode '{}' ;".format(end_point))
        time.sleep(3)

        # check status should be status not received
        result = client_local.query("show dnodes")
        dnodes_data = result.fetch_all()
        for dnode in dnodes_data:
            if dnode[1] == end_point and dnode[4] =="status not received":
                self.logger.info(" {} is dropping  , directly create {} status is 'status not received' ".format(end_point,end_point))
                self.logger.info(" {} work as expected , it will be remove again ".format(end_point))
            elif dnode[1] == end_point and dnode[4] !="status not received":
                self.logger.error(" {} work not as expected , it will be remove again ".format(end_point))
            else:
                pass
        client_local.execute("drop dnode '{}' ;".format(end_point))
        # clear all datas of this dropping dnodes , and set it as an new taosd service

        self.envMgr.stopDnode(end_point)
        time.sleep(5)

        # clear data
        config = self.envMgr.getCfg(end_point, "taosd", {"config": ""})[
            "config"]
        dataDir = config['dataDir']
        logDir = config['logDir']
        self.envMgr._remote.cmd(host , ["rm -rf {}".format(dataDir)])
        self.envMgr._remote.cmd(host , ["rm -rf {}".format(logDir)])
        self.envMgr.startDnode(end_point)

        # create endpoint again ,it will work 
        client_local.execute("create dnode '{}' ;".format(end_point))
        time.sleep(3)

        # check status should be status not received
        result = client_local.query("show dnodes")
        dnodes_data = result.fetch_all()
        for dnode in dnodes_data:
            if dnode[1] == end_point and dnode[4] =="ready":
                self.logger.info(" {} is dropping  , directly create {} status is 'ready' ".format(end_point,end_point))
            elif dnode[1] == end_point and dnode[4] !="ready":
                self.logger.error(" {} work not as expected , this is an error ".format(end_point))


    def cleanup(self):
        pass
    
    def run(self):
        
        db_name = self.db_name 
        replicas = self.replicas 
        stable_name = self.stable_name 
        table_name = self.table_name 
        table_num = self.table_num 
        row_num = self.row_num 
        
        client_0 = self.tdSql.get_connection(None, self.slave_node)
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

                if j>=100000 and j %100000 ==0:
                    
                    try:
                        client_0.execute("drop dnode '' ".format(self.master_node))
                    except taos.error.ProgrammingError as err :
                        self.logger.info("master dnode should not support dropping")
                    result = client_0.query("show dnodes")
                    dnodes_data = result.fetch_all()
                    status = False
                    for dnode_data in dnodes_data:
                        if dnode_data[1] == self.master_node:
                            self.logger.info("master dnode is aliving ")
                            status =True
                            break
                    if not status:
                        self.logger.info("master dnode {} has dead , this is an error ".format(self.master_node))
                    
                    # master mnode can be drop by stop first 
                    self.envMgr.stopDnode(self.master_node)
                    self.logger.info("stop dnode {}".format(self.master_node))
                    time.sleep(5)
                    result = client_0.query("show dnodes")
                    dnodes_data = result.fetch_all()
                    for dnode in dnodes_data:
                        if dnode[1] == self.master_node and dnode[4] =="offline":
                            self.logger.info(" {} is stopping  , directly create {} status is 'offline' ".format(self.master_node,self.master_node))
                            self.logger.info(" {} work as expected , it will be remove again ".format(self.master_node))
                        elif dnode[1] == self.master_node and dnode[4] !="offline":
                            self.logger.error(" {} work not as expected , it will be remove again ".format(self.master_node))
                        else:
                            pass
                    client_0.execute("drop dnode '{}' ;".format(self.master_node))
                    self.logger.info("drop dnode {}".format(self.master_node))
                    time.sleep(10)
                    # clear data
                    config = self.envMgr.getCfg(self.master_node, "taosd", {"config": ""})[
                        "config"]
                    dataDir = config['dataDir']
                    logDir = config['logDir']
                    
                    self.envMgr._remote.cmd(self.master_node.split(":")[0] , ["rm -rf {}".format(dataDir)])
                    self.envMgr._remote.cmd(self.master_node.split(":")[0] , ["rm -rf {}".format(logDir)])
                    config_dir = self.envMgr.getCfg(self.master_node, "taosd", {"config_dir": ""})[
                        "config_dir"]

                    self.envMgr._remote.cmd(self.master_node.split(":")[0] , ["sed 's/firstEP dnode_1:6030/firstEP dnode_2:6030/g'  {}".format(config_dir)])
                    self.logger.info("clear data of {}".format(self.master_node))
                    time.sleep(10)
                    self.envMgr.startDnode(self.master_node)
                    self.logger.info("start dnode {}".format(self.master_node))
                    time.sleep(2)
                    # create endpoint again ,it will work 
                    client_0.execute("create dnode '{}' ;".format(self.master_node))
                    time.sleep(3)

                    # check status should be status not received
                    result = client_0.query("show dnodes")
                    dnodes_data = result.fetch_all()
                    for dnode in dnodes_data:
                        if dnode[1] == self.master_node and dnode[4] =="ready":
                            self.logger.info(" {} is created  , directly create {} status is 'ready' ".format(self.master_node,self.master_node))
                        elif dnode[1] == self.master_node and dnode[4] !="ready":
                            self.logger.error(" {} work not as expected , this is an error ".format(self.master_node))

                    self.logger.info(sql)
                client_0.execute(sql)

        result = client_0.query("select count(*) from {}.{}".format(db_name , stable_name))
        query_data = result.fetch_all() 
        if query_data[0][0] ==self.row_num*self.table_num:
            self.logger.info(" expect {} rows , real {} rows ,check pass ".format(self.row_num*self.table_num ,query_data[0][0] ))
        else:
            self.logger.info(" expect {} rows , real {} rows ,check failed ".format(self.row_num*self.table_num ,query_data[0][0] ))

        client_0.close()
        self.logger.info("write thread exit")

        

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
            [test]<wenzhouwww> test case for cluster about 1.8 drop dnode and add dnode again  ... ;
        '''
        return case_description
