###################################################################
#           Copyright (c) 2016 by TAOS Technologies, Inc.
#                     All rights reserved.
#
#  This file is proprietary and confidential to TAOS Technologies.
#  No part of this file may be reproduced, stored, transmitted,
#  disclosed or used in any form or by any means other than as
#  expressly provided by the written permission from Jianhui Tao
#
###################################################################

# -*- coding: utf-8 -*-


from new_test_framework.utils import tdLog, tdSql, cluster, tdCom, AutoGen

import random
import os
import subprocess
import time
import shutil
    

class RestoreBasic:
    # init
    def init(self, replicaVar=1):
        self.replicaVar = int(replicaVar)
        self.dnodes_num = 5

        # get from global
        # test
        self.dnodes = cluster.dnodes
        num = len(self.dnodes)

        if num < self.dnodes_num :
            tdLog.exit(f" cluster dnode is less than {self.dnodes_num}. num={num}")

        # create data
        self.dbname = "db"
        self.stable = "st"
        self.child_count = 100
        self.insert_rows = 10000
        self.create_data()

    #  create data
    def create_data(self):
        gen = AutoGen()
        gen.create_db(self.dbname, 8, 3)
        gen.create_stable(self.stable, 5, 10, 8, 8)
        gen.create_child(self.stable, "d", self.child_count)
        gen.set_batch_size(1000)
        gen.insert_data(self.insert_rows)
        
        tdSql.execute(f"flush database {self.dbname}")
        # put some duplicate ts on wal
        gen.insert_data(self.insert_rows%100)

        for i in range(self.dnodes_num):
            sql = f"create qnode on dnode {i+1}"
            tdSql.execute(sql)


    # status
    def check_status_corrent(self):
        # query
        tdSql.query(f" show {self.dbname}.vgroups")

        # check 8 vgroups
        tdSql.checkRows(8)

        # check data corrent
        for i in range(8):
            leader = False
            for j in range(3):
                status = tdSql.getData(i, 4 + j*3)
                if status == "leader":
                    leader = True
                elif status == "follower":
                    pass
                else:
                    tdLog.info(f" check vgroups status not leader or follower. i={i} j={j} status={status}")
                    return False
            
            # check leader
            if  leader == False:
                tdLog.info(f" check vgroups not found leader i={i} ")
                return False

        # info
        tdLog.info("check vgroups status successfully.")
        return True

    # check data corrent
    def check_corrent(self):
        # check status
        status = False
        for i in range(100): 
            if self.check_status_corrent():
                 status = True
                 break
            else:
                time.sleep(1)
                tdLog.info(f"sleep 1s retry {i} to check status again...") 

        if status == False:
            tdLog.exit("check vgroups status failed, exit.")             
            
        # check rows count
        sql = f"select count(ts) from {self.dbname}.{self.stable}"
        tdSql.query(sql)
        tdSql.checkData(0, 0, self.child_count* self.insert_rows)


    # restore dnode
    def restore_dnode(self, index):
        tdLog.info(f"start restore dnode {index}")
        dnode = self.dnodes[index - 1]
        
        # stop dnode
        tdLog.info(f"stop dnode {index}")
        dnode.stoptaosd()
        time.sleep(1)

        # remove dnode folder
        try:
            shutil.rmtree(dnode.dataDir[0])
            tdLog.info(f"delete dir {dnode.dataDir} successful")
        except OSError as x:
            tdLog.exit(f"remove path {dnode.dataDir} error : {x.strerror}")

        dnode.starttaosd()
        
        # exec restore
        sql = f"restore dnode {index}"
        tdLog.info(sql)
        tdSql.execute(sql)
        self.check_corrent()

    def restore_dnode_prepare(self, index):
        tdLog.info(f"start restore dnode {index}")
        dnode = self.dnodes[index - 1]
        
        # stop dnode
        tdLog.info(f"stop dnode {index}")
        dnode.stoptaosd()
        time.sleep(1)

        # remove dnode folder
        try:
            shutil.rmtree(dnode.dataDir[0])
            tdLog.info(f"delete dir {dnode.dataDir} successful")
        except OSError as x:
            tdLog.exit(f"remove path {dnode.dataDir} error : {x.strerror}")

        dnode.starttaosd()
        
    def restore_dnode_exec(self, index):
        # exec restore
        sql = f"restore dnode {index}"
        tdLog.info(sql)
        tdSql.execute(sql)
        self.check_corrent()  

    def stop_dnode(self, index):
        dnode = self.dnodes[index - 1]

        dnode.starttaosd()
    # restore vnode
    def restore_vnode(self, index):
        tdLog.info(f"start restore vnode on dnode {index}")
        dnode = self.dnodes[index - 1]
        del_dir = f"{dnode.dataDir[0]}/vnode"

        # stop dnode
        tdLog.info(f"stop dnode {index}")
        dnode.stoptaosd()
        time.sleep(1)
        
        # remove dnode folder
        try:
            shutil.rmtree(del_dir)
            tdLog.info(f"delete dir {del_dir} successful")
        except OSError as x:
            tdLog.exit(f"remove path {del_dir} error : {x.strerror}")

        dnode.starttaosd()
        
        #newTdSql=tdCom.newTdSql()
        #t0 = threading.Thread(target=self.showTransactionThread, args=('', newTdSql))
        #t0.start()

        # exec restore
        sql = f"restore vnode on dnode {index}"
        tdLog.info(sql)
        tdSql.execute(sql)

        # check result
        self.check_corrent()

    def showTransactionThread(self, p, newTdSql):
        transid = 0

        count = 0
        started = 0
        while count < 100:
            sql = f"show transactions;"
            rows = newTdSql.query(sql)
            if rows > 0:
                started = 1
                transid = newTdSql.getData(0, 0)
                if transid > 0:
                    os.system("taos -s \"show transaction %d\G;\""%transid)
            else:
                transid = 0
            if started == 1 and transid == 0:
                break
            time.sleep(1)
            count += 1

    # restore mnode
    def restore_mnode(self, index):
        tdLog.info(f"start restore mnode {index}")
        dnode = self.dnodes[index - 1]
        del_dir = f"{dnode.dataDir[0]}/mnode"
        
        # stop dnode
        tdLog.info(f"stop dnode {index}")
        dnode.stoptaosd()
        time.sleep(1)

        # remove dnode folder
        try:
            shutil.rmtree(del_dir)
            tdLog.info(f"delete dir {del_dir} successful")
        except OSError as x:
            tdLog.exit(f"remove path {del_dir} error : {x.strerror}")

        dnode.starttaosd()
        
        # exec restore
        sql = f"restore mnode on dnode {index}"
        tdLog.info(sql)
        tdSql.execute(sql)
        self.check_corrent()


    # restore qnode
    def restore_qnode(self, index):
        tdLog.info(f"start restore qnode on dnode {index}")
        dnode = self.dnodes[index - 1]
        del_dir = f"{dnode.dataDir[0]}/qnode"

        # stop dnode
        tdLog.info(f"stop dnode {index}")
        dnode.stoptaosd()
        time.sleep(1)

        # remove dnode folder
        try:
            shutil.rmtree(del_dir)
            tdLog.info(f"delete dir {del_dir} successful")
        except OSError as x:
            tdLog.exit(f"remove path {del_dir} error : {x.strerror}")

        # start dnode 
        dnode.starttaosd()
        
        # exec restore
        sql = f"restore qnode on dnode {index}"
        tdLog.info(sql)
        tdSql.execute(sql)
        self.check_corrent()

        # path exist
        qfile = f"{del_dir}/qnode.json"
        if os.path.exists(qfile) == False:
            tdLog.exit(f"qnode restore failed. qnode.json is not exist. {qfile}")
        else:
            tdLog.info(f"check qnode.json restore ok. {qfile}")
    
    # stop
    def stop(self):
        tdSql.close()

