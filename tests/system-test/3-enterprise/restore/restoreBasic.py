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


from util.log import *
from util.cases import *
from util.sql import *
from util.common import *
from util.sqlset import *
from util.dnodes import *
from util.autogen import *
from util.cluster import *

import random
import os
import subprocess
import shutil
import time
    

class RestoreBasic:
    # init
    def init(self, conn, logSql, replicaVar=1):
        self.replicaVar = int(replicaVar)
        tdSql.init(conn.cursor())
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
                status = tdSql.getData(i, 4 + j*2)
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

        # remove dnode folder
        try:
            shutil.rmtree(dnode.dataDir)
            tdLog.info(f"delete dir {dnode.dataDir} successful")
        except OSError as x:
            tdLog.exit(f"remove path {dnode.dataDir} error : {x.strerror}")

        dnode.starttaosd()
        
        # exec restore
        sql = f"restore dnode {index}"
        tdLog.info(sql)
        tdSql.execute(sql)
        self.check_corrent()

    # restore vnode
    def restore_vnode(self, index):
        tdLog.info(f"start restore vnode on dnode {index}")
        dnode = self.dnodes[index - 1]
        del_dir = f"{dnode.dataDir}/vnode"

        # stop dnode
        tdLog.info(f"stop dnode {index}")
        dnode.stoptaosd()
        
        # remove dnode folder
        try:
            shutil.rmtree(del_dir)
            tdLog.info(f"delete dir {del_dir} successful")
        except OSError as x:
            tdLog.exit(f"remove path {del_dir} error : {x.strerror}")

        dnode.starttaosd()
        
        # exec restore
        sql = f"restore vnode on dnode {index}"
        tdLog.info(sql)
        tdSql.execute(sql)

        # check result
        self.check_corrent()

        
    # restore mnode
    def restore_mnode(self, index):
        tdLog.info(f"start restore mnode {index}")
        dnode = self.dnodes[index - 1]
        del_dir = f"{dnode.dataDir}/mnode"
        
        # stop dnode
        tdLog.info(f"stop dnode {index}")
        dnode.stoptaosd()

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
        del_dir = f"{dnode.dataDir}/qnode"

        # stop dnode
        tdLog.info(f"stop dnode {index}")
        dnode.stoptaosd()

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


