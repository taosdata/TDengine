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
from util.autogen import *

import random
import time
import traceback
import os
from   os import path


class TDTestCase:
    # init 
    def init(self, conn, logSql, replicaVar=1):
        self.replicaVar = int(replicaVar)
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor(), True)
        
        # autoGen
        self.autoGen = AutoGen()
        # init cluster path
        selfPath = os.path.dirname(os.path.realpath(__file__))
        if ("community" in selfPath):
            projPath = selfPath[:selfPath.find("community")]
        else:
            projPath = selfPath[:selfPath.find("tests")]
        self.projDir = f"{projPath}sim/"
        tdLog.info(f" init projPath={self.projDir}")

    def compactDatbase(self):
        # compact database
        tdSql.execute(f"compact database {self.dbname}", show=True)
        waitSeconds = 20
        if self.waitTranslation(waitSeconds) == False:
            tdLog.exit(f"translation can not finish after wait {waitSeconds} seconds")
            return          

    # check tsdb folder empty
    def check_tsdb_dir(self, tsdbDir):
        for vfile in os.listdir(tsdbDir):
            fileName, ext = os.path.splitext(vfile)
            pathFile = os.path.join(tsdbDir, vfile)
            ext = ext.lower()
            tdLog.info(f"check exist file {pathFile} ...")
            if ext == ".head" or ext == ".data" or ext == ".stt" or ext == ".sma":
                tdLog.info(f"found {pathFile} not to be deleted ...")
                real = True
                for i in range(50):
                    tdLog.info(f"i={i} compact and try again ...")
                    self.compactDatbase()
                    if os.path.exists(pathFile) is False:
                        real = False
                        break
                    else:
                        time.sleep(0.5)
                        tdLog.info(f"file real exist {pathFile} , sleep 500ms and try")

                if real is False:
                    continue
                fileStat = os.stat(pathFile)
                tdLog.exit(f" check file can not be deleted. file={pathFile} file size={fileStat.st_size}")

        return True

    # check vnode tsdb folder empty
    def check_filedelete(self):     
        # put all vnode to list
        for dnode in os.listdir(self.projDir):
            vnodesDir = self.projDir + f"{dnode}/data/vnode/"
            if os.path.isdir(vnodesDir) == False or dnode[:5] != "dnode":
                continue
            print(f"vnodesDir={vnodesDir}")
            # enum all vnode
            for vnode in os.listdir(vnodesDir):
                vnodeDir = path.join(vnodesDir, vnode)
                print(f"vnodeDir={vnodeDir}")
                if os.path.isdir(vnodesDir):
                    tsdbDir = path.join(vnodeDir, "tsdb")
                    if path.exists(tsdbDir) :
                        self.check_tsdb_dir(tsdbDir)
                        
    def waitTranslation(self, waitSeconds):
        # wait end
        for i in range(waitSeconds):
            sql ="show transactions;"
            rows = tdSql.query(sql)
            if rows == 0:
               return True
            tdLog.info(f"i={i} wait for translation finish ...")
            time.sleep(1)

        return False

    # run
    def run(self):
        # seed
        random.seed(int(time.time()))
        self.dbname = "deletecheck"
        stbname = "meters"
        childname= "d"
        child_cnt   = 2
        batch_size  = 8000
        insert_rows = 100015
        start_ts = 1600000000000

        self.autoGen.create_db(self.dbname)

        loop = 3
        for i in range(loop):
            self.autoGen.create_stable(stbname, 4, 10, 4, 8)
            self.autoGen.create_child(stbname, childname, child_cnt)
            self.autoGen.set_batch_size(batch_size)
            self.autoGen.insert_data(insert_rows)
            self.autoGen.set_start_ts(start_ts)
            
            if i % 2 == 1:
              tdSql.execute(f"flush database {self.dbname}", show=True)

            # drop stable
            tdSql.execute(f"drop table {self.dbname}.{stbname} ", show = True)

            self.compactDatbase()

            # check file delete
            self.check_filedelete()
            tdLog.info(f"loop = {i+1} / {loop} check file delete ok after drop table successfully.")

            start_ts += i*100000000

        
    # stop
    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)

tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())