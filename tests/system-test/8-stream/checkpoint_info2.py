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
import time

# -*- coding: utf-8 -*-


from util.log import *
from util.cases import *
from util.sql import *
from util.common import *
from util.sqlset import *
from util.cluster import *

# should be used by -N  option
class TDTestCase:
    updatecfgDict = {'checkpointInterval': 60 ,
                     'vdebugflag':143,
                     'ddebugflag':143
                     }
    def init(self, conn, logSql, replicaVar=1):
        self.replicaVar = int(replicaVar)
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor(), True)
    def find_checkpoint_info_file(self, dirpath, checkpointid, task_id):
        for root, dirs, files in os.walk(dirpath):
            if f'checkpoint{checkpointid}' in dirs:
                info_path = os.path.join(root, f'checkpoint{checkpointid}', 'info')
                if os.path.exists(info_path):
                    if task_id in info_path:
                        tdLog.info(f"info file found in {info_path}")
                        return info_path
                    else:
                        continue
                else:
                    tdLog.info(f"info file not found in {info_path}")
                    return None
            else:
                tdLog.info(f"no checkpoint{checkpointid} in {dirpath}")
    def get_dnode_info(self):
        '''
        get a dict from vnode to dnode
        '''
        self.vnode_dict = {}
        sql = 'select dnode_id, vgroup_id from information_schema.ins_vnodes where status = "leader"'
        result = tdSql.getResult(sql)
        for (dnode,vnode) in  result:
            self.vnode_dict[vnode] = dnode
    def print_time_info(self):
        '''
        sometimes, we need to wait for a while to check the info (for example, the checkpoint info file won't be created immediately after the redistribute)
        '''
        times= 0
        while(True):
            if(self.check_info()):
                tdLog.success(f'Time to finish is {times}')
                return 
            else:
                if times > 400:
                    tdLog.exit("time out")
                times += 10
                time.sleep(10)
    def check_info(self):
        '''
        first, check if the vnode is restored
        '''
        while(True):
            if(self.check_vnodestate()):
                break
        self.get_dnode_info()
        sql = 'select task_id, node_id, checkpoint_id, checkpoint_ver from information_schema.ins_stream_tasks where `level` = "source" or `level` = "agg" and node_type == "vnode"'
        for task_id, vnode, checkpoint_id, checkpoint_ver in tdSql.getResult(sql):
            dirpath = f"{cluster.dnodes[self.vnode_dict[vnode]-1].dataDir}/vnode/vnode{vnode}/"
            info_path = self.find_checkpoint_info_file(dirpath, checkpoint_id, task_id)
            if info_path is None:
                tdLog.info(f"info path: {dirpath} is null")
                return False
            with open(info_path, 'r') as f:
                info_id, info_ver = f.read().split()
                if int(info_id) != int(checkpoint_id) or int(info_ver) != int(checkpoint_ver):
                    tdLog.info(f"infoId: {info_id}, checkpointId: {checkpoint_id}, infoVer: {info_ver}, checkpointVer: {checkpoint_ver}")
                    return False
        return True

    def restart_stream(self):
        st = time.time()
        while True:
            sql = 'select status from information_schema.ins_stream_tasks where status<>"ready" '
            if len(tdSql.getResult(sql)) != 0:
                time.sleep(1)
                tdLog.info("wait for task to be ready, 1s")
            else:
                et = time.time()
                tdLog.info(f"wait for tasks to be ready: {et-st}s")
                break

        tdLog.debug("========restart stream========")
        for i in range(5):
            tdSql.execute("pause stream s1")
            time.sleep(2)
            tdSql.execute("resume stream s1")
    def initstream(self):
        tdLog.debug("========case1 start========")
        os.system("nohup taosBenchmark -y -B 1 -t 4 -S 500 -n 1000 -v 3  > /dev/null 2>&1 &")
        time.sleep(5)
        tdSql.execute("create snode on dnode 1")
        tdSql.execute("use test")
        tdSql.execute("create stream if not exists s1 trigger at_once  ignore expired 0 ignore update 0  fill_history 1 into st1 as select _wstart,sum(voltage),groupid from meters partition by groupid interval(1s)")
        tdLog.debug("========create stream using snode and insert data ok========")
        self.get_dnode_info()
    def redistribute_vnode(self):
        tdLog.debug("========redistribute vnode========")
        tdSql.redistribute_db_all_vgroups()
        self.get_dnode_info()
    def replicate_db(self):
        tdLog.debug("========replicate db========")
        while True:
            res = tdSql.getResult("SHOW TRANSACTIONS")
            if res == []:
                tdLog.debug("========== no transaction, begin to replicate db =========")
                tdSql.execute("alter database test replica 3")
                return
            else:
                time.sleep(5)
                continue
    def check_vnodestate(self):
        sql = 'select distinct restored from information_schema.ins_vnodes'
        if tdSql.getResult(sql) != [(True,)]:
            tdLog.debug(f"vnode not restored, wait 5s")
            time.sleep(5)
            return False
        else:
            return True
    def run(self):
        self.initstream()
        self.replicate_db()
        self.print_time_info()
        self.restart_stream()
        time.sleep(60)
        self.print_time_info()

    def stop(self):
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")

tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
