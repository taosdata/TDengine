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
from util.cluster import *
import threading
# should be used by -N  option
class TDTestCase:

    #updatecfgDict = {'checkpointInterval': 60 ,}
    def init(self, conn, logSql, replicaVar=1):
        print("========init========")

        self.replicaVar = int(replicaVar)
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor(), logSql)
    def find_checkpoint_info_file(self, dirpath, checkpointid, task_id):
        for root, dirs, files in os.walk(dirpath):
            if f'checkpoint{checkpointid}' in dirs:
                info_path = os.path.join(root, f'checkpoint{checkpointid}', 'info')
                if os.path.exists(info_path):
                    if task_id in info_path:
                        return info_path
                    else:
                        continue
                else:
                    return None
    def get_dnode_info(self):
        '''
        get a dict from vnode to dnode
        '''
        self.vnode_dict = {}
        sql = 'select dnode_id, vgroup_id from information_schema.ins_vnodes'
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
                if times > 200:
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
        sql = 'select task_id, node_id, checkpoint_id, checkpoint_ver from information_schema.ins_stream_tasks where `level` = "source" or `level` = "agg" and node_type == "vnode"'
        for task_id, vnode, checkpoint_id, checkpoint_ver in tdSql.getResult(sql):
            dirpath = f"{cluster.dnodes[self.vnode_dict[vnode]-1].dataDir}/vnode/vnode{vnode}/"
            info_path = self.find_checkpoint_info_file(dirpath, checkpoint_id, task_id)
            if info_path is None:
                return False
            with open(info_path, 'r') as f:
                info_id, info_ver = f.read().split()
                if int(info_id) != int(checkpoint_id) or int(info_ver) != int(checkpoint_ver):
                    return False
        return True

    def restart_stream(self):
        tdLog.debug("========restart stream========")
        for i in range(5):
            tdSql.execute("pause stream s1")
            time.sleep(2)
            tdSql.execute("resume stream s1")
    def initstream(self):
        tdLog.debug("========case1 start========")
        os.system("nohup taosBenchmark -y -B 1 -t 4 -S 500 -n 1000  -v 3  > /dev/null 2>&1 &")
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
        print("========run========")
        self.initstream()
        self.restart_stream()
        time.sleep(60)
        self.print_time_info()
        self.redistribute_vnode()
        self.restart_stream()
        time.sleep(60)
        self.print_time_info()

    def stop(self):
        print("========stop========")
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")
tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())