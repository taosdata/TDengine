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

from collections import defaultdict
import random
import string
import threading
import requests
import time
# import socketfrom

import taos
from util.log import *
from util.sql import *
from util.cases import *
from util.dnodes import *
from util.common import *

# class actionType(Enum):
#     CREATE_DATABASE = 0
#     CREATE_STABLE   = 1
#     CREATE_CTABLE   = 2
#     INSERT_DATA     = 3

class ClusterComCheck:
    def init(self, conn, logSql=False):
        tdSql.init(conn.cursor())
        # tdSql.init(conn.cursor(), logSql)  # output sql.txt file

    def checkDnodes(self,dnodeNumbers, timeout=30):
        count=0
        # print(tdSql)
        while count < timeout:
            tdSql.query("select * from information_schema.ins_dnodes")
            # tdLog.debug(tdSql.queryResult)
            status=0
            for i in range(dnodeNumbers):
                if tdSql.queryResult[i][4] == "ready":
                    status+=1
            # tdLog.info(status)

            if status == dnodeNumbers:
                tdLog.success("it find cluster with %d dnodes and check that all cluster dnodes are ready within %ds! " % (dnodeNumbers, count+1))
                return True
            time.sleep(1)
            count+=1
            
        else:
            tdSql.query("select * from information_schema.ins_dnodes")
            tdLog.debug(tdSql.queryResult)
            tdLog.exit("it find cluster with %d dnodes but  check that there dnodes are not ready within %ds ! "% (dnodeNumbers, timeout))

    def checkDbRows(self,dbNumbers):
        dbNumbers=int(dbNumbers)
        count=0
        while count < 5:
            tdSql.query("select * from information_schema.ins_databases where name!='collectd' ;")
            count+=1
            if tdSql.checkRows(dbNumbers+2):
                tdLog.success("we find %d databases and expect %d in clusters! " %(tdSql.queryRows,dbNumbers+2))
                return True
            else:
                continue
        else :
            tdLog.debug(tdSql.queryResult)
            tdLog.exit("we find %d databases but expect %d in clusters! " %(tdSql.queryRows,dbNumbers))

    def checkDb(self,dbNumbers,restartNumber,dbNameIndex, timeout=100):
        count=0
        alldbNumbers=(dbNumbers*restartNumber)+2
        while count < timeout:
            query_status=0
            for j in range(dbNumbers):
                for i in range(alldbNumbers):
                    tdSql.query("select * from information_schema.ins_databases;")
                    if "%s_%d"%(dbNameIndex,j) == tdSql.queryResult[i][0] :
                        if tdSql.queryResult[i][15] == "ready":
                            query_status+=1
                            tdLog.debug("check %s_%d that status is ready "%(dbNameIndex,j))
                        else:
                            sleep(1)
                            continue
            # print(query_status)
            if query_status == dbNumbers:
                tdLog.success(" check %d database and  all databases  are ready within %ds! " %(dbNumbers,count+1))
                return True
            count+=1

        else:
            tdLog.debug(tdSql.queryResult)
            tdLog.debug("query status is %d"%query_status)
            tdLog.exit("database is not ready within %ds"%(timeout+1))

    def checkData(self,dbname,stbname,stableCount,CtableCount,rowsPerSTable,):
        tdSql.execute("use %s"%dbname)
        tdSql.query("show %s.stables"%dbname)
        tdSql.checkRows(stableCount)
        tdSql.query("show  %s.tables"%dbname)
        tdSql.checkRows(CtableCount)
        for i in range(stableCount):
            tdSql.query("select count(*) from %s%d"%(stbname,i))
            tdSql.checkData(0,0,rowsPerSTable)
        return

    def checkMnodeStatus(self,mnodeNums):
        self.mnodeNums=int(mnodeNums)
        # self.leaderDnode=int(leaderDnode)
        tdLog.debug("start to check status of mnodes")
        count=0

        while count < 10:
            time.sleep(1)
            tdSql.query("select * from information_schema.ins_mnodes;")
            if tdSql.checkRows(self.mnodeNums) :
                tdLog.success("cluster has %d mnodes" %self.mnodeNums )

            if self.mnodeNums == 1:
                if  tdSql.queryResult[0][2]== 'leader' and  tdSql.queryResult[0][3]== 'ready'  :
                    tdLog.success("%d mnodes is ready in 10s"%self.mnodeNums)
                    return True
                count+=1
            elif self.mnodeNums == 3 :
                if  tdSql.queryResult[0][2]=='leader'  and  tdSql.queryResult[0][3]== 'ready' :
                    if  tdSql.queryResult[1][2]=='follower' and  tdSql.queryResult[1][3]== 'ready' :
                        if  tdSql.queryResult[2][2]=='follower' and  tdSql.queryResult[2][3]== 'ready' :
                            tdLog.success("%d mnodes is ready in 10s"%self.mnodeNums)
                            return True
                elif  tdSql.queryResult[1][2]=='leader'  and  tdSql.queryResult[1][3]== 'ready' :
                    if  tdSql.queryResult[0][2]=='follower' and  tdSql.queryResult[0][3]== 'ready' :
                        if  tdSql.queryResult[2][2]=='follower' and  tdSql.queryResult[2][3]== 'ready' :
                            tdLog.success("%d mnodes is ready in 10s"%self.mnodeNums)
                            return True
                elif  tdSql.queryResult[2][2]=='leader'  and  tdSql.queryResult[2][3]== 'ready' :
                    if  tdSql.queryResult[0][2]=='follower' and  tdSql.queryResult[0][3]== 'ready' :
                        if  tdSql.queryResult[1][2]=='follower' and  tdSql.queryResult[1][3]== 'ready' :
                            tdLog.success("%d mnodes is ready in 10s"%self.mnodeNums)
                            return True
                count+=1
            elif self.mnodeNums == 2 :
                if  tdSql.queryResult[0][2]=='leader' and  tdSql.queryResult[0][3]== 'ready' :
                    if  tdSql.queryResult[1][2]=='follower' and  tdSql.queryResult[1][3]== 'ready' :
                        tdLog.success("%d mnodes is ready in 10s"%self.mnodeNums)
                        return True
                elif tdSql.queryResult[1][2]=='leader' and  tdSql.queryResult[1][3]== 'ready' :
                    if  tdSql.queryResult[0][2]=='follower' and  tdSql.queryResult[0][3]== 'ready' :
                        tdLog.success("%d mnodes is ready in 10s"%self.mnodeNums)
                        return True
                count+=1
        else:
            tdLog.debug(tdSql.queryResult)
            tdLog.exit("cluster of %d  mnodes is not ready in 10s " %self.mnodeNums)




    def check3mnodeoff(self,offlineDnodeNo,mnodeNums=3):
        count=0
        while count < 10:
            time.sleep(1)
            tdSql.query("select * from information_schema.ins_mnodes;")
            if tdSql.checkRows(mnodeNums) :
                tdLog.success("cluster has %d mnodes" %self.mnodeNums )
            else:
                tdLog.exit("mnode number is correct")
            if offlineDnodeNo == 1:
                if  tdSql.queryResult[0][2]=='offline' :
                    if  tdSql.queryResult[1][2]=='leader':
                        if  tdSql.queryResult[2][2]=='follower':
                            tdLog.success("stop mnodes  on dnode %d  successfully in 10s" %offlineDnodeNo)
                            return True
                    elif tdSql.queryResult[1][2]=='follower':
                        if  tdSql.queryResult[2][2]=='leader':
                            tdLog.debug("stop mnodes  on dnode %d  successfully in 10s" %offlineDnodeNo)
                            return True
                count+=1
            elif offlineDnodeNo == 2:
                if  tdSql.queryResult[1][2]=='offline' :
                    if  tdSql.queryResult[0][2]=='leader':
                        if  tdSql.queryResult[2][2]=='follower':
                            tdLog.debug("stop mnodes  on dnode %d  successfully in 10s" %offlineDnodeNo)
                            return True
                    elif tdSql.queryResult[0][2]=='follower':
                        if  tdSql.queryResult[2][2]=='leader':
                            tdLog.debug("stop mnodes  on dnode %d  successfully in 10s" %offlineDnodeNo)
                            return True
                count+=1
            elif offlineDnodeNo == 3:
                if  tdSql.queryResult[2][2]=='offline' :
                    if  tdSql.queryResult[0][2]=='leader':
                        if  tdSql.queryResult[1][2]=='follower':
                            tdLog.debug("stop mnodes  on dnode %d  successfully in 10s" %offlineDnodeNo)
                            return True
                    elif tdSql.queryResult[0][2]=='follower':
                        if  tdSql.queryResult[1][2]=='leader':
                            tdLog.debug("stop mnodes  on dnode %d  successfully in 10s" %offlineDnodeNo)
                            return True
                count+=1
        else:
            tdLog.debug(tdSql.queryResult)
            tdLog.exit(f"stop mnodes  on dnode {offlineDnodeNo}  failed in 10s ")

    def check3mnode2off(self,mnodeNums=3):
        count=0
        while count < 10:
            time.sleep(1)
            tdSql.query("select * from information_schema.ins_mnodes;")
            if tdSql.checkRows(mnodeNums) :
                tdLog.success("cluster has %d mnodes" %self.mnodeNums )
            else:
                tdLog.exit("mnode number is correct")
            if  tdSql.queryResult[0][2]=='leader' :
                if  tdSql.queryResult[1][2]=='offline':
                    if  tdSql.queryResult[2][2]=='offline':
                        tdLog.success("stop mnodes of follower  on dnode successfully in 10s")
                        return True
            count+=1
        else:
            tdLog.debug(tdSql.queryResult)
            tdLog.exit("stop mnodes  on dnode 2 or 3 failed in 10s")

    def check_vgroups_status(self,vgroup_numbers=2,db_replica=3,count_number=10,db_name="db"):
        """ check vgroups status in 10s after db vgroups status is changed """
        vgroup_numbers = int(vgroup_numbers)
        self.db_replica = int(db_replica)
        tdLog.debug("start to check status of vgroups")
        count=0
        last_number=vgroup_numbers-1
        while count < count_number:
            time.sleep(1)
            tdSql.query(f"show  {db_name}.vgroups;")
            if  count == 0 :
                if tdSql.checkRows(vgroup_numbers) :
                    tdLog.success(f"{db_name} has {vgroup_numbers} vgroups" )
                else:
                    tdLog.exit(f"vgroup number of {db_name} is not correct")
            if self.db_replica == 1 :
                if  tdSql.queryResult[0][4] == 'leader' and tdSql.queryResult[last_number][4] == 'leader':
                    tdSql.query(f"select `replica` from information_schema.ins_databases where `name`='{db_name}';")
                    print("db replica :",tdSql.queryResult[0][0])
                    if tdSql.queryResult[0][0] == db_replica:
                        ready_time= (count + 1)
                        tdLog.success(f"all vgroups with replica {self.db_replica} of {db_name} are leaders in {count + 1} s")
                        return True
                count+=1
            elif self.db_replica == 3 :
                vgroup_status_first=[tdSql.queryResult[0][4],tdSql.queryResult[0][6],tdSql.queryResult[0][8]]

                vgroup_status_last=[tdSql.queryResult[last_number][4],tdSql.queryResult[last_number][6],tdSql.queryResult[last_number][8]]
                if  vgroup_status_first.count('leader') == 1 and vgroup_status_first.count('follower') == 2:
                    if vgroup_status_last.count('leader') == 1 and vgroup_status_last.count('follower') == 2:
                        tdSql.query(f"select `replica` from information_schema.ins_databases where `name`='{db_name}';")
                        print("db replica :",tdSql.queryResult[0][0])
                        if tdSql.queryResult[0][0] == db_replica:
                            ready_time= (count + 1)
                            tdLog.success(f"elections of {db_name}.vgroups with replica {self.db_replica}  are ready in {ready_time} s")
                            return True
                count+=1
        else:
            tdLog.debug(tdSql.queryResult)
            tdLog.notice(f"elections of {db_name} all vgroups with replica {self.db_replica}  are failed in {count} s ")
            caller = inspect.getframeinfo(inspect.stack()[1][0])
            args = (caller.filename, caller.lineno)
            tdLog.exit("%s(%d) failed " % args)




    def close(self):
        self.cursor.close()

clusterComCheck = ClusterComCheck()
