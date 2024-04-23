# author : wenzhouwww
from ssl import ALERT_DESCRIPTION_CERTIFICATE_UNOBTAINABLE
import taos
import sys
import time
import os 

from util.log import *
from util.sql import *
from util.cases import *
from util.dnodes import TDDnodes
from util.dnodes import TDDnode
from util.cluster import *

import time
import socket
import subprocess ,threading
sys.path.append(os.path.dirname(__file__))

class TDTestCase:
    def init(self, conn, logSql, replicaVar=1):
        self.replicaVar = int(replicaVar)
        tdLog.debug(f"start to excute {__file__}")
        tdSql.init(conn.cursor())
        self.host = socket.gethostname()
        self.mnode_list = {}
        self.dnode_list = {}
        self.ts = 1483200000000
        self.db_name ='testdb'
        self.replica = 3 
        self.vgroups = 10
        self.tb_nums = 10 
        self.row_nums = 100
        self.max_restart_time = 30
        self.restart_server_times = 2
        self.query_times = 5

    def getBuildPath(self):
        selfPath = os.path.dirname(os.path.realpath(__file__))
        if ("community" in selfPath):
            projPath = selfPath[:selfPath.find("community")]
        else:
            projPath = selfPath[:selfPath.find("tests")]

        for root, dirs, files in os.walk(projPath):
            if ("taosd" in files or "taosd.exe" in files):
                rootRealPath = os.path.dirname(os.path.realpath(root))
                if ("packaging" not in rootRealPath):
                    buildPath = root[:len(root) - len("/build/bin")]
                    break
        return buildPath

    def check_setup_cluster_status(self):
        tdSql.query("select * from information_schema.ins_mnodes")
        for mnode in tdSql.queryResult:
            name = mnode[1]
            info = mnode
            self.mnode_list[name] = info

        tdSql.query("select * from information_schema.ins_dnodes")
        for dnode in tdSql.queryResult:
            name = dnode[1]
            info = dnode
            self.dnode_list[name] = info

        count = 0
        is_leader = False
        mnode_name = ''
        for k,v in self.mnode_list.items():
            count +=1
            # only for 1 mnode
            mnode_name = k

            if v[2] =='leader':
                is_leader=True

        if count==1 and is_leader:
            tdLog.notice("===== depoly cluster success with 1 mnode as leader =====")
        else:
            tdLog.notice("===== depoly cluster fail with 1 mnode as leader =====")

        for k ,v in self.dnode_list.items():
            if k == mnode_name:
                if v[3]==0:
                    tdLog.notice("===== depoly cluster mnode only success at {} , support_vnodes is {} ".format(mnode_name,v[3]))
                else:
                    tdLog.notice("===== depoly cluster mnode only fail at {} , support_vnodes is {} ".format(mnode_name,v[3]))
            else:
                continue

    def create_db_check_vgroups(self):

        tdSql.execute("drop database if exists test")
        tdSql.execute("create database if not exists test replica 1 duration 300")
        time.sleep(3)
        tdSql.execute("use test")
        tdSql.execute(
        '''create table stb1
        (ts timestamp, c1 int, c2 bigint, c3 smallint, c4 tinyint, c5 float, c6 double, c7 bool, c8 binary(16),c9 nchar(32), c10 timestamp)
        tags (t1 int)
        '''
        )
        tdSql.execute(
            '''
            create table t1
            (ts timestamp, c1 int, c2 bigint, c3 smallint, c4 tinyint, c5 float, c6 double, c7 bool, c8 binary(16),c9 nchar(32), c10 timestamp)
            '''
        )
        
        for i in range(5):
            tdSql.execute("create table sub_tb_{} using stb1 tags({})".format(i,i))
        tdSql.query("show stables")
        tdSql.checkRows(1)
        tdSql.query("show tables")
        tdSql.checkRows(6)

        tdSql.query("show test.vgroups;")
        vgroups_infos = {}  # key is id: value is info list
        for vgroup_info in tdSql.queryResult:
            vgroup_id = vgroup_info[0]
            tmp_list = []
            for role in vgroup_info[3:-4]:
                if role in ['leader', 'leader*', 'leader**', 'follower']:
                    tmp_list.append(role)
            vgroups_infos[vgroup_id]=tmp_list

        for k , v in vgroups_infos.items():
            if len(v) == 1 and v[0] in ['leader', 'leader*', 'leader**']:
                tdLog.notice(" === create database replica only 1 role leader  check success of vgroup_id {} ======".format(k))
            else:
                tdLog.notice(" === create database replica only 1 role leader  check fail of vgroup_id {} ======".format(k))

    def create_db_replica_3_insertdatas(self, dbname, replica_num ,vgroup_nums ,tb_nums , row_nums ):
        newTdSql=tdCom.newTdSql()
        drop_db_sql = "drop database if exists {}".format(dbname)
        create_db_sql = "create database {} replica {} vgroups {}".format(dbname,replica_num,vgroup_nums)

        tdLog.notice(" ==== create database {} and insert rows begin =====".format(dbname))
        newTdSql.execute(drop_db_sql)
        time.sleep(3)
        newTdSql.execute(create_db_sql)
        time.sleep(5)
        newTdSql.execute("use {}".format(dbname))
        newTdSql.execute(
        '''create table stb1
        (ts timestamp, c1 int, c2 bigint, c3 smallint, c4 tinyint, c5 float, c6 double, c7 bool, c8 binary(32),c9 nchar(32), c10 timestamp)
        tags (t1 int)
        '''
        )
        newTdSql.execute(
            '''
            create table t1
            (ts timestamp, c1 int, c2 bigint, c3 smallint, c4 tinyint, c5 float, c6 double, c7 bool, c8 binary(32),c9 nchar(32), c10 timestamp)
            '''
        )
        
        for i in range(tb_nums):
            sub_tbname = "sub_tb_{}".format(i)
            newTdSql.execute("create table {} using stb1 tags({})".format(sub_tbname,i))
            # insert datas about new database

            for row_num in range(row_nums):
                if row_num % (int(row_nums*0.1)) == 0 :
                    tdLog.notice( " === database {} writing records {} rows".format(dbname , row_num ) )
                ts = self.ts + 1000*row_num
                newTdSql.execute(f"insert into {sub_tbname} values ({ts}, {row_num} ,{row_num}, 10 ,1 ,{row_num} ,{row_num},true,'bin_{row_num}','nchar_{row_num}',now) ")

        tdLog.notice(" ==== create database {} and insert rows execute end =====".format(dbname))

    
    def _get_stop_dnode_id(self,dbname ,dnode_role):
        tdSql.query("show {}.vgroups".format(dbname))
        vgroup_infos = tdSql.queryResult
        status = False
        for vgroup_info in vgroup_infos:
            if "error" not in vgroup_info:
                status = True
            else:
                status = False
        while status!=True :
            time.sleep(0.1)
            tdSql.query("show {}.vgroups".format(dbname))
            vgroup_infos = tdSql.queryResult
            for vgroup_info in vgroup_infos:
                if "error" not in vgroup_info:
                    status = True
                else:
                    status = False
            # print(status)
        for vgroup_info in vgroup_infos:
            leader_infos = vgroup_info[3:-4] 
            # print(vgroup_info)
            for ind ,role in enumerate(leader_infos):
                if role == dnode_role:
                    # print(ind,leader_infos)
                    self.stop_dnode_id = leader_infos[ind-1]
                    break

        return self.stop_dnode_id

    def wait_stop_dnode_OK(self , newTdSql):
    
        def _get_status():
            # newTdSql=tdCom.newTdSql()

            status =  ""
            newTdSql.query("select * from information_schema.ins_dnodes")
            dnode_infos = newTdSql.queryResult
            for dnode_info in dnode_infos:
                id = dnode_info[0]
                dnode_status = dnode_info[4]
                if id == self.stop_dnode_id:
                    status = dnode_status
                    break
            return status 

        status = _get_status()

        while status !="offline":
            time.sleep(0.1)
            status = _get_status()
            # tdLog.notice("==== stop dnode has not been stopped , endpoint is {}".format(self.stop_dnode))
        tdLog.notice("==== stop_dnode has stopped , id is {} ====".format(self.stop_dnode_id))

    def wait_start_dnode_OK(self , newTdSql ):
    
        def _get_status():
            # newTdSql=tdCom.newTdSql()
            status =  ""
            newTdSql.query("select * from information_schema.ins_dnodes")
            dnode_infos = newTdSql.queryResult
            for dnode_info in dnode_infos:
                id = dnode_info[0]
                dnode_status = dnode_info[4]
                if id == self.stop_dnode_id:
                    status = dnode_status
                    break
            return status 

        status = _get_status()
        while status !="ready":
            time.sleep(0.1)
            status = _get_status()
            # tdLog.notice("==== stop dnode has not been stopped , endpoint is {}".format(self.stop_dnode))
        tdLog.notice("==== stop_dnode has restart , id is {} ====".format(self.stop_dnode_id))

    def check_insert_status(self, newTdSql , dbname, tb_nums , row_nums):

        newTdSql.execute("use {}".format(dbname))
        newTdSql.query("select count(*) from {}.{}".format(dbname,'stb1'))
        # tdSql.checkData(0 , 0 , tb_nums*row_nums)
        newTdSql.query("select distinct tbname from {}.{}".format(dbname,'stb1'))
        # tdSql.checkRows(tb_nums)

    def loop_query_constantly(self, times ,  db_name, tb_nums ,row_nums):

        newTdSql=tdCom.newTdSql()
        for loop_time in range(times):
            tdLog.debug(" === query is going ,this is {}_th query === ".format(loop_time))
            
            self.check_insert_status( newTdSql ,db_name, tb_nums , row_nums)

    
    def loop_restart_follower_constantly(self, times , db_name):

        tdDnodes = cluster.dnodes
        newTdSql=tdCom.newTdSql()

        for loop_time in range(times):

            self.stop_dnode_id = self._get_stop_dnode_id(db_name , "follower")
            
            print(self.stop_dnode_id)
            # begin stop dnode 
            start = time.time()
            tdDnodes[self.stop_dnode_id-1].stoptaosd()
            self.wait_stop_dnode_OK(newTdSql)

            tdLog.notice(" === this is {}_th restart taosd === ".format(loop_time))

            # begin start dnode 
            tdDnodes[self.stop_dnode_id-1].starttaosd()
            self.wait_start_dnode_OK(newTdSql)
            end = time.time()
            time_cost = int(end -start)
            if time_cost > self.max_restart_time:
                tdLog.notice(" ==== restart dnode {} cost too much time , please check ====".format(self.stop_dnode_id))
            


    def run(self): 

        self.check_setup_cluster_status()
        self.create_db_check_vgroups()
        
        # start writing constantly 
        writing = threading.Thread(target = self.create_db_replica_3_insertdatas, args=(self.db_name , self.replica , self.vgroups , self.tb_nums , self.row_nums))
        writing.start()
        tdSql.query(" show {}.stables ".format(self.db_name))
        while not tdSql.queryResult:
            print(tdSql.queryResult)
            time.sleep(0.1)
            tdSql.query(" show {}.stables ".format(self.db_name))

        restart_servers = threading.Thread(target = self.loop_restart_follower_constantly, args = (self.restart_server_times ,self.db_name))
        restart_servers.start()

        reading = threading.Thread(target = self.loop_query_constantly, args=(self.query_times,self.db_name , self.tb_nums , self.row_nums))
        reading.start()
        
        writing.join()
        reading.join()
        restart_servers.join()

    def stop(self):
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")

tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())