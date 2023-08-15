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
import subprocess
import threading
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
        self.ts_step =1000
        self.db_name ='testdb'
        self.replica = 3
        self.vgroups = 1
        self.tb_nums = 10
        self.row_nums = 100
        self.stop_dnode_id = None
        self.loop_restart_times = 10
        self.current_thread = None
        self.max_restart_time = 5

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

    def _parse_datetime(self,timestr):
        try:
            return datetime.datetime.strptime(timestr, '%Y-%m-%d %H:%M:%S.%f')
        except ValueError:
            pass
        try:
            return datetime.datetime.strptime(timestr, '%Y-%m-%d %H:%M:%S')
        except ValueError:
            pass

    def mycheckRowCol(self, sql, row, col):
        caller = inspect.getframeinfo(inspect.stack()[2][0])
        if row < 0:
            args = (caller.filename, caller.lineno, sql, row)
            tdLog.exit("%s(%d) failed: sql:%s, row:%d is smaller than zero" % args)
        if col < 0:
            args = (caller.filename, caller.lineno, sql, row)
            tdLog.exit("%s(%d) failed: sql:%s, col:%d is smaller than zero" % args)
        if row > tdSql.queryRows:
            args = (caller.filename, caller.lineno, sql, row, tdSql.queryRows)
            tdLog.exit("%s(%d) failed: sql:%s, row:%d is larger than queryRows:%d" % args)
        if col > tdSql.queryCols:
            args = (caller.filename, caller.lineno, sql, col, tdSql.queryCols)
            tdLog.exit("%s(%d) failed: sql:%s, col:%d is larger than queryCols:%d" % args)

    def mycheckData(self, sql ,row, col, data):
        check_status = True
        self.mycheckRowCol(sql ,row, col)
        if tdSql.queryResult[row][col] != data:
            if tdSql.cursor.istype(col, "TIMESTAMP"):
                # suppose user want to check nanosecond timestamp if a longer data passed
                if (len(data) >= 28):
                    if pd.to_datetime(tdSql.queryResult[row][col]) == pd.to_datetime(data):
                        tdLog.info("sql:%s, row:%d col:%d data:%d == expect:%s" %
                            (sql, row, col, tdSql.queryResult[row][col], data))
                else:
                    if tdSql.queryResult[row][col] == self._parse_datetime(data):
                        tdLog.info("sql:%s, row:%d col:%d data:%s == expect:%s" %
                            (sql, row, col, tdSql.queryResult[row][col], data))
                return

            if str(tdSql.queryResult[row][col]) == str(data):
                tdLog.info("sql:%s, row:%d col:%d data:%s == expect:%s" %
                            (sql, row, col, tdSql.queryResult[row][col], data))
                return
            elif isinstance(data, float) and abs(tdSql.queryResult[row][col] - data) <= 0.000001:
                tdLog.info("sql:%s, row:%d col:%d data:%f == expect:%f" %
                            (sql, row, col, tdSql.queryResult[row][col], data))
                return
            else:
                caller = inspect.getframeinfo(inspect.stack()[1][0])
                args = (caller.filename, caller.lineno, sql, row, col, tdSql.queryResult[row][col], data)
                tdLog.info("%s(%d) failed: sql:%s row:%d col:%d data:%s != expect:%s" % args)

                check_status = False

        if data is None:
            tdLog.info("sql:%s, row:%d col:%d data:%s == expect:%s" %
                       (sql, row, col, tdSql.queryResult[row][col], data))
        elif isinstance(data, str):
            tdLog.info("sql:%s, row:%d col:%d data:%s == expect:%s" %
                       (sql, row, col, tdSql.queryResult[row][col], data))
        # elif isinstance(data, datetime.date):
        #     tdLog.info("sql:%s, row:%d col:%d data:%s == expect:%s" %
        #                (sql, row, col, tdSql.queryResult[row][col], data))
        elif isinstance(data, float):
            tdLog.info("sql:%s, row:%d col:%d data:%s == expect:%s" %
                       (sql, row, col, tdSql.queryResult[row][col], data))
        else:
            tdLog.info("sql:%s, row:%d col:%d data:%s == expect:%d" %
                       (sql, row, col, tdSql.queryResult[row][col], data))

        return check_status

    def mycheckRows(self, sql, expectRows):
        check_status = True
        if len(tdSql.queryResult) == expectRows:
            tdLog.info("sql:%s, queryRows:%d == expect:%d" % (sql, len(tdSql.queryResult), expectRows))
            return True
        else:
            caller = inspect.getframeinfo(inspect.stack()[1][0])
            args = (caller.filename, caller.lineno, sql, len(tdSql.queryResult), expectRows)
            tdLog.info("%s(%d) failed: sql:%s, queryRows:%d != expect:%d" % args)
            check_status = False
        return check_status

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
        tdLog.notice("==== stop_dnode has stopped , id is {}".format(self.stop_dnode_id))

    def wait_start_dnode_OK(self,newTdSql):
    
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
        tdLog.notice("==== stop_dnode has restart , id is {}".format(self.stop_dnode_id))

    def get_leader_infos(self ,dbname):

        newTdSql=tdCom.newTdSql()
        newTdSql.query("show {}.vgroups".format(dbname))
        vgroup_infos = newTdSql.queryResult

        leader_infos = set()
        for vgroup_info in vgroup_infos:
            leader_infos.add(vgroup_info[3:-4])

        return leader_infos

    def check_revote_leader_success(self, dbname, before_leader_infos , after_leader_infos):
        check_status = False
        vote_act = set(set(after_leader_infos)-set(before_leader_infos))
        if not vote_act:
            print("=======before_revote_leader_infos ======\n" , before_leader_infos)
            print("=======after_revote_leader_infos ======\n" , after_leader_infos)
            tdLog.info(" ===maybe revote not occured , there is no dnode offline ====")
        else:
            for vgroup_info in vote_act:
                for ind , role in enumerate(vgroup_info):
                    if role==self.stop_dnode_id:

                        if vgroup_info[ind+1] =="offline" and "leader" in vgroup_info:
                            tdLog.notice(" === revote leader ok , leader is {} now   ====".format(vgroup_info[list(vgroup_info).index("leader")-1]))
                            check_status = True
                        elif vgroup_info[ind+1] !="offline":
                            tdLog.notice(" === dnode {} should be offline ".format(self.stop_dnode_id))
                        else:
                            continue
                        break
        return check_status

    def start_benchmark_inserts(self):
        benchmark_build_path = self.getBuildPath() + '/build/bin/taosBenchmark'
        tdLog.notice("==== start taosBenchmark insert datas of database test ==== ")
        os.system(" {} -y -n 10000 -t 100  ".format(benchmark_build_path))

    def stop_leader_when_Benchmark_inserts(self,dbname , total_rows ):

        newTdSql=tdCom.newTdSql()
        
        # stop follower and insert datas , update tables and create new stables
        tdDnodes=cluster.dnodes
        tdSql.execute(" drop database if exists {} ".format(dbname))
        tdSql.execute(" create database {} replica {} vgroups {}".format(dbname , self.replica , self.vgroups))

        # start insert datas using taosBenchmark ,expect insert 10000 rows
        time.sleep(3)
        self.current_thread = threading.Thread(target=self.start_benchmark_inserts, args=())
        self.current_thread.start()
        tdSql.query(" select * from information_schema.ins_databases ")

        tdSql.query(" select count(*) from {}.{} ".format(dbname,"meters"))

        while not tdSql.queryResult:
            tdSql.query(" select count(*) from {}.{} ".format(dbname,"meters"))
        tdLog.debug(" === current insert  {} rows in database {} === ".format(tdSql.queryResult[0][0] , dbname))

        while (tdSql.queryResult[0][0] < total_rows/10):
            if tdSql.queryResult:
                tdLog.debug(" === current insert  {} rows in database {} === ".format(tdSql.queryResult[0][0] , dbname))
            time.sleep(0.01)
            tdSql.query(" select count(*) from {}.{} ".format(dbname,"meters"))

        tdLog.debug(" === database {} has write {} rows at least ====".format(dbname,total_rows/10))

        self.stop_dnode_id = self._get_stop_dnode_id(dbname ,"leader")

        # prepare stop leader of database
        before_leader_infos = self.get_leader_infos(dbname)

        tdDnodes[self.stop_dnode_id-1].stoptaosd()
        os.system("taos -s 'show dnodes;'")
        # self.current_thread.join()
        after_leader_infos = self.get_leader_infos(dbname)

        # start = time.time()
        # revote_status = self.check_revote_leader_success(dbname ,before_leader_infos , after_leader_infos)
        # while not revote_status:
        #     after_leader_infos = self.get_leader_infos(dbname)
        #     revote_status = self.check_revote_leader_success(dbname ,before_leader_infos , after_leader_infos)
        # end = time.time()
        # time_cost = end - start
        # tdLog.debug(" ==== revote leader of database {} cost time {}  ====".format(dbname , time_cost))

        self.current_thread.join()
        time.sleep(2)
        tdDnodes[self.stop_dnode_id-1].starttaosd()
        self.wait_start_dnode_OK(newTdSql)
        

        tdSql.query(" select count(*) from {}.{} ".format(dbname,"meters"))
        tdLog.debug(" ==== expected insert  {} rows of database {}  , really is {}".format(total_rows, dbname , tdSql.queryResult[0][0]))



    def run(self):

        # basic insert and check of cluster
        # self.check_setup_cluster_status()
        self.stop_leader_when_Benchmark_inserts('test' , 1000000 )
      

    def stop(self):
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")

tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
