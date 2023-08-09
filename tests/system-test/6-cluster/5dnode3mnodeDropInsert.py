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
import time
import socket
import subprocess
from multiprocessing import Process
import threading
import time
import inspect
import ctypes
class MyDnodes(TDDnodes):
    def __init__(self ,dnodes_lists):
        super(MyDnodes,self).__init__()
        self.dnodes = dnodes_lists  # dnode must be TDDnode instance
        self.simDeployed = False


class TDTestCase:

    def init(self, conn, logSql, replicaVar=1):
        tdLog.debug(f"start to excute {__file__}")
        self.TDDnodes = None
        self.ts = 1500000000000

    def buildcluster(self,dnodenumber):
        self.depoly_cluster(dnodenumber)
        self.master_dnode = self.TDDnodes.dnodes[0]
        self.host=self.master_dnode.cfgDict["fqdn"]
        conn1 = taos.connect(self.master_dnode.cfgDict["fqdn"] , config=self.master_dnode.cfgDir)
        tdSql.init(conn1.cursor())


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

    def _async_raise(self, tid, exctype):
        """raises the exception, performs cleanup if needed"""
        if not inspect.isclass(exctype):
            exctype = type(exctype)
        res = ctypes.pythonapi.PyThreadState_SetAsyncExc(tid, ctypes.py_object(exctype))
        if res == 0:
            raise ValueError("invalid thread id")
        elif res != 1:
            # """if it returns a number greater than one, you're in trouble,
            # and you should call it again with exc=NULL to revert the effect"""
            ctypes.pythonapi.PyThreadState_SetAsyncExc(tid, None)
            raise SystemError("PyThreadState_SetAsyncExc failed")

    def stop_thread(self,thread):
        self._async_raise(thread.ident, SystemExit)


    def createDbTbale(self,dbcountStart,dbcountStop,stbname,chilCount):
        # fisrt add data : db\stable\childtable\general table

        for couti in range(dbcountStart,dbcountStop):
            tdLog.debug("drop database if exists db%d" %couti)
            tdSql.execute("drop database if exists db%d" %couti)
            print("create database if not exists db%d replica 1 duration 300" %couti)
            tdSql.execute("create database if not exists db%d replica 1 duration 300" %couti)
            tdSql.execute("use db%d" %couti)
            tdSql.execute(
            '''create table %s
            (ts timestamp, c1 int, c2 bigint,c3 binary(16), c4 timestamp)
            tags (t1 int)
            '''%stbname
            )
            tdSql.execute(
                '''
                create table t1
                (ts timestamp, c1 int, c2 bigint, c3 smallint, c4 tinyint, c5 float, c6 double, c7 bool, c8 binary(16),c9 nchar(32), c10 timestamp)
                '''
            )
            for i in range(chilCount):
                tdSql.execute(f'create table {stbname}_{i+1} using {stbname} tags ( {i+1} )')

    def insertTabaleData(self,dbcountStart,dbcountStop,stbname,chilCount,ts_start,rowCount):
        #  insert data : create childtable and data

        for couti in range(dbcountStart,dbcountStop):
            tdSql.execute("use db%d" %couti)
            pre_insert = "insert into "
            sql = pre_insert
            chilCount=int(chilCount)
            allRows=chilCount*rowCount
            tdLog.debug("doing insert data into stable-index:%s rows:%d ..."%(stbname, allRows))
            exeStartTime=time.time()
            for i in range(0,chilCount):
                sql += " %s_%d values "%(stbname,i)
                for j in range(rowCount):
                    sql += "(%d, %d, %d,'taos_%d',%d) "%(ts_start + j*1000, j, j, j, ts_start + j*1000)
                    if j >0 and j%4000 == 0:
                        # print(sql)
                        tdSql.execute(sql)
                        sql = "insert into %s_%d values " %(stbname,i)
            # end sql
            if sql != pre_insert:
                # print(sql)
                print(len(sql))
                tdSql.execute(sql)
            exeEndTime=time.time()
            spendTime=exeEndTime-exeStartTime
            speedInsert=allRows/spendTime
            tdLog.debug("spent %.2fs to INSERT  %d rows into %s , insert rate is  %.2f rows/s... [OK]"% (spendTime,allRows,stbname,speedInsert))

    def checkData(self,dbname,stbname,stableCount,CtableCount,rowsPerSTable,):
        tdSql.execute("use %s"%dbname)
        tdSql.query("show stables")
        tdSql.checkRows(stableCount)
        tdSql.query("show tables")
        tdSql.checkRows(CtableCount)
        for i in range(stableCount):
            tdSql.query("select count(*) from %s%d"%(stbname,i))
            tdSql.checkData(0,0,rowsPerSTable)
        return


    def depoly_cluster(self ,dnodes_nums):

        testCluster = False
        valgrind = 0
        hostname = socket.gethostname()
        dnodes = []
        start_port = 6030
        start_port_sec = 6130
        for num in range(1, dnodes_nums+1):
            dnode = TDDnode(num)
            dnode.addExtraCfg("firstEp", f"{hostname}:{start_port}")
            dnode.addExtraCfg("fqdn", f"{hostname}")
            dnode.addExtraCfg("serverPort", f"{start_port + (num-1)*100}")
            dnode.addExtraCfg("monitorFqdn", hostname)
            dnode.addExtraCfg("monitorPort", 7043)
            dnode.addExtraCfg("secondEp", f"{hostname}:{start_port_sec}")
            dnodes.append(dnode)

        self.TDDnodes = MyDnodes(dnodes)
        self.TDDnodes.init("")
        self.TDDnodes.setTestCluster(testCluster)
        self.TDDnodes.setValgrind(valgrind)

        self.TDDnodes.setAsan(tdDnodes.getAsan())
        self.TDDnodes.stopAll()
        for dnode in self.TDDnodes.dnodes:
            self.TDDnodes.deploy(dnode.index,{})

        for dnode in self.TDDnodes.dnodes:
            self.TDDnodes.starttaosd(dnode.index)

        # create cluster
        for dnode in self.TDDnodes.dnodes[1:]:
            # print(dnode.cfgDict)
            dnode_id = dnode.cfgDict["fqdn"] +  ":" +dnode.cfgDict["serverPort"]
            dnode_first_host = dnode.cfgDict["firstEp"].split(":")[0]
            dnode_first_port = dnode.cfgDict["firstEp"].split(":")[-1]
            cmd = f" taos -h {dnode_first_host} -P {dnode_first_port} -s ' create dnode \"{dnode_id} \" ' ;"
            print(cmd)
            os.system(cmd)

        time.sleep(2)
        tdLog.info(" create cluster with %d dnode  done! " %dnodes_nums)

    def checkdnodes(self,dnodenumber):
        count=0
        while count < 10:
            time.sleep(1)
            statusReadyBumber=0
            tdSql.query("select * from information_schema.ins_dnodes;")
            if tdSql.checkRows(dnodenumber) :
                print("dnode is %d nodes"%dnodenumber)
            for i in range(dnodenumber):
                if tdSql.queryResult[i][4] !='ready'  :
                    status=tdSql.queryResult[i][4]
                    print("dnode:%d status is %s "%(i,status))
                    break
                else:
                    statusReadyBumber+=1
            print(statusReadyBumber)
            if statusReadyBumber == dnodenumber :
                print("all of %d mnodes is ready in 10s "%dnodenumber)
                return True
                break
            count+=1
        else:
            print("%d mnodes is not ready in 10s "%dnodenumber)
            return False


    def check3mnode(self):
        count=0
        while count < 10:
            time.sleep(1)
            tdSql.query("select * from information_schema.ins_mnodes;")
            if tdSql.checkRows(3) :
                print("mnode is  three nodes")
            if  tdSql.queryResult[0][2]=='leader' :
                if  tdSql.queryResult[1][2]=='follower':
                    if  tdSql.queryResult[2][2]=='follower':
                        print("three mnodes is ready in 10s")
                        break
            elif tdSql.queryResult[0][2]=='follower' :
                if  tdSql.queryResult[1][2]=='leader':
                    if  tdSql.queryResult[2][2]=='follower':
                        print("three mnodes is ready in 10s")
                        break
            elif tdSql.queryResult[0][2]=='follower' :
                if  tdSql.queryResult[1][2]=='follower':
                    if  tdSql.queryResult[2][2]=='leader':
                        print("three mnodes is ready in 10s")
                        break
            count+=1
        else:
            print("three mnodes is not ready in 10s ")
            return -1

        tdSql.query("select * from information_schema.ins_mnodes;")
        tdSql.checkRows(3)
        tdSql.checkData(0,1,'%s:6030'%self.host)
        tdSql.checkData(0,3,'ready')
        tdSql.checkData(1,1,'%s:6130'%self.host)
        tdSql.checkData(1,3,'ready')
        tdSql.checkData(2,1,'%s:6230'%self.host)
        tdSql.checkData(2,3,'ready')

    def check3mnode1off(self):
        count=0
        while count < 10:
            time.sleep(1)
            tdSql.query("select * from information_schema.ins_mnodes;")
            if tdSql.checkRows(3) :
                print("mnode is  three nodes")
            if  tdSql.queryResult[0][2]=='offline' :
                if  tdSql.queryResult[1][2]=='leader':
                    if  tdSql.queryResult[2][2]=='follower':
                        print("stop mnodes  on dnode 2 successfully in 10s")
                        break
                elif tdSql.queryResult[1][2]=='follower':
                    if  tdSql.queryResult[2][2]=='leader':
                        print("stop mnodes  on dnode 2 successfully in 10s")
                        break
            count+=1
        else:
            print("stop mnodes  on dnode 2 failed in 10s ")
            return -1
        tdSql.error("drop mnode on dnode 1;")

        tdSql.query("select * from information_schema.ins_mnodes;")
        tdSql.checkRows(3)
        tdSql.checkData(0,1,'%s:6030'%self.host)
        tdSql.checkData(0,2,'offline')
        tdSql.checkData(0,3,'ready')
        tdSql.checkData(1,1,'%s:6130'%self.host)
        tdSql.checkData(1,3,'ready')
        tdSql.checkData(2,1,'%s:6230'%self.host)
        tdSql.checkData(2,3,'ready')

    def check3mnode2off(self):
        count=0
        while count < 40:
            time.sleep(1)
            tdSql.query("select * from information_schema.ins_mnodes;")
            if tdSql.checkRows(3) :
                print("mnode is  three nodes")
            if  tdSql.queryResult[0][2]=='leader' :
                if  tdSql.queryResult[1][2]=='offline':
                    if  tdSql.queryResult[2][2]=='follower':
                        print("stop mnodes  on dnode 2 successfully in 10s")
                        break
            count+=1
        else:
            print("stop mnodes  on dnode 2 failed in 10s ")
            return -1
        tdSql.error("drop mnode on dnode 2;")

        tdSql.query("select * from information_schema.ins_mnodes;")
        tdSql.checkRows(3)
        tdSql.checkData(0,1,'%s:6030'%self.host)
        tdSql.checkData(0,2,'leader')
        tdSql.checkData(0,3,'ready')
        tdSql.checkData(1,1,'%s:6130'%self.host)
        tdSql.checkData(1,2,'offline')
        tdSql.checkData(1,3,'ready')
        tdSql.checkData(2,1,'%s:6230'%self.host)
        tdSql.checkData(2,2,'follower')
        tdSql.checkData(2,3,'ready')

    def check3mnode3off(self):
        count=0
        while count < 10:
            time.sleep(1)
            tdSql.query("select * from information_schema.ins_mnodes;")
            if tdSql.checkRows(3) :
                print("mnode is  three nodes")
            if  tdSql.queryResult[0][2]=='leader' :
                if  tdSql.queryResult[2][2]=='offline':
                    if  tdSql.queryResult[1][2]=='follower':
                        print("stop mnodes  on dnode 3 successfully in 10s")
                        break
            count+=1
        else:
            print("stop mnodes  on dnode 3 failed in 10s")
            return -1
        tdSql.error("drop mnode on dnode 3;")
        tdSql.query("select * from information_schema.ins_mnodes;")
        tdSql.checkRows(3)
        tdSql.checkData(0,1,'%s:6030'%self.host)
        tdSql.checkData(0,2,'leader')
        tdSql.checkData(0,3,'ready')
        tdSql.checkData(1,1,'%s:6130'%self.host)
        tdSql.checkData(1,2,'follower')
        tdSql.checkData(1,3,'ready')
        tdSql.checkData(2,1,'%s:6230'%self.host)
        tdSql.checkData(2,2,'offline')
        tdSql.checkData(2,3,'ready')

    def five_dnode_three_mnode(self,dnodenumber):
        # testcase parameters
        vgroups=1
        dbcountStart=0
        dbcountStop=1
        dbname="db"
        stbname="stb"
        tablesPerStb=1000
        rowsPerTable=100
        startTs=1640966400000  # 2022-01-01 00:00:00.000

        tdSql.query("select * from information_schema.ins_dnodes;")
        tdSql.checkData(0,1,'%s:6030'%self.host)
        tdSql.checkData(4,1,'%s:6430'%self.host)
        tdSql.checkData(0,4,'ready')
        tdSql.checkData(4,4,'ready')
        tdSql.query("select * from information_schema.ins_mnodes;")
        tdSql.checkRows(1)
        tdSql.checkData(0,1,'%s:6030'%self.host)
        tdSql.checkData(0,2,'leader')
        tdSql.checkData(0,3,'ready')

        # fisr add three mnodes;
        tdSql.execute("create mnode on dnode 2")
        tdSql.execute("create mnode on dnode 3")

        # fisrt check statut ready
        self.check3mnode()

        tdSql.error("create mnode on dnode 2")
        tdSql.query("select * from information_schema.ins_dnodes;")
        print(tdSql.queryResult)
        tdLog.debug("stop all of mnode ")

        #  drop follower of mnode and insert data
        self.createDbTbale(dbcountStart, dbcountStop,stbname,tablesPerStb)
        #(method) insertTabaleData: (dbcountStart: Any, dbcountStop: Any, stbname: Any, chilCount: Any, ts_start: Any, rowCount: Any) -> None
        threads=threading.Thread(target=self.insertTabaleData, args=(
        dbcountStart,
        dbcountStop,
        stbname,
        tablesPerStb,
        startTs,
        rowsPerTable))

        threads.start()
        dropcount =0
        while dropcount <= 10:
            for i in range(1,3):
                tdLog.debug("drop mnode on dnode %d"%(i+1))
                tdSql.execute("drop mnode on dnode %d"%(i+1))
                tdSql.query("select * from information_schema.ins_mnodes;")
                count=0
                while count<10:
                    time.sleep(1)
                    tdSql.query("select * from information_schema.ins_mnodes;")
                    if tdSql.checkRows(2):
                        print("drop mnode %d successfully"%(i+1))
                        break
                    count+=1
                tdLog.debug("create mnode on dnode %d"%(i+1))
                tdSql.execute("create mnode on dnode %d"%(i+1))
                count=0
                while count<10:
                    time.sleep(1)
                    tdSql.query("select * from information_schema.ins_mnodes;")
                    if tdSql.checkRows(3):
                        print("drop mnode %d successfully"%(i+1))
                        break
                    count+=1
            dropcount+=1
        threads.join()
        self.check3mnode()



    def getConnection(self, dnode):
        host = dnode.cfgDict["fqdn"]
        port = dnode.cfgDict["serverPort"]
        config_dir = dnode.cfgDir
        return taos.connect(host=host, port=int(port), config=config_dir)


    def run(self):
        # print(self.master_dnode.cfgDict)
        self.buildcluster(5)
        self.five_dnode_three_mnode(5)

    def stop(self):
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")

tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
