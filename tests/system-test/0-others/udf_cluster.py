import taos
import sys
import time
import os

from util.log import *
from util.sql import *
from util.cases import *
from util.dnodes import TDDnodes
from util.dnodes import TDDnode
import socket
import subprocess

class MyDnodes(TDDnodes):
    def __init__(self ,dnodes_lists):
        super(MyDnodes,self).__init__()
        self.dnodes = dnodes_lists  # dnode must be TDDnode instance
        self.simDeployed = False

class TDTestCase:

    def init(self, conn, logSql, replicaVar=1):
        self.replicaVar = int(replicaVar)
        tdLog.debug(f"start to excute {__file__}")
        self.TDDnodes = None
        self.depoly_cluster(3)
        self.master_dnode = self.TDDnodes.dnodes[0]
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

    def prepare_udf_so(self):
        selfPath = os.path.dirname(os.path.realpath(__file__))

        if ("community" in selfPath):
            projPath = selfPath[:selfPath.find("community")]
        else:
            projPath = selfPath[:selfPath.find("tests")]
        print(projPath)

        libudf1 = subprocess.Popen('find %s -name "libudf1.so"|grep lib|head -n1'%projPath , shell=True, stdout=subprocess.PIPE,stderr=subprocess.STDOUT).stdout.read().decode("utf-8")
        libudf2 = subprocess.Popen('find %s -name "libudf2.so"|grep lib|head -n1'%projPath , shell=True, stdout=subprocess.PIPE,stderr=subprocess.STDOUT).stdout.read().decode("utf-8")
        os.system("mkdir /tmp/udf/")
        os.system("sudo cp %s /tmp/udf/ "%libudf1.replace("\n" ,""))
        os.system("sudo cp  %s /tmp/udf/ "%libudf2.replace("\n" ,""))


    def prepare_data(self):

        tdSql.execute("drop database if exists db")
        tdSql.execute("create database if not exists db replica 1 duration 300")
        tdSql.execute("use db")
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
        for i in range(4):
            tdSql.execute(f'create table ct{i+1} using stb1 tags ( {i+1} )')

        for i in range(9):
            tdSql.execute(
                f"insert into ct1 values ( now()-{i*10}s, {1*i}, {11111*i}, {111*i}, {11*i}, {1.11*i}, {11.11*i}, {i%2}, 'binary{i}', 'nchar{i}', now()+{1*i}a )"
            )
            tdSql.execute(
                f"insert into ct4 values ( now()-{i*90}d, {1*i}, {11111*i}, {111*i}, {11*i}, {1.11*i}, {11.11*i}, {i%2}, 'binary{i}', 'nchar{i}', now()+{1*i}a )"
            )
        tdSql.execute("insert into ct1 values (now()-45s, 0, 0, 0, 0, 0, 0, 0, 'binary0', 'nchar0', now()+8a )")
        tdSql.execute("insert into ct1 values (now()+10s, 9, -99999, -999, -99, -9.99, -99.99, 1, 'binary9', 'nchar9', now()+9a )")
        tdSql.execute("insert into ct1 values (now()+15s, 9, -99999, -999, -99, -9.99, NULL, 1, 'binary9', 'nchar9', now()+9a )")
        tdSql.execute("insert into ct1 values (now()+20s, 9, -99999, -999, NULL, -9.99, -99.99, 1, 'binary9', 'nchar9', now()+9a )")

        tdSql.execute("insert into ct4 values (now()-810d, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL ) ")
        tdSql.execute("insert into ct4 values (now()-400d, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL ) ")
        tdSql.execute("insert into ct4 values (now()+9d, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL  ) ")

        tdSql.execute(
            f'''insert into t1 values
            ( '2020-04-21 01:01:01.000', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL )
            ( '2020-10-21 01:01:01.000', 1, 11111, 111, 11, 1.11, 11.11, 1, "binary1", "nchar1", now()+1a )
            ( '2020-12-31 01:01:01.000', 2, 22222, 222, 22, 2.22, 22.22, 0, "binary2", "nchar2", now()+2a )
            ( '2021-01-01 01:01:06.000', 3, 33333, 333, 33, 3.33, 33.33, 0, "binary3", "nchar3", now()+3a )
            ( '2021-05-07 01:01:10.000', 4, 44444, 444, 44, 4.44, 44.44, 1, "binary4", "nchar4", now()+4a )
            ( '2021-07-21 01:01:01.000', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL )
            ( '2021-09-30 01:01:16.000', 5, 55555, 555, 55, 5.55, 55.55, 0, "binary5", "nchar5", now()+5a )
            ( '2022-02-01 01:01:20.000', 6, 66666, 666, 66, 6.66, 66.66, 1, "binary6", "nchar6", now()+6a )
            ( '2022-10-28 01:01:26.000', 7, 00000, 000, 00, 0.00, 00.00, 1, "binary7", "nchar7", "1970-01-01 08:00:00.000" )
            ( '2022-12-01 01:01:30.000', 8, -88888, -888, -88, -8.88, -88.88, 0, "binary8", "nchar8", "1969-01-01 01:00:00.000" )
            ( '2022-12-31 01:01:36.000', 9, -99999999999999999, -999, -99, -9.99, -999999999999999999999.99, 1, "binary9", "nchar9", "1900-01-01 00:00:00.000" )
            ( '2023-02-21 01:01:01.000', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL )
            '''
        )

        tdSql.execute("create table tb (ts timestamp , num1 int , num2 int, num3 double , num4 binary(30))")
        tdSql.execute(
            f'''insert into tb values
            ( '2020-04-21 01:01:01.000',   NULL,    1,       1,     "binary1" )
            ( '2020-10-21 01:01:01.000',   1,       1,      1.11,   "binary1" )
            ( '2020-12-31 01:01:01.000',   2,    22222,      22,    "binary1" )
            ( '2021-01-01 01:01:06.000',   3,    33333,      33,    "binary1" )
            ( '2021-05-07 01:01:10.000',   4,    44444,      44,    "binary1" )
            ( '2021-07-21 01:01:01.000',   NULL,   NULL,     NULL,  "binary1" )
            ( '2021-09-30 01:01:16.000',   5,    55555,      55,    "binary1" )
            ( '2022-02-01 01:01:20.000',   6,    66666,      66,    "binary1" )
            ( '2022-10-28 01:01:26.000',   0,    00000,      00,    "binary1" )
            ( '2022-12-01 01:01:30.000',   8,   -88888,     -88,    "binary1" )
            ( '2022-12-31 01:01:36.000',   9, -9999999,     -99,    "binary1" )
            ( '2023-02-21 01:01:01.000',  NULL,    NULL,    NULL,   "binary1" )
            '''
        )


    def create_udf_function(self ):

        for i in range(10):
            # create  scalar functions
            tdSql.execute("create function udf1 as '/tmp/udf/libudf1.so' outputtype int bufSize 8;")

            # create aggregate functions

            tdSql.execute("create aggregate function udf2 as '/tmp/udf/libudf2.so' outputtype double bufSize 8;")

            # functions = tdSql.getResult("show functions")
            # function_nums = len(functions)
            # if function_nums == 2:
            #     tdLog.info("create two udf functions success ")

            # drop functions

            tdSql.execute("drop function udf1")
            tdSql.execute("drop function udf2")

            functions = tdSql.getResult("show functions")
            for function in functions:
                if "udf1" in function[0] or  "udf2" in function[0]:
                    tdLog.info("drop udf functions failed ")
                    tdLog.exit("drop udf functions failed")

                tdLog.info("drop two udf functions success ")

        # create  scalar functions
        tdSql.execute("create function udf1 as '/tmp/udf/libudf1.so' outputtype int bufSize 8;")

        # create aggregate functions

        tdSql.execute("create aggregate function udf2 as '/tmp/udf/libudf2.so' outputtype double bufSize 8;")

        functions = tdSql.getResult("show functions")
        function_nums = len(functions)
        if function_nums == 2:
            tdLog.info("create two udf functions success ")

    def basic_udf_query(self , dnode):

        mytdSql = self.getConnection(dnode)
        # scalar functions

        mytdSql.execute("use db ")

        result = mytdSql.query("select num1 , udf1(num1) ,num2 ,udf1(num2),num3 ,udf1(num3),num4 ,udf1(num4) from tb")
        data = result.fetch_all()
        print(data)
        if data == [(None, None, 1, 88, 1.0, 88, 'binary1', 88), (1, 88, 1, 88, 1.11, 88, 'binary1', 88), (2, 88, 22222, 88, 22.0, 88, 'binary1', 88), (3, 88, 33333, 88, 33.0, 88, 'binary1', 88), (4, 88, 44444, 88, 44.0, 88, 'binary1', 88), (None, None, None, None, None, None, 'binary1', 88), (5, 88, 55555, 88, 55.0, 88, 'binary1', 88), (6, 88, 66666, 88, 66.0, 88, 'binary1', 88), (0, 88, 0, 88, 0.0, 88, 'binary1', 88), (8, 88, -88888, 88, -88.0, 88, 'binary1', 88), (9, 88, -9999999, 88, -99.0, 88, 'binary1', 88), (None, None, None, None, None, None, 'binary1', 88)]:
            tdLog.info(" UDF query check ok at :dnode_index %s" %dnode.index)
        else:
            tdLog.info(" UDF query check failed at :dnode_index %s" %dnode.index)
            tdLog.exit("query check failed at :dnode_index %s" %dnode.index )

        result = mytdSql.query("select udf1(c1,c6), udf1(c1) ,udf1(c6) from stb1 order by ts")
        data = result.fetch_all()
        print(data)
        if data == [(None, None, None), (88, 88, 88), (88, 88, 88), (88, 88, 88), (88, 88, 88), (None, None, None), (88, 88, 88), (88, 88, 88), (88, 88, 88), (88, 88, 88), (88, 88, 88), (88, 88, 88), (88, 88, 88), (88, 88, 88), (88, 88, 88), (88, 88, 88), (88, 88, 88), (88, 88, 88), (88, 88, 88), (88, 88, 88), (88, 88, 88), (88, 88, 88), (None, 88, None), (88, 88, 88), (None, None, None)]:
            tdLog.info(" UDF query check ok at :dnode_index %s" %dnode.index)
        else:
            tdLog.info(" UDF query check failed at :dnode_index %s" %dnode.index)
            tdLog.exit("query check failed at :dnode_index %s" %dnode.index )

        result = mytdSql.query("select udf2(c1,c6), udf2(c1) ,udf2(c6) from stb1 ")
        data = result.fetch_all()
        print(data)
        expect_data = [(266.47194411419747, 25.514701644346147, 265.247614503882)]
        status = True
        for index in range(len(expect_data[0])):
            if abs(expect_data[0][index] - data[0][index]) >0.0001:
                status = False
                break

        if status :
            tdLog.info(" UDF query check ok at :dnode_index %s" %dnode.index)
        else:
            tdLog.info(" UDF query check failed at :dnode_index %s" %dnode.index)
            tdLog.exit("query check failed at :dnode_index %s" %dnode.index )

        result = mytdSql.query("select udf2(num1,num2,num3), udf2(num1) ,udf2(num2) from tb ")
        data = result.fetch_all()
        print(data)
        expect_data = [(10000949.554622812, 15.362291495737216, 10000949.553189287)]
        status = True
        for index in range(len(expect_data[0])):
            if abs(expect_data[0][index] - data[0][index]) >0.0001:
                status = False
                break

        if status :
            tdLog.info(" UDF query check ok at :dnode_index %s" %dnode.index)
        else:
            tdLog.info(" UDF query check failed at :dnode_index %s" %dnode.index)
            tdLog.exit("query check failed at :dnode_index %s" %dnode.index )


    def check_UDF_query(self):

        for i in range(20):
            for dnode in self.TDDnodes.dnodes:
                self.basic_udf_query(dnode)


    def depoly_cluster(self ,dnodes_nums):

        testCluster = False
        valgrind = 0
        hostname = socket.gethostname()
        dnodes = []
        start_port = 6030
        for num in range(1, dnodes_nums+1):
            dnode = TDDnode(num)
            dnode.addExtraCfg("firstEp", f"{hostname}:{start_port}")
            dnode.addExtraCfg("fqdn", f"{hostname}")
            dnode.addExtraCfg("serverPort", f"{start_port + (num-1)*100}")
            dnode.addExtraCfg("monitorFqdn", hostname)
            dnode.addExtraCfg("monitorPort", 7043)
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
            self.TDDnodes.start(dnode.index)

        # create cluster

        for dnode in self.TDDnodes.dnodes:
            print(dnode.cfgDict)
            dnode_id = dnode.cfgDict["fqdn"] +  ":" +dnode.cfgDict["serverPort"]
            dnode_first_host = dnode.cfgDict["firstEp"].split(":")[0]
            dnode_first_port = dnode.cfgDict["firstEp"].split(":")[-1]
            cmd = f" taos -h {dnode_first_host} -P {dnode_first_port} -s ' create dnode \"{dnode_id} \" ' ;"
            print(cmd)
            os.system(cmd)

        time.sleep(2)
        tdLog.info(" create cluster done! ")



    def getConnection(self, dnode):
        host = dnode.cfgDict["fqdn"]
        port = dnode.cfgDict["serverPort"]
        config_dir = dnode.cfgDir
        return taos.connect(host=host, port=int(port), config=config_dir)

    def restart_udfd(self, dnode):

        buildPath = self.getBuildPath()

        if (buildPath == ""):
            tdLog.exit("taosd not found!")
        else:
            tdLog.info("taosd found in %s" % buildPath)

        cfgPath = dnode.cfgDir

        udfdPath = buildPath +'/build/bin/udfd'

        for i in range(5):

            tdLog.info(" loop restart udfd  %d_th  at dnode_index : %s" % (i ,dnode.index))
            self.basic_udf_query(dnode)
            # stop udfd cmds
            get_processID = "ps -ef | grep -w udfd | grep %s | grep 'root' | grep -v grep| grep -v defunct | awk '{print $2}'"%cfgPath
            processID = subprocess.check_output(get_processID, shell=True).decode("utf-8")
            stop_udfd = " kill -9 %s" % processID
            os.system(stop_udfd)
            self.basic_udf_query(dnode)

    def test_restart_udfd_All_dnodes(self):

        for dnode in self.TDDnodes.dnodes:
            tdLog.info(" start restart udfd for dnode_index :%s" %dnode.index )
            self.restart_udfd(dnode)


    def run(self):  # sourcery skip: extract-duplicate-method, remove-redundant-fstring
        print(self.master_dnode.cfgDict)
        self.prepare_data()
        self.prepare_udf_so()
        self.create_udf_function()
        self.basic_udf_query(self.master_dnode)
        # self.check_UDF_query()
        self.restart_udfd(self.master_dnode)
        # self.test_restart_udfd_All_dnodes()



    def stop(self):
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")

tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
