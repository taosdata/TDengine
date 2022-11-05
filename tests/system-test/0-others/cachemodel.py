import taos
import sys ,os ,json
import datetime
import inspect
import subprocess

from util.log import *
from util.sql import *
from util.cases import *
from util.dnodes import *


class TDTestCase:
    updatecfgDict = {'debugFlag': 143 ,"cDebugFlag":143,"uDebugFlag":143 ,"rpcDebugFlag":143 , "tmrDebugFlag":143 ,
    "jniDebugFlag":143 ,"simDebugFlag":143,"dDebugFlag":143, "dDebugFlag":143,"vDebugFlag":143,"mDebugFlag":143,"qDebugFlag":143,
    "wDebugFlag":143,"sDebugFlag":143,"tsdbDebugFlag":143,"tqDebugFlag":143 ,"fsDebugFlag":143 ,"udfDebugFlag":143}
    def init(self, conn, logSql, replicaVar=1):
        self.replicaVar = int(replicaVar)
        tdLog.debug(f"start to excute {__file__}")
        tdSql.init(conn.cursor(), True)

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

    def illegal_params(self):

        illegal_params = ["1","0","NULL","False","True" ,"keep","now" ,"*" , "," ,"_" , "abc" ,"keep"]

        for value in illegal_params:

            tdSql.error("create database testdb replica 1 cachemodel '%s' " %value)

        unexpected_numbers = [-1 , 0.0 , 3.0 , 4,  10 , 100]

        for number in unexpected_numbers:
            tdSql.error("create database testdb replica 1 cachemodel %s " %number)

    def getCacheModelStr(self, value):
        numbers = {
            0 : "none",
            1 : "last_row",
            2 : "last_value",
            3 : "both"
        }
        return numbers.get(value, 'other')

    def getCacheModelNum(self,str):
        numbers = {
            "none" : 0,
            "last_row" : 1,
            "last_value" : 2,
            "both" : 3

        }
        return numbers.get(str, 'other')

    def prepare_datas(self):
        for i in range(4):
            str = self.getCacheModelStr(i)
            tdSql.execute("create database testdb_%s replica 1 cachemodel '%s' " %(str, str))
            tdSql.execute("use testdb_%s"%str)
            tdSql.execute("create stable st(ts timestamp , c1 int ,c2 float ) tags(ind int) ")
            tdSql.execute("create table tb1 using st tags(1) ")
            tdSql.execute("create table tb2 using st tags(2) ")

            for k in range(10):
                tdSql.execute(" insert into tb1 values(now , %d, %f)" %(k,k*10) )
                tdSql.execute(" insert into tb2 values(now , %d, %f)" %(k,k*10) )

    def check_cachemodel_sets(self):


        # check cache_last value for database

        tdSql.query(" select * from information_schema.ins_databases ")
        databases_infos = tdSql.queryResult
        cache_lasts = {}
        for db_info in databases_infos:
            dbname = db_info[0]
            # print(dbname)
            cache_last_value = db_info[18]
            # print(cache_last_value)
            if dbname in ["information_schema" , "performance_schema"]:
                continue
            cache_lasts[dbname]=self.getCacheModelNum(cache_last_value)


        # cache_last_set value
        for k , v in cache_lasts.items():

            if k=="testdb_"+str(self.getCacheModelStr(v)):
                tdLog.info(" database %s cache_last value check pass, value is %s "%(k,self.getCacheModelStr(v)) )
            else:
                tdLog.exit(" database %s cache_last value check fail, value is %s "%(k,self.getCacheModelStr(v)) )

        # # check storage layer implementation


        buildPath = self.getBuildPath()
        if (buildPath == ""):
            tdLog.exit("taosd not found!")
        else:
            tdLog.info("taosd found in %s" % buildPath)
        dataPath = buildPath + "/../sim/dnode1/data"
        abs_vnodePath = os.path.abspath(dataPath)+"/vnode/"
        tdLog.info("abs_vnodePath: %s" % abs_vnodePath)

        tdSql.query(" select * from information_schema.ins_dnodes ")
        dnode_id  = tdSql.queryResult[0][0]

        for dbname in cache_lasts.keys():
            # print(dbname)
            tdSql.execute(" use %s" % dbname)
            tdSql.query(" show vgroups ")
            vgroups_infos = tdSql.queryResult
            for vgroup_info in vgroups_infos:
                vnode_json = abs_vnodePath + "/vnode" +f"{vgroup_info[0]}/" + "vnode.json"
                vnode_info_of_db = f"cat {vnode_json}"
                vnode_info = subprocess.check_output(vnode_info_of_db, shell=True).decode("utf-8")
                infoDict = json.loads(vnode_info)
                vnode_json_of_dbname = f"{dnode_id}."+ dbname
                config = infoDict["config"]
                if infoDict["config"]["dbname"] == vnode_json_of_dbname:
                    if "cacheLast" in infoDict["config"]:
                        if int(infoDict["config"]["cacheLast"]) != cache_lasts[dbname]:
                            tdLog.exit("cachemodel value is error in vnode.json of vnode%d "%(vgroup_info[0]))
                        else:
                            tdLog.info("cachemodel value is success in vnode.json of vnode%d "%(vgroup_info[0]))
                    else:
                        tdLog.exit("cacheLast not found in vnode.json of vnode%d "%(vgroup_info[0]))

    def restart_check_cachemodel_sets(self):

        for i in range(3):
            tdSql.query("select * from information_schema.ins_dnodes")
            index = tdSql.getData(0, 0)
            tdDnodes.stop(index)
            tdDnodes.start(index)
            time.sleep(3)
            self.check_cachemodel_sets()


    def run(self):  # sourcery skip: extract-duplicate-method, remove-redundant-fstring
        self.illegal_params()
        self.prepare_datas()
        self.check_cachemodel_sets()
        self.restart_check_cachemodel_sets()

    def stop(self):
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")

tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
