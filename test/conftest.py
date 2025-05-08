import pytest
import subprocess
import sys
import os
from new_test_framework import taostest
import logging
from new_test_framework.utils import tdSql, etool, tdLog, BeforeTest, eutil

import taos
import taosws
import yaml


def pytest_addoption(parser):
    parser.addoption("--yaml_file", action="store",
                    help="config YAML file in env directory")
    parser.addoption("-N", action="store", 
                    help="start dnodes numbers in clusters")
    parser.addoption("-M", action="store",
                    help="create mnode numbers in clusters")
    parser.addoption("-R", action="store_true",
                    help="restful realization form")
    parser.addoption("-Q", action="store",
                    help="set queryPolicy in one dnode")
    parser.addoption("-D", action="store",
                    help="set disk number on each level. range 1 ~ 10")
    parser.addoption("-K", action="store_true",
                    help="taoskeeper realization form")
    parser.addoption("-L", action="store",
                    help="set multiple level number. range 1 ~ 3")
    parser.addoption("-C", action="store",
                    help="create Dnode Numbers in one cluster")
    parser.addoption("-I", action="store",
                    help="independentMnode Mnode")
    parser.addoption("-A", action="store_true",
                    help="address sanitizer mode")
    parser.addoption("--replica", action="store",
                    help="the number of replicas")
    parser.addoption("--tsim", action="store",
                    help="tsim test file")
    parser.addoption("--skip_test", action="store_true",
                    help="Only do deploy or install without running test")
    parser.addoption("--skip_deploy", action="store_true",
                    help="Only run test without start TDengine")
    parser.addoption("--debug_log", action="store_true",
                    help="Enable debug log output.")
    #parser.addoption("--setup_all", action="store_true",
    #                help="Setup environment once before all tests running")


def pytest_configure(config):
    #log_level = logging.DEBUG if config.getoption("--debug_log") else logging.INFO
    #logging.basicConfig(level=log_level, format='%(asctime)s [%(levelname)s]: %(message)s')

    config.addinivalue_line(
        "markers",
        "tsim: mark test to have its name dynamically modified based on --tsim file name"
    )


@pytest.fixture(scope="session", autouse=True)
def before_test_session(request):
    before_test = BeforeTest(request)
    request.session.before_test = before_test
    request.session.root_dir = request.config.rootdir

    # 部署参数解析，存入session变量
    if request.config.getoption("-M"):
        request.session.mnodes_num = int(request.config.getoption("-M"))
    else:
        request.session.mnodes_num = 1
    if request.config.getoption("--tsim"):
        request.session.tsim_file = request.config.getoption("--tsim")
    else:
        request.session.tsim_file = None
    if request.config.getoption("--replica"):
        request.session.replicaVar = int(request.config.getoption("--replica"))
    else:
        request.session.replicaVar = 1
    if request.config.getoption("--skip_deploy"):
        request.session.skip_deploy = True
    else:
        request.session.skip_deploy = False
    if request.config.getoption("--skip_test"):
        request.session.skip_test = True
    else:
        request.session.skip_test = False

        # 读取环境变量并存入 session 变量
    request.session.taos_bin_path = os.getenv('TAOS_BIN_PATH', None)
    request.session.taos_bin_path = request.session.before_test.get_taos_bin_path(request.session.taos_bin_path)
    
    request.session.work_dir = os.getenv('WORK_DIR', None)
    request.session.work_dir = request.session.before_test.get_and_mkdir_workdir(request.session.work_dir)
    
    # 获取yaml文件，缓存到servers变量中，供cls使用
    if request.config.getoption("--yaml_file"):
        root_dir = request.config.rootdir
        request.session.yaml_file = request.config.getoption("--yaml_file")
        yaml_file = request.config.getoption("--yaml_file")
        # 构建 env 目录下的 yaml_file 路径
        yaml_file_path = os.path.join(root_dir, 'env', request.session.yaml_file)

        # 检查文件是否存在
        if not os.path.isfile(yaml_file_path):
            raise pytest.UsageError(f"YAML file '{yaml_file_path}' does not exist.")

        request.session.before_test.get_config_from_yaml(request, yaml_file_path)
        tdLog.debug(f"taos_bin_path: {request.session.taos_bin_path}")
        if request.session.taos_bin_path is None:
            raise pytest.UsageError("TAOS_BIN_PATH is not set")
    # 配置参数解析，存入session变量
    else:
        tdLog.debug(f"taos_bin_path: {request.session.taos_bin_path}")
        if request.session.taos_bin_path is None:
            raise pytest.UsageError("TAOS_BIN_PATH is not set")
        if request.config.getoption("-N"):
            request.session.denodes_num = int(request.config.getoption("-N"))
        else:
            request.session.denodes_num = 1
        
        if request.config.getoption("-R"):
            request.session.restful = True
        else:
            request.session.restful = False
        if request.config.getoption("-K"):
            request.session.taoskeeper = True
        else:
            request.session.taoskeeper = False
        if request.config.getoption("-Q"):   
            request.session.query_policy = int(request.config.getoption("-Q"))
        else:
            request.session.query_policy = 1
        if request.config.getoption("-I"):   
            request.session.independentMnode = bool(request.config.getoption("-I"))
        else:
            request.session.independentMnode = False
        if request.config.getoption("-D"):
            request.session.disk = int(request.config.getoption("-D"))
        else:
            request.session.disk = 1
        if request.config.getoption("-L"):
            request.session.level = int(request.config.getoption("-L"))
        else:
            request.session.level = 1
        if request.config.getoption("-C"):
            request.session.create_dnode_num = int(request.config.getoption("-C"))
        else:
            request.session.create_dnode_num = request.session.denodes_num
        if request.config.getoption("-A"):
            request.session.asan = True
        else:
            request.session.asan = False
        
        request.session.before_test.get_config_from_param(request)
    
    
    if request.session.work_dir is None:
        raise pytest.UsageError("WORK_DIR is not set")
    


@pytest.fixture(scope="class", autouse=True)
def before_test_class(request):
    '''
    获取session中的配置，建立连接
    测试结束后断开连接，清理环境
    '''
    tdLog.debug(f"Current class name: {request.cls.__name__}")
    
    # 获取用例中可能需要用到的session变量
    request.cls.yaml_file = request.session.yaml_file
    request.cls.host = request.session.host
    request.cls.port = request.session.port
    request.cls.cfg_path = request.session.cfg_path
    request.cls.dnode_nums = request.session.denodes_num
    request.cls.mnode_nums = request.session.mnodes_num
    request.cls.restful = request.session.restful
    request.cls.taoskeeper = request.session.taoskeeper
    request.cls.query_policy = request.session.query_policy
    request.cls.replicaVar = request.session.replicaVar
    request.cls.tsim_file = request.session.tsim_file
    request.cls.tsim_path = request.session.tsim_path
    request.cls.taos_bin_path = request.session.taos_bin_path
    request.cls.lib_path = request.session.lib_path
    request.cls.work_dir = request.session.work_dir
    
    # 如果用例中定义了updatecfgDict，则更新配置
    if hasattr(request.cls, "updatecfgDict"):
        request.session.before_test.update_cfg(request.cls.updatecfgDict)

    # 部署taosd，包括启动dnode，mnode，adapter
    if not request.session.skip_deploy:
        request.session.before_test.deploy_taos(request.cls.yaml_file, request.session.mnodes_num)
   
        # 建立连接
        request.cls.conn = request.session.before_test.get_taos_conn(request)
        tdSql.init(request.cls.conn.cursor())
        tdSql.replica = request.session.replicaVar
        tdLog.debug(tdSql.query(f"show dnodes", row_tag=True))

        # 为兼容老用例，初始化原框架连接
        #tdSql_pytest.init(request.cls.conn.cursor())
        #tdSql_army.init(request.cls.conn.cursor())

        # 处理 -C 参数，如果未设置 -C 参数，create_dnode_num 和 -N 参数相同
        for i in range(1, request.session.create_dnode_num):
            tdSql.execute(f"create dnode localhost port {6030+i*100}")

        # 处理-Q参数，如果-Q参数不等于1，则创建qnode，并设置queryPolicy
        if request.session.query_policy != 1:
            tdSql.execute(f'alter local "queryPolicy" "{request.session.query_policy}"')
            tdSql.execute("create qnode on dnode 1")
            tdSql.execute("show local variables")
    
        # 为老用例兼容，初始化老框架部分实例
        request.session.before_test.init_dnode_cluster(request, dnode_nums=request.cls.dnode_nums, mnode_nums=request.cls.mnode_nums, independentMnode=True, level=request.session.level, disk=request.session.disk)
    
    if request.session.skip_test:
        pytest.skip("skip test")
    # ============================
    # 兼容army 初始化caseBase
    request.cls.tmpdir = "tmp"

    # record server information
    request.cls.dnodeNum = request.session.denodes_num
    request.cls.mnodeNum = request.session.mnodes_num
    request.cls.mLevel = request.session.level
    request.cls.mLevelDisk = request.session.disk
    # test case information
    request.cls.db     = "db"
    request.cls.stb    = "stb"
    request.cls.checkColName = "ic"
    # sql 
    request.cls.sqlSum = f"select sum({request.cls.checkColName}) from {request.cls.stb}"
    request.cls.sqlMax = f"select max({request.cls.checkColName}) from {request.cls.stb}"
    request.cls.sqlMin = f"select min({request.cls.checkColName}) from {request.cls.stb}"
    request.cls.sqlAvg = f"select avg({request.cls.checkColName}) from {request.cls.stb}"
    request.cls.sqlFirst = f"select first(ts) from {request.cls.stb}"
    request.cls.sqlLast  = f"select last(ts) from {request.cls.stb}"
    # ============================

    yield

    # 测试结束后断开连接，清理环境
    if not request.session.skip_deploy:
        #tdSql.close()
        #tdSql_pytest.close()
        #tdSql_army.close()
        tdSql.close()
        request.cls.conn.close()
        if_success = True
        tdLog.debug(f"{request.session.items}")
        for item in request.session.items:
            # 检查是否属于当前类
            if item.cls is request.node.cls and (hasattr(item, "rep_call") or hasattr(item, "rep_setup") or hasattr(item, "rep_teardown")):
                if hasattr(item, "rep_call") and item.rep_call.outcome == "failed":
                    tdLog.debug(f"    失败原因: {str(item.rep_call.longrepr)}")
                    if_success = False
                if hasattr(item, "rep_setup") and item.rep_setup.outcome == "failed":
                    tdLog.debug(f"    失败原因: {str(item.rep_setup.longrepr)}")
                    if_success = False
                if hasattr(item, "rep_teardown") and item.rep_teardown.outcome == "failed":
                    tdLog.debug(f"    失败原因: {str(item.rep_teardown.longrepr)}")
                    if_success = False
                elif hasattr(item, "rep_call") and item.rep_call.outcome == "error":
                    tdLog.debug(f"    错误原因: {str(item.rep_call.longrepr)}")
                    if_success = False
                if hasattr(item, "rep_teardown") and item.rep_teardown.outcome == "error":
                    tdLog.debug(f"    错误原因: {str(item.rep_teardown.longrepr)}")
                    if_success = False
                if hasattr(item, "rep_setup") and item.rep_setup.outcome == "error":
                    tdLog.debug(f"    错误原因: {str(item.rep_setup.longrepr)}")
                    if_success = False
        if if_success:
            tdLog.info(f"successfully executed")
            request.session.before_test.destroy(request.cls.yaml_file)

@pytest.fixture(scope="class", autouse=True)
def add_common_methods(request):
    # 兼容原老框架，添加CaseBase方法

    def stop(self):
        tdSql.close()
    
    request.cls.stop = stop

    def createDb(self, options=""):
        sql = f"create database {self.db} {options}"
        tdSql.execute(sql, show=True)
    
    request.cls.createDb = createDb

    def trimDb(self, show = False):
        tdSql.execute(f"trim database {self.db}", show = show)
    
    request.cls.trimDb = trimDb

    def compactDb(self, show = False):
        tdSql.execute(f"compact database {self.db}", show = show)
    
    request.cls.compactDb = compactDb

    def flushDb(self, show = False):
        tdSql.execute(f"flush database {self.db}", show = show)

    request.cls.flushDb = flushDb

    def dropDb(self, show = False):
        tdSql.execute(f"drop database {self.db}", show = show)

    request.cls.dropDb = dropDb

    def dropStream(self, sname, show = False):
        tdSql.execute(f"drop stream {sname}", show = show)

    request.cls.dropStream = dropStream

    def splitVGroups(self):
        vgids = self.getVGroup(self.db)
        selid = random.choice(vgids)
        sql = f"split vgroup {selid}"
        tdSql.execute(sql, show=True)
        if self.waitTransactionZero() is False:
            tdLog.exit(f"{sql} transaction not finished")
            return False
        return True

    request.cls.splitVGroups = splitVGroups

    def alterReplica(self, replica):
        sql = f"alter database {self.db} replica {replica}"
        tdSql.execute(sql, show=True)
        if self.waitTransactionZero() is False:
            logger.exit(f"{sql} transaction not finished")
            return False
        return True

    request.cls.alterReplica = alterReplica

    def balanceVGroup(self):
        sql = f"balance vgroup"
        tdSql.execute(sql, show=True)
        if self.waitTransactionZero() is False:
            logger.exit(f"{sql} transaction not finished")
            return False
        return True

    request.cls.balanceVGroup = balanceVGroup
    
    def balanceVGroupLeader(self):
        sql = f"balance vgroup leader"
        tdSql.execute(sql, show=True)
        if self.waitTransactionZero() is False:
            logger.exit(f"{sql} transaction not finished")
            return False
        return True

    request.cls.balanceVGroupLeader = balanceVGroupLeader
    def balanceVGroupLeaderOn(self, vgId):
        sql = f"balance vgroup leader on {vgId}"
        tdSql.execute(sql, show=True)
        if self.waitTransactionZero() is False:
            logger.exit(f"{sql} transaction not finished")
            return False
        return True

    request.cls.balanceVGroupLeaderOn = balanceVGroupLeaderOn

    def balanceVGroupLeaderOn(self, vgId):
        sql = f"balance vgroup leader on {vgId}"
        tdSql.execute(sql, show=True)
        if self.waitTransactionZero() is False:
            logger.exit(f"{sql} transaction not finished")
            return False
    
    request.cls.balanceVGroupLeaderOn = balanceVGroupLeaderOn
#
#  check db correct
#                

    # basic
    def checkInsertCorrect(self, difCnt = 0):
        # check count
        sql = f"select count(*) from {self.stb}"
        tdSql.checkAgg(sql, self.childtable_count * self.insert_rows)

        # check child table count
        sql = f" select count(*) from (select count(*) as cnt , tbname from {self.stb} group by tbname) where cnt = {self.insert_rows} "
        tdSql.checkAgg(sql, self.childtable_count)

        # check step
        sql = f"select count(*) from (select diff(ts) as dif from {self.stb} partition by tbname order by ts desc) where dif != {self.timestamp_step}"
        tdSql.checkAgg(sql, difCnt)

    request.cls.checkInsertCorrect = checkInsertCorrect
    # save agg result
    def snapshotAgg(self):        
        self.sum =  tdSql.getFirstValue(self.sqlSum)
        self.avg =  tdSql.getFirstValue(self.sqlAvg)
        self.min =  tdSql.getFirstValue(self.sqlMin)
        self.max =  tdSql.getFirstValue(self.sqlMax)
        self.first = tdSql.getFirstValue(self.sqlFirst)
        self.last  = tdSql.getFirstValue(self.sqlLast)

    request.cls.snapshotAgg = snapshotAgg
    # check agg 
    def checkAggCorrect(self):
        tdSql.checkAgg(self.sqlSum, self.sum)
        tdSql.checkAgg(self.sqlAvg, self.avg)
        tdSql.checkAgg(self.sqlMin, self.min)
        tdSql.checkAgg(self.sqlMax, self.max)
        tdSql.checkAgg(self.sqlFirst, self.first)
        tdSql.checkAgg(self.sqlLast,  self.last)

    request.cls.checkAggCorrect = checkAggCorrect
    # self check 
    def checkConsistency(self, col):
        # top with max
        sql = f"select max({col}) from {self.stb}"
        expect = tdSql.getFirstValue(sql)
        sql = f"select top({col}, 5) from {self.stb}"
        tdSql.checkFirstValue(sql, expect)

        #bottom with min
        sql = f"select min({col}) from {self.stb}"
        expect = tdSql.getFirstValue(sql)
        sql = f"select bottom({col}, 5) from {self.stb}"
        tdSql.checkFirstValue(sql, expect)

        # order by asc limit 1 with first
        sql = f"select last({col}) from {self.stb}"
        expect = tdSql.getFirstValue(sql)
        sql = f"select {col} from {self.stb} order by _c0 desc limit 1"
        tdSql.checkFirstValue(sql, expect)

        # order by desc limit 1 with last
        sql = f"select first({col}) from {self.stb}"
        expect = tdSql.getFirstValue(sql)
        sql = f"select {col} from {self.stb} order by _c0 asc limit 1"
        tdSql.checkFirstValue(sql, expect)
    
    request.cls.checkConsistency = checkConsistency

    # check sql1 is same result with sql2
    def checkSameResult(self, sql1, sql2):
        logger.info(f"sql1={sql1}")
        logger.info(f"sql2={sql2}")
        logger.info("compare sql1 same with sql2 ...")

        # sql
        rows1 = tdSql.query(sql1,queryTimes=2)
        res1 = copy.deepcopy(tdSql.res)

        tdSql.query(sql2,queryTimes=2)
        res2 = tdSql.res

        rowlen1 = len(res1)
        rowlen2 = len(res2)
        errCnt = 0

        if rowlen1 != rowlen2:
            logger.error(f"both row count not equal. rowlen1={rowlen1} rowlen2={rowlen2} ")
            return False
        
        for i in range(rowlen1):
            row1 = res1[i]
            row2 = res2[i]
            collen1 = len(row1)
            collen2 = len(row2)
            if collen1 != collen2:
                logger.error(f"both col count not equal. collen1={collen1} collen2={collen2}")
                return False
            for j in range(collen1):
                if row1[j] != row2[j]:
                    logger.info(f"error both column value not equal. row={i} col={j} col1={row1[j]} col2={row2[j]} .")
                    errCnt += 1

        if errCnt > 0:
            logger.error(f"sql2 column value different with sql1. different count ={errCnt} ")

        logger.info("sql1 same result with sql2.")
    
    request.cls.checkSameResult = checkSameResult
#
#   get db information
#

    # get vgroups
    def getVGroup(self, dbName):
        vgidList = []
        sql = f"select vgroup_id from information_schema.ins_vgroups where db_name='{dbName}'"
        res = tdSql.getResult(sql)
        rows = len(res)
        for i in range(rows):
            vgidList.append(res[i][0])

        return vgidList
    
    request.cls.getVGroup = getVGroup
    
    # get distributed rows
    def getDistributed(self, tbName):
        sql = f"show table distributed {tbName}"
        tdSql.query(sql)
        dics = {}
        i = 0
        for i in range(tdSql.getRows()):
            row = tdSql.getData(i, 0)
            #print(row)
            row = row.replace('[', '').replace(']', '')
            #print(row)
            items = row.split(' ')
            #print(items)
            for item in items:
                #print(item)
                v = item.split('=')
                #print(v)
                if len(v) == 2:
                    dics[v[0]] = v[1]
            if i > 5:
                break
        print(dics)
        return dics
    
    request.cls.getDistributed = getDistributed

#
#   util 
#
    
    # wait transactions count to zero , return False is translation not finished
    def waitTransactionZero(self, seconds = 300, interval = 1):
        # wait end
        for i in range(seconds):
            sql ="show transactions;"
            rows = tdSql.query(sql)
            if rows == 0:
                logger.info("transaction count became zero.")
                return True
            #tdLog.info(f"i={i} wait ...")
            time.sleep(interval)
        
        return False    
    
    request.cls.waitTransactionZero = waitTransactionZero
    def waitCompactsZero(self, seconds = 300, interval = 1):
        # wait end
        for i in range(seconds):
            sql ="show compacts;"
            rows = tdSql.query(sql)
            if rows == 0:
                logger.info("compacts count became zero.")
                return True
            #tdLog.info(f"i={i} wait ...")
            time.sleep(interval)
        
        return False
    
    request.cls.waitCompactsZero = waitCompactsZero

    # check file exist
    def checkFileExist(self, pathFile):
        if os.path.exists(pathFile) == False:
            logger.error(f"file not exist {pathFile}")

    # check list not exist
    def checkListNotEmpty(self, lists, tips=""):
        if len(lists) == 0:
            logger.error(f"list is empty {tips}")

    request.cls.checkListNotEmpty = checkListNotEmpty

#
#  str util
#
    # covert list to sql format string
    def listSql(self, lists, sepa = ","):
        strs = ""
        for ls in lists:
            if strs != "":
                strs += sepa
            strs += f"'{ls}'"
        return strs

    request.cls.listSql = listSql
#
#  taosBenchmark 
#
    
    # run taosBenchmark and check insert Result
    def insertBenchJson(self, jsonFile, options="", checkStep=False):
        # exe insert 
        cmd = f"{options} -f {jsonFile}"        
        etool.runBinFile("taosBenchmark", command = cmd)

        #
        # check insert result
        #
        with open(jsonFile, "r") as file:
            data = json.load(file)
        
        db  = data["databases"][0]["dbinfo"]["name"]        
        stb = data["databases"][0]["super_tables"][0]["name"]
        child_count = data["databases"][0]["super_tables"][0]["childtable_count"]
        insert_rows = data["databases"][0]["super_tables"][0]["insert_rows"]
        timestamp_step = data["databases"][0]["super_tables"][0]["timestamp_step"]
        
        # drop
        try:
            drop = data["databases"][0]["dbinfo"]["drop"]
        except:
            drop = "yes"

        # command is first
        if options.find("-Q") != -1:
            drop = "no"

        # cachemodel
        try:
            cachemode = data["databases"][0]["dbinfo"]["cachemodel"]
        except:
            cachemode = None

        # vgropus
        try:
            vgroups   = data["databases"][0]["dbinfo"]["vgroups"]
        except:
            vgroups = None

        logger.info(f"get json info: db={db} stb={stb} child_count={child_count} insert_rows={insert_rows} \n")
        
        # all count insert_rows * child_table_count
        sql = f"select * from {db}.{stb}"
        tdSql.query(sql)
        tdSql.checkRows(child_count * insert_rows)

        # timestamp step
        if checkStep:
            sql = f"select * from (select diff(ts) as dif from {db}.{stb} partition by tbname) where dif != {timestamp_step};"
            tdSql.query(sql)
            tdSql.checkRows(0)

        if drop.lower() == "yes":
            # check database optins 
            sql = f"select `vgroups`,`cachemodel` from information_schema.ins_databases where name='{db}';"
            tdSql.query(sql)

            if cachemode != None:
                
                value = eutil.removeQuota(cachemode)                
                logger.info(f" deal both origin={cachemode} after={value}")
                tdSql.checkData(0, 1, value)

            if vgroups != None:
                tdSql.checkData(0, 0, vgroups)

        return db, stb,child_count, insert_rows

    request.cls.insertBenchJson = insertBenchJson



def pytest_collection_modifyitems(config, items):
    tsim_path = config.getoption("--tsim")
    if tsim_path:
        for item in items:
            if item.get_closest_marker("tsim"):
                tsim_name = f"{os.path.split(os.path.dirname(tsim_path))[-1]}_{os.path.splitext(os.path.basename(tsim_path))[0]}"
                item.name = f"{tsim_name}"  # 有效，名称可以修改
                item._nodeid = "::".join(item._nodeid.split('::')[:-1]) + f"::{tsim_name}"  # 有效，名称可以修改
                tdLog.debug(item.name)
                tdLog.debug(item._nodeid)
                params = {'params': tsim_name}  # 你的参数组合
                param_ids = [tsim_name]  # 自定义参数ID
                
                # 创建参数化标记
                marker = pytest.mark.parametrize(
                    argnames='params',
                    argvalues=[tsim_name],
                    ids=param_ids
                )
                item.add_marker(marker)
                
                # 确保Allure能识别新参数
                item.callspec = type('CallSpec', (), {'params': params, 'id': param_ids[0]})
    else:
        name_suffix = ""
        if config.getoption('-N'):
            name_suffix += f"_N{config.getoption('-N')}"
        if config.getoption('-M'):
            name_suffix += f"_M{config.getoption('-M')}"
        if config.getoption('-R'):
            name_suffix += f"_R"
        if config.getoption('-Q'):
            name_suffix += f"_Q"
        if config.getoption('-D'):
            name_suffix += f"_D{config.getoption('-D')}"
        if config.getoption('-L'):
            name_suffix += f"_L{config.getoption('-L')}"
        if config.getoption('-C'):
            name_suffix += f"_C{config.getoption('-C')}"
        if config.getoption('-I'):
            name_suffix += f"_I"
        if config.getoption('--replica'):
            name_suffix += f"_replica{config.getoption('--replica')}"

            
        for item in items:
            if name_suffix != "":
                item.name = f"{item.name}{name_suffix}"  # 有效，名称可以修改
                item._nodeid = "::".join(item._nodeid.split('::')[:-1]) + f"::{item.name}"  # 有效，名称可以修改
                tdLog.debug(item.name)
                tdLog.debug(item._nodeid)
                params = {'params': name_suffix}  # 你的参数组合
                param_ids = [name_suffix]  # 自定义参数ID
                
                # 创建参数化标记
                marker = pytest.mark.parametrize(
                    argnames='params',
                    argvalues=[name_suffix],
                    ids=param_ids
                )
                item.add_marker(marker)
                
                # 确保Allure能识别新参数
                item.callspec = type('CallSpec', (), {'params': params, 'id': param_ids[0]})


@pytest.hookimpl(tryfirst=True, hookwrapper=True)
def pytest_runtest_makereport(item, call):
    outcome = yield
    rep = outcome.get_result()
    setattr(item, "rep_" + rep.when, rep)