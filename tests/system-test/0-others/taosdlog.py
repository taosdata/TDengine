import taos
import sys
import time
import os
import os.path
import platform
import concurrent.futures
import shutil  # Add this import

from util.log import *
from util.sql import *
from util.cases import *
from util.dnodes import *
import time
import socket
import subprocess

class TDTestCase:

    def init(self, conn, logSql, replicaVar=1):
        self.replicaVar = int(replicaVar)
        tdLog.debug(f"start to excute {__file__}")
        tdSql.init(conn.cursor())
        tdSql.prepare()
        self.buildPath = self.getBuildPath()
        if (self.buildPath == ""):
            tdLog.exit("taosd not found!")
        else:
            tdLog.info("taosd found in %s" % self.buildPath)
        self.taosdPath = self.buildPath + "/build/bin/taosd"
        self.taosPath = self.buildPath + "/build/bin/taos"
        self.logPath = self.buildPath + "/../sim/dnode1/log"
        tdLog.info("taosd path: %s" % self.taosdPath)
        tdLog.info("taos path: %s" % self.taosPath)
        tdLog.info("log path: %s" % self.logPath)
        self.commonCfgDict = {}

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

    def prepareCfg(self, cfgPath, cfgDict):
        tdLog.info("make dir %s" % cfgPath)
        os.makedirs(cfgPath, exist_ok=True)
        with open(cfgPath + "/taos.cfg", "w") as f:
            for key in self.commonCfgDict:
                f.write("%s %s\n" % (key, self.commonCfgDict[key]))
            for key in cfgDict:
                f.write("%s %s\n" % (key, cfgDict[key]))
        if not os.path.exists(cfgPath + "/taos.cfg"):
            tdLog.exit("taos.cfg not found in %s" % cfgPath)
        else:
            tdLog.info("taos.cfg found in %s" % cfgPath)

    def checkProcessPid(self,processName):
        i=0
        while i<60:
            tdLog.info(f"wait stop {processName}")
            processPid = subprocess.getstatusoutput(f'ps aux|grep {processName} |grep -v "grep"|awk \'{{print $2}}\'')[1]
            tdLog.info(f"times:{i},{processName}-pid:{processPid}")
            if(processPid == ""):
                break
            i += 1
            time.sleep(1)
        else:
            tdLog.info(f'this processName is not stoped in 60s')

    def closeTaosd(self, signal=9):
        tdLog.info("Closing taosd")
        if platform.system().lower() == 'windows':
            psCmd = "for /f %%a in ('wmic process where \"name='taosd.exe' and CommandLine like '%%dnode%d%%'\" get processId ^| xargs echo ^| awk ^'{print $2}^' ^&^& echo aa') do @(ps | grep %%a | awk '{print $1}' | xargs)" % (self.index)
        else:
            psCmd = "ps -ef | grep -w taosd | grep -v grep | awk '{print $2}' | xargs"
        tdLog.info(f"psCmd:{psCmd}")

        try:
            rem = subprocess.run(psCmd, shell=True, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            processID = rem.stdout.decode().strip()
            tdLog.info(f"processID:{processID}")
        except Exception as e:
            tdLog.info(f"closeTaosd error:{e}")
            processID = ""
        tdLog.info(f"processID:{processID}")
        onlyKillOnceWindows = 0
        while(processID):
            if not platform.system().lower() == 'windows' or (onlyKillOnceWindows == 0 and platform.system().lower() == 'windows'):
                killCmd = "kill -%d %s > /dev/null 2>&1" % (signal, processID)
                if platform.system().lower() == 'windows':
                    killCmd = "kill -%d %s > nul 2>&1" % (signal, processID)
                tdLog.info(f"killCmd:{killCmd}")
                os.system(killCmd)
                tdLog.info(f"killed taosd process {processID}")
                onlyKillOnceWindows = 1
            time.sleep(1)
            try:
                rem = subprocess.run(psCmd, shell=True, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                processID = rem.stdout.decode().strip()
            except Exception as e:
                tdLog.info(f"closeTaosd error:{e}")
                processID = ""

    def closeTaos(self, signal=9):
        tdLog.info("Closing taos")
        if platform.system().lower() == 'windows':
            psCmd = "for /f %%a in ('wmic process where \"name='taos.exe'\" get processId ^| xargs echo ^| awk ^'{print $2}^' ^&^& echo aa') do @(ps | grep %%a | awk '{print $1}' | xargs)"
        else:
            psCmd = "ps -ef | grep -w taos | grep -v grep | awk '{print $2}'"
        tdLog.info(f"psCmd:{psCmd}")

        try:
            rem = subprocess.run(psCmd, shell=True, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            processID = rem.stdout.decode().strip()
            tdLog.info(f"processID:{processID}")
        except Exception as e:
            tdLog.info(f"closeTaos error:{e}")
            processID = ""
        tdLog.info(f"processID:{processID}")
        onlyKillOnceWindows = 0
        while(processID):
            if not platform.system().lower() == 'windows' or (onlyKillOnceWindows == 0 and platform.system().lower() == 'windows'):
                killCmd = "kill -%d %s > /dev/null 2>&1" % (signal, processID)
                if platform.system().lower() == 'windows':
                    killCmd = "kill -%d %s > nul 2>&1" % (signal, processID)
                tdLog.info(f"killCmd:{killCmd}")
                os.system(killCmd)
                tdLog.info(f"killed taos process {processID}")
                onlyKillOnceWindows = 1
            time.sleep(1)
            try:
                rem = subprocess.run(psCmd, shell=True, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                processID = rem.stdout.decode().strip()
            except Exception as e:
                tdLog.info(f"closeTaos error:{e}")
                processID = ""

    def openBin(self, binPath, waitSec=5):
        tdLog.info(f"Opening {binPath}")
        try:
            process = subprocess.Popen(binPath, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            time.sleep(waitSec)
            if process.poll() is None:
                tdLog.info(f"{binPath} started successfully")
            else:
                error = process.stderr.read().decode(encoding="utf-8").strip()
                raise Exception(f"Failed to start {binPath}: {error}")
        except Exception as e:
            raise Exception(f"Failed to start {binPath}: %s" % repr(e))

    def openTaosd(self, args="", waitSec=8):
        self.openBin(f'{self.taosdPath} {args}', waitSec)

    def openTaos(self, args="", waitSec=3):
        self.openBin(f'{self.taosPath} {args}', waitSec)


    def prepare_logoutput(self, desc, port, logOutput):
        tdLog.info("Preparing %s, port:%s" % (desc, port))
        dnodePath = self.buildPath + "/../sim/dnode%s" % port
        tdLog.info("remove dnodePath:%s" % dnodePath)
        try:
            shutil.rmtree(dnodePath)
        except Exception as e:
            tdLog.info(f"Failed to remove directory {dnodePath}: {e}")
        try:
            self.prepareCfg(dnodePath, {"serverPort": port,
                "dataDir": dnodePath + os.sep + "data",
                "logDir": dnodePath + os.sep + "log"})
        except Exception as e:
            tdLog.info(f"Failed to prepare configuration for {dnodePath}: {e}")
            return
        try:
            self.openTaosd(f"-c {dnodePath} {logOutput}")
            self.openTaos(f"-c {dnodePath} {logOutput}")
        except Exception as e:
            tdLog.info(f"Failed to open taosd or taos for {dnodePath}: {e}")

    def prepare_stdout(self):
        list = self.prepare_list[0]
        self.prepare_logoutput(list[0], list[1], "-o " + list[0])

    def check_stdout(self):
        tdLog.info("Running check stdout")
        dnodePath = self.buildPath + "/../sim/dnode%s" % self.prepare_list[0][1]
        tdSql.checkEqual(False, os.path.isfile(f"{dnodePath}/log/taosdlog.0"))
        tdSql.checkEqual(False, os.path.isfile(f"{dnodePath}/log/taoslog0.0"))

    def prepare_stderr(self):
        list = self.prepare_list[1]
        self.prepare_logoutput(list[0], list[1], "--log-output " + list[0])

    def check_stderr(self):
        tdLog.info("Running check stderr")
        dnodePath = self.buildPath + "/../sim/dnode%s" % self.prepare_list[1][1]
        tdSql.checkEqual(False, os.path.isfile(f"{dnodePath}/log/taosdlog.0"))
        tdSql.checkEqual(False, os.path.isfile(f"{dnodePath}/log/taoslog0.0"))

    def prepare_dev_null(self):
        list = self.prepare_list[2]
        self.prepare_logoutput(list[0], list[1], "--log-output=" + list[0])

    def check_dev_null(self):
        tdLog.info("Running check /dev/null")
        dnodePath = self.buildPath + "/../sim/dnode%s" % self.prepare_list[2][1]
        tdSql.checkEqual(False, os.path.isfile(f"{dnodePath}/log/taosdlog.0"))
        tdSql.checkEqual(False, os.path.isfile(f"{dnodePath}/log/taoslog0.0"))
    
    def prepare_fullpath(self):
        list = self.prepare_list[3]
        dnodePath = self.buildPath + "/../sim/dnode%s" % self.prepare_list[3][1]
        self.prepare_logoutput(list[0], list[1], "-o " + dnodePath + "/log0/" )
    
    def check_fullpath(self):
        tdLog.info("Running check fullpath")
        logPath = self.buildPath + "/../sim/dnode%s/log0/" % self.prepare_list[3][1]
        tdSql.checkEqual(True, os.path.exists(f"{logPath}taosdlog.0"))
        tdSql.checkEqual(True, os.path.exists(f"{logPath}taoslog0.0"))
    
    def prepare_fullname(self):
        list = self.prepare_list[4]
        dnodePath = self.buildPath + "/../sim/dnode%s" % self.prepare_list[4][1]
        self.prepare_logoutput(list[0], list[1], "--log-output " + dnodePath + "/log0/" + list[0])
        dnodePath = self.buildPath + "/../sim/dnode%s" % self.prepare_list[4][1]

    def check_fullname(self):
        tdLog.info("Running check fullname")
        logPath = self.buildPath + "/../sim/dnode%s/log0/" % self.prepare_list[4][1]
        tdSql.checkEqual(True, os.path.exists(logPath + self.prepare_list[4][0] + ".0"))
        tdSql.checkEqual(True, os.path.exists(logPath + self.prepare_list[4][0] + "0.0"))
    
    def prepare_relativepath(self):
        list = self.prepare_list[5]
        self.prepare_logoutput(list[0], list[1], "--log-output=" + "log0/")
    
    def check_relativepath(self):
        tdLog.info("Running check relativepath")
        logPath = self.buildPath + "/../sim/dnode%s/log/log0/" % self.prepare_list[5][1]
        tdSql.checkEqual(True, os.path.exists(logPath + "taosdlog.0"))
        tdSql.checkEqual(True, os.path.exists(logPath + "taoslog0.0"))
    
    def prepare_relativename(self):
        list = self.prepare_list[6]
        self.prepare_logoutput(list[0], list[1], "-o " + "log0/" + list[0])
    def check_relativename(self):
        tdLog.info("Running check relativename")
        logPath = self.buildPath + "/../sim/dnode%s/log/log0/" % self.prepare_list[6][1]
        tdSql.checkEqual(True, os.path.exists(logPath + self.prepare_list[6][0] + ".0"))
        tdSql.checkEqual(True, os.path.exists(logPath + self.prepare_list[6][0] + "0.0"))
    
    def prepare_filename(self):
        list = self.prepare_list[7]
        self.prepare_logoutput(list[0], list[1], "--log-output " + list[0])
    def check_filename(self):
        tdLog.info("Running check filename")
        logPath = self.buildPath + "/../sim/dnode%s/log/" % self.prepare_list[7][1]
        tdSql.checkEqual(True, os.path.exists(logPath + self.prepare_list[7][0] + ".0"))
        tdSql.checkEqual(True, os.path.exists(logPath + self.prepare_list[7][0] + "0.0"))
    
    def prepare_empty(self):
        list = self.prepare_list[8]
        self.prepare_logoutput(list[0], list[1], "--log-output=" + list[0])
    def check_empty(self):
        tdLog.info("Running check empty")
        logPath = self.buildPath + "/../sim/dnode%s/log" % self.prepare_list[8][1]
        tdSql.checkEqual(False, os.path.exists(f"{logPath}/taosdlog.0"))
        tdSql.checkEqual(False, os.path.exists(f"{logPath}/taoslog.0"))

    def prepareCheckResources(self):
        self.prepare_list = [["stdout", "10030"], ["stderr", "10031"], ["/dev/null", "10032"], 
                             ["fullpath", "10033"], ["fullname", "10034"], ["relativepath", "10035"], 
                             ["relativename", "10036"], ["filename", "10037"], ["empty", "10038"]]
        self.check_functions = {
            self.prepare_stdout: self.check_stdout,
            self.prepare_stderr: self.check_stderr,
            self.prepare_dev_null: self.check_dev_null,
            self.prepare_fullpath: self.check_fullpath,
            self.prepare_fullname: self.check_fullname,
            self.prepare_relativepath: self.check_relativepath,
            self.prepare_relativename: self.check_relativename,
            self.prepare_filename: self.check_filename,
            self.prepare_empty: self.check_empty,
        }

    def checkLogOutput(self):
        self.closeTaosd()
        self.closeTaos()
        self.prepareCheckResources()
        with concurrent.futures.ThreadPoolExecutor() as executor:
            prepare_futures = [executor.submit(prepare_func) for prepare_func, _ in self.check_functions.items()]
            for future in concurrent.futures.as_completed(prepare_futures):
                try:
                    future.result()
                except Exception as e:
                    raise Exception(f"Error in prepare function: {e}")
            
            check_futures = [executor.submit(check_func) for _, check_func in self.check_functions.items()]
            for future in concurrent.futures.as_completed(check_futures):
                try:
                    future.result()
                except Exception as e:
                    raise Exception(f"Error in prepare function: {e}")
        self.closeTaosd()
        self.closeTaos()

    def is_windows(self):
        return os.name == 'nt'

    def run(self):
        self.checkLogOutput()

    def stop(self):
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")

tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
