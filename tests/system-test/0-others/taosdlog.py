import concurrent.futures
import os
import os.path
import platform
import shutil
import subprocess
import time

from util.log import *
from util.sql import *
from util.cases import *
from util.dnodes import *

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

    def closeBin(self, binName):
        tdLog.info("Closing %s" % binName)
        if platform.system().lower() == 'windows':
            psCmd = "for /f %%a in ('wmic process where \"name='%s.exe'\" get processId ^| xargs echo ^| awk ^'{print $2}^' ^&^& echo aa') do @(ps | grep %%a | awk '{print $1}' | xargs)" % binName
        else:
            psCmd = "ps -ef | grep -w %s | grep -v grep | awk '{print $2}'" % binName
        tdLog.info(f"psCmd:{psCmd}")

        try:
            rem = subprocess.run(psCmd, shell=True, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            processID = rem.stdout.decode().strip()
            tdLog.info(f"processID:{processID}")
        except Exception as e:
            tdLog.info(f"closeBin error:{e}")
            processID = ""
        onlyKillOnceWindows = 0
        while(processID):
            if not platform.system().lower() == 'windows' or (onlyKillOnceWindows == 0 and platform.system().lower() == 'windows'):
                killCmd = "kill -9 %s > /dev/null 2>&1" % processID
                if platform.system().lower() == 'windows':
                    killCmd = "kill -9 %s > nul 2>&1" % processID
                tdLog.info(f"killCmd:{killCmd}")
                os.system(killCmd)
                tdLog.info(f"killed {binName} process {processID}")
                onlyKillOnceWindows = 1
            time.sleep(1)
            try:
                rem = subprocess.run(psCmd, shell=True, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                processID = rem.stdout.decode().strip()
            except Exception as e:
                tdLog.info(f"closeBin error:{e}")
                processID = ""

    def closeTaosd(self, signal=9):
        self.closeBin("taosd")

    def closeTaos(self, signal=9):
        self.closeBin("taos")

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

    def openTaos(self, args="", waitSec=5):
        self.openBin(f'{self.taosPath} {args}', waitSec)


    def prepare_logoutput(self, desc, port, logOutput, skipOpenBin=False):
        tdLog.info("Preparing %s, port:%s" % (desc, port))
        dnodePath = self.buildPath + "/../sim/dnode%s" % port
        tdLog.info("remove dnodePath:%s" % dnodePath)
        try:
            if os.path.exists(dnodePath):
                shutil.rmtree(dnodePath)
        except Exception as e:
            raise Exception(f"Failed to remove directory {dnodePath}: {e}")
        try:
            self.prepareCfg(dnodePath, {"serverPort": port,
                "dataDir": dnodePath + os.sep + "data",
                "logDir": dnodePath + os.sep + "log"})
        except Exception as e:
            raise Exception(f"Failed to prepare configuration for {dnodePath}: {e}")
        try:
            self.openTaosd(f"-c {dnodePath} {logOutput}")
            self.openTaos(f"-c {dnodePath} {logOutput}")
        except Exception as e:
            if(skipOpenBin):
                tdLog.info(f"Failed to prepare taosd and taos with log output {logOutput}: {e}")
            else:
                raise Exception(f"Failed to prepare taosd and taos with log output {logOutput}: {e}")


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
        # self.prepare_logoutput(list[0], list[1], "--log-output=" + list[0])

    def check_dev_null(self):
        tdLog.info("Running check /dev/null")
        # dnodePath = self.buildPath + "/../sim/dnode%s" % self.prepare_list[2][1]
        # tdSql.checkEqual(False, os.path.isfile(f"{dnodePath}/log/taosdlog.0"))
        # tdSql.checkEqual(False, os.path.isfile(f"{dnodePath}/log/taoslog0.0"))

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
        self.prepare_logoutput(list[0], list[1], "--log-output=" + list[0], True)
    def check_empty(self):
        tdLog.info("Running check empty")
        logPath = self.buildPath + "/../sim/dnode%s/log" % self.prepare_list[8][1]
        tdSql.checkEqual(False, os.path.exists(f"{logPath}/taosdlog.0"))
        tdSql.checkEqual(False, os.path.exists(f"{logPath}/taoslog.0"))

    def prepare_illegal(self):
        list = self.prepare_list[9]
        self.prepare_logoutput(list[0], list[1], "--log-output=" + list[0], True)
    def check_illegal(self):
        tdLog.info("Running check empty")
        logPath = self.buildPath + "/../sim/dnode%s/log" % self.prepare_list[9][1]
        tdSql.checkEqual(False, os.path.exists(f"{logPath}/taosdlog.0"))
        tdSql.checkEqual(False, os.path.exists(f"{logPath}/taoslog.0"))

    def prepareCheckResources(self):
        self.prepare_list = [["stdout", "10030"], ["stderr", "10031"], ["/dev/null", "10032"], ["fullpath", "10033"],
                             ["fullname", "10034"], ["relativepath", "10035"], ["relativename", "10036"], ["filename", "10037"], 
                             ["empty", "10038"], ["illeg?al", "10039"]]
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
            self.prepare_illegal: self.check_illegal,
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

    def checkLogRotate(self):
        tdLog.info("Running check log rotate")
        dnodePath = self.buildPath + "/../sim/dnode10050"
        logRotateAfterBoot = 6 # LOG_ROTATE_BOOT
        self.closeTaosd()
        self.closeTaos()
        try:
            if os.path.exists(dnodePath):
                shutil.rmtree(dnodePath)
            self.prepareCfg(dnodePath, {"serverPort": 10050,
                "dataDir": dnodePath + os.sep + "data",
                "logDir": dnodePath + os.sep + "log",
                "logKeepDays": "3" })
        except Exception as e:
             raise Exception(f"Failed to prepare configuration for {dnodePath}: {e}")

        nowSec = int(time.time())
        stubFile99 = f"{dnodePath}/log/taosdlog.99"
        stubFile101 = f"{dnodePath}/log/taosdlog.101"
        stubGzFile98 = f"{dnodePath}/log/taosdlog.98.gz"
        stubGzFile102 = f"{dnodePath}/log/taosdlog.102.gz"
        stubFileNow = f"{dnodePath}/log/taosdlog.{nowSec}"
        stubGzFileNow = f"{dnodePath}/log/taosdlog.%d.gz" % (nowSec - 1)
        stubGzFileKeep = f"{dnodePath}/log/taosdlog.%d.gz" % (nowSec - 86400 * 2)
        stubGzFileDel = f"{dnodePath}/log/taosdlog.%d.gz" % (nowSec - 86400 * 3)
        stubFiles = [stubFile99, stubFile101, stubGzFile98, stubGzFile102, stubFileNow, stubGzFileNow, stubGzFileKeep, stubGzFileDel]

        try:
            os.makedirs(f"{dnodePath}/log", exist_ok=True)
            for stubFile in stubFiles:
                with open(stubFile, "w") as f:
                    f.write("test log rotate")
        except Exception as e:
            raise Exception(f"Failed to prepare log files for {dnodePath}: {e}")
        
        tdSql.checkEqual(True, os.path.exists(stubFile101))
        tdSql.checkEqual(True, os.path.exists(stubGzFile102))
        tdSql.checkEqual(True, os.path.exists(stubFileNow))
        tdSql.checkEqual(True, os.path.exists(stubGzFileDel))

        self.openTaosd(f"-c {dnodePath}")
        self.openTaos(f"-c {dnodePath}")

        tdLog.info("wait %d seconds for log rotate" % (logRotateAfterBoot + 2))
        time.sleep(logRotateAfterBoot + 2)

        tdSql.checkEqual(True, os.path.exists(stubFile99))
        tdSql.checkEqual(False, os.path.exists(stubFile101))
        tdSql.checkEqual(False, os.path.exists(f'{stubFile101}.gz'))
        tdSql.checkEqual(True, os.path.exists(stubGzFile98))
        tdSql.checkEqual(True, os.path.exists(f'{stubFileNow}.gz'))
        tdSql.checkEqual(True, os.path.exists(stubGzFileNow))
        tdSql.checkEqual(True, os.path.exists(stubGzFileKeep))
        tdSql.checkEqual(False, os.path.exists(stubGzFile102))
        tdSql.checkEqual(False, os.path.exists(stubGzFileDel))
        tdSql.checkEqual(True, os.path.exists(f"{dnodePath}/log/taosdlog.0"))
        tdSql.checkEqual(True, os.path.exists(f"{dnodePath}/log/taoslog0.0"))

    def run(self):
        self.checkLogOutput()
        self.checkLogRotate()
        self.closeTaosd()
        self.closeTaos()

    def stop(self):
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")

tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
