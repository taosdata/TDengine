# Copyright (c) 2022 TAOS Data, Inc. <jhtao@taosdata.com>
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This program is free software: you can use, redistribute, and/or modify
# it under the terms of the GNU Affero General Public License, version 3
# or later ("AGPL"), as published by the Free Software Foundation.
#
# This program is distributed in the hope that it will be useful, but WITHOUT
# ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
# FITNESS FOR A PARTICULAR PURPOSE.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program. If not, see <http://www.gnu.org/licenses/>.

from new_test_framework.utils import tdLog, tdSql, tdCom, tdDnodes
import concurrent.futures
import os
import os.path
import platform
import subprocess
import time
import shutil
import shlex

class TestTaosdlog:

    def setup_class(cls):
        tdLog.debug(f"start to excute {__file__}")
        cls.buildPath = tdCom.getBuildPath()
        if (cls.buildPath == ""):
            tdLog.exit("taosd not found!")
        else:
            tdLog.info("taosd found in %s" % cls.buildPath)
        cls.simRoot = tdDnodes.dnodes[0].path.replace("\\", "/")
        cls.ownedProcesses = []
        suffix = ".exe" if os.name == "nt" else ""
        cls.taosdPath = cls.buildPath + "/build/bin/taosd" + suffix
        cls.taosPath = cls.buildPath + "/build/bin/taos" + suffix
        cls.logPath = tdDnodes.dnodes[0].logDir
        tdLog.info("taosd path: %s" % cls.taosdPath)
        tdLog.info("taos path: %s" % cls.taosPath)
        tdLog.info("log path: %s" % cls.logPath)
        cls.commonCfgDict = {}


    def teardown_class(cls):
        if os.name == "nt":
            cls().closeTaosd()
            cls().closeTaos()

    def checkLogBak(self, logPath, expectLogBak):
        if platform.system().lower() == 'windows':
           return True
        result = False
        try:
            for file in os.listdir(logPath):
                file_path = os.path.join(logPath, file)
                if os.path.isdir(file_path):
                    continue
                if file.endswith('.gz'):
                    if expectLogBak:
                        result = True
                    else:
                        raise Exception(f"Error: Found .gz file: {file_path}")
                if '.' in file:
                    prefix, num_part = file.split('.', 1)
                    logNum=0
                    if num_part.isdigit():
                        logNum = int(num_part)
                    if logNum > 100:
                        if not expectLogBak:
                            raise Exception(f"Error: Found log file number >= 100: {file_path}")
        except Exception as e:
            raise Exception(f"Error: error occurred. Reason: {e}")
        return result

    def checkTargetStrInFiles(self, filePaths, targetStr):
        result = False
        for filePath in filePaths:
            if not os.path.exists(filePath):
                continue
            try:
                with open(filePath, 'r', encoding='utf-8') as file:
                    for line in file:
                        if targetStr in line:
                            result = True
                            break
            except Exception as e:
                continue
        return result

    def logRotateOccurred(self, logFiles, targetStr, maxRetry=60):
        result = False
        for i in range(maxRetry):
            if self.checkTargetStrInFiles(logFiles, targetStr):
                result = True
                break
            tdLog.info(f"wait {i+1} second(s) for log rotate")
            time.sleep(1)
        return result

    def checkLogCompress(self):
        tdLog.info("Running check log compress")
        dnodePath = self.simRoot + "/dnode1"
        logPath = f"{dnodePath}/log"
        taosdLogFiles = [f"{logPath}/taosdlog.0", f"{logPath}/taosdlog.1"]
        # Debug/ASSERT_NOT_CORE builds delay startup rotation up to 30 minutes.
        tdSql.execute("alter dnode 1 'resetlog'")
        logRotateStr="process log rotation"
        logRotateResult = self.logRotateOccurred(taosdLogFiles, logRotateStr)
        tdSql.checkEqual(True, logRotateResult)
        if platform.system().lower() != 'windows':
            tdSql.checkEqual(False, self.checkLogBak(logPath, False))
        tdSql.execute("alter all dnodes 'logKeepDays 3'")
        tdSql.execute("alter all dnodes 'numOfLogLines 1000'")
        tdSql.execute("alter all dnodes 'debugFlag 143'")
        logCompress=False
        for i in range(30):
            logCompress=self.checkLogBak(logPath, True)
            if logCompress:
                break
            tdLog.info(f"wait {i+1} second(s) for log compress")
            time.sleep(1)
        tdSql.checkEqual(True, logCompress)
        tdSql.execute("alter all dnodes 'numOfLogLines 1000000'")
        tdSql.execute("alter all dnodes 'debugFlag 131'")

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
        if os.name == "nt":
            if binName == "taosd":
                tdDnodes.stopAll()
            from new_test_framework.utils.server.win_process import stop_taosd_windows
            for proc, name, cfg in self.ownedProcesses:
                if name != binName or proc.poll() is not None:
                    continue
                if name == "taosd":
                    if not stop_taosd_windows(config_dir=cfg, log=tdLog):
                        tdLog.exit(f"Failed to stop owned daemon {cfg}")
                else:
                    proc.terminate()
                proc.wait(timeout=30)
            return

        psCmd = "ps -ef | grep -w %s | grep -v grep | awk '{print $2}'" % binName
        tdLog.info(f"psCmd:{psCmd}")
        try:
            rem = subprocess.run(psCmd, shell=True, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            processID = rem.stdout.decode().strip()
            tdLog.info(f"processID:{processID}")
        except Exception as e:
            tdLog.info(f"closeBin error:{e}")
            processID = ""
        while(processID):
            killCmd = "kill -9 %s > /dev/null 2>&1" % processID
            tdLog.info(f"killCmd:{killCmd}")
            os.system(killCmd)
            tdLog.info(f"killed {binName} process {processID}")
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
        if os.name == "nt":
            from new_test_framework.utils.server.win_process import start_taosd_windows
            arguments = shlex.split(args)
            cfg = arguments[arguments.index("-c") + 1]
            proc = start_taosd_windows(self.taosdPath, cfg, log=tdLog)
            self.ownedProcesses.append((proc, "taosd", cfg))
            time.sleep(waitSec)
            if proc.poll() is not None:
                tdLog.exit(f"Owned daemon exited: {proc.returncode}")
            return
        self.openBin(f'{self.taosdPath} {args}', waitSec)

    def openTaos(self, args="", waitSec=5):
        if os.name == "nt":
            arguments = shlex.split(args)
            cfg = arguments[arguments.index("-c") + 1]
            result = subprocess.run([self.taosPath, *arguments, "-s", "select server_version()"],
                                    capture_output=True, encoding="utf-8", errors="replace", timeout=30)
            if result.returncode or "DB error:" in result.stdout + result.stderr:
                tdLog.exit(f"CLI log probe failed: {result.stdout} {result.stderr}")
            return
        self.openBin(f'{self.taosPath} {args}', waitSec)


    def prepare_logoutput(self, desc, port, logOutput, skipOpenBin=False):
        tdLog.info("Preparing %s, port:%s" % (desc, port))
        dnodePath = self.simRoot + "/dnode%s" % port
        tdLog.info("remove dnodePath:%s" % dnodePath)
        try:
            if os.path.exists(dnodePath):
                shutil.rmtree(dnodePath)
        except Exception as e:
            raise Exception(f"Failed to remove directory {dnodePath}: {e}")
        try:
            self.prepareCfg(dnodePath, {"serverPort": port,
                "dataDir": dnodePath + "/data",
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
        dnodePath = self.simRoot + "/dnode%s" % self.prepare_list[0][1]
        tdSql.checkEqual(False, os.path.isfile(f"{dnodePath}/log/taosdlog.0"))
        tdSql.checkEqual(False, os.path.isfile(f"{dnodePath}/log/taoslog0.0"))

    def prepare_stderr(self):
        list = self.prepare_list[1]
        self.prepare_logoutput(list[0], list[1], "--log-output " + list[0])

    def check_stderr(self):
        tdLog.info("Running check stderr")
        dnodePath = self.simRoot + "/dnode%s" % self.prepare_list[1][1]
        tdSql.checkEqual(False, os.path.isfile(f"{dnodePath}/log/taosdlog.0"))
        tdSql.checkEqual(False, os.path.isfile(f"{dnodePath}/log/taoslog0.0"))

    def prepare_dev_null(self):
        list = self.prepare_list[2]
        # self.prepare_logoutput(list[0], list[1], "--log-output=" + list[0])

    def check_dev_null(self):
        tdLog.info("Running check /dev/null")
        # dnodePath = self.simRoot + "/dnode%s" % self.prepare_list[2][1]
        # tdSql.checkEqual(False, os.path.isfile(f"{dnodePath}/log/taosdlog.0"))
        # tdSql.checkEqual(False, os.path.isfile(f"{dnodePath}/log/taoslog0.0"))

    def prepare_fullpath(self):
        list = self.prepare_list[3]
        dnodePath = self.simRoot + "/dnode%s" % self.prepare_list[3][1]
        self.prepare_logoutput(list[0], list[1], "-o " + dnodePath + "/log0/" )

    def check_fullpath(self):
        tdLog.info("Running check fullpath")
        logPath = self.simRoot + "/dnode%s/log0/" % self.prepare_list[3][1]
        tdSql.checkEqual(True, os.path.exists(f"{logPath}taosdlog.0"))
        tdSql.checkEqual(True, os.path.exists(f"{logPath}taoslog0.0"))

    def prepare_fullname(self):
        list = self.prepare_list[4]
        dnodePath = self.simRoot + "/dnode%s" % self.prepare_list[4][1]
        self.prepare_logoutput(list[0], list[1], "--log-output " + dnodePath + "/log0/" + list[0])
        dnodePath = self.simRoot + "/dnode%s" % self.prepare_list[4][1]

    def check_fullname(self):
        tdLog.info("Running check fullname")
        logPath = self.simRoot + "/dnode%s/log0/" % self.prepare_list[4][1]
        tdSql.checkEqual(True, os.path.exists(logPath + self.prepare_list[4][0] + ".0"))
        tdSql.checkEqual(True, os.path.exists(logPath + self.prepare_list[4][0] + "0.0"))

    def prepare_relativepath(self):
        list = self.prepare_list[5]
        self.prepare_logoutput(list[0], list[1], "--log-output=" + "log0/")

    def check_relativepath(self):
        tdLog.info("Running check relativepath")
        logPath = self.simRoot + "/dnode%s/log/log0/" % self.prepare_list[5][1]
        tdSql.checkEqual(True, os.path.exists(logPath + "taosdlog.0"))
        tdSql.checkEqual(True, os.path.exists(logPath + "taoslog0.0"))

    def prepare_relativename(self):
        list = self.prepare_list[6]
        self.prepare_logoutput(list[0], list[1], "-o " + "log0/" + list[0])
    def check_relativename(self):
        tdLog.info("Running check relativename")
        logPath = self.simRoot + "/dnode%s/log/log0/" % self.prepare_list[6][1]
        tdSql.checkEqual(True, os.path.exists(logPath + self.prepare_list[6][0] + ".0"))
        tdSql.checkEqual(True, os.path.exists(logPath + self.prepare_list[6][0] + "0.0"))

    def prepare_filename(self):
        list = self.prepare_list[7]
        self.prepare_logoutput(list[0], list[1], "--log-output " + list[0])
    def check_filename(self):
        tdLog.info("Running check filename")
        logPath = self.simRoot + "/dnode%s/log/" % self.prepare_list[7][1]
        tdSql.checkEqual(True, os.path.exists(logPath + self.prepare_list[7][0] + ".0"))
        tdSql.checkEqual(True, os.path.exists(logPath + self.prepare_list[7][0] + "0.0"))

    def prepare_empty(self):
        list = self.prepare_list[8]
        self.prepare_logoutput(list[0], list[1], "--log-output=" + list[0], True)
    def check_empty(self):
        tdLog.info("Running check empty")
        logPath = self.simRoot + "/dnode%s/log" % self.prepare_list[8][1]
        tdSql.checkEqual(False, os.path.exists(f"{logPath}/taosdlog.0"))
        tdSql.checkEqual(False, os.path.exists(f"{logPath}/taoslog.0"))

    def prepare_illegal(self):
        list = self.prepare_list[9]
        self.prepare_logoutput(list[0], list[1], "--log-output=" + list[0], True)
    def check_illegal(self):
        tdLog.info("Running check illegal")
        logPath = self.simRoot + "/dnode%s/log" % self.prepare_list[9][1]
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
                    raise Exception(f"Error in check function: {e}")

    def checkLogRotate(self):
        tdLog.info("Running check log rotate")
        dnodePath = self.simRoot + "/dnode10050"
        logRotateAfterBoot = 60 # Reset triggers old-file close and rotation after 20s.
        self.closeTaosd()
        self.closeTaos()
        try:
            if os.path.exists(dnodePath):
                shutil.rmtree(dnodePath)
            self.prepareCfg(dnodePath, {"serverPort": 10050, "firstEp": "localhost:10050", "fqdn": "localhost",
                "dataDir": dnodePath + "/data",
                "logDir": dnodePath + "/log",
                "logKeepDays": "-3" })
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
        if os.name == "nt":
            # Reset before the inactivity threshold would skip the new stubs;
            # the next automatic scan can be 30 minutes away in Debug builds.
            time.sleep(32)
            result = subprocess.run([self.taosPath, "-c", dnodePath, "-s", "alter dnode 1 'resetlog'"],
                                    capture_output=True, encoding="utf-8", errors="replace", timeout=30)
            if result.returncode or "DB error:" in result.stdout + result.stderr:
                tdLog.exit(f"Cannot trigger rotation: {result.stdout} {result.stderr}")

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

    def test_taosdlog(self):
        """Taosd log
        
        1. Check log compress
        2. Check log output
        3. Check log rotate
        4. Close taosd and taos

        
        Since: v3.0.0.0

        Labels: common,ci,integration,functional

        Jira: None

        History:
            - 2025-12-02 Alex Duan Migrated from uncatalog/system-test/0-others/test_taosdlog.py

        """
        tdSql.prepare()
        self.checkLogCompress()
        # --log-output options are not supported on Windows
        if platform.system().lower() != 'windows':
            self.checkLogOutput()
        self.checkLogRotate()
        self.closeTaosd()
        self.closeTaos()



