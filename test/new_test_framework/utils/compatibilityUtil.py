# Copyright (c) 2016 TAOS Data, Inc. <jhtao@taosdata.com>
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

# -*- coding: utf-8 -*-
import taos
import taosws
import sys
import os
import re
import time
import platform
import subprocess
import psutil
import shutil

import importlib
from pathlib import Path
from .compatibility_package import prepare_oss_package
from .log import *
from .sql import *
from .server.dnodes import *
from .common import *
from taos.tmq import Consumer
from new_test_framework.utils import clusterComCheck
from taos.error import TmqError

# Load enterprise package downloader from taos-internal
_current_dir = os.path.dirname(os.path.realpath(__file__))
_enterprise_downloader_path = os.path.abspath(
    os.path.join(_current_dir, "../../../../taos-internal/utils/download_enterprise_package.py")
)
if os.path.exists(_enterprise_downloader_path):
    _spec = importlib.util.spec_from_file_location("download_enterprise_package", _enterprise_downloader_path)
    _mod = importlib.util.module_from_spec(_spec)
    _spec.loader.exec_module(_mod)
    _downloader = _mod.EnterprisePackageDownloader()
else:
    _downloader = None



deletedDataSql = '''drop database if exists deldata;create database deldata duration 100 stt_trigger 1; ;use deldata;
                            create table deldata.stb1 (ts timestamp, c1 int, c2 bigint, c3 smallint, c4 tinyint, c5 float, c6 double, c7 bool, c8 binary(16),c9 nchar(32), c10 timestamp) tags (t1 int);
                            create table deldata.ct1 using deldata.stb1 tags ( 1 );
                            insert into deldata.ct1 values ( now()-0s, 0, 0, 0, 0, 0.0, 0.0, 0, 'binary0', 'nchar0', now()+0a ) ( now()-10s, 1, 11111, 111, 11, 1.11, 11.11, 1, 'binary1', 'nchar1', now()+1a ) ( now()-20s, 2, 22222, 222, 22, 2.22, 22.22, 0, 'binary2', 'nchar2', now()+2a ) ( now()-30s, 3, 33333, 333, 33, 3.33, 33.33, 1, 'binary3', 'nchar3', now()+3a );
                            select avg(c1) from deldata.ct1;
                            delete from deldata.stb1;
                            flush database deldata;
                            insert into deldata.ct1 values ( now()-0s, 0, 0, 0, 0, 0.0, 0.0, 0, 'binary0', 'nchar0', now()+0a ) ( now()-10s, 1, 11111, 111, 11, 1.11, 11.11, 1, 'binary1', 'nchar1', now()+1a ) ( now()-20s, 2, 22222, 222, 22, 2.22, 22.22, 0, 'binary2', 'nchar2', now()+2a ) ( now()-30s, 3, 33333, 333, 33, 3.33, 33.33, 1, 'binary3', 'nchar3', now()+3a );
                            delete from deldata.ct1;
                            insert into deldata.ct1 values ( now()-0s, 0, 0, 0, 0, 0.0, 0.0, 0, 'binary0', 'nchar0', now()+0a );
                            flush database deldata;'''

tableNumbers=100
recordNumbers1=1000
recordNumbers2=1000
first_consumer_rows=0

topic_select_sql = "select current,voltage,phase from test.meters where voltage >= 10;"
select_topic = "select_test_meters_topic"
db_topic = "db_test_topic"
stable_topic = "stable_test_meters_topic"
dbname = "test"
stb = f"{dbname}.meters"

class CompatibilityBase:

    def _getProcessPids(self, processNameOrPid):
        target = str(processNameOrPid).strip()
        if not target:
            return []

        if target.isdigit():
            result = subprocess.run(
                ["ps", "-p", target, "-o", "pid="],
                capture_output=True,
                text=True,
                check=False,
            )
            return [line.strip() for line in result.stdout.splitlines()
                    if line.strip()]

        result = subprocess.run(
            ["ps", "-eo", "pid=,comm="],
            capture_output=True,
            text=True,
            check=False,
        )
        pids = []
        for line in result.stdout.splitlines():
            parts = line.strip().split(None, 1)
            if len(parts) != 2:
                continue
            pid, command = parts
            if command == target:
                pids.append(pid)
        return pids

    def checkProcessPid(self,processName):
        tdLog.info(f"checkProcessPid {processName}")
        i=0
        while i<60:
            tdLog.info(f"wait stop {processName}")
            processPids = self._getProcessPids(processName)
            processPid = " ".join(processPids)
            tdLog.info(f"times:{i},{processName}-pid:{processPid}")
            if(processPid == ""):
                break
            i += 1
            time.sleep(1)
        else:
            tdLog.error(f'{processName} is not stopped in 60s')

    def version_compare(self, version1, version2):
        """
        Compare two version strings.
        Returns 1 if version1 > version2, -1 if version1 < version2, 0 if equal
        """
        v1_parts = [int(x) for x in version1.split('.')]
        v2_parts = [int(x) for x in version2.split('.')]
        
        # Pad shorter version with zeros
        max_len = max(len(v1_parts), len(v2_parts))
        v1_parts.extend([0] * (max_len - len(v1_parts)))
        v2_parts.extend([0] * (max_len - len(v2_parts)))
        
        for v1, v2 in zip(v1_parts, v2_parts):
            if v1 > v2:
                return 1
            elif v1 < v2:
                return -1
        return 0

    # Modified installTaosd to accept version parameter
    def installTaosdForRollingUpgrade(self, dnodePaths, base_version):
        tdLog.info(f"Downloading and extracting enterprise version {base_version} (no install)")
        try:
            bin_dir, lib_dir = _downloader.download_and_extract(base_version, "enterprise")
        except Exception as e:
            tdLog.error(f"Failed to download enterprise version {base_version}: {e}")
            return False
        self.old_bin_dir = bin_dir
        self.old_lib_dir = lib_dir
        extract_dir = os.path.dirname(bin_dir)
        tdLog.info(f"Extracted: bin={bin_dir}, lib={lib_dir}")

        if os.name == "nt":
            return self._start_windows_rolling_baseline(dnodePaths)

        for dnodePath in dnodePaths:
            tdLog.info(f"start taosd: rm -rf {dnodePath}data/* && LD_LIBRARY_PATH={self.old_lib_dir} {self.old_bin_dir}/taosd -c {dnodePath}cfg/ > /dev/null 2>&1 &")
            os.system(f"rm -rf {dnodePath}data/* && LD_LIBRARY_PATH={self.old_lib_dir} {self.old_bin_dir}/taosd -c {dnodePath}cfg/ > /dev/null 2>&1 &")
            os.system(f"killall taosadapter")
            taosadapter_toml_src = os.path.join(extract_dir, "cfg", "taosadapter.toml")
            if os.path.exists(taosadapter_toml_src):
                os.system(f"cp {taosadapter_toml_src} {dnodePath}cfg/taosadapter.toml")
                taosadapter_cfg = dnodePath + "cfg/taosadapter.toml"
                taosadapter_log_path = dnodePath + "log/"
                tdLog.info(f"taosadapter_cfg:{taosadapter_cfg}, taosadapter_log_path:{taosadapter_log_path}")
                self.alter_string_in_file(taosadapter_cfg,"#path = \"/var/log/taos\"",f"path = \"{taosadapter_log_path}\"")
                self.alter_string_in_file(taosadapter_cfg,"taosConfigDir = \"\"",f"taosConfigDir = \"{dnodePath}cfg/\"")
                tdLog.info(f"{self.old_bin_dir}/taosadapter --version")
                os.system(f"{self.old_bin_dir}/taosadapter --version")
                tdLog.info(f"LD_LIBRARY_PATH={self.old_lib_dir} {self.old_bin_dir}/taosadapter -c {taosadapter_cfg} > /dev/null 2>&1 &")
                os.system(f"LD_LIBRARY_PATH={self.old_lib_dir} {self.old_bin_dir}/taosadapter -c {taosadapter_cfg} > /dev/null 2>&1 &")
            else:
                tdLog.info(f"taosadapter.toml not found at {taosadapter_toml_src}, skipping taosadapter startup")
            time.sleep(5)
        
        return True

    def _start_windows_rolling_baseline(self, dnode_paths):
        if not dnode_paths:
            raise ValueError("No dnode paths provided for rolling baseline")
        env = os.environ.copy()
        env.pop("LD_PRELOAD", None)
        env.pop("LD_LIBRARY_PATH", None)
        env["PATH"] = self.old_bin_dir + os.pathsep + env.get("PATH", "")
        processes = []
        try:
            for directory in dnode_paths:
                node = Path(directory)
                cfg = node / "cfg"
                if not (cfg / "taos.cfg").is_file():
                    raise FileNotFoundError(f"Missing baseline configuration: {cfg}")
                # Only this node's data is reset; no global process-name kills.
                data = node / "data"
                if data.exists():
                    shutil.rmtree(data)
                data.mkdir()
                (node / "log").mkdir(exist_ok=True)
                with (node / "log" / "baseline-startup.log").open("ab") as output:
                    process = subprocess.Popen(
                        [os.path.join(self.old_bin_dir, "taosd.exe"), "-c", str(cfg)],
                        env=env, stdout=output, stderr=subprocess.STDOUT,
                        creationflags=subprocess.CREATE_NEW_CONSOLE,
                    )
                processes.append(process)
                tdLog.info(f"Started Windows baseline pid={process.pid}, cfg={cfg}")
            self._wait_old_server_ready(str(Path(dnode_paths[0]) / "cfg"))
            for process in processes:
                if process.poll() is not None:
                    raise RuntimeError(f"Baseline taosd pid={process.pid} exited: {process.returncode}")
        except BaseException:
            for process in processes:
                if process.poll() is None:
                    process.kill()
                process.wait(timeout=60)
            raise
        return True

    def _wait_old_server_ready(self, cPath, timeout=120):
        """Wait for the extracted server before starting compatibility data setup."""
        env = os.environ.copy()
        env["LD_LIBRARY_PATH"] = self.old_lib_dir
        program = "taos"
        if os.name == "nt":
            program += ".exe"
            env.pop("LD_PRELOAD", None)
            env.pop("LD_LIBRARY_PATH", None)
            env["PATH"] = self.old_bin_dir + os.pathsep + env.get("PATH", "")
        deadline = time.monotonic() + timeout
        last_output = ""
        while time.monotonic() < deadline:
            try:
                result = subprocess.run(
                    [os.path.join(self.old_bin_dir, program), "-c", cPath,
                     "-s", "show dnodes"],
                    env=env, capture_output=True, text=True,
                    timeout=min(10, max(0.1, deadline - time.monotonic())),
                )
                last_output = result.stdout + result.stderr
                if result.returncode == 0 and re.search(
                    r"\|\s*ready\s*\|", result.stdout, re.IGNORECASE
                ):
                    tdLog.info("Old-version dnode is ready")
                    return
            except subprocess.TimeoutExpired as exc:
                last_output = str(exc)
            time.sleep(min(1, max(0, deadline - time.monotonic())))
        raise RuntimeError(
            f"Old-version dnode did not become ready in {timeout}s: {last_output}"
        )

    def installTaosd(self, bPath, cPath, base_version):
        packagePath = "/usr/local/src/"
        dataPath = cPath + "/../data/"
        packageType = "server"

        # Check if version is 3.3.7.0 or later
        if self.version_compare(base_version, "3.3.7.0") >= 0:
            # Use new download URL format for 3.3.7.0 and later versions
            if platform.system() == "Linux" and platform.machine() == "aarch64":
                packageName = f"tdengine-tsdb-oss-{base_version}-linux-arm64.tar.gz"
                download_url = f"https://downloads.taosdata.com/tdengine-tsdb-oss/{base_version}/{packageName}"
            else:
                packageName = f"tdengine-tsdb-oss-{base_version}-linux-x64.tar.gz"
                download_url = f"https://downloads.taosdata.com/tdengine-tsdb-oss/{base_version}/{packageName}"
        else:
            # Use old download URL format for versions before 3.3.7.0
            if platform.system() == "Linux" and platform.machine() == "aarch64":
                packageName = "TDengine-"+ packageType + "-" + base_version + "-Linux-arm64.tar.gz"
            else:
                packageName = "TDengine-"+ packageType + "-" + base_version + "-Linux-x64.tar.gz"
                
            # Determine download URL
            download_url = f"https://www.taosdata.com/assets-download/3.0/{packageName}"
            
        self.old_bin_dir, self.old_lib_dir = prepare_oss_package(
            packagePath, packageName, download_url
        )
        extract_dir = os.path.dirname(self.old_bin_dir)

        os.system(f"pkill -9 taosd")
        self.checkProcessPid("taosd")

        tdLog.info(f"start taosd: rm -rf {dataPath}/* && LD_LIBRARY_PATH={self.old_lib_dir} {self.old_bin_dir}/taosd -c {cPath} > /dev/null 2>&1 &")
        os.system(f"rm -rf {dataPath}/* && LD_LIBRARY_PATH={self.old_lib_dir} {self.old_bin_dir}/taosd -c {cPath} > /dev/null 2>&1 &")
        os.system(f"killall taosadapter")
        self.checkProcessPid("taosadapter")

        taosadapter_toml_src = os.path.join(extract_dir, "cfg", "taosadapter.toml")
        if os.path.exists(taosadapter_toml_src):
            os.system(f"cp {taosadapter_toml_src} {cPath}/taosadapter.toml")
            taosadapter_cfg = cPath + "/taosadapter.toml"
            taosadapter_log_path = cPath + "/../log/"
            tdLog.info(f"taosadapter_cfg:{taosadapter_cfg}, taosadapter_log_path:{taosadapter_log_path}")
            self.alter_string_in_file(taosadapter_cfg,"#path = \"/var/log/taos\"",f"path = \"{taosadapter_log_path}\"")
            self.alter_string_in_file(taosadapter_cfg,"taosConfigDir = \"\"",f"taosConfigDir = \"{cPath}\"")
            tdLog.info(f"{self.old_bin_dir}/taosadapter --version")
            os.system(f"{self.old_bin_dir}/taosadapter --version")
            tdLog.info(f"LD_LIBRARY_PATH={self.old_lib_dir} {self.old_bin_dir}/taosadapter -c {taosadapter_cfg} > /dev/null 2>&1 &")
            os.system(f"LD_LIBRARY_PATH={self.old_lib_dir} {self.old_bin_dir}/taosadapter -c {taosadapter_cfg} > /dev/null 2>&1 &")
        else:
            tdLog.info(f"taosadapter.toml not found at {taosadapter_toml_src}, skipping taosadapter startup")
        self._wait_old_server_ready(cPath)


    def buildTaosd(self,bPath):
        os.system(f"cd {bPath}")

    def is_list_same_as_ordered_list(self,unordered_list, ordered_list):
        sorted_list = sorted(unordered_list)
        return sorted_list == ordered_list

    def alter_string_in_file(self,file,old_str,new_str):
        """
        replace str in file
        :param file
        :param old_str
        :param new_str
        :return:
        """
        file_data = ""
        with open(file, "r", encoding="utf-8") as f:
            for line in f:
                if old_str in line:
                    line = line.replace(old_str,new_str)
                file_data += line
        with open(file,"w",encoding="utf-8") as f:
            f.write(file_data)

    def _hasProcess(self, processName):
        return bool(self._getProcessPids(processName))

    def stopTaosdCompletely(self):
        """Stop taosd, taos and taosadapter processes for compatibility
        tests.

        Tries graceful shutdown first (systemctl stop, then up to 5
        rounds of pkill), and falls back to SIGKILL plus freeing TCP
        port 6030 via fuser if any process is still alive. Waits for
        taosd and taosadapter to fully exit before returning.
        """
        tdLog.info("stop taosd service")
        os.system("systemctl stop taosd 2>/dev/null || true")

        for _ in range(5):
            os.system("pkill taosd 2>/dev/null || true")
            os.system("pkill taos 2>/dev/null || true")
            os.system("pkill taosadapter 2>/dev/null || true")
            if (not self._hasProcess("taosd") and
                    not self._hasProcess("taos") and
                    not self._hasProcess("taosadapter")):
                break
            time.sleep(1)
        else:
            tdLog.info("force kill remaining compatibility processes")
            os.system("pkill -9 taosd 2>/dev/null || true")
            os.system("pkill -9 taos 2>/dev/null || true")
            os.system("pkill -9 taosadapter 2>/dev/null || true")
            os.system("fuser -k -n tcp 6030 2>/dev/null || true")

        self.checkProcessPid("taosd")
        self.checkProcessPid("taosadapter")

    def prepareDataOnOldVersion(self, base_version, bPath,corss_major_version):
        time.sleep(5)
        global dbname, stb, first_consumer_rows
        # Use extracted old-version binaries to avoid polluting system paths
        _old_taos = f"LD_LIBRARY_PATH={self.old_lib_dir} {self.old_bin_dir}/taos"
        _old_bench = f"LD_LIBRARY_PATH={self.old_lib_dir} {self.old_bin_dir}/taosBenchmark"
        _old_adapter_bin = f"{self.old_bin_dir}/taosadapter"
        tdLog.printNoPrefix(f"==========step1:prepare and check data in old version-{base_version}")
        tdLog.info(f" {_old_bench} -t {tableNumbers} -n {recordNumbers1} -v 1 -O 5  -y ")
        subprocess.run(
            f"{_old_bench} -t {tableNumbers} -n {recordNumbers1} -v 1 -O 5 -y",
            shell=True, check=True,
        )
        os.system(f"{_old_taos} -s 'alter database test   keep 365000 '")
        os.system(f"{_old_taos} -s 'alter database test  cachemodel \"both\" '")
        os.system(f"{_old_taos} -s 'select last(*) from test.meters '")
        os.system(f"{_old_taos} -s 'flush database test '")
        os.system(f"{_old_taos} -s \"insert into test.d1 values (now+1s, 11, 190, 0.21), (now+2s, 11, 190, 0.21), (now+3s, 11, 190, 0.21), ('2015-07-14 08:39:59.001', 11, 190, 0.21), ('2032-08-14 08:39:59.001 ', 11, 190, 0.21) test.d3  values  (now+6s, 11, 190, 0.21), (now+7s, 11, 190, 0.21), (now+8s, 11, 190, 0.21), ('2033-07-14 08:39:59.000', 119, 191, 0.25) test.d3  (ts) values ('2033-07-14 08:39:58.000');\"")
        os.system(f"{_old_taos} -s 'select last(*) from test.meters '")
        os.system(f"{_old_taos} -s 'flush database test '")

        os.system(f"{_old_taos} -s \"insert into test.d1 values (now+11s, 11, 190, 0.21), (now+12s, 11, 190, 0.21), (now+13s, 11, 190, 0.21), (now+14s, 11, 190, 0.21), (now+15s, 11, 190, 0.21) test.d3  values  (now+16s, 11, 190, 0.21), (now+17s, 11, 190, 0.21), (now+18s, 11, 190, 0.21), (now+19s, 119, 191, 0.25) test.d3  (ts) values (now+20s);\"")
        subprocess.run(
            f"{_old_bench} -f cases/18-StreamProcessing/30-OldPyCases/json/com_alltypedata.json -y",
            shell=True, check=True,
        )
        os.system(f"{_old_taos} -s 'flush database curdb '")
        os.system(f"{_old_taos} -s 'alter database curdb  cachemodel \"both\" '")
        os.system(f"{_old_taos} -s 'select count(*) from curdb.meters '")
        os.system(f"{_old_taos} -s 'select last(*) from curdb.meters '")
        os.system(f"{_old_taos} -s 'select sum(fc) from curdb.meters '")
        os.system(f"{_old_taos} -s 'select avg(ic) from curdb.meters '")
        os.system(f"{_old_taos} -s 'select min(ui) from curdb.meters '")
        os.system(f"{_old_taos} -s 'select max(bi) from curdb.meters '")

        # Stream processing functionality removed - migrated to new stream computing framework

        # create db/stb/select topic
        os.system(f'{_old_taos} -s  "create topic if not exists {db_topic} with meta as database test" ')

        os.system(f'{_old_taos} -s  "create topic if not exists {stable_topic}  as stable test.meters where tbname like \\"d3\\";" ')

        
        os.system(f'{_old_taos} -s  "create topic if not exists {select_topic}  as {topic_select_sql}" ')

        os.system(f'{_old_taos} -s  "use test;show topics;" ')
        os.system(f"  {_old_adapter_bin} --version " )
        consumer_dict = {
            "group.id": "g1",
            "td.connect.websocket.scheme": "ws",
            "td.connect.user": "root",
            "td.connect.pass": "taosdata",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": "false",
        }

        consumer = taosws.Consumer(consumer_dict)
        try:
            consumer.subscribe([select_topic])
        except TmqError as e:
            tdLog.exit(f"subscribe error: {e}")
        
        # A None from poll() only means no new message within 1s, not that the
        # topic has no data; under heavy load the first batch may take longer
        # than 1s. We only consume the first batch to establish the
        # first_consumer_rows baseline, so give it a 30s grace window to avoid
        # a slow start recording too few rows and skewing later checks.
        first_msg_deadline = time.time() + 30
        while True:
            message = consumer.poll(timeout=1.0)
            if message:
                for block in message:
                    first_consumer_rows += block.nrows()
                consumer.commit(message)
                tdLog.info(f"topic:{select_topic} ,first consumer rows is {first_consumer_rows} in old version")
                break
            tdLog.info("first message is null, keep waiting for first batch")
            if time.time() > first_msg_deadline:
                tdLog.info("no first message within 30s, break")
                break

        consumer.close()
        
        tdLog.info(f" {_old_bench} -f cases/18-StreamProcessing/30-OldPyCases/json/compa4096.json -y  ")
        os.system(f"{_old_bench} -f cases/18-StreamProcessing/30-OldPyCases/json/compa4096.json -y > /dev/null 2>&1")
        os.system(f"{_old_bench} -f cases/18-StreamProcessing/30-OldPyCases/json/all_insertmode_alltypes.json -y > /dev/null 2>&1")

        # os.system(f"{_old_taos} -s 'flush database db4096 '")
        os.system(f"{_old_taos} -f cases/18-StreamProcessing/30-OldPyCases/json/TS-3131.tsql")

        # add deleted  data
        os.system(f'{_old_taos} -s "{deletedDataSql}" ')

        if corss_major_version:
            cmd = f" LD_LIBRARY_PATH={bPath}/build/lib  {bPath}/build/bin/taos -h localhost ;"
            tdLog.info(f"new  client version  connect to old version taosd, commad return value:{cmd}")
            if os.system(cmd) == 0:
                raise Exception("failed to execute system command. cmd: %s" % cmd)
        
    def _upgrade_dnode(self, bPath, dnode_path):
        cfg = os.path.normcase(os.path.realpath(os.path.join(dnode_path, "cfg")))
        matches = []
        for process in psutil.process_iter(["name", "cmdline"]):
            try:
                if process.info["name"] not in {"taosd", "taosd.exe"}:
                    continue
                arguments = process.info["cmdline"] or []
                if "-c" not in arguments:
                    continue
                index = arguments.index("-c") + 1
                if index < len(arguments) and os.path.normcase(os.path.realpath(arguments[index])) == cfg:
                    matches.append(process)
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                continue
        if len(matches) != 1:
            raise RuntimeError(f"Expected one old taosd for {cfg}, found {len(matches)}")
        old_process = matches[0]
        tdLog.info(f"Stopping old taosd pid={old_process.pid}, cfg={cfg}")
        old_process.kill()
        old_process.wait(timeout=60)
        env = os.environ.copy()
        env["LD_LIBRARY_PATH"] = os.path.join(bPath, "build", "lib")
        program = "taosd"
        if os.name == "nt":
            program += ".exe"
            env.pop("LD_PRELOAD", None)
            env.pop("LD_LIBRARY_PATH", None)
            env["PATH"] = os.path.join(bPath, "build", "bin") + os.pathsep + env.get("PATH", "")
        startup_log = os.path.join(dnode_path, "log", "upgrade-startup.log")
        # start_new_session only isolates POSIX processes. Windows teardown
        # broadcasts CTRL_BREAK within the target console, so the upgraded
        # server must never inherit the CI runner's console.
        launch_options = ({"creationflags": subprocess.CREATE_NEW_CONSOLE}
                          if os.name == "nt" else {"start_new_session": True})
        with open(startup_log, "ab") as output:
            process = subprocess.Popen(
                [os.path.join(bPath, "build", "bin", program), "-c", cfg],
                env=env, stdout=output, stderr=subprocess.STDOUT, **launch_options,
            )
        tdLog.info(f"Started upgraded taosd pid={process.pid}, cfg={cfg}, log={startup_log}")
        return process

    def updateNewVersion(self, bPath, cPaths, upgrade, retry_times=300):
        tdLog.printNoPrefix("==========step2:update new version ")
        upgraded_processes = []
        if upgrade in (0, 1):
            paths = cPaths[:1] if upgrade == 0 else cPaths
            if not paths:
                raise RuntimeError("No dnode paths provided for upgrade")
            for path in paths:
                upgraded_processes.append(self._upgrade_dnode(bPath, path))
        elif upgrade == 2:
            tdLog.info("no upgrade mode")
            self.buildTaosd(bPath)
            os.system(f"nohup {bPath}/build/bin/taosd -c {cPaths[0]} > /dev/null 2>&1 &")
        else:
            raise ValueError(f"Unknown upgrade mode: {upgrade}")
        self.checkstatus(retry_times=retry_times)
        for process in upgraded_processes:
            if process.poll() is not None:
                raise RuntimeError(f"Upgraded taosd pid={process.pid} exited: {process.returncode}")

    def checkTagSizeAndAlterStb(self,tdsql):
        tdsql.query("select * from information_schema.ins_tags where db_name = 'db_all_insert_mode'")
        for i in range(tdsql.queryRows):
            tag_type = tdsql.queryResult[i][4]
            if "NCHAR" not in tag_type:
                continue
            
            tag_size =  int(tag_type.split('(')[1].split(')')[0])
            tag_value = tdsql.queryResult[i][5]
            if len(tag_value) > tag_size:
                new_tag_size = tag_size
                while new_tag_size < len(tag_value):
                    new_tag_size = new_tag_size * 2
                db_name = tdsql.queryResult[i][1]
                stable_name = tdsql.queryResult[i][2]
                tag_name = tdsql.queryResult[i][3]
                if new_tag_size <= tag_size:
                    continue
                tdLog.info(f"ALTER STABLE {db_name}.{stable_name} MODIFY TAG {tag_name} nchar({new_tag_size})")
                tdLog.info(f"current tag_value is {tag_value} and tag value len is {len(tag_value)} and tag_size is {tag_size}")
                tdsql.execute(f"ALTER STABLE {db_name}.{stable_name} MODIFY TAG {tag_name} nchar({new_tag_size})")
                #check tag size
                max_try_times = 100
                try_times = 0
                while try_times < max_try_times:
                    tdLog.info(f"select * from information_schema.ins_tags where db_name = '{db_name}' and stable_name = '{stable_name}' and tag_name = '{tag_name}'")
                    tdsql.query(f"select * from information_schema.ins_tags where db_name = '{db_name}' and stable_name = '{stable_name}' and tag_name = '{tag_name}'")
                    real_tag_type = tdsql.queryResult[0][4]
                    real_tag_size =  int(real_tag_type.split('(')[1].split(')')[0])
                    if real_tag_size == new_tag_size:
                        tdLog.info(f"success to alter tag size from {tag_size} to {new_tag_size}")
                        break
                    time.sleep(0.5)
                    try_times += 1
                self.checkTagSizeAndAlterStb(tdsql)



    def verifyData(self,corss_major_version):
        tdLog.printNoPrefix(f"==========step3:prepare and check data in new version")
        time.sleep(1)
        tdsql=tdCom.newTdSql()
        tdLog.info(tdsql)
        if corss_major_version:
            cmd = f" LD_LIBRARY_PATH={self.old_lib_dir}  {self.old_bin_dir}/taos -h localhost ;"
            tdLog.info(os.system(cmd))
            if os.system(cmd) == 0:
                raise Exception("failed to execute system command. cmd: %s" % cmd)
        
        tdsql.query(f"SELECT SERVER_VERSION();")
        nowServerVersion=tdsql.queryResult[0][0]
        tdLog.info(f"New server version is {nowServerVersion}")
        tdsql.query(f"SELECT CLIENT_VERSION();")
        nowClientVersion=tdsql.queryResult[0][0]
        tdLog.info(f"New client version is {nowClientVersion}")

        
        tdsql.query(f"select last(*) from curdb.meters")
        tdLog.info(tdsql.queryResult)

        # deal table schema is too old issue
        self.checkTagSizeAndAlterStb(tdsql)

        tdsql.query(f"select * from db_all_insert_mode.sml_json")    
        tdsql.checkRows(16)
    
        tdsql.query(f"select * from db_all_insert_mode.sml_line")     
        tdsql.checkRows(16)   
        tdsql.query(f"select * from db_all_insert_mode.sml_telnet")  
        tdsql.checkRows(16)    
        tdsql.query(f"select * from db_all_insert_mode.rest")    
        tdsql.checkRows(16)    
        tdsql.query(f"select * from db_all_insert_mode.stmt")  
        tdsql.checkRows(16)  
        tdsql.query(f"select * from db_all_insert_mode.sml_rest_json")    
        tdsql.checkRows(16)  
        tdsql.query(f"select * from db_all_insert_mode.sml_rest_line")    
        tdsql.checkRows(16)      
        tdsql.query(f"select * from db_all_insert_mode.sml_rest_telnet")    
        tdsql.checkRows(16)  

        tdsql.query(f"select count(*) from {stb}")
        tdsql.checkData(0,0,tableNumbers*recordNumbers1+20)
        # Stream verification removed - migrated to new stream computing framework
        


        # checkout db4096
        tdsql.query("select count(*) from db4096.stb0")
        tdsql.checkData(0,0,50000)
        
        # checkout deleted data
        tdsql.execute("insert into deldata.ct1 values ( now()-0s, 0, 0, 0, 0, 0.0, 0.0, 0, 'binary0', 'nchar0', now()+0a ) ( now()-10s, 1, 11111, 111, 11, 1.11, 11.11, 1, 'binary1', 'nchar1', now()+1a ) ( now()-20s, 2, 22222, 222, 22, 2.22, 22.22, 0, 'binary2', 'nchar2', now()+2a ) ( now()-30s, 3, 33333, 333, 33, 3.33, 33.33, 1, 'binary3', 'nchar3', now()+3a );")
        tdsql.execute("flush database deldata;")
        tdsql.query("select avg(c1) from deldata.ct1;")

    def verifyBackticksInTaosSql(self,bPath,base_version):
        tdsql=tdCom.newTdSql()
        tdLog.printNoPrefix("==========step4:verify backticks in taos Sql-TD18542")
        tdsql.execute("drop database if exists db")
        tdsql.execute("create database db")
        tdsql.execute("use db")
        tdsql.execute("create stable db.stb1 (ts timestamp, c1 int) tags (t1 int);")
        tdsql.execute("insert into db.ct1 using db.stb1 TAGS(1) values(now(),11);")
        tdsql.error(" insert into `db.ct2` using db.stb1 TAGS(9) values(now(),11);")
        tdsql.error(" insert into db.`db.ct2` using db.stb1 TAGS(9) values(now(),11);")
        tdsql.execute("insert into `db`.ct3 using db.stb1 TAGS(3) values(now(),13);")
        tdsql.query("select * from db.ct3")
        tdsql.checkData(0,1,13)
        tdsql.execute("insert into db.`ct4` using db.stb1 TAGS(4) values(now(),14);")
        tdsql.query("select * from db.ct4")
        tdsql.checkData(0,1,14)

        #check retentions
        tdsql=tdCom.newTdSql()
        tdsql.query("describe  information_schema.ins_databases;")
        qRows=tdsql.queryRows   
        comFlag=True
        j=0
        while comFlag:
            for i in  range(qRows) :
                if tdsql.queryResult[i][0] == "retentions" :
                    tdLog.info("parameters include retentions")
                    comFlag=False
                    break
                else :
                    comFlag=True
                    j=j+1
            if j == qRows:
                tdLog.info("parameters don't include retentions")
                import inspect
                caller = inspect.getframeinfo(inspect.stack()[0][0])
                args = (caller.filename, caller.lineno)
                tdLog.exit("%s(%d) failed" % args)

        # Stream checking removed - migrated to new stream computing framework

        #check TS-3131
        tdsql.query("select *,tbname from d0.almlog where mcid='m0103';")
        tdsql.checkRows(6)
        expectList = [0,3003,20031,20032,20033,30031]
        resultList = []
        for i in range(6):
            resultList.append(tdsql.queryResult[i][3])
        tdLog.info(resultList)
        if self.is_list_same_as_ordered_list(resultList,expectList):
            tdLog.info("The unordered list is the same as the ordered list.")
        else:
            tdLog.exit("The unordered list is not the same as the ordered list.")


        # check database test and last
        # first check
        
        tdsql.query(f"select last(*) from test.meters group by tbname")
        tdLog.info(tdsql.queryResult)
        # tdsql.checkRows(tableNumbers)
        
        tdsql.query(f"select last_row(*) from test.meters group by tbname")
        tdLog.info(tdsql.queryResult)
        # tdsql.checkRows(tableNumbers)

        tdsql.query(f"select last_row(*) from test.meters partition by tbname")
        tdLog.info(tdsql.queryResult)
        # tdsql.checkRows(tableNumbers)
        
        if self.version_compare(base_version, "3.3.6.0") >= 0:
            tdsql.query(f"select last(*) from test.meters")
            tdLog.info(tdsql.queryResult)
            tdsql.checkData(0,0,"2033-07-14 08:39:59.000")
            tdsql.checkData(0,1,119) 
            tdsql.checkData(0,2,191)
            tdsql.checkData(0,3,0.25)
            
            tdsql.query(f"select last_row(*) from test.meters")
            tdLog.info(tdsql.queryResult)
            tdsql.checkData(0,0,"2033-07-14 08:39:59.000")
            tdsql.checkData(0,1,119) 
            tdsql.checkData(0,2,191)
            tdsql.checkData(0,3,0.25)

            tdsql.query(f"select last(*) from test.d1")
            tdLog.info(tdsql.queryResult)       
            tdsql.checkData(0,0,"2032-08-14 08:39:59.001")
            tdsql.checkData(0,1,11) 
            tdsql.checkData(0,2,190)
            tdsql.checkData(0,3,0.21)      

        # update data and check
        tdsql.execute("insert into test.d2 values ('2033-07-14 08:39:59.002', 139, 182, 1.10) (now+2s, 12, 191, 0.22) test.d2  (ts) values ('2033-07-14 08:39:59.003');")
        tdsql.execute("insert into test.d2 values (now+5s, 4.3, 104, 0.4);")

        tdsql.query(f"select last(*) from test.meters")
        tdLog.info(tdsql.queryResult)
        tdsql.checkData(0,0,"2033-07-14 08:39:59.003")
        tdsql.checkData(0,1,139) 
        tdsql.checkData(0,2,182)
        tdsql.checkData(0,3,1.10)

        # repeately insert data and check
        tdsql.execute("insert into test.d1 values (now+1s, 11, 190, 0.21) (now+2s, 12, 191, 0.22) ('2033-07-14 08:40:01.001', 16, 180, 0.53);")

        tdsql.query(f"select last(*) from test.d1")
        tdLog.info(tdsql.queryResult)
        tdsql.checkData(0,0,"2033-07-14 08:40:01.001")
        tdsql.checkData(0,1,16)
        tdsql.checkData(0,2,180)
        tdsql.checkData(0,3,0.53)
        
        tdsql.query(f"select last(*) from test.meters")
        tdLog.info(tdsql.queryResult)
        tdsql.checkData(0,0,"2033-07-14 08:40:01.001")
        tdsql.checkData(0,1,16)
        tdsql.checkData(0,2,180)
        tdsql.checkData(0,3,0.53)

        tdsql.query(f"select last_row(*) from test.meters")
        tdLog.info(tdsql.queryResult)
        tdsql.checkData(0,0,"2033-07-14 08:40:01.001")
        tdsql.checkData(0,1,16)
        tdsql.checkData(0,2,180)
        tdsql.checkData(0,3,0.53)

        # check alter config
        tdsql.execute('alter all dnodes "debugFlag 131"')
        tdsql.execute('alter dnode 1 "debugFlag 143"')
        tdsql.execute('alter local "debugFlag 131"')

        # check tmq
        conn = taos.connect()

        consumer = Consumer(
            {
                "group.id": "g1",
                "td.connect.user": "root",
                "td.connect.pass": "taosdata",
                "enable.auto.commit": "true",
                "experimental.snapshot.enable":  "true",
            }
        )
        consumer.subscribe([select_topic])
        consumer_rows = 0
        # A None from poll() only means no new message within 1s, not that the
        # subscription is drained. After a cold upgrade the first subscription
        # must register the consumer and scan snapshots, so under heavy load
        # the first batch may exceed 1s; breaking on a single empty poll would
        # mistake the slow start for "0 rows". Instead: allow 30s for the
        # first batch, then treat 10 consecutive empty polls (~10s of silence)
        # as consumed.
        silent_polls = 0
        first_msg_deadline = time.time() + 30
        while True:
            message = consumer.poll(timeout=1.0)
            tdLog.info(f" null {message}")
            if message:
                silent_polls = 0
                for block in message:
                    consumer_rows += block.nrows()
                tdLog.info(f"consumer rows is {consumer_rows}")
                continue
            silent_polls += 1
            if consumer_rows > 0 and silent_polls >= 10:
                tdLog.info("consumer has completed and break")
                break
            if consumer_rows == 0 and time.time() > first_msg_deadline:
                tdLog.info("consumer has completed and break (no first message within 30s)")
                break
        consumer.close()
        tdsql.query(f"{topic_select_sql}")
        all_rows = tdsql.queryRows
        if consumer_rows < all_rows - first_consumer_rows :
            tdLog.exit(f"consumer rows is {consumer_rows}, less than {all_rows - first_consumer_rows}")
        tdsql.query("show topics;")
        tdsql.checkRows(3)
        tdsql.execute(f"drop topic {select_topic};",queryTimes=10)
        tdsql.execute(f"drop topic {db_topic};",queryTimes=10)
        tdsql.execute(f"drop topic {stable_topic};",queryTimes=10)

        os.system(f" LD_LIBRARY_PATH={bPath}/build/lib  {bPath}/build/bin/taosBenchmark -t {tableNumbers} -n {recordNumbers2} -y > /dev/null 2>&1")
        tdsql.query(f"select count(*) from {stb}")
        tdsql.checkData(0,0,tableNumbers*recordNumbers2)

    def checkstatus(self, retry_times=300):
        # Sleep before checking status to avoid dnodes not ready issue.
        # NOTE: retry_times must be large enough to cover slow vnode restore after an
        # upgrade. When the old version left no WAL snapshot (e.g. 3.3.3.0 data), the
        # new version has to replay and commit the whole dataset on restore, which can
        # take several minutes (e.g. a 4094-column table from compa4096.json). Bumped
        # from 60 to 300 to avoid flaky "vnodes are not ready" (see tsdb!1495 follow-up).
        time.sleep(30)
        tdsql=tdCom.newTdSql()
        dnodes_ready = False
        for i in range(retry_times):
            tdsql.query("show dnodes;")
            dnode_nums = tdsql.queryRows
            ready_nums = 0

            for j in range(tdsql.queryRows):
                if tdsql.queryResult[j][4] == "ready":
                    ready_nums += 1
            if ready_nums == dnode_nums:
                dnodes_ready = True
                break

            time.sleep(1)

        if not dnodes_ready:
            tdLog.exit(f"dnodes are not ready in {retry_times}s")
        tdLog.info(f"dnodes are ready in {retry_times}s")

        modes_ready = False
        for i in range(retry_times):
            tdsql.query("show mnodes;")
            mnode_nums = tdsql.queryRows
            ready_nums = 0
            for j in range(tdsql.queryRows):
                if tdsql.queryResult[j][3] == "ready":
                    ready_nums += 1
            if ready_nums == mnode_nums:
                modes_ready = True
                break
            time.sleep(1)

        if not modes_ready:
            tdLog.exit(f"mnodes are not ready in {retry_times}s")
        tdLog.info(f"mnodes are ready in {retry_times}s")

    
        vnodes_ready = False
        for i in range(retry_times):
            tdsql.query("show vnodes;")
            vnode_nums = tdsql.queryRows
            ready_nums = 0
            for j in range(tdsql.queryRows):
                if str(tdsql.queryResult[j][6]).lower() == "true":
                    ready_nums += 1
            if ready_nums == vnode_nums:
                vnodes_ready = True
                break
            time.sleep(1)
    
        if not vnodes_ready:
            tdLog.exit(f"vnodes are not ready in {retry_times}s") 
        tdLog.info(f"vnodes are ready in {retry_times}s")

# Create instance for compatibility
tdCb = CompatibilityBase()
