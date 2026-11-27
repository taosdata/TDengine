
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

from new_test_framework.utils import tdLog, tdSql, tdDnodes, tdCom
import taos
import sys
import time
import socket
import platform
import subprocess
import psutil
from new_test_framework.utils.eutil import start_test_process
if platform.system().lower() != 'windows':
    import pexpect as taosExpect
import os


def taos_command(buildPath, key, value, expectString, cfgDir, sqlString='', key1='', value1=''):
    if len(key) == 0:
        tdLog.exit("taos test key is null!")

    is_windows = platform.system().lower() == 'windows'
    if is_windows:
        command = [os.path.join(buildPath, 'build', 'bin', 'taos.exe')]
        if cfgDir:
            command.extend(['-c', os.path.normpath(cfgDir)])
        if key == 'p':
            command.append('-p' + value)
        else:
            command.append('-' + key)
            if value:
                command.append(value[1:-1] if value.startswith('"') and value.endswith('"') else value)
        if key1:
            command.append('-' + key1)
            if value1:
                command.append(value1[1:-1] if value1.startswith('"') and value1.endswith('"') else value1)
        tdLog.info("taos cmd: %s" % subprocess.list2cmdline(command))
        try:
            child = subprocess.Popen(command, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                     stderr=subprocess.STDOUT, text=True, encoding='utf-8', errors='replace')
            input_text = (sqlString + "\n" if sqlString else '') + 'quit\n'
            output, _ = child.communicate(input=input_text, timeout=120)
        except subprocess.TimeoutExpired:
            child.kill()
            output, _ = child.communicate()
            tdLog.error('taos process timed out after 120 seconds')
            return 'TAOS_FAIL', output
        except OSError as error:
            tdLog.error('unable to start taos: %s' % error)
            return 'TAOS_FAIL', str(error)
        print('taos process return code: %s, content:\n%s' % (child.returncode, output))
        if expectString and expectString not in output and not any(k in (key, key1) for k in ('A', 'C', 'V')):
            return 'TAOS_FAIL', output
        if sqlString:
            markers = ('Query OK', 'Create OK', 'Insert OK', 'Drop OK')
            return ('TAOS_OK' if child.returncode == 0 and any(m in output for m in markers) else 'TAOS_FAIL'), output
        if any(k in (key, key1) for k in ('A', 'C', 'V')):
            return 'TAOS_OK', output.strip()
        return ('TAOS_OK' if child.returncode == 0 else 'TAOS_FAIL'), output

    taosCmd = buildPath + '/build/bin/taos '
    if len(cfgDir) != 0:
        taosCmd = taosCmd + '-c ' + cfgDir

    taosCmd = taosCmd + ' -' + key
    if len(value) != 0:
        if key == 'p':
            taosCmd = taosCmd + value
        else:
            taosCmd = taosCmd + ' ' + value

    if len(key1) != 0:
        taosCmd = taosCmd + ' -' + key1
        if key1 == 'p':
            taosCmd = taosCmd + value1
        else:
            if len(value1) != 0:
                taosCmd = taosCmd + ' ' + value1

    tdLog.info ("taos cmd: %s" % taosCmd)

    child = taosExpect.spawn(taosCmd, timeout=3)
    #output = child.readline()
    #print (output.decode())
    if len(expectString) != 0:
        i = child.expect([expectString, taosExpect.TIMEOUT, taosExpect.EOF], timeout=6)
    else:
        i = child.expect([taosExpect.TIMEOUT, taosExpect.EOF], timeout=6)

    if platform.system().lower() == 'windows':
        retResult = child.before
    else:
        retResult = child.before.decode()
    print("expect() return code: %d, content:\n %s\n"%(i, retResult))
    #print(child.after.decode())
    if i == 0:
        print ('taos login success! Here can run sql, taos> ')
        if len(sqlString) != 0:
            child.sendline (sqlString)
            w = child.expect(["Query OK", taosExpect.TIMEOUT, taosExpect.EOF], timeout=1)
            if platform.system().lower() == 'windows':
                retResult = child.before
            else:
                retResult = child.before.decode()
            if w == 0:
                return "TAOS_OK", retResult
            else:
                return "TAOS_FAIL", retResult
        else:
            if key == 'A' or key1 == 'A' or key == 'C' or key1 == 'C' or key == 'V' or key1 == 'V':
                return "TAOS_OK", retResult
            else:
                return  "TAOS_OK", retResult
    else:
        if key == 'A' or key1 == 'A' or key == 'C' or key1 == 'C' or key == 'V' or key1 == 'V':
            return "TAOS_OK", retResult
        else:
            return "TAOS_FAIL", retResult

class TestTaosShellNetChk:
    #updatecfgDict = {'clientCfg': {'serverPort': 7080, 'firstEp': 'trd02:7080', 'secondEp':'trd02:7080'},\
    #                 'serverPort': 7080, 'firstEp': 'trd02:7080'}
    hostname = socket.gethostname()
    if (platform.system().lower() == 'windows' and not tdDnodes.dnodes[0].remoteIP == ""):
        try:
            config = eval(tdDnodes.dnodes[0].remoteIP )
            hostname = config["host"]
        except Exception:
            hostname = tdDnodes.dnodes[0].remoteIP
    serverPort = '7080'
    rpcDebugFlagVal = '143'
    clientCfgDict = {'serverPort': '', 'firstEp': '', 'secondEp':'', 'rpcDebugFlag':'135', 'fqdn':''}
    clientCfgDict["serverPort"]    = serverPort
    clientCfgDict["firstEp"]       = hostname + ':' + serverPort
    clientCfgDict["secondEp"]      = hostname + ':' + serverPort
    clientCfgDict["rpcDebugFlag"]  = rpcDebugFlagVal
    clientCfgDict["fqdn"] = hostname

    updatecfgDict = {'clientCfg': {}, 'serverPort': '', 'firstEp': '', 'secondEp':'', 'rpcDebugFlag':'135', 'fqdn':''}
    updatecfgDict["clientCfg"]  = clientCfgDict
    updatecfgDict["serverPort"] = serverPort
    updatecfgDict["firstEp"]    = hostname + ':' + serverPort
    updatecfgDict["secondEp"]   = hostname + ':' + serverPort
    updatecfgDict["fqdn"] = hostname

    print ("===================: ", updatecfgDict)

    def init(self, conn, logSql, replicaVar=1):
        self.replicaVar = int(replicaVar)
        tdLog.debug(f"start to excute {__file__}")
        tdSql.init(conn.cursor())

    def test_tool_taos_shell_net_chk(self):
        """taos-CLI net check
        
        1. taosd normal running
        2. taos-CLI -k expect 2: service ok
        3. taosd stop
        4. taos-CLI -k expect 0: unavailable
        5. taos-CLI -n server start
        6. taos-CLI -n client expect response is received
        7. taosd restart
        8. taos-CLI -k expect 2: service ok        

        Since: v3.0.0.0

        Labels: common,ci,integration,functional

        Jira: None

        History:
            - 2025-12-02 Alex Duan Migrated from uncatalog/system-test/0-others/test_taos_shell_net_chk.py

        """
        tdSql.prepare()
        tdSql.query("create user testpy pass 'testpy243#@'")

        buildPath = tdCom.getBuildPath()
        if (buildPath == ""):
            tdLog.exit("taosd not found!")
        else:
            tdLog.info("taosd found in %s" % buildPath)
        cfgPath = tdDnodes.sim.cfgDir
        tdLog.info("cfgPath: %s" % cfgPath)

        checkNetworkStatus = ['0: unavailable', '1: network ok', '2: service ok', '3: service degraded', '4: exiting']
        netrole            = ['client', 'server']

        keyDict = {'h':'', 'P':'6030', 'p':'testpy243#@', 'u':'testpy', 'a':'', 'A':'', 'c':'', 'C':'', 's':'', 'r':'', 'f':'', \
                   'k':'', 't':'', 'n':'', 'l':'1024', 'N':'100', 'V':'', 'd':'db', 'w':'30', '-help':'', '-usage':'', '?':''}

        keyDict['h'] = self.hostname
        keyDict['c'] = cfgPath
        keyDict['P'] = self.serverPort

        tdLog.printNoPrefix("================================ parameter: -k")
        sqlString = ''
        retCode, retVal = taos_command(buildPath, "k", '', "", keyDict['c'], sqlString)
        if "2: service ok" in retVal:
            tdLog.info("taos -k success")
        else:
            tdLog.info(retVal)
            tdLog.exit("taos -k fail 1")

        # stop taosd
        tdDnodes.stop(1)
        #sleep(10)
        #tdDnodes.start(1)
        #sleep(5)
        retCode, retVal = taos_command(buildPath, "k", '', "", keyDict['c'], sqlString)
        if "0: unavailable" in retVal:
            tdLog.info("taos -k success")
        else:
            tdLog.info(retVal)
            tdLog.exit("taos -k fail 2")

        # restart taosd
        tdDnodes.start(1)
        #sleep(5)
        retCode, retVal = taos_command(buildPath, "k", '', "", keyDict['c'], sqlString)
        if "2: service ok" in retVal:
            tdLog.info("taos -k success")
        else:
            tdLog.info(retVal)
            tdLog.exit("taos -k fail 3")

        tdLog.printNoPrefix("================================ parameter: -n")
        # stop taosd
        tdDnodes.stop(1)

        server_process = None
        try:
            role   = 'server'
            if platform.system().lower() == 'windows':
                server_process = start_test_process(
                    [os.path.join(buildPath, 'build', 'bin', 'taos.exe'),
                     '-c', cfgPath, '-P', self.serverPort, '-n', role],
                    stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                )
                deadline = time.monotonic() + 15
                while time.monotonic() < deadline:
                    if server_process.poll() is not None:
                        tdLog.exit(f'network test server exited: {server_process.returncode}')
                    connections = psutil.Process(server_process.pid).net_connections(kind='tcp')
                    if any(c.status == psutil.CONN_LISTEN and
                           c.laddr.port == int(self.serverPort) for c in connections):
                        break
                    time.sleep(0.1)
                else:
                    tdLog.exit('network test server did not start listening within 15s')
            else:
                taosCmd = 'nohup ' + buildPath + '/build/bin/taos -c ' + keyDict['c']
                taosCmd = taosCmd + ' -n ' + role + ' > /dev/null 2>&1 &'
                print (taosCmd)
                os.system(taosCmd)

            pktLen = '2000'
            pktNum = '10'
            role   = 'client'
            if platform.system().lower() == 'windows':
                taosCmd = subprocess.list2cmdline([
                    os.path.join(buildPath, 'build', 'bin', 'taos.exe'),
                    '-h', '127.0.0.1', '-c', cfgPath, '-P', self.serverPort,
                ])
            else:
                taosCmd = buildPath + '/build/bin/taos -c ' + keyDict['c']
            taosCmd = taosCmd + ' -n ' + role + ' -l ' + pktLen + ' -N ' +  pktNum
            print (taosCmd)
            if platform.system().lower() == 'windows':
                client_process = start_test_process(
                    [os.path.join(buildPath, 'build', 'bin', 'taos.exe'),
                     '-h', '127.0.0.1', '-c', cfgPath, '-P', self.serverPort,
                     '-n', role, '-l', pktLen, '-N', pktNum],
                    stdin=subprocess.DEVNULL, stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT, text=True, encoding='utf-8', errors='replace',
                )
                try:
                    retResult, _ = client_process.communicate(timeout=120)
                except subprocess.TimeoutExpired:
                    client_process.kill()
                    client_process.communicate()
                    tdLog.exit('network test client timed out after 120 seconds')
                if client_process.returncode != 0:
                    tdLog.exit(f'network test client failed: {retResult}')
                i = 1
            else:
                child = taosExpect.spawn(taosCmd, timeout=3)
                i = child.expect([taosExpect.TIMEOUT, taosExpect.EOF], timeout=6)
                retResult = child.before.decode()
            print("expect() return code: %d, content:\n %s\n"%(i, retResult))
            #print(child.after.decode())
            if i == 0:
                tdLog.exit('taos -n server fail!')

            expectString1 = 'response is received, size:' + pktLen
            expectSTring2 = pktNum + '/' + pktNum
            if expectString1 in retResult and expectSTring2 in retResult:
                tdLog.info("taos -n client success")
            else:
                tdLog.exit('taos -n client fail!')
        finally:
            if platform.system().lower() == 'windows':
                if server_process is not None:
                    if server_process.poll() is None:
                        server_process.kill()
                    server_process.wait(timeout=10)
            else:
                tdLog.info("pkill -9 taos")
                # os.system('pkill -9 taos')



