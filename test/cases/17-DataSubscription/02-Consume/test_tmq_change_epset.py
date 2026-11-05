# Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
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

import taos
import sys
import time
import socket
import os
import json
import threading
import subprocess
from  datetime import datetime

from new_test_framework.utils import tdLog, tdSql, etool, tdCom, tdDnodes
from taos.tmq import *
from taos import *


class TestTmqBugs:
    updatecfgDict = {
        'debugFlag': 135,
        'asynclog': 0
    }

    clientCfgDict = {'debugFlag': 135, 'asynclog': 0}
    updatecfgDict["clientCfg"] = clientCfgDict

    def setup_class(cls):
        tdLog.debug(f"start to excute {__file__}")


    def buildSubscription(self):
        tdSql.execute(f'create database d1 vgroups 1')
        tdSql.execute(f'use d1')
        tdSql.execute(f'create table st(ts timestamp, i int) tags(t int)')
        tdSql.execute(f'insert into t1 using st tags(1) values(now, 1) (now+1s, 2)')

        tdSql.execute(f'create topic t1 with meta as database d1')

        consumer_dict = {
            "group.id": "g1",
            "td.connect.user": "root",
            "td.connect.pass": "taosdata",
            "auto.offset.reset": "earliest",
        }
        consumer1 = Consumer(consumer_dict)

        try:
            consumer1.subscribe(["t1"])
        except TmqError:
            tdLog.exit(f"subscribe error")

        try:
            while True:
                res = consumer1.poll(1)
                if not res:
                    break
        finally:
            consumer1.close()

    def getNewFqdn(self):
        """Generate a new fqdn different from localhost."""
        return "tdengine-test-host"

    def getNewPort(self):
        """Return a new port different from the default 6030."""
        return 7030

    def addHostEntry(self, fqdn):
        """Add a host entry mapping fqdn to 127.0.0.1 in /etc/hosts."""
        try:
            cmd = f'grep -q "{fqdn}" /etc/hosts || echo "127.0.0.1 {fqdn}" | sudo tee -a /etc/hosts > /dev/null'
            os.system(cmd)
            tdLog.info(f"added hosts entry: 127.0.0.1 {fqdn}")
        except Exception as e:
            tdLog.exit(f"failed to add host entry: {e}")

    def removeHostEntry(self, fqdn):
        """Remove the host entry for fqdn from /etc/hosts."""
        try:
            cmd = f'sudo sed -i "" "/127.0.0.1 {fqdn}/d" /etc/hosts 2>/dev/null || sudo sed -i "/127.0.0.1 {fqdn}/d" /etc/hosts'
            os.system(cmd)
            tdLog.info(f"removed hosts entry for {fqdn}")
        except Exception as e:
            tdLog.info(f"failed to remove host entry: {e}")

    def waitTransactionZero(self, seconds=60, interval=2):
        """Wait for all transactions to complete. Returns True if no pending transactions."""
        for i in range(seconds // interval):
            rows = tdSql.query("show transactions;")
            if rows == 0:
                tdLog.info("no pending transactions")
                return True
            tdLog.info(f"waiting for transactions to complete, current count: {rows}")
            time.sleep(interval)
        return False

    def rewriteEndpoint(self, oldFqdn, oldPort, newFqdn, newPort):
        """Rewrite every persisted endpoint while taosd is stopped."""
        binary = os.path.join(tdCom.getBuildPath(), "build", "bin", "taosd")
        dnodeDir = tdDnodes.getDnodeDir(1)
        cfgDir = os.path.join(dnodeDir, "cfg")
        reports = os.path.join(dnodeDir, "precheck")
        os.makedirs(reports, exist_ok=True)

        result = subprocess.run(
            [binary, "-c", cfgDir, "--rewrite-endpoints-precheck", reports,
             "--map", f"{oldFqdn}:{oldPort}={newFqdn}:{newPort}"],
            capture_output=True, text=True, timeout=120)
        if result.returncode != 0:
            tdLog.exit(f"endpoint precheck failed: {result.stdout}{result.stderr}")

        result = subprocess.run(
            [binary, "-c", cfgDir, "--rewrite-endpoints-apply", reports],
            capture_output=True, text=True, timeout=120)
        if result.returncode != 0:
            tdLog.exit(f"endpoint apply failed: {result.stdout}{result.stderr}")
        tdLog.info(f"endpoint rewritten: {oldFqdn}:{oldPort} -> {newFqdn}:{newPort}")

    def test_tmq_change_epset(self):
        """TMQ consume after the endpoint is rewritten offline

        Verify that after taosd's endpoint (fqdn + port) is changed with the
        offline endpoint rewrite command, the subscription created before the
        change can still be consumed and no unfinished transaction is left.

        1. Build a subscription, then stop taosd normally.
        2. Rewrite every persisted endpoint with --rewrite-endpoints-precheck
           and --rewrite-endpoints-apply.
        3. Start taosd on the new endpoint.
        4. Consume from the topic and verify no unfinished transaction remains.
        5. Verify a leftover ep.json is ignored and reported in the log.

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-04-14 wangmm Created
            - 2026-10-27 Bomin Zhang Use the offline endpoint rewrite command
              instead of ep.json

        """
        oldFqdn = "localhost"
        oldPort = 6030
        newFqdn = self.getNewFqdn()
        newPort = self.getNewPort()

        self.buildSubscription()
        tdLog.info("subscription built")

        tdDnodes.stoptaosd(1)
        time.sleep(2)

        self.addHostEntry(newFqdn)
        try:
            self.rewriteEndpoint(oldFqdn, oldPort, newFqdn, newPort)

            # A leftover ep.json must not be needed any more, and must not
            # change the endpoint either.
            epJson = os.path.join(tdDnodes.getDnodeDir(1), "data", "dnode", "ep.json")
            with open(epJson, "w") as handle:
                json.dump({"dnodes": [{"id": 1, "fqdn": oldFqdn, "port": oldPort,
                                       "new_fqdn": oldFqdn, "new_port": oldPort}]},
                          handle)

            tdDnodes.starttaosd(1)
            time.sleep(5)

            connection = taos.connect(host=newFqdn, port=newPort, user="root", password="taosdata")
            try:
                cursor = connection.cursor()
                cursor.execute("show dnodes")
                endpoints = [row[1] for row in cursor.fetchall()]
                cursor.close()
            finally:
                connection.close()
            if f"{newFqdn}:{newPort}" not in endpoints:
                tdLog.exit(f"expected {newFqdn}:{newPort} in show dnodes, got {endpoints}")

            logFile = os.path.join(tdDnodes.getDnodeDir(1), "log", "taosdlog.0")
            with open(logFile, "r", errors="replace") as handle:
                content = handle.read()
            if "ep.json" not in content or epJson not in content:
                tdLog.exit("leftover ep.json was not reported in the log")

            consumer_dict = {
                "group.id": "g1",
                "td.connect.user": "root",
                "td.connect.pass": "taosdata",
                "auto.offset.reset": "earliest",
                "td.connect.ip": newFqdn,
                "td.connect.port": newPort,
            }
            consumer = Consumer(consumer_dict)
            try:
                consumer.subscribe(["t1"])
                rows = 0
                while True:
                    res = consumer.poll(1)
                    if not res:
                        break
                    rows += 1
            finally:
                consumer.close()
            if rows == 0:
                tdLog.exit("no message consumed after the endpoint changed")

            if not self.waitTransactionZero():
                tdLog.exit("unfinished transaction remains after the endpoint changed")
        finally:
            self.removeHostEntry(newFqdn)

        tdLog.info("test_tmq_change_epset ................ [passed]")
