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

    def modifyTaosCfg(self, newFqdn, newPort):
        """Modify taos.cfg to use new fqdn and serverPort."""
        cfgPath = os.path.join(tdDnodes.getDnodeDir(1), "cfg", "taos.cfg")
        tdLog.info(f"modifying taos.cfg: {cfgPath}")

        lines = []
        with open(cfgPath, 'r') as f:
            lines = f.readlines()

        with open(cfgPath, 'w') as f:
            for line in lines:
                stripped = line.strip()
                if stripped.startswith("fqdn"):
                    f.write(f"fqdn            {newFqdn}\n")
                elif stripped.startswith("firstEp"):
                    f.write(f"firstEp         {newFqdn}:{newPort}\n")
                elif stripped.startswith("serverPort"):
                    f.write(f"serverPort      {newPort}\n")
                else:
                    f.write(line)

        tdLog.info(f"taos.cfg modified: fqdn={newFqdn}, serverPort={newPort}")

    def modifyDnodeJson(self, newFqdn, newPort):
        """Modify dnode.json to use new fqdn and port."""
        dnodeJsonPath = os.path.join(tdDnodes.getDnodeDir(1), "data", "dnode", "dnode.json")
        tdLog.info(f"modifying dnode.json: {dnodeJsonPath}")

        with open(dnodeJsonPath, 'r') as f:
            data = json.load(f)

        if "dnodes" in data:
            for dnode in data["dnodes"]:
                dnode["fqdn"] = newFqdn
                dnode["port"] = newPort

        with open(dnodeJsonPath, 'w') as f:
            json.dump(data, f, indent=2)

        tdLog.info(f"dnode.json modified: fqdn={newFqdn}, port={newPort}")

    def createEpJson(self, oldFqdn, oldPort, newFqdn, newPort):
        """Create ep.json to record epset change mapping."""
        epJsonPath = os.path.join(tdDnodes.getDnodeDir(1), "data", "dnode", "ep.json")
        tdLog.info(f"creating ep.json: {epJsonPath}")

        epData = {
            "dnodes": [
                {
                    "id": 1,
                    "fqdn": oldFqdn,
                    "port": oldPort,
                    "new_fqdn": newFqdn,
                    "new_port": newPort
                }
            ]
        }

        with open(epJsonPath, 'w') as f:
            json.dump(epData, f, indent=2)

        tdLog.info(f"ep.json created with mapping: {oldFqdn}:{oldPort} -> {newFqdn}:{newPort}")

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

    def checkTransactionExists(self, seconds=30, interval=2):
        """Check that there IS a pending transaction. Returns True if found."""
        for i in range(seconds // interval):
            rows = tdSql.query("show transactions;")
            if rows > 0:
                tdLog.info(f"found {rows} pending transaction(s)")
                return True
            time.sleep(interval)
        tdLog.info("no pending transactions found")
        return False

    '''
    Test TMQ epset change scenario:
    1. Build subscription (triggers rebalance transaction with epset in WAL)
    2. Force kill taosd
    3. Modify config to change epset (taos.cfg fqdn/port, dnode.json)
    4. Start taosd - WAL replay with old epset causes stuck transaction
    5. Verify unfinished transaction exists
    6. Stop taosd
    7. Create ep.json with old->new epset mapping
    8. Start taosd - ep.json resolves the epset mismatch
    9. Verify no unfinished transactions
    '''
    def test_tmq_change_epset(self):
        """TMQ consume with epset change after force kill

        Verify that after changing taosd's epset (fqdn + port), WAL replay
        of old rebalance transactions fails until ep.json provides the
        epset mapping. With ep.json in place, taosd can resolve the old
        epset and complete the pending transactions.

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-04-14 wangmm Created

        """
        oldFqdn = "localhost"
        oldPort = 6030
        newFqdn = self.getNewFqdn()
        newPort = self.getNewPort()

        # step 1: build subscription (triggers rebalance, writes epset to WAL)
        self.buildSubscription()
        tdLog.info("subscription built, rebalance transaction written to WAL")

        # step 2: force kill taosd (leave WAL unfinished)
        tdLog.info("force killing taosd")
        tdDnodes.forcestop(1)
        time.sleep(2)

        # step 3: add new fqdn to /etc/hosts
        self.addHostEntry(newFqdn)

        # step 4: modify taos.cfg and dnode.json with new epset
        self.modifyTaosCfg(newFqdn, newPort)
        self.modifyDnodeJson(newFqdn, newPort)

        # step 5: start taosd - WAL replay with old epset should cause stuck transaction
        tdLog.info("starting taosd with new epset (no ep.json)")
        tdDnodes.starttaosd(1)
        time.sleep(5)

        # step 6: reconnect with new endpoint and check for unfinished transaction
        # verify 10 consecutive times that there is a pending transaction
        newConn = taos.connect(host=newFqdn, port=newPort, user="root", password="taosdata")
        newCursor = newConn.cursor()
        consecutiveCount = 0
        for _ in range(60):
            newCursor.execute("show transactions")
            rows = newCursor.fetchall()
            if len(rows) > 0:
                consecutiveCount += 1
                tdLog.info(f"pending transaction detected ({consecutiveCount}/10)")
                if consecutiveCount >= 10:
                    break
            else:
                consecutiveCount = 0
            time.sleep(1)
        newCursor.close()
        newConn.close()
        if consecutiveCount < 10:
            self.removeHostEntry(newFqdn)
            tdLog.exit(f"expected unfinished transaction 10 consecutive times, but only got {consecutiveCount}")
        tdLog.info("confirmed unfinished transaction exists (10 consecutive checks)")

        # step 7: stop taosd
        tdLog.info("stopping taosd to add ep.json")
        tdDnodes.stoptaosd(1)
        time.sleep(2)

        # step 8: create ep.json with old->new epset mapping
        self.createEpJson(oldFqdn, oldPort, newFqdn, newPort)

        # step 9: start taosd again - ep.json should resolve the epset mismatch
        tdLog.info("starting taosd with ep.json")
        tdDnodes.starttaosd(1)
        time.sleep(5)

        # step 10: check that there are no unfinished transactions
        # verify 10 consecutive times that there are no pending transactions
        newConn2 = taos.connect(host=newFqdn, port=newPort, user="root", password="taosdata")
        newCursor2 = newConn2.cursor()
        consecutiveZero = 0
        for _ in range(60):
            newCursor2.execute("show transactions")
            rows2 = newCursor2.fetchall()
            if len(rows2) == 0:
                consecutiveZero += 1
                tdLog.info(f"no pending transaction ({consecutiveZero}/10)")
                if consecutiveZero >= 10:
                    break
            else:
                consecutiveZero = 0
                tdLog.info(f"still have {len(rows2)} pending transaction(s), resetting counter")
            time.sleep(1)
        newCursor2.close()
        newConn2.close()
        if consecutiveZero < 10:
            self.removeHostEntry(newFqdn)
            tdLog.exit(f"expected no transactions 10 consecutive times, but only got {consecutiveZero}")
        tdLog.info("confirmed no unfinished transactions (10 consecutive checks), ep.json resolved the epset change")

        # cleanup: remove hosts entry
        self.removeHostEntry(newFqdn)
        tdLog.info("test_tmq_change_epset ................ [passed]")
