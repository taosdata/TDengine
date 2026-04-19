from new_test_framework.utils import tdLog, tdSql, epath, sc
import time
import subprocess
import json
import os


class TestClusterArbitratorAppliedIndex:

    def setup_class(cls):
        cls.db = "db"
        cls.stb = "meters"
        cls.ctb = "d0"
        cls.insert_rows = 100000

    def do_arbitrator_applied_index(self):
        tdLog.info("create database")
        tdSql.execute("drop database if exists db;")
        tdSql.execute('CREATE DATABASE db vgroups 1 replica 2;')

        if self.waitTransactionZero() is False:
            tdLog.exit("create db transaction not finished")
            return False

        time.sleep(1)

        tdSql.execute("use db;")

        tdLog.info("create stable")
        tdSql.execute("CREATE STABLE meters (ts timestamp, current float, voltage int, phase float) TAGS (location binary(64), groupId int);")

        if self.waitTransactionZero() is False:
            tdLog.exit("create stable transaction not finished")
            return False

        tdLog.info("create table")
        tdSql.execute("CREATE TABLE d0 USING meters TAGS (\"California.SanFrancisco\", 2);")

        tdLog.info("waiting vgroup is sync")
        count = 0
        while count < 100:
            tdSql.query("show arbgroups;")

            if tdSql.getData(0, 4) == True:
                break

            tdLog.info("wait %d seconds for is sync" % count)
            time.sleep(1)

            count += 1

        if count == 100:
            tdLog.exit("arbgroup sync failed")
            return

        # find and stop the follower dnode
        stoppedDnode = 0
        count = 0
        while count < 100:
            tdSql.query("show db.vgroups;")

            if (tdSql.getData(0, 4) == "follower") and (tdSql.getData(0, 7) == "leader"):
                tdLog.info("stop dnode2 (follower)")
                sc.dnodeStop(2)
                stoppedDnode = 2
                break

            if (tdSql.getData(0, 7) == "follower") and (tdSql.getData(0, 4) == "leader"):
                tdLog.info("stop dnode3 (follower)")
                sc.dnodeStop(3)
                stoppedDnode = 3
                break

            time.sleep(1)
            count += 1

        if count == 100:
            tdLog.exit("check leader and stop follower failed")
            return

        # wait for assigned leader status
        tdLog.info("waiting for assigned leader status")
        count = 0
        while count < 100:
            tdSql.query("show db.vgroups;")

            if (tdSql.getData(0, 4) == "assigned ") or (tdSql.getData(0, 7) == "assigned "):
                break

            tdLog.info("wait %d seconds for set assigned" % count)
            time.sleep(1)

            count += 1

        if count == 100:
            tdLog.exit("check assigned failed")
            return

        # insert 100000 rows using taosBenchmark with JSON config
        tdLog.info("insert %d rows data using taosBenchmark" % self.insert_rows)
        benchmark_cfg = {
            "filetype": "insert",
            "cfgdir": "/etc/taos",
            "host": "localhost",
            "port": 6030,
            "user": "root",
            "password": "taosdata",
            "thread_count": 4,
            "confirm_parameter_prompt": "no",
            "databases": [{
                "dbinfo": {
                    "name": "db",
                    "drop": "no"
                },
                "super_tables": [{
                    "name": "meters",
                    "child_table_exists": "yes",
                    "childtable_count": 1,
                    "childtable_prefix": "d",
                    "insert_rows": self.insert_rows,
                    "insert_mode": "taosc",
                    "timestamp_step": 1000,
                    "start_timestamp": "now-100d",
                    "columns": [
                        {"type": "float", "name": "current"},
                        {"type": "int", "name": "voltage"},
                        {"type": "float", "name": "phase"}
                    ],
                    "tags": [
                        {"type": "binary", "name": "location", "len": 64},
                        {"type": "int", "name": "groupId"}
                    ]
                }]
            }]
        }
        json_file = "/tmp/benchmark_arb_test.json"
        with open(json_file, "w") as f:
            json.dump(benchmark_cfg, f)
        cmd = "taosBenchmark -f %s -y" % json_file
        tdLog.info("taosBenchmark cmd: %s" % cmd)
        ret = subprocess.run(cmd, shell=True, capture_output=True, text=True)
        if ret.returncode != 0:
            tdLog.exit("taosBenchmark failed: %s" % ret.stderr)
            return

        # check inserted data is correct
        tdLog.info("check inserted data")
        tdSql.query("SELECT count(*) FROM db.meters;")
        rows = tdSql.getData(0, 0)
        tdLog.info("total rows: %d" % rows)
        if rows < self.insert_rows:
            tdLog.exit("inserted rows %d less than expected %d" % (rows, self.insert_rows))
            return

        # restart the stopped dnode
        tdLog.info("start dnode%d" % stoppedDnode)
        sc.dnodeStart(stoppedDnode)

        # wait for vgroup sync (arbgroup is_sync becomes true)
        tdLog.info("waiting vgroup is sync after restart")
        count = 0
        while count < 200:
            tdSql.query("show arbgroups;")

            if tdSql.getData(0, 4) == True:
                break

            tdLog.info("wait %d seconds for is sync" % count)
            time.sleep(1)

            count += 1

        if count == 200:
            tdLog.exit("arbgroup sync failed after restart")
            return

        # check vgroup status - should have leader/follower (not restoring)
        tdLog.info("check vgroup status after sync")
        count = 0
        while count < 100:
            tdSql.query("show db.vgroups;")
            status1 = tdSql.getData(0, 4)
            status2 = tdSql.getData(0, 7)
            tdLog.info("vgroup status: dnode2=%s, dnode3=%s" % (status1, status2))

            if (status1 == "leader" or status1 == "follower") and \
               (status2 == "leader" or status2 == "follower"):
                break

            time.sleep(1)
            count += 1

        if count == 100:
            tdLog.exit("vgroup did not recover to leader/follower status")
            return

        # verify data is still correct after sync
        tdLog.info("verify data after sync")
        tdSql.query("SELECT count(*) FROM db.meters;")
        rowsAfter = tdSql.getData(0, 0)
        tdLog.info("total rows after sync: %d" % rowsAfter)
        if rowsAfter != rows:
            tdLog.exit("data mismatch after sync: before=%d, after=%d" % (rows, rowsAfter))
            return

        tdLog.info("check show arbgroups result")
        tdSql.query("show arbgroups;")
        tdSql.checkRows(1)

        print("do arbitrator applied index ........... [passed]")

    def test_cluster_arbitrator_applied_index(self):
        """Cluster arbitrator applied index check

        1. Create cluster with 3 dnodes
        2. Create a database and a stable with 2 replicas
        3. Create a child table
        4. Stop one dnode which is follower
        5. Check the vgroup status to be assigned
        6. Insert 100000 rows data into the child table by benchmark
        7. Check inserted data is correct
        8. Restart the dnodes
        9. Check the vgroup status to be candidate
        10. Check "show arbgroups" result

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-04-13 Created for testing applied index gap check during assigned leader stepdown

        """
        self.do_arbitrator_applied_index()
