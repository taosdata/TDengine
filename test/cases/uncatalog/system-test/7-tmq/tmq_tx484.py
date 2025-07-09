import os


from new_test_framework.utils import tdLog, tdSql, tdDnodes
from taos.tmq import *
from taos import *

class TestCase:
    updatecfgDict = {'debugFlag': 135, 'asynclog': 0}
    clientCfgDict = {'debugFlag': 135, 'asynclog': 0}
    updatecfgDict["clientCfg"] = clientCfgDict
    def setup_class(cls):
        tdLog.debug(f"start to excute {__file__}")

    def printData(self, res, check):
        if not res:
            print("res null")
            return
        val = res.value()
        if val is None:
            return
        for block in val:
            data = block.fetchall()
            for element in data:
                if (element[2] != check):
                    tdLog.exit(f"error: {element[2]} != {check}")

    def check(self):
        tdSql.execute(f'create database if not exists db vgroups 1 wal_retention_period 10')
        tdSql.execute(f'use db')
        tdSql.execute(f'CREATE STABLE meters (ts TIMESTAMP, current FLOAT, voltage INT, phase FLOAT) TAGS (location BINARY(64), groupId INT)')
        tdSql.execute("INSERT INTO d1001 USING meters TAGS('California.SanFrancisco', 2) VALUES('2018-10-05 14:38:05.000',10.30000,210,0.31000)")
        tdSql.execute("INSERT INTO d1002 USING meters TAGS('California.SanFrancisco', 2) VALUES('2018-10-06 14:38:05.000',10.30000,219,0.31000)")
        tdSql.execute("INSERT INTO d1003 USING meters TAGS('California.SanFrancisco', 2) VALUES('2018-10-07 14:38:05.000',10.30000,2193,0.31000)")
        tdSql.execute("INSERT INTO d1004 USING meters TAGS('California.SanFrancisco', 2) VALUES('2018-10-08 14:38:05.000',10.30000,2194,0.31000)")

        tdSql.execute(f'create topic t0 as select * from meters')

        consumer_dict = {
            "group.id": "g0",
            "td.connect.user": "root",
            "td.connect.pass": "taosdata",
            "auto.offset.reset": "earliest",
            "session.timeout.ms": "100000",
            "enable.auto.commit": "false",
            "min.poll.rows": "1",
        }
        consumer = Consumer(consumer_dict)

        consumer.subscribe(["t0"])
        res = consumer.poll(1)
        self.printData(res, 210)

        consumer.close()

        consumer = Consumer(consumer_dict)
        consumer.subscribe(["t0"])
        res = consumer.poll(1)
        self.printData(res, 210) 

        consumer.commit()
        consumer.close()

        consumer = Consumer(consumer_dict)
        consumer.subscribe(["t0"])
        res = consumer.poll(1)
        self.printData(res, 219)    
        consumer.close()

        tdDnodes.stoptaosd(1)
        tdDnodes.starttaosd(1)

        consumer = Consumer(consumer_dict)
        consumer.subscribe(["t0"])
        res = consumer.poll(1)
        self.printData(res, 219) 
        consumer.commit()
        consumer.close()

        cmdStr = 'kill -9 `pgrep taosd`'
        if os.system(cmdStr) != 0:
            tdLog.exit(cmdStr)

        tdDnodes.starttaosd(1)

        consumer = Consumer(consumer_dict)
        consumer.subscribe(["t0"])
        res = consumer.poll(1)
        self.printData(res, 2193) 

    def test_tmq_tx484(self):
        """summary: xxx

        description: xxx

        Since: xxx

        Labels: xxx

        Jira: xxx

        Catalog:
        - xxx:xxx

        History:
        - xxx
        - xxx

        """
        self.check()

        tdLog.success(f"{__file__} successfully executed")

