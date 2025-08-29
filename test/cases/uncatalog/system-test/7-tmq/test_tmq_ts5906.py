from new_test_framework.utils import tdLog, tdSql
from taos.tmq import *
from taos import *


class TestCase:
    updatecfgDict = {'debugFlag': 143, 'asynclog': 0}

    def setup_class(cls):
        tdLog.debug(f"start to excute {__file__}")

    def check(self):
        tdSql.execute(f'create database if not exists db vgroups 1')
        tdSql.execute(f'use db')
        tdSql.execute(f'CREATE STABLE meters (ts TIMESTAMP, current FLOAT, voltage INT, phase FLOAT) TAGS (location BINARY(64), groupId INT)')
        tdSql.execute("INSERT INTO d1001 USING meters TAGS('California.SanFrancisco1', 2) VALUES('2018-10-05 14:38:05.000',10.30000,219,0.31000)")


        tdSql.execute(f'create topic t0 as select * from meters')

        consumer_dict = {
            "group.id": "g1",
            "td.connect.user": "root",
            "td.connect.pass": "taosdata",
            "auto.offset.reset": "earliest",
        }
        consumer = Consumer(consumer_dict)

        try:
            consumer.subscribe(["t0"])
        except TmqError:
            tdLog.exit(f"subscribe error")

        index = 0;
        try:
            while True:
                if index == 2:
                    break
                res = consumer.poll(5)
                print(res)
                if not res:
                    print("res null")
                    break
                val = res.value()
                if val is None:
                    continue
                for block in val:
                    data = block.fetchall()
                    for element in data:
                        print(f"data len: {len(data)}")
                        print(element)
                    if index == 0 and data[0][-1] != 2:
                        tdLog.exit(f"error: {data[0][-1]}")
                    if index == 1 and data[0][-1] != 100:
                        tdLog.exit(f"error: {data[0][-1]}")

                tdSql.execute("alter table d1001 set tag groupId = 100")
                tdSql.execute("INSERT INTO d1001 VALUES('2018-10-05 14:38:06.000',10.30000,219,0.31000)")
                index += 1
        finally:
            consumer.close()


    def test_tmq_replay(self):
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

