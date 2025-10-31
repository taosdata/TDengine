import time
import math
import random
from new_test_framework.utils import (
    tdLog,
    tdSql,
    tdStream,
    StreamTableType,
    StreamTable,
)
from random import randint


class TestSnodeReplicas:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_snode_mgmt_replicas(self):
        """Snode: replica test

        Test the failover of 2-replica snodes.

        Catalog:
            - Streams:Snode

        Since: v3.3.3.7

        Labels: common,ci

        Jira: None

        History:
            - 2025-6-18 dapan Created

        """

        tdStream.dropAllStreamsAndDbs()

        self.createSnodeTest()
        self.dropSnodeTest()
        self.mixedSnodeTest()

    def checkSnodeIdList(self, idList, excludeList):
        for i in idList:
            if i in excludeList:
                raise Exception(
                    f"snode idList error:{idList}, excludeList:{excludeList}"
                )

    def checkSnodeIdListMatch(self, idList, expectedList):
        if len(idList) != len(expectedList):
            raise Exception(
                f"snode idList length error:{idList}, expectedList:{expectedList}"
            )

        for i in idList:
            if i not in expectedList:
                raise Exception(
                    f"snode idList error:{idList}, expectedList:{expectedList}"
                )

    def checkSnodeReplica(self, replicaList, expectedList):
        if 1 == len(replicaList) and 0 != replicaList[0]:
            raise Exception(
                f"snode replicaList error:{replicaList}, expectedList:{expectedList}"
            )

        if 1 == len(replicaList):
            return

        for i in replicaList:
            if i not in expectedList:
                raise Exception(
                    f"snode replicaList error:{replicaList}, expectedList:{expectedList}"
                )

    def checkSnodeAsReplicaOf(self, asReplicaOf, expectedList):
        asReplicaOfList = list()
        noneVal = 0
        for element in asReplicaOf:
            if element == "None":
                noneVal += 1
            else:
                replicaNum = 0
                for item in element.split(","):
                    replicaNum += 1
                    asReplicaOfList.append(int(item))
                if replicaNum > 2 or replicaNum <= 0:
                    raise Exception(f"snode asReplicaOf error:{element}")

        # 检查是否有重复值
        if len(asReplicaOfList) != len(set(asReplicaOfList)):
            raise Exception(
                f"snode asReplicaOfList contains duplicates: {asReplicaOfList}"
            )

        asReplicaOfList.sort()
        if len(expectedList) > 1 and asReplicaOfList != expectedList:
            raise Exception(
                f"snodes asReplicaOf error:{asReplicaOf}, list:{asReplicaOfList}, expectedList:{expectedList}, nonVal:{noneVal}"
            )
        if 1 == len(expectedList) and 0 != len(asReplicaOfList):
            raise Exception(
                f"snodes asReplicaOf error:{asReplicaOf}, list:{asReplicaOfList}, expectedList:{expectedList}, nonVal:{noneVal}"
            )

    @staticmethod
    def generate_random_list(count, start=1, end=8):
        if count > (end - start + 1):
            raise ValueError("Count exceeds the range of unique numbers available.")
        return random.sample(range(start, end + 1), count)

    def createSnodeTest(self):
        tdLog.info(f"create snode test")

        tdStream.createSnode(1)
        tdSql.checkResultsByFunc(
            f"show snodes;",
            lambda: tdSql.getRows() == 1
            and tdSql.getData(0, 0) == 1
            and tdSql.getData(0, 3) == 0
            and tdSql.getData(0, 4) == "None",
        )

        tdStream.createSnode(2)
        tdSql.checkResultsByFunc(
            f"select * from information_schema.ins_snodes order by id;",
            lambda: tdSql.getRows() == 2
            and tdSql.getData(0, 0) == 1
            and tdSql.getData(0, 3) == 2
            and tdSql.getData(0, 4) == "2"
            and tdSql.getData(1, 0) == 2
            and tdSql.getData(1, 3) == 1
            and tdSql.getData(1, 4) == "1",
        )

        tdStream.createSnode(3)
        tdStream.createSnode(4)
        tdStream.createSnode(5)
        tdStream.createSnode(6)
        tdStream.createSnode(7)
        tdStream.createSnode(8)

        tdSql.query("select * from information_schema.ins_snodes order by id;")
        tdSql.checkRows(8)
        ids = tdSql.getColData(0)
        if ids != list(range(1, 9)):
            raise Exception(f"snode list error:{ids}")
        replicaList = tdSql.getColData(3)
        asReplicaOf = tdSql.getColData(4)
        print(f"replicaList: {replicaList}")
        print(f"asReplicaOf: {asReplicaOf}")
        self.checkSnodeReplica(replicaList, list(range(1, 9)))
        self.checkSnodeAsReplicaOf(asReplicaOf, list(range(1, 9)))

        tdLog.info("======over")

    def dropSnodeTest(self):
        tdLog.info(f"drop snode test")

        for i in range(1, 10):
            random_list = self.generate_random_list(randint(1, 8))
            print(f"{i}th drop test, dropList:{random_list}")
            for m in random_list:
                tdStream.dropSnode(m)

            tdSql.query("select * from information_schema.ins_snodes order by id;")
            tdSql.checkRows(8 - len(random_list))
            ids = tdSql.getColData(0)
            self.checkSnodeIdList(ids, random_list)

            replicaList = tdSql.getColData(3)
            asReplicaOf = tdSql.getColData(4)
            print(f"replicaList: {replicaList}")
            print(f"asReplicaOf: {asReplicaOf}")
            self.checkSnodeReplica(replicaList, ids)
            self.checkSnodeAsReplicaOf(asReplicaOf, ids)

            for m in random_list:
                tdStream.createSnode(m)

            tdSql.query("select * from information_schema.ins_snodes order by id;")
            tdSql.checkRows(8)

        tdLog.info("======over")

    def mixedSnodeTest(self):
        tdLog.info(f"mixed snode test")

        snodeList = list(range(1, 9))
        for i in range(1, 100):
            if len(snodeList) < 1:
                snodeId = randint(1, 8)
                snodeList.append(snodeId)
                tdStream.createSnode(snodeId)
            elif len(snodeList) >= 8:
                snodeId = randint(1, 8)
                snodeList.remove(snodeId)
                tdStream.dropSnode(snodeId)
            else:
                if randint(0, 1) == 0:
                    snodeId = randint(1, 8)
                    if snodeId in snodeList:
                        tdSql.error(f"create snode on dnode {snodeId}")
                        print(f"{i}th mixed test, create snode on {snodeId} error")
                        continue

                    snodeList.append(snodeId)
                    tdStream.createSnode(snodeId)
                else:
                    snodeId = randint(1, 8)
                    if snodeId not in snodeList:
                        tdSql.error(f"drop snode on dnode {snodeId}")
                        print(f"{i}th mixed test, drop snode on {snodeId} error")
                        continue

                    snodeList.remove(snodeId)
                    tdStream.dropSnode(snodeId)

            print(f"{i}th mixed test, snodeList:{snodeList}")

            tdSql.query("select * from information_schema.ins_snodes order by id;")
            tdSql.checkRows(len(snodeList))
            ids = tdSql.getColData(0)
            self.checkSnodeIdListMatch(ids, snodeList)

            replicaList = tdSql.getColData(3)
            asReplicaOf = tdSql.getColData(4)
            print(f"replicaList: {replicaList}")
            print(f"asReplicaOf: {asReplicaOf}")
            self.checkSnodeReplica(replicaList, ids)
            self.checkSnodeAsReplicaOf(asReplicaOf, ids)

        tdLog.info("======over")
