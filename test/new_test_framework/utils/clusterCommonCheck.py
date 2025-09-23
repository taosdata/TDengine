###################################################################
#           Copyright (c) 2016 by TAOS Technologies, Inc.
#                     All rights reserved.
#
#  This file is proprietary and confidential to TAOS Technologies.
#  No part of this file may be reproduced, stored, transmitted,
#  disclosed or used in any form or by any means other than as
#  expressly provided by the written permission from Jianhui Tao
#
###################################################################

# -*- coding: utf-8 -*-

from collections import defaultdict
import random
import string
import threading
import requests
import time
import taos

from .log import *
from .sql import *
from .server.dnodes import *
from .common import *

# class actionType(Enum):
#     CREATE_DATABASE = 0
#     CREATE_STABLE   = 1
#     CREATE_CTABLE   = 2
#     INSERT_DATA     = 3


class ClusterComCheck:
    def init(self, conn, logSql=False):
        tdSql.init(conn.cursor())
        # tdSql.init(conn.cursor(), logSql)  # output sql.txt file

    def checkDnodes(self, dnodeNum, timeout=100):
        count = 0

        while count < timeout:
            tdSql.query("select * from information_schema.ins_dnodes")
            status = 0
            for i in range(len(tdSql.queryResult)):
                if tdSql.queryResult[i][4] == "ready":
                    status += 1

            if status == dnodeNum:
                tdLog.success(f"{dnodeNum} dnodes ready within {count}s!")
                return True
            else:
                tdLog.info(f"{dnodeNum} dnodes not ready, {status}:{tdSql.queryRows}")

            time.sleep(1)
            count += 1

        else:
            tdSql.query("select * from information_schema.ins_dnodes")
            tdLog.debug(tdSql.queryResult)
            tdLog.exit(f"{dnodeNum} dnodes not ready within {timeout}s!")

    def checkClusterAlive(self, status, timeout=100):
        count = 0

        while count < timeout:
            tdSql.query("show cluster alive")

            if tdSql.queryResult[0][0] == status:
                tdLog.success(
                    "show cluster alive return %d within %ds!" % (status, count)
                )
                return True

            time.sleep(1)
            count += 1

        else:
            tdLog.exit(
                "show cluster alive does not return %d within %ds!" % (status, timeout)
            )

    def checkDbAlive(self, dbname, status, timeout=100):
        count = 0

        while count < timeout:
            tdSql.query(f"show {dbname}.alive")

            if tdSql.queryResult[0][0] == status:
                tdLog.success(
                    "show %s.alive return %d within %ds!" % (dbname, status, count)
                )
                return True

            time.sleep(1)
            count += 1

        else:
            tdLog.exit(
                "show %s.alive does not return %d within %ds!"
                % (dbname, status, timeout)
            )

    def checkDnodeSupportVnodes(self, dnodeIndex, vnodes, timeout=100):
        count = 0

        while count < timeout:
            tdSql.query(
                f"select * from information_schema.ins_dnodes where id = {dnodeIndex}"
            )

            if tdSql.queryResult[0][3] == vnodes:
                tdLog.success(
                    "dnode:%d supportVnodes==%d within %ds!"
                    % (dnodeIndex, vnodes, count)
                )
                return True

            time.sleep(1)
            count += 1

        else:
            tdLog.exit(
                "dnode:%d supportVnodes!=%d does not return within %ds!"
                % (dnodeIndex, vnodes, timeout)
            )

    def checkTransactions(self, timeout=100):
        count = 0

        while count < timeout:
            tdSql.query(f"show transactions")

            if tdSql.queryRows == 0:
                tdLog.success("show transactions return 0 rows within {count}s!")
                return True

            if count % 5 == 0:
                tdLog.info(
                    f"show transactions return {tdSql.queryRows} rows within {count}s!"
                )
            time.sleep(1)
            count += 1

        else:
            tdLog.exit(f"show transactions not return 0 rows within {timeout}s!")

    def checkDbReady(self, dbname, timeout=100):
        count = 0

        while count < timeout:
            tdSql.query(f"show {dbname}.vgroups")

            leaderNum = 0
            for i in range(tdSql.queryRows):
                if (
                    tdSql.queryResult[i][4] == "leader"
                    or tdSql.queryResult[i][7] == "leader"
                    or tdSql.queryResult[i][10] == "leader"
                ):
                    leaderNum = leaderNum + 1
                    tdLog.success(
                        f"db:{dbname} vgId:{tdSql.queryResult[i][0]} has leader within {count}s!, {tdSql.queryResult[i][3]}:{tdSql.queryResult[i][4]}, {tdSql.queryResult[i][6]}:{tdSql.queryResult[i][7]}, {tdSql.queryResult[i][9]}:{tdSql.queryResult[i][10]}"
                    )
                else:
                    tdLog.info(
                        f"db:{dbname} vgId:{tdSql.queryResult[i][0]} no leader within {count}s!"
                    )

            if leaderNum == tdSql.queryRows:
                tdLog.info(
                    f"db:{dbname} vgroups:{tdSql.queryRows} has leader within {count}s!"
                )
                break

            time.sleep(1)
            count += 1

        else:
            tdLog.exit(f"{dbname} not ready within {timeout}s!")

    def checkDbRows(self, dbNumbers):
        dbNumbers = int(dbNumbers)
        count = 0
        while count < 5:
            tdSql.query(
                "select * from information_schema.ins_databases where name!='collectd' ;"
            )
            count += 1
            if tdSql.checkRows(dbNumbers + 2):
                tdLog.success(
                    "we find %d databases and expect %d in clusters! "
                    % (tdSql.queryRows, dbNumbers + 2)
                )
                return True
            else:
                continue
        else:
            tdLog.debug(tdSql.queryResult)
            tdLog.exit(
                "we find %d databases but expect %d in clusters! "
                % (tdSql.queryRows, dbNumbers)
            )

    def checkDb(self, dbNumbers, restartNumber, dbNameIndex, timeout=100):
        count = 0
        alldbNumbers = (dbNumbers * restartNumber) + 2
        while count < timeout:
            query_status = 0
            for j in range(dbNumbers):
                for i in range(alldbNumbers):
                    tdSql.query("select * from information_schema.ins_databases;")
                    if "%s_%d" % (dbNameIndex, j) == tdSql.queryResult[i][0]:
                        if tdSql.queryResult[i][15] == "ready":
                            query_status += 1
                            tdLog.debug(
                                "check %s_%d that status is ready " % (dbNameIndex, j)
                            )
                        else:
                            sleep(1)
                            continue
            # print(query_status)
            if query_status == dbNumbers:
                tdLog.success(
                    " check %d database and  all databases  are ready within %ds! "
                    % (dbNumbers, count + 1)
                )
                return True
            count += 1

        else:
            tdLog.debug(tdSql.queryResult)
            tdLog.debug("query status is %d" % query_status)
            tdLog.exit("database is not ready within %ds" % (timeout + 1))

    def checkData(
        self,
        dbname,
        stbname,
        stableCount,
        CtableCount,
        rowsPerSTable,
    ):
        tdSql.execute("use %s" % dbname)
        tdSql.query("show %s.stables" % dbname)
        tdSql.checkRows(stableCount)
        tdSql.query("show  %s.tables" % dbname)
        tdSql.checkRows(CtableCount)
        for i in range(stableCount):
            tdSql.query("select count(*) from %s%d" % (stbname, i))
            tdSql.checkData(0, 0, rowsPerSTable)
        return

    def checkMnodeStatus(self, mnodeNum, checkFollower=True):
        tdLog.debug(f"check mnodes:{mnodeNum} status")
        count = 0

        while count < 30:
            time.sleep(1)
            tdSql.query("select * from information_schema.ins_mnodes;")
            if tdSql.checkRows(mnodeNum):
                tdLog.success("cluster has %d mnodes" % mnodeNum)

            if mnodeNum == 1:
                tdLog.info(f"{tdSql.queryResult[0][2]}")
                if tdSql.queryResult[0][2] == "leader":
                    tdLog.success(f"{mnodeNum} mnodes ready in {count}s")
                    return True
                count += 1
            elif mnodeNum == 3:
                tdLog.info(
                    f"{tdSql.queryResult[0][2]}, {tdSql.queryResult[1][2]}, {tdSql.queryResult[2][2]}"
                )
                if tdSql.queryResult[0][2] == "leader":
                    if not checkFollower:
                        tdLog.success(f"{mnodeNum} mnodes ready in {count}s")
                        return True
                    elif tdSql.queryResult[1][2] == "follower":
                        if tdSql.queryResult[2][2] == "follower":
                            tdLog.success(f"{mnodeNum} mnodes ready in {count}s")
                            return True
                elif tdSql.queryResult[1][2] == "leader":
                    if not checkFollower:
                        tdLog.success(f"{mnodeNum} mnodes ready in {count}s")
                        return True
                    elif tdSql.queryResult[0][2] == "follower":
                        if tdSql.queryResult[2][2] == "follower":
                            tdLog.success(f"{mnodeNum} mnodes ready in {count}s")
                            return True
                elif tdSql.queryResult[2][2] == "leader":
                    if not checkFollower:
                        tdLog.success(f"{mnodeNum} mnodes ready in {count}s")
                        return True
                    elif tdSql.queryResult[0][2] == "follower":
                        if tdSql.queryResult[1][2] == "follower":
                            tdLog.success(f"{mnodeNum} mnodes ready in {count}s")
                            return True
                count += 1
            elif mnodeNum == 2:
                tdLog.info(f"{tdSql.queryResult[0][2]}, {tdSql.queryResult[1][2]}")
                if tdSql.queryResult[0][2] == "leader":
                    if not checkFollower:
                        tdLog.success(f"{mnodeNum} mnodes ready in {count}s")
                        return True
                    elif tdSql.queryResult[1][2] == "follower":
                        tdLog.success(f"{mnodeNum} mnodes ready in {count}s")
                        return True
                elif tdSql.queryResult[1][2] == "leader":
                    if not checkFollower:
                        tdLog.success(f"{mnodeNum} mnodes ready in {count}s")
                        return True
                    elif tdSql.queryResult[0][2] == "follower":
                        tdLog.success(f"{mnodeNum} mnodes ready in {count}s")
                        return True
                count += 1
        else:
            tdLog.debug(tdSql.queryResult)
            tdLog.exit(f"{mnodeNum} mnodes not ready in {count}s")

    def check3mnodeoff(self, offlineDnodeNo, mnodeNum=3):
        count = 0
        while count < 30:
            time.sleep(1)
            tdSql.query("select * from information_schema.ins_mnodes;")
            if tdSql.checkRows(mnodeNum):
                tdLog.success("cluster has %d mnodes" % mnodeNum)
            else:
                tdLog.exit("mnode number is correct")
            if offlineDnodeNo == 1:
                if tdSql.queryResult[0][2] == "offline":
                    if tdSql.queryResult[1][2] == "leader":
                        if tdSql.queryResult[2][2] == "follower":
                            tdLog.success(
                                "stop mnodes  on dnode %d  successfully in 10s"
                                % offlineDnodeNo
                            )
                            return True
                    elif tdSql.queryResult[1][2] == "follower":
                        if tdSql.queryResult[2][2] == "leader":
                            tdLog.debug(
                                "stop mnodes  on dnode %d  successfully in 10s"
                                % offlineDnodeNo
                            )
                            return True
                count += 1
            elif offlineDnodeNo == 2:
                if tdSql.queryResult[1][2] == "offline":
                    if tdSql.queryResult[0][2] == "leader":
                        if tdSql.queryResult[2][2] == "follower":
                            tdLog.debug(
                                "stop mnodes  on dnode %d  successfully in 10s"
                                % offlineDnodeNo
                            )
                            return True
                    elif tdSql.queryResult[0][2] == "follower":
                        if tdSql.queryResult[2][2] == "leader":
                            tdLog.debug(
                                "stop mnodes  on dnode %d  successfully in 10s"
                                % offlineDnodeNo
                            )
                            return True
                count += 1
            elif offlineDnodeNo == 3:
                if tdSql.queryResult[2][2] == "offline":
                    if tdSql.queryResult[0][2] == "leader":
                        if tdSql.queryResult[1][2] == "follower":
                            tdLog.debug(
                                "stop mnodes  on dnode %d  successfully in 10s"
                                % offlineDnodeNo
                            )
                            return True
                    elif tdSql.queryResult[0][2] == "follower":
                        if tdSql.queryResult[1][2] == "leader":
                            tdLog.debug(
                                "stop mnodes  on dnode %d  successfully in 10s"
                                % offlineDnodeNo
                            )
                            return True
                count += 1
        else:
            tdLog.debug(tdSql.queryResult)
            tdLog.exit(f"stop mnodes  on dnode {offlineDnodeNo}  failed in 10s ")

    def check3mnode2off(self, mnodeNum=3):
        count = 0
        while count < 30:
            time.sleep(1)
            tdSql.query("select * from information_schema.ins_mnodes;")
            if tdSql.checkRows(mnodeNum):
                tdLog.success("cluster has %d mnodes" % mnodeNum)
            else:
                tdLog.exit("mnode number is correct")
            if tdSql.queryResult[0][2] == "leader":
                if tdSql.queryResult[1][2] == "offline":
                    if tdSql.queryResult[2][2] == "offline":
                        tdLog.success(
                            "stop mnodes of follower  on dnode successfully in 10s"
                        )
                        return True
            count += 1
        else:
            tdLog.debug(tdSql.queryResult)
            tdLog.exit("stop mnodes  on dnode 2 or 3 failed in 10s")

    def check_vgroups_status_with_offline(
        self, vgroup_numbers=2, db_replica=3, count_number=10, db_name="db"
    ):
        """
        n nodes cluster, 3 replica database
        return 1, n leaders, stable status
        return 2, 0 < num of leader < n, stable status
        return 0, no leader, stable status
        return -1, Elections not yet completed, unstable status
        """
        vgroup_numbers = int(vgroup_numbers)
        self.db_replica = int(db_replica)
        tdLog.debug("start to check status of vgroups")
        count = 0
        leader_number = 0
        while count < count_number:
            time.sleep(1)
            count += 1
            tdSql.query(f"show {db_name}.vgroups;")
            if tdSql.getRows() != vgroup_numbers:
                continue
            for i in range(vgroup_numbers):
                print(tdSql.queryResult[i])
                if "leader" in tdSql.queryResult[i]:
                    leader_number += 1
                elif (
                    tdSql.queryResult[i].count("follower")
                    + tdSql.queryResult[i].count("candidate")
                    >= 2
                ):
                    tdLog.debug("Elections not yet completed")
                    return -1
                else:  # only one 'follower' or 'offline'
                    tdLog.debug(
                        "Not in compliance with Raft protocol, unable to complete election"
                    )
            if leader_number == vgroup_numbers:
                tdLog.debug("Leader election for all vgroups completed")
                return 1
            elif leader_number == 0:
                tdLog.debug("all vnodes is follower")
                return 0
            else:
                tdLog.debug(
                    f"there is {vgroup_numbers} vgroups, and leader elections for {leader_number} vgroups competed"
                )
                return 2
        else:
            tdLog.debug(tdSql.queryResult)
            tdLog.notice(
                f"elections of {db_name} all vgroups with replica {self.db_replica}  are failed in {count} s "
            )

    def check_vgroups_status(
        self, vgroup_numbers=2, db_replica=3, count_number=10, db_name="db"
    ):
        """check vgroups status in 10s after db vgroups status is changed"""
        vgroup_numbers = int(vgroup_numbers)
        self.db_replica = int(db_replica)
        tdLog.debug("start to check status of vgroups")
        count = 0
        last_number = vgroup_numbers - 1
        while count < count_number:
            time.sleep(1)
            count += 1
            print("check vgroup count :", count)
            tdSql.query(f"show  {db_name}.vgroups;")
            if tdSql.getRows() != vgroup_numbers:
                continue
            if self.db_replica == 1:
                if (
                    tdSql.queryResult[0][4] == "leader"
                    and tdSql.queryResult[last_number][4] == "leader"
                ):
                    tdSql.query(
                        f"select `replica` from information_schema.ins_databases where `name`='{db_name}';"
                    )
                    print("db replica :", tdSql.queryResult[0][0])
                    if tdSql.queryResult[0][0] == db_replica:
                        tdLog.success(
                            f"all vgroups with replica {self.db_replica} of {db_name} are leaders in {count} s"
                        )
                        return True

            elif self.db_replica == 3:
                vgroup_status_first = [
                    tdSql.queryResult[0][4],
                    tdSql.queryResult[0][7],
                    tdSql.queryResult[0][10],
                ]

                vgroup_status_last = [
                    tdSql.queryResult[last_number][4],
                    tdSql.queryResult[last_number][7],
                    tdSql.queryResult[last_number][10],
                ]
                if (
                    vgroup_status_first.count("leader") == 1
                    and vgroup_status_first.count("follower") == 2
                ):
                    if (
                        vgroup_status_last.count("leader") == 1
                        and vgroup_status_last.count("follower") == 2
                    ):
                        tdSql.query(
                            f"select `replica` from information_schema.ins_databases where `name`='{db_name}';"
                        )
                        print("db replica :", tdSql.queryResult[0][0])
                        if tdSql.queryResult[0][0] == db_replica:
                            tdLog.success(
                                f"elections of {db_name}.vgroups with replica {self.db_replica}  are ready in {count} s"
                            )
                            return True
        else:
            tdLog.debug(tdSql.queryResult)
            tdLog.notice(
                f"elections of {db_name} all vgroups with replica {self.db_replica}  are failed in {count} s "
            )
            caller = inspect.getframeinfo(inspect.stack()[1][0])
            args = (caller.filename, caller.lineno)
            tdLog.exit("%s(%d) failed " % args)

    def close(self):
        self.cursor.close()


clusterComCheck = ClusterComCheck()
