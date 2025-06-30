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


class StreamUtil:
    def createSnode(self, index=1):
        sql = f"create snode on dnode {index}"
        tdSql.execute(sql)

        tdSql.query("show snodes")
        tdSql.checkKeyExist(index)

    def dropSnode(self, index=1):
        sql = f"drop snode on dnode {index}"
        tdSql.query(sql)

    def checkStreamStatus(self, stream_name=""):
        return

        for loop in range(60):
            if stream_name == "":
                tdSql.query(f"select * from information_schema.ins_stream_tasks")
                if tdSql.getRows() == 0:
                    continue
                tdSql.query(
                    f'select * from information_schema.ins_stream_tasks where status != "ready"'
                )
                if tdSql.getRows() == 0:
                    return
            else:
                tdSql.query(
                    f'select stream_name, status from information_schema.ins_stream_tasks where stream_name = "{stream_name}" and status == "ready"'
                )
                if tdSql.getRows() == 1:
                    return
            time.sleep(1)

        tdLog.exit(f"stream task status not ready in {loop} seconds")

    def dropAllStreamsAndDbs(self):
        streamList = tdSql.query("show streams", row_tag=True)
        for r in range(len(streamList)):
            tdSql.execute(f"drop stream {streamList[r][0]}")

        dbList = tdSql.query("show databases", row_tag=True)
        for r in range(len(dbList)):
            if (
                dbList[r][0] != "information_schema"
                and dbList[r][0] != "performance_schema"
            ):
                tdSql.execute(f"drop database {dbList[r][0]}")

        tdLog.info(f"drop {len(dbList)} databases, {len(streamList)} streams")


tdStream = StreamUtil()
