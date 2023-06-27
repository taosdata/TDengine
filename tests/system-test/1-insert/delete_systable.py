
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

import random
import string

from numpy import logspace
from util import constant
from util.log import *
from util.cases import *
from util.sql import *
from util.common import *
from util.sqlset import TDSetSql

info_schema_db = "information_schema"
perf_schema_db = "performance_schema"

info_schema_tables = [
    "ins_dnodes",
    "ins_mnodes",
    "ins_modules",
    "ins_qnodes",
    "ins_snodes",
    "ins_cluster",
    "ins_databases",
    "ins_functions",
    "ins_indexes",
    "ins_stables",
    "ins_tables",
    "ins_tags",
    "ins_columns",
    "ins_users",
    "ins_grants",
    "ins_vgroups",
    "ins_configs",
    "ins_dnode_variables",
    "ins_topics",
    "ins_subscriptions",
    "ins_streams",
    "ins_streams_tasks",
    "ins_vnodes",
    "ins_user_privileges"
]

perf_schema_tables = [
    "perf_connections",
    "perf_queries",
    "perf_consumers",
    "perf_trans",
    "perf_apps"
]

class TDTestCase:
    def init(self, conn, logSql, replicaVar=1):
        self.replicaVar = int(replicaVar)
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor())

    def delete_systb(self):
        tdSql.execute(f'use {info_schema_db}')
        for i in info_schema_tables:
            tdSql.error(f'delete from {i}')
            tdSql.error(f'delete from {info_schema_db}.{i}')
            tdSql.error(f'delete from {i} where ts >= 0')
            tdSql.error(f'delete from {info_schema_db}.{i} where ts >= 0')

        tdSql.execute(f'use {perf_schema_db}')
        for i in perf_schema_tables:
            tdSql.error(f'delete from {i}')
            tdSql.error(f'delete from {perf_schema_db}.{i}')
            tdSql.error(f'delete from {i} where ts >= 0')
            tdSql.error(f'delete from {perf_schema_db}.{i} where ts >= 0')

    def drop_systb(self):
        tdSql.execute(f'use {info_schema_db}')
        for i in info_schema_tables:
            tdSql.error(f'drop table {i}')
            tdSql.error(f'drop {info_schema_db}.{i}')
        tdSql.error(f'drop database {info_schema_db}')

        tdSql.execute(f'use {perf_schema_db}')
        for i in perf_schema_tables:
            tdSql.error(f'drop table {i}')
            tdSql.error(f'drop table {perf_schema_db}.{i}')
        tdSql.error(f'drop database {perf_schema_db}')

    def delete_from_systb(self):
        self.delete_systb()
        self.drop_systb()
    def run(self):
        self.delete_from_systb()
        tdDnodes.stoptaosd(1)
        tdDnodes.starttaosd(1)
        self.delete_from_systb()
    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)

tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
