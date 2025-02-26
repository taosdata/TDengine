# tests/test_new/xxx/xxx/test_xxx.py
# import ...
'''
./pytest.sh python3 ./test.py -f storage/compact/test_compact_meta.py
'''

import taos
import sys
from math import inf

from util.dnodes import tdDnodes
from util.sql import *
from util.cases import *
from util.log import *

sys.path.append("../tests/pytest")


class TestCompactMeta:
    def caseDescription(self):
        '''
        case1<Hongze Cheng>: [TS-5445] Compact Meta Data
        '''
        return

    def init(self, conn, logSql, replicaVer=1):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor(), True)
        self.conn = conn

    def run(self):
        self.test_case1()

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)

    def test_case1(self):
        """
        Description:
            1. Alter child table tags
            2. Make sure compact meta works
        """
        db_name = 'db1'
        stb_name = 'stb1'
        ctb_name_prefix = 'ctb'
        num_child_tables = 10000

        # Drop database
        sql = f'drop database if exists {db_name}'
        tdSql.execute(sql)

        # Create database
        sql = f'create database {db_name} vgroups 1'
        tdSql.execute(sql)

        # Create super table
        sql = f'create table {db_name}.{stb_name} (ts timestamp, c1 int, c2 int) tags(t1 int)'
        tdSql.execute(sql)

        # Create child tables
        for i in range(1, num_child_tables+1):
            sql = f'create table {db_name}.{ctb_name_prefix}{i} using {db_name}.{stb_name} tags({i})'
            tdSql.execute(sql)

        # Alter child table tags
        for i in range(1, num_child_tables+1):
            sql = f'alter table {db_name}.{ctb_name_prefix}{i} set tag t1 = {i+1}'
            tdSql.execute(sql)

        # Compact meta
        sql = f'compact database {db_name} meta_only'
        tdSql.execute(sql)

    def test_case2(self):
        """
        Description:
            After compact, the snapshot still works
        """
        pass


tdCases.addWindows(__file__, TestCompactMeta())
tdCases.addLinux(__file__, TestCompactMeta())
