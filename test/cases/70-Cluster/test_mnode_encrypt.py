from new_test_framework.utils import tdLog, tdSql
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
from cluster_common_create import *
from cluster_common_check import *


class TestMnodeEncrypt:

    def setup_class(cls):
        tdLog.debug(f"start to excute {__file__}")


    def test_mnode_encrypt(self):
        """Cluster mnode encrypt

        1. Create database db
        2. Create stable st with tags
        3. Create 4 child tables t0,t1,t2,t3 using st
        4. Check create child tables number is 4
        5. Insert data into t0,t1,t2,t3
        6. Query and check data from t0,t1,t2,t3


        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-11-01 Alex Duan Migrated from uncatalog/system-test/6-cluster/test_compact_db_conflict.py

        """
        tdSql.execute('create database if not exists db')
        tdSql.execute('use db')
        tdSql.execute('create table st (ts timestamp, i int, j float, k double) tags(a int)')
        
        for i in range(0, 2):
            tdSql.execute("create table if not exists db.t%d using db.st tags(%d)" % (i, i))
        

        for i in range(2, 4):
            tdSql.execute("create table if not exists db.t%d using db.st tags(%d)" % (i, i))

        sql = "show db.tables"
        tdSql.query(sql)
        tdSql.checkRows(4)
        
        timestamp = 1530374400000
        for i in range (4) :
            val = i
            sql = "insert into db.t%d values(%d, %d, %d, %d)" % (i, timestamp, val, val, val)
            tdSql.execute(sql)

        for i in range ( 4) :
            val = i
            sql = "select * from db.t%d" % (i)
            tdSql.query(sql)
            tdSql.checkRows(1)

        tdLog.success(f"{__file__} successfully executed")