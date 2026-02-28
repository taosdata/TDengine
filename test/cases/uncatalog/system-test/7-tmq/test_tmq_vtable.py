import taos
import sys
import time
import socket
import os
import threading

from new_test_framework.utils import tdLog, tdSql, tdCom

class TestCase:
    updatecfgDict = {'debugFlag': 135, 'asynclog': 0}
    # self.cmdStr = '%s/build/bin/tmq_ts5466'%(buildPath)
    self.cmdStr = '/root/code/TDengine/debug/build/bin/tmq_ts5466'
    
    def setup_class(cls):
        tdLog.debug(f"start to excute {__file__}")

    def prepareData(self):
        tdSql.execute(f'create database if not exists db_src vgroups 1')
        tdSql.execute(f'create database if not exists db_dst  vgroups 1')
        tdSql.execute(f'use db_src')

        # create child table
        tdSql.execute(f'create stable if not exists stb_src (ts timestamp, c1 int, c2 binary(32), c3 nchar(32)) tags (location binary(32))')
        tdSql.execute(f'insert into c_t1 using stb_src tags("c_t1") values(1669092069061, 1, "c_t1", "c_t1")')
        tdSql.execute(f'insert into c_t2 using stb_src tags("c_t2") values(1669092069062, 2, "c_t2", "c_t2")')

        # create normal table
        tdSql.execute(f'create table n_t1 (ts timestamp, c1 int, c2 binary(32), c3 nchar(32))')
        tdSql.execute(f'create table n_t2 (ts timestamp, c1 int, c2 binary(32), c3 nchar(32))') 
        tdSql.execute(f'insert into n_t1 values(1669092069011, 11, "n_t1", "n_t1")')
        tdSql.execute(f'insert into n_t2 values(1669092069012, 22, "n_t2", "n_t2")')

        # create virtual child table
        tdSql.execute(f'create stable if not exists v_stb (ts timestamp, c1 int, c2 binary(32), c3 nchar(32)) tags (location binary(32)) virtual 1')
        tdSql.execute(f'create vtable v_t1 (c1 from db_src.c_t1.c1) using v_stb tags("v_t1")')
        tdSql.execute(f'create vtable v_t2 (c1 from db_src.c_t2.c1, c2 from n_t1.c2, c3 from n_t2.c3) using v_stb tags("v_t2")')

        # create virtual normal table
        tdSql.execute(f'create vtable v_n_t1 (c1 from db_src.c_t1.c1)')
        tdSql.execute(f'create vtable v_n_t2 (c1 from db_src.c_t2.c1, c2 from n_t1.c2, c3 from n_t2.c3)')

        # alter virtual child table
        tdSql.execute(f'alter vtable v_t1 alter column c1 set null')
        tdSql.execute(f'alter vtable v_t2 alter column c3 set c_t2.c3')

        # alter virtual normal table
        tdSql.execute(f'alter vtable v_n_t1 add column c2 binary(32) from n_t1.c2')
        tdSql.execute(f'alter vtable v_n_t1 add column c3 binary(32) from c_t2.c3')
        tdSql.execute(f'alter vtable v_n_t2 alter column c2 set null')
        tdSql.execute(f'alter vtable v_n_t2 alter column c3 set c_t2.c3')

# snapshot = true, stable topic / db topic, check get_raw/tmq_get_json
# snapshot = false, stable topic / db topic, check get_raw/write_raw
# ref is from another db table col, virtual_normal_table, virtual_child_table
# ref is all, or ref is partial
# create vtable, create normal v table.
# add col for normal v table
# alter ref for child vtable

    def run_cmd(self, topic, snapshot):
        # buildPath = tdCom.getBuildPath()
        # cmdStr = '%s/build/bin/tmq_ts5466'%(buildPath)
        cmdStr = f'/root/code/TDengine/debug/build/bin/tmq_ts5466 {topic} {snapshot}'
        tdLog.info(cmdStr)
        os.system(cmdStr)

    def case_snapshot_db_topic(self):
        topicName = "snapshot_db_topic"
        tdSql.execute(f"create topic {topicName} with meta as database db_src")
        self.run_cmd(topicName, "true")

    def case_snapshot_stb_topic(self):
        topicName = "snapshot_stb_topic"
        tdSql.execute(f"create topic {topicName} with meta as stable v_stb")
        self.run_cmd(topicName, "true")

    def case_non_snapshot_db_topic(self):
        topicName = "non_snapshot_db_topic"
        tdSql.execute(f"create topic {topicName} with meta as database db_src")
        self.run_cmd(topicName, "false")

    def case_non_snapshot_stb_topic(self):
        topicName = "non_snapshot_stb_topic"
        tdSql.execute(f"create topic {topicName} with meta as stable v_stb")
        self.run_cmd(topicName, "false")

    def test_tmq_vtable(self):
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
        
        self.prepareData()
        self.case_snapshot_db_topic()
        self.case_snapshot_stb_topic()
        self.case_non_snapshot_db_topic()
        self.case_non_snapshot_stb_topic()
        
        tdLog.success(f"{__file__} successfully executed")
