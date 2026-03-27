import taos
import sys
import time
import socket
import os
import threading
import subprocess

from new_test_framework.utils import tdLog, tdSql, tdDnodes, tdCom

class TestCase:
    clientCfgDict = {'debugFlag': 135}
    updatecfgDict = {'debugFlag': 135, 'asynclog': 0}
    updatecfgDict["clientCfg"] = clientCfgDict
    simClientCfg = ""

    def setup_class(cls):
        tdLog.debug(f"start to excute {__file__}")

    def prepareData(self):
        tdSql.execute(f'create database if not exists db_src vgroups 1')
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
        tdSql.execute(f'create vtable v_c_t1 (c1 from db_src.c_t1.c1) using v_stb tags("v_c_t1")')
        tdSql.execute(f'create vtable v_c_t2 (c1 from db_src.c_t2.c1, c2 from n_t1.c2, c3 from n_t2.c3) using v_stb tags("v_c_t2")')

        # create virtual normal table
        tdSql.execute(f'create vtable v_n_t1 (ts timestamp, c1 int from db_src.c_t1.c1)')
        tdSql.execute(f'create vtable v_n_t2 (ts timestamp, c1 int from db_src.c_t2.c1, c2 binary(32) from n_t1.c2, c3 nchar(32) from n_t2.c3)')

        # alter virtual child table
        tdSql.execute(f'alter vtable v_c_t1 alter column c1 set null')
        tdSql.execute(f'alter vtable v_c_t2 alter column c3 set c_t2.c3')

        # alter virtual normal table
        tdSql.execute(f'alter vtable v_n_t1 add column c2 binary(32) from n_t1.c2')
        tdSql.execute(f'alter vtable v_n_t1 add column c3 nchar(32) from c_t2.c3')
        tdSql.execute(f'alter vtable v_n_t2 alter column c2 set null')
        tdSql.execute(f'alter vtable v_n_t2 alter column c3 set c_t2.c3')

    def writeDataAgain(self):
        # create virtual child table
        tdSql.execute(f'create stable if not exists v_stb1 (ts timestamp, c1 int, c2 binary(32), c3 nchar(32)) tags (location binary(32)) virtual 1')
        tdSql.execute(f'create vtable v_c_t11 (c1 from db_src.c_t1.c1) using v_stb1 tags("v_c_t1")')
        tdSql.execute(f'create vtable v_c_t12 (c1 from db_src.c_t2.c1, c2 from n_t1.c2, c3 from n_t2.c3) using v_stb1 tags("v_c_t2")')

        # create virtual normal table
        tdSql.execute(f'create vtable v_n_t11 (ts timestamp, c1 int from db_src.c_t1.c1)')
        tdSql.execute(f'create vtable v_n_t12 (ts timestamp, c1 int from db_src.c_t2.c1, c2 binary(32) from n_t1.c2, c3 nchar(32) from n_t2.c3)')

        # alter virtual child table
        tdSql.execute(f'alter vtable v_c_t1 alter column c1 set db_src.c_t2.c1')
        tdSql.execute(f'alter vtable v_c_t2 alter column c3 set c_t1.c3')

        # alter virtual normal table
        tdSql.execute(f'alter vtable v_n_t1 add column c4 binary(32) from n_t1.c2')
        tdSql.execute(f'alter vtable v_n_t1 alter column c3 set null')

    def run_cmd(self, topic, snapshot, json, writeData):
        buildPath = tdCom.getBuildPath()
        cmdStr = f'{buildPath}/build/bin/tmq_vtable'
        subprocess.run([cmdStr, topic, snapshot, json, self.simClientCfg, writeData])

    def case_snapshot_db_topic(self, snapshot_db_topic_json):
        tdLog.info("case_snapshot_db_topic start ...")
        topicName = "snapshot_db_topic"
        tdSql.execute(f"create topic {topicName} with meta as database db_src")
        tdSql.execute(f"create database {topicName} vgroups 1")

        os.system(f"echo '\ntmqWriteRefDB {topicName}' >> {self.simClientCfg}")
        os.system(f"echo '\ntmqWriteCheckRef true' >> {self.simClientCfg}")

        self.run_cmd(topicName, "true", snapshot_db_topic_json, "true")
        self.checkDataVChildTable(topicName)
        self.checkDataVNormalTable(topicName)

        self.checkDataVChildAgainTable(topicName)
        self.checkDataVNormalAgainTable(topicName)
        tdLog.info("case_snapshot_db_topic end ...")

    def case_snapshot_stb_topic(self, snapshot_stb_topic_json):
        tdLog.info("case_snapshot_stb_topic start ...")
        topicName = "snapshot_stb_topic"
        tdSql.execute(f"create topic {topicName} with meta as stable v_stb")
        tdSql.execute(f"create database {topicName} vgroups 1")

        os.system(f"echo '\ntmqWriteRefDB db_src' >> {self.simClientCfg}")
        os.system(f"echo '\ntmqWriteCheckRef true' >> {self.simClientCfg}")

        self.run_cmd(topicName, "true", snapshot_stb_topic_json, "true")
        self.checkDataVChildTable(topicName)
        tdLog.info("case_snapshot_stb_topic end ...")

    def case_non_snapshot_db_topic(self):
        tdLog.info("case_non_snapshot_db_topic start ...")
        non_snapshot_db_topic_json = '{"type":"create","tableType":"super","tableName":"stb_src","columns":[{"name":"ts","type":9,"isPrimarykey":false,"encode":"delta-i","compress":"lz4","level":"medium"},{"name":"c1","type":4,"isPrimarykey":false,"encode":"simple8b","compress":"lz4","level":"medium"},{"name":"c2","type":8,"length":32,"isPrimarykey":false,"encode":"disabled","compress":"zstd","level":"medium"},{"name":"c3","type":10,"length":32,"isPrimarykey":false,"encode":"disabled","compress":"zstd","level":"medium"}],"tags":[{"name":"location","type":8,"length":32}]}{"type":"create","tableType":"child","tableName":"c_t1","using":"stb_src","tagNum":1,"tags":[{"name":"location","type":8,"value":"\\"c_t1\\""}],"createList":[{"tableName":"c_t1","using":"stb_src","tagNum":1,"tags":[{"name":"location","type":8,"value":"\\"c_t1\\""}]},{"tableName":"c_t2","using":"stb_src","tagNum":1,"tags":[{"name":"location","type":8,"value":"\\"c_t2\\""}]}]}{"type":"create","tableType":"normal","tableName":"n_t1","columns":[{"name":"ts","type":9,"isPrimarykey":false,"encode":"delta-i","compress":"lz4","level":"medium"},{"name":"c1","type":4,"isPrimarykey":false,"encode":"simple8b","compress":"lz4","level":"medium"},{"name":"c2","type":8,"length":32,"isPrimarykey":false,"encode":"disabled","compress":"zstd","level":"medium"},{"name":"c3","type":10,"length":32,"isPrimarykey":false,"encode":"disabled","compress":"zstd","level":"medium"}],"tags":[]}{"type":"create","tableType":"normal","tableName":"n_t2","columns":[{"name":"ts","type":9,"isPrimarykey":false,"encode":"delta-i","compress":"lz4","level":"medium"},{"name":"c1","type":4,"isPrimarykey":false,"encode":"simple8b","compress":"lz4","level":"medium"},{"name":"c2","type":8,"length":32,"isPrimarykey":false,"encode":"disabled","compress":"zstd","level":"medium"},{"name":"c3","type":10,"length":32,"isPrimarykey":false,"encode":"disabled","compress":"zstd","level":"medium"}],"tags":[]}{"type":"create","tableType":"super","isVirtual":true,"tableName":"v_stb","columns":[{"name":"ts","type":9,"isPrimarykey":false,"encode":"delta-i","compress":"lz4","level":"medium"},{"name":"c1","type":4,"isPrimarykey":false,"encode":"simple8b","compress":"lz4","level":"medium"},{"name":"c2","type":8,"length":32,"isPrimarykey":false,"encode":"disabled","compress":"zstd","level":"medium"},{"name":"c3","type":10,"length":32,"isPrimarykey":false,"encode":"disabled","compress":"zstd","level":"medium"}],"tags":[{"name":"location","type":8,"length":32}]}{"type":"create","tableType":"child","tableName":"v_c_t1","refs":[{"colName":"c1","refDbName":"db_src","refTableName":"c_t1","refColName":"c1"}],"using":"v_stb","tagNum":1,"tags":[{"name":"location","type":8,"value":"\\"v_c_t1\\""}],"createList":[]}{"type":"create","tableType":"child","tableName":"v_c_t2","refs":[{"colName":"c1","refDbName":"db_src","refTableName":"c_t2","refColName":"c1"},{"colName":"c2","refDbName":"db_src","refTableName":"n_t1","refColName":"c2"},{"colName":"c3","refDbName":"db_src","refTableName":"n_t2","refColName":"c3"}],"using":"v_stb","tagNum":1,"tags":[{"name":"location","type":8,"value":"\\"v_c_t2\\""}],"createList":[]}{"type":"create","tableType":"normal","isVirtual":true,"tableName":"v_n_t1","columns":[{"name":"ts","type":9,"isPrimarykey":false,"encode":"delta-i","compress":"lz4","level":"medium"},{"name":"c1","type":4,"ref":{"refDbName":"db_src","refTableName":"c_t1","refColName":"c1"},"isPrimarykey":false,"encode":"simple8b","compress":"lz4","level":"medium"}],"tags":[]}{"type":"create","tableType":"normal","isVirtual":true,"tableName":"v_n_t2","columns":[{"name":"ts","type":9,"isPrimarykey":false,"encode":"delta-i","compress":"lz4","level":"medium"},{"name":"c1","type":4,"ref":{"refDbName":"db_src","refTableName":"c_t2","refColName":"c1"},"isPrimarykey":false,"encode":"simple8b","compress":"lz4","level":"medium"},{"name":"c2","type":8,"length":32,"ref":{"refDbName":"db_src","refTableName":"n_t1","refColName":"c2"},"isPrimarykey":false,"encode":"disabled","compress":"zstd","level":"medium"},{"name":"c3","type":10,"length":32,"ref":{"refDbName":"db_src","refTableName":"n_t2","refColName":"c3"},"isPrimarykey":false,"encode":"disabled","compress":"zstd","level":"medium"}],"tags":[]}{"type":"alter","tableType":"","tableName":"v_c_t1","alterType":17,"colName":"c1"}{"type":"alter","tableType":"","tableName":"v_c_t2","alterType":16,"colName":"c3","refDbName":"db_src","refTbName":"c_t2","refColName":"c3"}{"type":"alter","tableType":"normal","tableName":"v_n_t1","alterType":18,"colName":"c2","colType":8,"colLength":32,"refDbName":"db_src","refTbName":"n_t1","refColName":"c2"}{"type":"alter","tableType":"normal","tableName":"v_n_t1","alterType":18,"colName":"c3","colType":10,"colLength":32,"refDbName":"db_src","refTbName":"c_t2","refColName":"c3"}{"type":"alter","tableType":"","tableName":"v_n_t2","alterType":17,"colName":"c2"}{"type":"alter","tableType":"","tableName":"v_n_t2","alterType":16,"colName":"c3","refDbName":"db_src","refTbName":"c_t2","refColName":"c3"}'
        topicName = "non_snapshot_db_topic"
        tdSql.execute(f"create topic {topicName} with meta as database db_src")
        tdSql.execute(f"create database {topicName} vgroups 1")

        os.system(f"echo '\ntmqWriteRefDB {topicName}' >> {self.simClientCfg}")
        os.system(f"echo '\ntmqWriteCheckRef false' >> {self.simClientCfg}")

        self.run_cmd(topicName, "false", non_snapshot_db_topic_json, "true")
        self.checkDataVChildTable(topicName)
        self.checkDataVNormalTable(topicName)
        tdLog.info("case_non_snapshot_db_topic end ...")

    def case_non_snapshot_stb_topic(self):
        tdLog.info("case_non_snapshot_stb_topic start ...")
        non_snapshot_stb_topic_json = '{"type":"create","tableType":"super","isVirtual":true,"tableName":"v_stb","columns":[{"name":"ts","type":9,"isPrimarykey":false,"encode":"delta-i","compress":"lz4","level":"medium"},{"name":"c1","type":4,"isPrimarykey":false,"encode":"simple8b","compress":"lz4","level":"medium"},{"name":"c2","type":8,"length":32,"isPrimarykey":false,"encode":"disabled","compress":"zstd","level":"medium"},{"name":"c3","type":10,"length":32,"isPrimarykey":false,"encode":"disabled","compress":"zstd","level":"medium"}],"tags":[{"name":"location","type":8,"length":32}]}{"type":"create","tableType":"child","tableName":"v_c_t1","refs":[{"colName":"c1","refDbName":"db_src","refTableName":"c_t1","refColName":"c1"}],"using":"v_stb","tagNum":1,"tags":[{"name":"location","type":8,"value":"\\"v_c_t1\\""}],"createList":[]}{"type":"create","tableType":"child","tableName":"v_c_t2","refs":[{"colName":"c1","refDbName":"db_src","refTableName":"c_t2","refColName":"c1"},{"colName":"c2","refDbName":"db_src","refTableName":"n_t1","refColName":"c2"},{"colName":"c3","refDbName":"db_src","refTableName":"n_t2","refColName":"c3"}],"using":"v_stb","tagNum":1,"tags":[{"name":"location","type":8,"value":"\\"v_c_t2\\""}],"createList":[]}{"type":"alter","tableType":"","tableName":"v_c_t1","alterType":17,"colName":"c1"}{"type":"alter","tableType":"","tableName":"v_c_t2","alterType":16,"colName":"c3","refDbName":"db_src","refTbName":"c_t2","refColName":"c3"}'
        topicName = "non_snapshot_stb_topic"
        tdSql.execute(f"create topic {topicName} with meta as stable v_stb")
        tdSql.execute(f"create database {topicName} vgroups 1")

        os.system(f"echo '\ntmqWriteRefDB db_src' >> {self.simClientCfg}")
        os.system(f"echo '\ntmqWriteCheckRef true' >> {self.simClientCfg}")

        self.run_cmd(topicName, "false", non_snapshot_stb_topic_json, "true")
        self.checkDataVChildTable(topicName)
        tdLog.info("case_non_snapshot_stb_topic end ...")
    
    def case_non_snapshot_no_meta_stb_topic(self):
        tdLog.info("case_non_snapshot_stb_topic start ...")
        non_snapshot_no_meta_stb_json = ''
        topicName = "non_snapshot_no_meta_stb_topic"
        tdSql.execute(f"create topic {topicName} as stable v_stb")
        tdSql.execute(f"create database {topicName} vgroups 1")

        os.system(f"echo '\ntmqWriteRefDB db_src' >> {self.simClientCfg}")

        self.run_cmd(topicName, "false", non_snapshot_no_meta_stb_json, "false")
        tdLog.info("case_non_snapshot_no_meta_stb_topic end ...")
    
    def case_non_snapshot_query_topic(self):
        tdLog.info("case_non_snapshot_query_topic start ...")
        topicName = "non_snapshot_query_topic"
        tdSql.error(f"create topic {topicName} as select * from v_stb")
        tdSql.error(f"create topic {topicName} as select * from v_c_t1")
        tdSql.error(f"create topic {topicName} as select * from v_n_t1")
        tdLog.info("case_non_snapshot_query_topic end ...")

    def case_non_snapshot_check_ref_db_topic(self):
        tdLog.info("case_non_snapshot_check_ref_db_topic start ...")
        topicName = "non_snapshot_check_ref_db_topic"
        tdSql.execute(f"create topic {topicName} with meta as stable v_stb")
        tdSql.execute(f"create database {topicName} vgroups 1")

        os.system(f"echo '\ntmqWriteRefDB {topicName}' >> {self.simClientCfg}")
        os.system(f"echo '\ntmqWriteCheckRef true' >> {self.simClientCfg}")

        buildPath = tdCom.getBuildPath()
        cmdStr = f'{buildPath}/build/bin/tmq_vtable'
        ret = os.system(f"{cmdStr} {topicName} 'false' '' {self.simClientCfg} 'true'")
        if ret == 0 :
            tdLog.exit(cmdStr)

        tdLog.info("case_non_snapshot_check_ref_db_topic end ...")

    def checkDataVChildTable(self, dbName):
        tdSql.checkResultsBySql(
            sql="select * from db_src.v_c_t1",
            exp_sql=f"select * from {dbName}.v_c_t1",
        )
        tdSql.checkResultsBySql(
            sql="select * from db_src.v_c_t2",
            exp_sql=f"select * from {dbName}.v_c_t2",
        )
    
    def checkDataVNormalTable(self, dbName):
        tdSql.checkResultsBySql(
            sql="select * from db_src.v_n_t1",
            exp_sql=f"select * from {dbName}.v_n_t1",
        )
        tdSql.checkResultsBySql(
            sql="select * from db_src.v_n_t2",
            exp_sql=f"select * from {dbName}.v_n_t2",
        )

    def checkDataVChildAgainTable(self, dbName):
        tdSql.checkResultsBySql(
            sql="select * from db_src.v_c_t11",
            exp_sql=f"select * from {dbName}.v_c_t11",
        )
        tdSql.checkResultsBySql(
            sql="select * from db_src.v_c_t12",
            exp_sql=f"select * from {dbName}.v_c_t12",
        )
    
    def checkDataVNormalAgainTable(self, dbName):
        tdSql.checkResultsBySql(
            sql="select * from db_src.v_n_t11",
            exp_sql=f"select * from {dbName}.v_n_t11",
        )
        tdSql.checkResultsBySql(
            sql="select * from db_src.v_n_t12",
            exp_sql=f"select * from {dbName}.v_n_t12",
        )

    def test_tmq_vtable(self):
        """summary: test tmq vtable

        description: 
        snapshot = true, stable topic / db topic, check get_raw/tmq_get_json
        snapshot = false, stable topic / db topic, check get_raw/write_raw
        ref is from another db table col, virtual_normal_table, virtual_child_table
        ref is all, or ref is partial
        create vtable, create normal v table.
        add col for normal v table
        alter ref for child vtable

        Since: 3.4.1.0

        Labels: tmq,vtable

        Jira: https://project.feishu.cn/taosdata_td/feature/detail/6593807450

        Catalog:
        - tmq:vtable

        History:
        - created by WangMingming in 2026.03.02

        """
        tdSql.execute(f'alter dnode 1 "debugflag 135"')
        
        self.prepareData()
        self.simClientCfg = os.path.join(tdDnodes.getSimCfgPath(), "taos.cfg")

        # self.case_non_snapshot_query_topic()
        # self.case_non_snapshot_db_topic()
        self.case_non_snapshot_stb_topic()
        # self.case_non_snapshot_no_meta_stb_topic()

        # tdSql.execute(f'flush database db_src')
        # self.writeDataAgain()

        snapshotVerIsCommitVer = True
        snapshot_db_topic_json = ''
        snapshot_stb_topic     = ''
        if snapshotVerIsCommitVer:
            # handle->snapshotVer = walGetCommittedVer(pTq->pVnode->pWal);
            snapshot_db_topic_json = '{"tmq_meta_version":"1.0","metas":[{"type":"create","tableType":"super","tableName":"stb_src","columns":[{"name":"ts","type":9,"isPrimarykey":false,"encode":"delta-i","compress":"lz4","level":"medium"},{"name":"c1","type":4,"isPrimarykey":false,"encode":"simple8b","compress":"lz4","level":"medium"},{"name":"c2","type":8,"length":32,"isPrimarykey":false,"encode":"disabled","compress":"zstd","level":"medium"},{"name":"c3","type":10,"length":32,"isPrimarykey":false,"encode":"disabled","compress":"zstd","level":"medium"}],"tags":[{"name":"location","type":8,"length":32}]},{"type":"create","tableType":"child","tableName":"c_t1","using":"stb_src","tagNum":1,"tags":[{"name":"location","type":8,"value":"\\"c_t1\\""}],"createList":[]},{"type":"create","tableType":"child","tableName":"c_t2","using":"stb_src","tagNum":1,"tags":[{"name":"location","type":8,"value":"\\"c_t2\\""}],"createList":[]},{"type":"create","tableType":"normal","tableName":"n_t1","columns":[{"name":"ts","type":9,"isPrimarykey":false,"encode":"delta-i","compress":"lz4","level":"medium"},{"name":"c1","type":4,"isPrimarykey":false,"encode":"simple8b","compress":"lz4","level":"medium"},{"name":"c2","type":8,"length":32,"isPrimarykey":false,"encode":"disabled","compress":"zstd","level":"medium"},{"name":"c3","type":10,"length":32,"isPrimarykey":false,"encode":"disabled","compress":"zstd","level":"medium"}],"tags":[]},{"type":"create","tableType":"normal","tableName":"n_t2","columns":[{"name":"ts","type":9,"isPrimarykey":false,"encode":"delta-i","compress":"lz4","level":"medium"},{"name":"c1","type":4,"isPrimarykey":false,"encode":"simple8b","compress":"lz4","level":"medium"},{"name":"c2","type":8,"length":32,"isPrimarykey":false,"encode":"disabled","compress":"zstd","level":"medium"},{"name":"c3","type":10,"length":32,"isPrimarykey":false,"encode":"disabled","compress":"zstd","level":"medium"}],"tags":[]},{"type":"create","tableType":"super","isVirtual":true,"tableName":"v_stb","columns":[{"name":"ts","type":9,"isPrimarykey":false,"encode":"delta-i","compress":"lz4","level":"medium"},{"name":"c1","type":4,"isPrimarykey":false,"encode":"simple8b","compress":"lz4","level":"medium"},{"name":"c2","type":8,"length":32,"isPrimarykey":false,"encode":"disabled","compress":"zstd","level":"medium"},{"name":"c3","type":10,"length":32,"isPrimarykey":false,"encode":"disabled","compress":"zstd","level":"medium"}],"tags":[{"name":"location","type":8,"length":32}]},{"type":"create","tableType":"child","tableName":"v_c_t1","refs":[{"colName":"c1","refDbName":"db_src","refTableName":"c_t2","refColName":"c1"}],"using":"v_stb","tagNum":1,"tags":[{"name":"location","type":8,"value":"\\"v_c_t1\\""}],"createList":[]},{"type":"create","tableType":"child","tableName":"v_c_t2","refs":[{"colName":"c1","refDbName":"db_src","refTableName":"c_t2","refColName":"c1"},{"colName":"c2","refDbName":"db_src","refTableName":"n_t1","refColName":"c2"},{"colName":"c3","refDbName":"db_src","refTableName":"c_t1","refColName":"c3"}],"using":"v_stb","tagNum":1,"tags":[{"name":"location","type":8,"value":"\\"v_c_t2\\""}],"createList":[]},{"type":"create","tableType":"normal","isVirtual":true,"tableName":"v_n_t1","columns":[{"name":"ts","type":9,"isPrimarykey":false,"encode":"delta-i","compress":"lz4","level":"medium"},{"name":"c1","type":4,"ref":{"refDbName":"db_src","refTableName":"c_t1","refColName":"c1"},"isPrimarykey":false,"encode":"simple8b","compress":"lz4","level":"medium"},{"name":"c2","type":8,"length":32,"ref":{"refDbName":"db_src","refTableName":"n_t1","refColName":"c2"},"isPrimarykey":false,"encode":"disabled","compress":"zstd","level":"medium"},{"name":"c3","type":10,"length":32,"isPrimarykey":false,"encode":"disabled","compress":"zstd","level":"medium"},{"name":"c4","type":8,"length":32,"ref":{"refDbName":"db_src","refTableName":"n_t1","refColName":"c2"},"isPrimarykey":false,"encode":"disabled","compress":"zstd","level":"medium"}],"tags":[]},{"type":"create","tableType":"normal","isVirtual":true,"tableName":"v_n_t2","columns":[{"name":"ts","type":9,"isPrimarykey":false,"encode":"delta-i","compress":"lz4","level":"medium"},{"name":"c1","type":4,"ref":{"refDbName":"db_src","refTableName":"c_t2","refColName":"c1"},"isPrimarykey":false,"encode":"simple8b","compress":"lz4","level":"medium"},{"name":"c2","type":8,"length":32,"isPrimarykey":false,"encode":"disabled","compress":"zstd","level":"medium"},{"name":"c3","type":10,"length":32,"ref":{"refDbName":"db_src","refTableName":"c_t2","refColName":"c3"},"isPrimarykey":false,"encode":"disabled","compress":"zstd","level":"medium"}],"tags":[]},{"type":"create","tableType":"super","isVirtual":true,"tableName":"v_stb1","columns":[{"name":"ts","type":9,"isPrimarykey":false,"encode":"delta-i","compress":"lz4","level":"medium"},{"name":"c1","type":4,"isPrimarykey":false,"encode":"simple8b","compress":"lz4","level":"medium"},{"name":"c2","type":8,"length":32,"isPrimarykey":false,"encode":"disabled","compress":"zstd","level":"medium"},{"name":"c3","type":10,"length":32,"isPrimarykey":false,"encode":"disabled","compress":"zstd","level":"medium"}],"tags":[{"name":"location","type":8,"length":32}]},{"type":"create","tableType":"child","tableName":"v_c_t11","refs":[{"colName":"c1","refDbName":"db_src","refTableName":"c_t1","refColName":"c1"}],"using":"v_stb1","tagNum":1,"tags":[{"name":"location","type":8,"value":"\\"v_c_t1\\""}],"createList":[]},{"type":"create","tableType":"child","tableName":"v_c_t12","refs":[{"colName":"c1","refDbName":"db_src","refTableName":"c_t2","refColName":"c1"},{"colName":"c2","refDbName":"db_src","refTableName":"n_t1","refColName":"c2"},{"colName":"c3","refDbName":"db_src","refTableName":"n_t2","refColName":"c3"}],"using":"v_stb1","tagNum":1,"tags":[{"name":"location","type":8,"value":"\\"v_c_t2\\""}],"createList":[]},{"type":"create","tableType":"normal","isVirtual":true,"tableName":"v_n_t11","columns":[{"name":"ts","type":9,"isPrimarykey":false,"encode":"delta-i","compress":"lz4","level":"medium"},{"name":"c1","type":4,"ref":{"refDbName":"db_src","refTableName":"c_t1","refColName":"c1"},"isPrimarykey":false,"encode":"simple8b","compress":"lz4","level":"medium"}],"tags":[]},{"type":"create","tableType":"normal","isVirtual":true,"tableName":"v_n_t12","columns":[{"name":"ts","type":9,"isPrimarykey":false,"encode":"delta-i","compress":"lz4","level":"medium"},{"name":"c1","type":4,"ref":{"refDbName":"db_src","refTableName":"c_t2","refColName":"c1"},"isPrimarykey":false,"encode":"simple8b","compress":"lz4","level":"medium"},{"name":"c2","type":8,"length":32,"ref":{"refDbName":"db_src","refTableName":"n_t1","refColName":"c2"},"isPrimarykey":false,"encode":"disabled","compress":"zstd","level":"medium"},{"name":"c3","type":10,"length":32,"ref":{"refDbName":"db_src","refTableName":"n_t2","refColName":"c3"},"isPrimarykey":false,"encode":"disabled","compress":"zstd","level":"medium"}],"tags":[]}]}'
            snapshot_stb_topic     = '{"tmq_meta_version":"1.0","metas":[{"type":"create","tableType":"super","isVirtual":true,"tableName":"v_stb","columns":[{"name":"ts","type":9,"isPrimarykey":false,"encode":"delta-i","compress":"lz4","level":"medium"},{"name":"c1","type":4,"isPrimarykey":false,"encode":"simple8b","compress":"lz4","level":"medium"},{"name":"c2","type":8,"length":32,"isPrimarykey":false,"encode":"disabled","compress":"zstd","level":"medium"},{"name":"c3","type":10,"length":32,"isPrimarykey":false,"encode":"disabled","compress":"zstd","level":"medium"}],"tags":[{"name":"location","type":8,"length":32}]},{"type":"create","tableType":"child","tableName":"v_c_t1","refs":[{"colName":"c1","refDbName":"db_src","refTableName":"c_t2","refColName":"c1"}],"using":"v_stb","tagNum":1,"tags":[{"name":"location","type":8,"value":"\\"v_c_t1\\""}],"createList":[]},{"type":"create","tableType":"child","tableName":"v_c_t2","refs":[{"colName":"c1","refDbName":"db_src","refTableName":"c_t2","refColName":"c1"},{"colName":"c2","refDbName":"db_src","refTableName":"n_t1","refColName":"c2"},{"colName":"c3","refDbName":"db_src","refTableName":"c_t1","refColName":"c3"}],"using":"v_stb","tagNum":1,"tags":[{"name":"location","type":8,"value":"\\"v_c_t2\\""}],"createList":[]}]}'
        else:
            # if snapshotVerIsCommitVer == False,should change code in tqMeta.c:handle->snapshotVer = walGetSnapshotVer(pTq->pVnode->pWal);
            snapshot_db_topic_json = '{"tmq_meta_version":"1.0","metas":[{"type":"create","tableType":"super","tableName":"stb_src","columns":[{"name":"ts","type":9,"isPrimarykey":false,"encode":"delta-i","compress":"lz4","level":"medium"},{"name":"c1","type":4,"isPrimarykey":false,"encode":"simple8b","compress":"lz4","level":"medium"},{"name":"c2","type":8,"length":32,"isPrimarykey":false,"encode":"disabled","compress":"zstd","level":"medium"},{"name":"c3","type":10,"length":32,"isPrimarykey":false,"encode":"disabled","compress":"zstd","level":"medium"}],"tags":[{"name":"location","type":8,"length":32}]},{"type":"create","tableType":"child","tableName":"c_t1","using":"stb_src","tagNum":1,"tags":[{"name":"location","type":8,"value":"\\"c_t1\\""}],"createList":[]},{"type":"create","tableType":"child","tableName":"c_t2","using":"stb_src","tagNum":1,"tags":[{"name":"location","type":8,"value":"\\"c_t2\\""}],"createList":[]},{"type":"create","tableType":"normal","tableName":"n_t1","columns":[{"name":"ts","type":9,"isPrimarykey":false,"encode":"delta-i","compress":"lz4","level":"medium"},{"name":"c1","type":4,"isPrimarykey":false,"encode":"simple8b","compress":"lz4","level":"medium"},{"name":"c2","type":8,"length":32,"isPrimarykey":false,"encode":"disabled","compress":"zstd","level":"medium"},{"name":"c3","type":10,"length":32,"isPrimarykey":false,"encode":"disabled","compress":"zstd","level":"medium"}],"tags":[]},{"type":"create","tableType":"normal","tableName":"n_t2","columns":[{"name":"ts","type":9,"isPrimarykey":false,"encode":"delta-i","compress":"lz4","level":"medium"},{"name":"c1","type":4,"isPrimarykey":false,"encode":"simple8b","compress":"lz4","level":"medium"},{"name":"c2","type":8,"length":32,"isPrimarykey":false,"encode":"disabled","compress":"zstd","level":"medium"},{"name":"c3","type":10,"length":32,"isPrimarykey":false,"encode":"disabled","compress":"zstd","level":"medium"}],"tags":[]},{"type":"create","tableType":"super","isVirtual":true,"tableName":"v_stb","columns":[{"name":"ts","type":9,"isPrimarykey":false,"encode":"delta-i","compress":"lz4","level":"medium"},{"name":"c1","type":4,"isPrimarykey":false,"encode":"simple8b","compress":"lz4","level":"medium"},{"name":"c2","type":8,"length":32,"isPrimarykey":false,"encode":"disabled","compress":"zstd","level":"medium"},{"name":"c3","type":10,"length":32,"isPrimarykey":false,"encode":"disabled","compress":"zstd","level":"medium"}],"tags":[{"name":"location","type":8,"length":32}]},{"type":"create","tableType":"child","tableName":"v_c_t1","refs":[],"using":"v_stb","tagNum":1,"tags":[{"name":"location","type":8,"value":"\\"v_c_t1\\""}],"createList":[]},{"type":"create","tableType":"child","tableName":"v_c_t2","refs":[{"colName":"c1","refDbName":"db_src","refTableName":"c_t2","refColName":"c1"},{"colName":"c2","refDbName":"db_src","refTableName":"n_t1","refColName":"c2"},{"colName":"c3","refDbName":"db_src","refTableName":"c_t2","refColName":"c3"}],"using":"v_stb","tagNum":1,"tags":[{"name":"location","type":8,"value":"\\"v_c_t2\\""}],"createList":[]},{"type":"create","tableType":"normal","isVirtual":true,"tableName":"v_n_t1","columns":[{"name":"ts","type":9,"isPrimarykey":false,"encode":"delta-i","compress":"lz4","level":"medium"},{"name":"c1","type":4,"ref":{"refDbName":"db_src","refTableName":"c_t1","refColName":"c1"},"isPrimarykey":false,"encode":"simple8b","compress":"lz4","level":"medium"},{"name":"c2","type":8,"length":32,"ref":{"refDbName":"db_src","refTableName":"n_t1","refColName":"c2"},"isPrimarykey":false,"encode":"disabled","compress":"zstd","level":"medium"},{"name":"c3","type":10,"length":32,"ref":{"refDbName":"db_src","refTableName":"c_t2","refColName":"c3"},"isPrimarykey":false,"encode":"disabled","compress":"zstd","level":"medium"}],"tags":[]},{"type":"create","tableType":"normal","isVirtual":true,"tableName":"v_n_t2","columns":[{"name":"ts","type":9,"isPrimarykey":false,"encode":"delta-i","compress":"lz4","level":"medium"},{"name":"c1","type":4,"ref":{"refDbName":"db_src","refTableName":"c_t2","refColName":"c1"},"isPrimarykey":false,"encode":"simple8b","compress":"lz4","level":"medium"},{"name":"c2","type":8,"length":32,"isPrimarykey":false,"encode":"disabled","compress":"zstd","level":"medium"},{"name":"c3","type":10,"length":32,"ref":{"refDbName":"db_src","refTableName":"c_t2","refColName":"c3"},"isPrimarykey":false,"encode":"disabled","compress":"zstd","level":"medium"}],"tags":[]}]}{"tmq_meta_version":"1.0","metas":[{"type":"create","tableType":"super","isVirtual":true,"tableName":"v_stb1","columns":[{"name":"ts","type":9,"isPrimarykey":false,"encode":"delta-i","compress":"lz4","level":"medium"},{"name":"c1","type":4,"isPrimarykey":false,"encode":"simple8b","compress":"lz4","level":"medium"},{"name":"c2","type":8,"length":32,"isPrimarykey":false,"encode":"disabled","compress":"zstd","level":"medium"},{"name":"c3","type":10,"length":32,"isPrimarykey":false,"encode":"disabled","compress":"zstd","level":"medium"}],"tags":[{"name":"location","type":8,"length":32}]},{"type":"create","tableType":"child","tableName":"v_c_t11","refs":[{"colName":"c1","refDbName":"db_src","refTableName":"c_t1","refColName":"c1"}],"using":"v_stb1","tagNum":1,"tags":[{"name":"location","type":8,"value":"\\"v_c_t1\\""}],"createList":[]},{"type":"create","tableType":"child","tableName":"v_c_t12","refs":[{"colName":"c1","refDbName":"db_src","refTableName":"c_t2","refColName":"c1"},{"colName":"c2","refDbName":"db_src","refTableName":"n_t1","refColName":"c2"},{"colName":"c3","refDbName":"db_src","refTableName":"n_t2","refColName":"c3"}],"using":"v_stb1","tagNum":1,"tags":[{"name":"location","type":8,"value":"\\"v_c_t2\\""}],"createList":[]},{"type":"create","tableType":"normal","isVirtual":true,"tableName":"v_n_t11","columns":[{"name":"ts","type":9,"isPrimarykey":false,"encode":"delta-i","compress":"lz4","level":"medium"},{"name":"c1","type":4,"ref":{"refDbName":"db_src","refTableName":"c_t1","refColName":"c1"},"isPrimarykey":false,"encode":"simple8b","compress":"lz4","level":"medium"}],"tags":[]},{"type":"create","tableType":"normal","isVirtual":true,"tableName":"v_n_t12","columns":[{"name":"ts","type":9,"isPrimarykey":false,"encode":"delta-i","compress":"lz4","level":"medium"},{"name":"c1","type":4,"ref":{"refDbName":"db_src","refTableName":"c_t2","refColName":"c1"},"isPrimarykey":false,"encode":"simple8b","compress":"lz4","level":"medium"},{"name":"c2","type":8,"length":32,"ref":{"refDbName":"db_src","refTableName":"n_t1","refColName":"c2"},"isPrimarykey":false,"encode":"disabled","compress":"zstd","level":"medium"},{"name":"c3","type":10,"length":32,"ref":{"refDbName":"db_src","refTableName":"n_t2","refColName":"c3"},"isPrimarykey":false,"encode":"disabled","compress":"zstd","level":"medium"}],"tags":[]},{"type":"alter","tableType":"","tableName":"v_c_t1","alterType":16,"colName":"c1","refDbName":"db_src","refTbName":"c_t2","refColName":"c1"},{"type":"alter","tableType":"","tableName":"v_c_t2","alterType":16,"colName":"c3","refDbName":"db_src","refTbName":"c_t1","refColName":"c3"},{"type":"alter","tableType":"normal","tableName":"v_n_t1","alterType":18,"colName":"c4","colType":8,"colLength":32,"refDbName":"db_src","refTbName":"n_t1","refColName":"c2"},{"type":"alter","tableType":"","tableName":"v_n_t1","alterType":17,"colName":"c3"}]}'
            snapshot_stb_topic     = '{"tmq_meta_version":"1.0","metas":[{"type":"create","tableType":"super","isVirtual":true,"tableName":"v_stb","columns":[{"name":"ts","type":9,"isPrimarykey":false,"encode":"delta-i","compress":"lz4","level":"medium"},{"name":"c1","type":4,"isPrimarykey":false,"encode":"simple8b","compress":"lz4","level":"medium"},{"name":"c2","type":8,"length":32,"isPrimarykey":false,"encode":"disabled","compress":"zstd","level":"medium"},{"name":"c3","type":10,"length":32,"isPrimarykey":false,"encode":"disabled","compress":"zstd","level":"medium"}],"tags":[{"name":"location","type":8,"length":32}]},{"type":"create","tableType":"child","tableName":"v_c_t1","refs":[],"using":"v_stb","tagNum":1,"tags":[{"name":"location","type":8,"value":"\\"v_c_t1\\""}],"createList":[]},{"type":"create","tableType":"child","tableName":"v_c_t2","refs":[{"colName":"c1","refDbName":"db_src","refTableName":"c_t2","refColName":"c1"},{"colName":"c2","refDbName":"db_src","refTableName":"n_t1","refColName":"c2"},{"colName":"c3","refDbName":"db_src","refTableName":"c_t2","refColName":"c3"}],"using":"v_stb","tagNum":1,"tags":[{"name":"location","type":8,"value":"\\"v_c_t2\\""}],"createList":[]}]}{"tmq_meta_version":"1.0","metas":[{"type":"alter","tableType":"","tableName":"v_c_t1","alterType":16,"colName":"c1","refDbName":"db_src","refTbName":"c_t2","refColName":"c1"},{"type":"alter","tableType":"","tableName":"v_c_t2","alterType":16,"colName":"c3","refDbName":"db_src","refTbName":"c_t1","refColName":"c3"}]}'
        
        self.case_snapshot_db_topic(snapshot_db_topic_json)
        self.case_snapshot_stb_topic(snapshot_stb_topic)
        
        self.case_non_snapshot_check_ref_db_topic()

        tdLog.success(f"{__file__} successfully executed")
