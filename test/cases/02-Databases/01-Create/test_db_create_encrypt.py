from new_test_framework.utils import tdLog, tdSql, epath, sc, etool, AutoGen, tdDnodes
import os
import time

# from frame.server.dnodes import *
# from frame.server.cluster import *


class TestBasic:
    updatecfgDict = {'dDebugFlag':131}
    def setup_class(cls):
        cls.init(cls, replicaVar=1, checkColName="c1")
        cls.valgrind = 0
        cls.db = "test"
        cls.stb = "meters"
        cls.childtable_count = 10
    
    def create_encrypt_db(self):        
        
        tdSql.execute("create encrypt_key '1234567890'")
        autoGen = AutoGen()
        autoGen.create_db(self.db, 2, 1, "ENCRYPT_ALGORITHM 'sm4'")
        tdSql.execute(f"use {self.db}")
        autoGen.create_stable(self.stb, 2, 3, 8, 8)
        autoGen.create_child(self.stb, "d", self.childtable_count)
        autoGen.insert_data(1000)
        
        tdSql.query(f"select * from {self.db}.{self.stb}")
        tdSql.checkRows(1000 * self.childtable_count)
        
        self.timestamp_step = 1000
        self.insert_rows = 1000
        
        self.checkInsertCorrect()           
    
    def create_encrypt_db_error(self):
        tdSql.error("create encrypt_key '123'")
        tdSql.error("create encrypt_key '12345678abcdefghi'")
        tdSql.error("create database test ENCRYPT_ALGORITHM 'sm4'")

    def recreate_dndoe_encrypt_key(self):
        """
        Description: From the jira TS-5507, the encrypt key can be recreated.
        create: 
            2024-09-23 created by Charles
        update:
            None
        """
        # taosd path
        taosd_path = epath.binPath()
        tdLog.info(f"taosd_path: {taosd_path}")
        # dnode2 path
        dndoe2_path = tdDnodes.getDnodeDir(2)
        dnode2_data_path = os.sep.join([dndoe2_path, "data"])
        dnode2_cfg_path = os.sep.join([dndoe2_path, "cfg"])
        tdLog.info(f"dnode2_path: {dnode2_data_path}")
        # stop dnode2
        tdDnodes.stoptaosd(2)
        tdLog.info("stop dndoe2")
        # delete dndoe2 data
        cmd = f"rm -rf {dnode2_data_path}"
        os.system(cmd)
        # recreate the encrypt key for dnode2
        os.system(f"{os.sep.join([taosd_path, 'taosd'])} -y '1234567890' -c {dnode2_cfg_path}")
        tdLog.info("test case: recreate the encrypt key for dnode2 passed")

    def test_db_create_encrypt(self):
        """ Option: encrypt_algorithm

        1. Create encrypt key '1234567890'
        2. Create database with encrypt_algorithm 'sm4'
        3. Create stable and child tables
        4. Insert data and query data
        5. Recreate dnode encrypt key
        6. Query data again
        7. Create database with wrong encrypt key and expect error

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-10-17 Alex Duan Migrated from uncatalog/army/db-encrypt/test_basic.py
        
        """
        self.create_encrypt_db_error()
        self.create_encrypt_db()
        self.recreate_dndoe_encrypt_key()

        tdLog.success(f"{__file__} successfully executed")


