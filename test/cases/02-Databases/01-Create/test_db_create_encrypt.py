from new_test_framework.utils import (
    tdLog, tdSql, epath, sc, etool, AutoGen, tdDnodes,
    generate_encrypt_keys
)
import os
import time


class TestBasic:
    # Configure taosd settings
    updatecfgDict = {'dDebugFlag': 131}
    
    # Configure encryption - keys will be generated BEFORE taosd starts
    # All keys are optional: if not specified, taosk will auto-generate them
    # For this test, we specify keys because we need them for SQL operations
    encryptionConfig = {
        'svrKey': '1234567890',      # Specify for testing (optional in general)
        'dbKey': '1234567890_db',    # Specify for testing (optional in general)
        # 'dataKey': 'custom_data',  # Optional: specify custom data key
        # 'generateConfig': True,    # Default: True
        # 'generateMeta': True,      # Default: True
        # 'generateData': True,      # Default: True
    }
    
    @classmethod
    def setup_class(cls):
        cls.init(cls, replicaVar=1, checkColName="c1")
        cls.valgrind = 0
        cls.db = "test"
        cls.stb = "meters"
        cls.childtable_count = 10
        cls.encrypt_key = '1234567890'

    
    def create_encrypt_db(self):        
        
        autoGen = AutoGen()
        autoGen.create_db(self.db, 2, 1, "ENCRYPT_ALGORITHM 'SM4-CBC'")
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
        Test recreating encryption keys for dnode
        
        From jira TS-5507: the encrypt key can be recreated.
        Created: 2024-09-23 by Charles
        Updated: 2025-12-29 to use taosk instead of taosd -y
        """
        dndoe2_path = tdDnodes.getDnodeDir(2)
        dnode2_data_path = os.sep.join([dndoe2_path, "data"])
        dnode2_cfg_path = os.sep.join([dndoe2_path, "cfg"])
        tdLog.info(f"dnode2 data path: {dnode2_data_path}")
        
        # Stop dnode2
        tdDnodes.stoptaosd(2)
        tdLog.info("Stopped dnode2")
        
        # Delete dnode2 data directory
        cmd = f"rm -rf {dnode2_data_path}"
        os.system(cmd)
        tdLog.info(f"Deleted dnode2 data directory")
        
        # Recreate encryption keys using taosk tool
        success, msg = generate_encrypt_keys(
            cfg_path=dnode2_cfg_path,
            svr_key=self.encrypt_key,
            db_key=f'{self.encrypt_key}_db',
            force=True
        )
        
        if not success:
            tdLog.exit(f"Failed to recreate encryption keys for dnode2: {msg}")
        
        tdLog.info("Successfully recreated encryption keys for dnode2")

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


