from new_test_framework.utils import tdLog, tdSql, epath, sc, etool, AutoGen, tdDnodes
import os
import time

# from frame.server.dnodes import *
# from frame.server.cluster import *


class TestBasic:
    updatecfgDict = {'dDebugFlag':131}
    
    encryptConfig = {
        "svrKey": "1234567890",
        "dbKey": "1234567890",
        "dataKey": "1234567890",
        "generateConfig": True,
        "generateMeta": True,
        "generateData": True
    }

    def setup_class(cls):
        cls.init(cls, replicaVar=1, checkColName="c1")
        cls.valgrind = 0
        cls.db = "test1"
        cls.stb = "meters"
        cls.childtable_count = 10
    
    def create_encrypt_sm4_db(self):        
        
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

    def create_encrypt_aes_db(self):        
        
        autoGen = AutoGen()
        self.db = "test2"
        autoGen.create_db(self.db, 2, 1, "ENCRYPT_ALGORITHM 'AES-128-CBC'")
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

    def check_encrypt_algorithm(self):
        tdSql.query(f"show encrypt_algorithms")
        tdSql.checkRows(6)
        tdSql.checkData(0, 0, 1)
        tdSql.checkData(0, 1, "SM4-CBC")
        tdSql.checkData(0, 6, "SM4-CBC:SM4")
        tdSql.checkData(1, 0, 2)
        tdSql.checkData(1, 1, "AES-128-CBC")
        tdSql.checkData(1, 6, "AES-128-CBC")
        tdSql.checkData(2, 0, 3)
        tdSql.checkData(2, 1, "SM3")
        tdSql.checkData(2, 6, "SM3")
        tdSql.checkData(3, 0, 4)
        tdSql.checkData(3, 1, "SHA-256")
        tdSql.checkData(3, 6, "SHA-256")
        tdSql.checkData(4, 0, 5)
        tdSql.checkData(4, 1, "SM2")
        tdSql.checkData(5, 0, 6)
        tdSql.checkData(5, 1, "RSA")

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
        self.check_encrypt_algorithm()
        self.create_encrypt_db_error()
        self.create_encrypt_sm4_db()
        self.create_encrypt_aes_db()

        tdLog.success(f"{__file__} ")


