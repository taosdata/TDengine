import taos
import sys
import os
import subprocess
import glob
import shutil
import time

from frame.log import *
from frame.cases import *
from frame.sql import *
from frame.srvCtl import *
from frame.caseBase import *
from frame import *
from frame.autogen import *
# from frame.server.dnodes import *
# from frame.server.cluster import *


class TDTestCase(TBase):
    
    def init(self, conn, logSql, replicaVar=1):
        super(TDTestCase, self).init(conn, logSql, replicaVar=1, checkColName="c1")
        self.valgrind = 0
        self.db = "test"
        self.stb = "meters"
        self.childtable_count = 10
        tdSql.init(conn.cursor(), logSql)  
    
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

    def run(self):
        self.create_encrypt_db_error()
        self.create_encrypt_db()

    def stop(self):
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")


tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
