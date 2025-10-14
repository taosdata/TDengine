from new_test_framework.utils import tdLog, tdSql
import os
import time
import platform

class TestInsertFromCsv:
    def setup_class(cls):
        tdLog.debug(f"start to excute {__file__}")
        #tdSql.init(conn.cursor(), logSql), True)

        cls.testcasePath = os.path.split(__file__)[0]
        cls.csv_file_path = os.path.join(cls.testcasePath, "test_insert_from_csv.csv")
        if platform.system() == "Windows":
            cls.csv_file_path = cls.csv_file_path.replace('\\', '\\\\')
        cls.database = "test_insert_csv_db"
        cls.table = "test_insert_csv_tbl"

    def insert_from_csv(self):
        tdSql.execute(f"drop database if exists {self.database}")
        tdSql.execute(f"create database {self.database}")
        tdSql.execute(f"use {self.database}")
        tdSql.execute(f"create table {self.table} (ts timestamp, c1 nchar(16), c2 double, c3 int)")
        tdSql.execute(f"insert into {self.table} file '{self.csv_file_path}'")    
        tdSql.query(f"select count(*) from {self.table}")
        tdSql.checkData(0, 0, 5)

    def test_write_import_csv(self):
        """ From CSV File

        1. Create table and import data from csv file
        2. Check the imported data

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-9-16 Alex Duan Migrated from uncatalog/system-test/1-insert/test_insert_from_csv.py

        """
        tdSql.prepare()
        
        startTime_all = time.time() 
        self.insert_from_csv()
        endTime_all = time.time()
        print("total time %ds" % (endTime_all - startTime_all))
        
        tdLog.success(f"{__file__} successfully executed")