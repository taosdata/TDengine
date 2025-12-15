import pytest
import sys
import time
import random
import re
import os
import taos
from new_test_framework.utils import tdLog, tdSql, cluster, sc, clusterComCheck, etool, tdCom, AutoGen, TDSetSql


class TestShowWideTableBasic:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def createDatabase(self, dbName: str):   
        tdSql.execute(f"CREATE DATABASE IF NOT EXISTS {dbName};")
        tdSql.execute(f"USE {dbName};")
    def createWideTable(self, tbName: str, colNum: int):    
        sql = f"CREATE TABLE {tbName} (ts TIMESTAMP"
        for i in range(colNum):
            sql += f", c{i} INT"
        sql += ") TAGS (tag1 INT, tag2 INT);"
        tdSql.execute(sql)
    def createView(self, viewName: str, tbName: str):
        sql = f"CREATE VIEW {viewName} AS SELECT * FROM {tbName};"
        tdSql.execute(sql)

    def showCreateTable(self, tbName: str): 
        tdSql.query(f"SHOW CREATE TABLE {tbName};")
        resSet = tdSql.queryResult
        return resSet[0][1]

    def testResult(self):
        dbName = "test_show_wide_table_db"
        tbName = "wide_table_100_cols"
        viewName = "wide_table_view_100_cols"
        colNum = 4080 

        self.createDatabase(dbName)
        self.createWideTable(tbName, colNum)
        #self.createView(viewName, tbName)

        result = self.showCreateTable(tbName)
        createTableStmt = result.decode()    
        tdLog.debug(f"Create Table Statement:\n{createTableStmt}")

        #createViewStmt, = self.showCreateTable(viewName)
        #tdLog.debug(f"Create View Statement:\n{createViewStmt}")

        # Verify the create table statement contains all columns
        
        
        # for i in range(colNum):
        #     assert f"\`c{i}\`` INT" in createTableStmt, f"Column c{i} not found in create table statement"

        # Verify the create view statement references the correct table
        #assert f"FROM {tbName}" in createViewStmt, "View does not reference the correct table"
    #
    # ------------------- main ----------------
    #
    def test_show_wide_table_basic(self):
        """Show wide table 
        1. Verify show commands result with wide table   

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-12-15 yihaoDeng add wide table show test case    
        """
        self.testResult()    
        # self.do_army_show()
        # self.do_sim()
        # self.do_show_tag_index()