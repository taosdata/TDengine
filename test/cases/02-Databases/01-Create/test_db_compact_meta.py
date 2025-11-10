# tests/test_new/xxx/xxx/test_xxx.py
# import ...
'''
./pytest.sh python3 ./test.py -f storage/compact/test_compact_meta.py
'''

from new_test_framework.utils import tdLog, tdSql
import taos
import sys
import time
from math import inf

import inspect
import random

class TestCompactMeta:
    def caseDescription(self):
        '''
        case1<Hongze Cheng>: [TS-5445] Compact Meta Data
        '''
        return

    def setup_class(cls):
        tdLog.debug("start to execute %s" % __file__)

    def test_compact_meta(self):
        """Database compact meta
        
        1. Create datbase and super table
        2. Create many child tables
        3. Insert data into child tables
        4. Alter child table tags
        5. Query data from child tables to verify
        6. Compact meta only
        7. Insert more data into child tables
        8. Query data from child tables to verify again
        9. Alter super table schema many times
        10. Query data from child tables to verify again
        11. Alter super table schema
        12. Make sure compact meta works
        
        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-11-04 Alex Duan Migrated from cases/uncatalog/test_new/storage/compact/test_compact_meta.py
 
        """
        self.run_case1()
        self.run_case2()

        tdLog.success("%s successfully executed" % __file__)

    def run_case1(self):
        """
        Description:
            1. Alter child table tags
            2. Make sure compact meta works
        """
        tdLog.info(f'case {inspect.currentframe().f_code.co_name} start')

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
        tdLog.info(
            f'case {inspect.currentframe().f_code.co_name}: create {num_child_tables} child tables')
        for i in range(1, num_child_tables+1):
            if i % 100 == 0:
                tdLog.info(
                    f'case {inspect.currentframe().f_code.co_name}: create {i} child tables')
            sql = f'create table {db_name}.{ctb_name_prefix}{i} using {db_name}.{stb_name} tags({i})'
            tdSql.execute(sql)

        # Insert some data
        tdLog.info(
            f'case {inspect.currentframe().f_code.co_name}: insert data to child tables')
        for i in range(1, num_child_tables+1):
            if i % 100 == 0:
                tdLog.info(
                    f'case {inspect.currentframe().f_code.co_name}: insert data to {i} child tables')
            sql = f'insert into {db_name}.{ctb_name_prefix}{i} values(now, 1, 2)'
            tdSql.execute(sql)

        # Alter child table tags
        tdLog.info(
            f'case {inspect.currentframe().f_code.co_name}: alter child table tags')
        for i in range(1, num_child_tables+1):
            if i % 100 == 0:
                tdLog.info(
                    f'case {inspect.currentframe().f_code.co_name}: altered {i} child tables')
            sql = f'alter table {db_name}.{ctb_name_prefix}{i} set tag t1 = {i+1}'
            tdSql.execute(sql)

        # Randomly select 100 child tables to do query
        tdLog.info(
            f'case {inspect.currentframe().f_code.co_name}: randomly select 100 child tables to query')
        selected_tables = random.sample(range(1, num_child_tables + 1), 100)
        selected_tables.extend([1, num_child_tables])
        for i, table_idx in enumerate(selected_tables):
            # Query data from the child table
            sql = f'select count(*) from {db_name}.{stb_name} where t1 = {table_idx + 1}'
            tdSql.query(sql)
            tdSql.checkData(0, 0, 1)  # Check c2 column value

        # Compact meta
        tdLog.info(
            f'case {inspect.currentframe().f_code.co_name}: start to compact meta')
        sql = f'compact database {db_name} meta_only'
        tdSql.execute(sql)

        # Wait for the compact is done
        tdLog.info(
            f'case {inspect.currentframe().f_code.co_name}: wait compact is done')
        while True:
            sql = 'show compacts'
            rows = tdSql.query(sql)
            if rows == 0:
                break
            time.sleep(1)

        # Write more data
        tdLog.info(
            f'case {inspect.currentframe().f_code.co_name}: insert more data to child tables')
        for i in range(1, num_child_tables+1):
            if i % 100 == 0:
                tdLog.info(
                    f'case {inspect.currentframe().f_code.co_name}: insert data to {i} child tables')
            sql = f'insert into {db_name}.{ctb_name_prefix}{i} values(now, 1, 2)'
            tdSql.execute(sql)

        # Randomly select 100 child tables to do query
        tdLog.info(
            f'case {inspect.currentframe().f_code.co_name}: query data again to verify')
        for i, table_idx in enumerate(selected_tables):
            # Query data from the child table
            sql = f'select count(*) from {db_name}.{stb_name} where t1 = {table_idx + 1}'
            tdSql.query(sql)
            tdSql.checkData(0, 0, 2)  # Check c2 column value

    def run_case2(self):
        """
        Description:
            1. Alter super table schema
            2. Make sure compact meta works
        """
        tdLog.info(f'case {inspect.currentframe().f_code.co_name} start')

        db_name = 'db2'
        stb_name = 'stb2'
        ctb_name_prefix = 'ctb'
        num_child_tables = 1000

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
        tdLog.info(
            f'case {inspect.currentframe().f_code.co_name}: create {num_child_tables} child tables')
        for i in range(1, num_child_tables+1):
            if i % 100 == 0:
                tdLog.info(
                    f'case {inspect.currentframe().f_code.co_name}: create {i} child tables')
            sql = f'create table {db_name}.{ctb_name_prefix}{i} using {db_name}.{stb_name} tags({i})'
            tdSql.execute(sql)

        # Insert some data
        tdLog.info(
            f'case {inspect.currentframe().f_code.co_name}: insert data to child tables')
        for i in range(1, num_child_tables+1):
            if i % 100 == 0:
                tdLog.info(
                    f'case {inspect.currentframe().f_code.co_name}: insert data to {i} child tables')
            sql = f'insert into {db_name}.{ctb_name_prefix}{i} (ts, c1) values (now, 1)'
            tdSql.execute(sql)

        # Alter super table schema
        tdLog.info(
            f'case {inspect.currentframe().f_code.co_name}: alter super table schema')
        for i in range(3, 2000):
            if i % 100 == 0:
                tdLog.info(
                    f'case {inspect.currentframe().f_code.co_name}: altered {i} times of super table schema')
            # Add a column
            sql = f'alter table {db_name}.{stb_name} add column c{i} int'
            tdSql.execute(sql)

            # Drop a column
            sql = f'alter table {db_name}.{stb_name} drop column c{i}'
            tdSql.execute(sql)

        # Randomly select 100 child tables to do query
        tdLog.info(
            f'case {inspect.currentframe().f_code.co_name}: randomly select 100 child tables to query')
        selected_tables = random.sample(range(1, num_child_tables + 1), 100)
        selected_tables.extend([1, num_child_tables])
        for i, table_idx in enumerate(selected_tables):
            # Query data from the child table
            sql = f'select count(*) from {db_name}.{stb_name} where t1 = {table_idx}'
            tdSql.query(sql)
            tdSql.checkData(0, 0, 1)  # Check c2 column value

        # Compact meta
        tdLog.info(
            f'case {inspect.currentframe().f_code.co_name}: start to compact meta')
        sql = f'compact database {db_name} meta_only'
        tdSql.execute(sql)

        # Wait for the compact is done
        tdLog.info(
            f'case {inspect.currentframe().f_code.co_name}: wait compact is done')
        while True:
            sql = 'show compacts'
            rows = tdSql.query(sql)
            if rows == 0:
                break
            time.sleep(1)

        # Write more data
        tdLog.info(
            f'case {inspect.currentframe().f_code.co_name}: insert more data to child tables')
        for i in range(1, num_child_tables+1):
            if i % 100 == 0:
                tdLog.info(
                    f'case {inspect.currentframe().f_code.co_name}: insert data to {i} child tables')
            sql = f'insert into {db_name}.{ctb_name_prefix}{i} values(now, 1, 2)'
            tdSql.execute(sql)

        # Randomly select 100 child tables to do query
        tdLog.info(
            f'case {inspect.currentframe().f_code.co_name}: query data again to verify')
        for i, table_idx in enumerate(selected_tables):
            # Query data from the child table
            sql = f'select count(*) from {db_name}.{stb_name} where t1 = {table_idx}'
            tdSql.query(sql)
            tdSql.checkData(0, 0, 2)  # Check c2 column value


