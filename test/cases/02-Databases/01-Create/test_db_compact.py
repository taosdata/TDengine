from new_test_framework.utils import tdLog, tdSql
import taos
import sys
import time
import socket
import os
import threading
import platform

class TestDbCompact:

    def setup_class(cls):
        cls.default_compact_options = [ "0d", "0d,0d", "0h"]
        cls.compact_options = [["db00", "0m", "-0d,0", "0", "0d", "0d,0d", "0h"],
                                ["db01", "0m", "-2d,-1", "0", "0d", "-2d,-1d", "0h"],
                                ["db02", "2880m", "-61d,-1", "0", "2d", "-61d,-1d", "0h"],
                                ["db03", "48h", "-87840m,-60", "1h", "2d", "-61d,-60d", "1h"],
                                ["db04", "2d", "-87840m,-1440h", "12", "2d", "-61d,-60d", "12h"],
                                ["db05", "2", "-61,-1440h", "23h", "2d", "-61d,-60d", "23h"],
                                ]

    #
    # ------------------- test_compact.py ----------------
    #
    def do_compact(self):
        tdSql.query("CREATE DATABASE power KEEP 365 DURATION 10 BUFFER 16 WAL_LEVEL 1 vgroups 1 replica 1;")

        tdSql.query("CREATE DATABASE power1 KEEP 365 DURATION 10 BUFFER 16 WAL_LEVEL 1 vgroups 1 replica 1;")

        #first
        tdSql.query("compact database power;")

        tdLog.info("compact id:%d"%tdSql.queryResult[0][1])

        tdSql.query("show compact %d;"%tdSql.queryResult[0][1])

        tdLog.info("detail:%d"%tdSql.queryRows)

        #second
        tdSql.query("compact database power1;")

        tdLog.info("compact id:%d"%tdSql.queryResult[0][1])

        tdSql.query("show compact %d;"%tdSql.queryResult[0][1])

        tdLog.info("detail:%d"%tdSql.queryRows)


        #kill
        tdSql.query("show compacts;")
        number1 = tdSql.queryResult[0][0]
        number2 = tdSql.queryResult[1][0]

        #first 
        tdLog.info("kill compact %d;"%number1)  
        tdSql.query("kill compact %d;"%number1)

        #second
        tdLog.info("kill compact %d;"%number2) 
        tdSql.query("kill compact %d;"%number2)


        #show
        count = 0
        tdLog.info("query progress")
        while count < 50:
            tdSql.query("show compact %d;"%number1)

            row1 = tdSql.queryRows

            tdSql.query("show compact %d;"%number2)

            row2 = tdSql.queryRows

            tdLog.info("compact%d:detail count:%d"%(number1, row1))
            tdLog.info("compact%d:detail count:%d"%(number2, row2))

            if row1 == 0 and row2 == 0 :
                break

            time.sleep(1)

            count +=1
            #tdLog.info("loop%d"%count)

        if row1 != 0 or row2 != 0:
            tdLog.exit("compact failed")

        print("\ndo compact ............................ [passed]")

    #
    # ------------------- test_compact_auto.py ----------------
    #
    def create_db_compact(self):
        tdLog.info("create db compact options")
        for item in self.compact_options:
            tdSql.execute(f'create database {item[0]} compact_interval {item[1]} compact_time_range {item[2]} compact_time_offset {item[3]} duration 1d')
            tdSql.query(f'select * from information_schema.ins_databases where name = "{item[0]}"')
            tdSql.checkEqual(tdSql.queryResult[0][34], item[4])
            tdSql.checkEqual(tdSql.queryResult[0][35], item[5])
            tdSql.checkEqual(tdSql.queryResult[0][36], item[6])
            tdSql.query(f'show create database {item[0]}')
            tdSql.checkEqual(tdSql.queryResult[0][0], item[0])
            tdSql.checkEqual(True, f'COMPACT_INTERVAL {item[4]} COMPACT_TIME_RANGE {item[5]} COMPACT_TIME_OFFSET {item[6]}' in tdSql.queryResult[0][1])
            tdSql.execute(f'drop database {item[0]}')

    def checkShowCreateWithTimeout(self, db, expectResult, timeout=30):
        result = False
        for i in range(timeout):
            tdSql.query(f'show create database `%s`' %(db))
            tdSql.checkEqual(tdSql.queryResult[0][0], db)
            if expectResult in tdSql.queryResult[0][1]:
                result = True
                break
            time.sleep(1)
        if result == False:
            raise Exception(f"Unexpected result of 'show create database `{db}`':{tdSql.queryResult[0][1]}, expect:{expectResult}")

    def alter_db_compact(self):
        tdLog.info("alter db compact options together")
        for item in self.compact_options:
            tdSql.execute(f'create database {item[0]} duration 1d')
            tdSql.query(f'select * from information_schema.ins_databases where name = "{item[0]}"')
            tdSql.checkEqual(tdSql.queryResult[0][34], self.default_compact_options[0])
            tdSql.checkEqual(tdSql.queryResult[0][35], self.default_compact_options[1])
            tdSql.checkEqual(tdSql.queryResult[0][36], self.default_compact_options[2])
            tdSql.query(f'show create database {item[0]}')
            tdSql.checkEqual(tdSql.queryResult[0][0], item[0])
            tdSql.checkEqual(True, f'COMPACT_INTERVAL {self.default_compact_options[0]} COMPACT_TIME_RANGE {self.default_compact_options[1]} COMPACT_TIME_OFFSET {self.default_compact_options[2]}' in tdSql.queryResult[0][1])
            tdSql.execute(f'alter database {item[0]} compact_interval {item[1]} compact_time_range {item[2]} compact_time_offset {item[3]}')
            tdSql.query(f'select * from information_schema.ins_databases where name = "{item[0]}"')
            tdSql.checkEqual(tdSql.queryResult[0][34], item[4])
            tdSql.checkEqual(tdSql.queryResult[0][35], item[5])
            tdSql.checkEqual(tdSql.queryResult[0][36], item[6])
        for item in self.compact_options:
            self.checkShowCreateWithTimeout(item[0], f'COMPACT_INTERVAL {item[4]} COMPACT_TIME_RANGE {item[5]} COMPACT_TIME_OFFSET {item[6]}')
            tdSql.execute(f'drop database {item[0]}')
        
        tdLog.info("alter db compact options separately")
        compact_separate_options = [["db100", "0m", "-0d,0", "0", "0d", "0d,0d", "0h"],
                                    ["db101", "10m", "-2d,-1", "1", "10m", "-2d,-1d", "1h"]]
        for item in compact_separate_options:
            tdSql.execute(f'create database {item[0]} duration 1d')
            tdSql.query(f'select * from information_schema.ins_databases where name = "{item[0]}"')
            tdSql.checkEqual(tdSql.queryResult[0][34], self.default_compact_options[0])
            tdSql.checkEqual(tdSql.queryResult[0][35], self.default_compact_options[1])
            tdSql.checkEqual(tdSql.queryResult[0][36], self.default_compact_options[2])
            tdSql.query(f'show create database {item[0]}')
            tdSql.checkEqual(tdSql.queryResult[0][0], item[0])
            tdSql.checkEqual(True, f'COMPACT_INTERVAL {self.default_compact_options[0]} COMPACT_TIME_RANGE {self.default_compact_options[1]} COMPACT_TIME_OFFSET {self.default_compact_options[2]}' in tdSql.queryResult[0][1])
            tdSql.execute(f'alter database {item[0]} compact_time_offset {item[3]}', queryTimes=10)
            tdSql.query(f'select * from information_schema.ins_databases where name = "{item[0]}"')
            tdSql.checkEqual(tdSql.queryResult[0][34], self.default_compact_options[0])
            tdSql.checkEqual(tdSql.queryResult[0][35], self.default_compact_options[1])
            tdSql.checkEqual(tdSql.queryResult[0][36], item[6])
            self.checkShowCreateWithTimeout(item[0], f'COMPACT_INTERVAL {self.default_compact_options[0]} COMPACT_TIME_RANGE {self.default_compact_options[1]} COMPACT_TIME_OFFSET {item[6]}')
            tdSql.execute(f'alter database {item[0]} compact_interval {item[1]}', queryTimes=10)
            tdSql.query(f'select * from information_schema.ins_databases where name = "{item[0]}"')
            tdSql.checkEqual(tdSql.queryResult[0][34], item[4])
            tdSql.checkEqual(tdSql.queryResult[0][35], self.default_compact_options[1])
            tdSql.checkEqual(tdSql.queryResult[0][36], item[6])
            self.checkShowCreateWithTimeout(item[0], f'COMPACT_INTERVAL {item[4]} COMPACT_TIME_RANGE {self.default_compact_options[1]} COMPACT_TIME_OFFSET {item[6]}')
            tdSql.execute(f'alter database {item[0]} compact_time_range {item[2]}', queryTimes=10)
            tdSql.query(f'select * from information_schema.ins_databases where name = "{item[0]}"')
            tdSql.checkEqual(tdSql.queryResult[0][34], item[4])
            tdSql.checkEqual(tdSql.queryResult[0][35], item[5])
            tdSql.checkEqual(tdSql.queryResult[0][36], item[6])
            self.checkShowCreateWithTimeout(item[0], f'COMPACT_INTERVAL {item[4]} COMPACT_TIME_RANGE {item[5]} COMPACT_TIME_OFFSET {item[6]}')
            tdSql.execute(f'alter database {item[0]} compact_time_offset {item[3]}', queryTimes=10)
            tdSql.query(f'select * from information_schema.ins_databases where name = "{item[0]}"')
            tdSql.checkEqual(tdSql.queryResult[0][34], item[4])
            tdSql.checkEqual(tdSql.queryResult[0][35], item[5])
            tdSql.checkEqual(tdSql.queryResult[0][36], item[6])
            self.checkShowCreateWithTimeout(item[0], f'COMPACT_INTERVAL {item[4]} COMPACT_TIME_RANGE {item[5]} COMPACT_TIME_OFFSET {item[6]}')
        for item in compact_separate_options:
            tdSql.execute(f'drop database {item[0]}', queryTimes=10)

    def compact_error(self):
        compact_err_list = [["compact_time_range 86400m,61d", "Invalid option compact_time_range: 86400m, start time should be in range: [-5256000m, -14400m]"],
                            ["compact_time_range 60,61", "Invalid option compact_time_range: 86400m, start time should be in range: [-5256000m, -14400m]"],
                            ["compact_time_range 60d,61d", "Invalid option compact_time_range: 86400m, start time should be in range: [-5256000m, -14400m]"],
                            ["compact_time_range -60,-60", "Invalid option compact_time_range: -86400m,-86400m, start time should be less than end time"],
                            ["compact_time_range -60,-1440h", "Invalid option compact_time_range: -86400m,-86400m, start time should be less than end time"],
                            ["compact_time_range -60d,-61d", "Invalid option compact_time_range: -86400m,-87840m, start time should be less than end time"],
                            ["compact_time_range -5256001m,-1", "Invalid option compact_time_range: -5256001m, start time should be in range: [-5256000m, -14400m]"],
                            ["compact_time_range -199999999999m,-199999999998m", "start time should be in range: [-5256000m, -14400m]"],
                            ["compact_time_range -99999999999999999999m,-99999999999999999998m", "Invalid value type: -99999999999999999999"],
                            ["compact_time_range -60d,-1", "Invalid option compact_time_range: -1440m, end time should be in range: [-5256000m, -14400m]"],
                            ["compact_interval 24h compact_time_range -60,61", "Invalid option compact_time_range: 87840m, end time should be in range: [-5256000m, -14400m]"],
                            ["compact_interval 100 compact_time_range -60d,61d", "Invalid option compact_time_range: 87840m, end time should be in range: [-5256000m, -14400m]"],
                            ["compact_time_range -60d,87840m", "Invalid option compact_time_range: 87840m, end time should be in range: [-5256000m, -14400m]"],
                            ["compact_time_range -100,-90s", "Invalid option compact_time_range end unit: s, only m, h, d allowed"],
                            ["compact_interval 10m compact_time_range -120d,-14400m compact_time_offset -1", "syntax error near"],
                            ["compact_time_range -100,-99d compact_interval -1", "syntax error near"],
                            ["compact_time_range 0", "Invalid option compact_time_range, should have 2 value"],
                            ["compact_time_range -100", "Invalid option compact_time_range, should have 2 value"],
                            ["compact_time_range -100,-90,-80", "Invalid option compact_time_range, should have 2 value"],
                            ["compact_time_range -100;-90", "Invalid option compact_time_range, should have 2 value"],
                            ["compact_time_range -100:-90", "syntax error near"],
                            ["compact_time_range -100 -90", "syntax error near"],
                            ["compact_interval 1m", "Invalid option compact_interval: 1, valid range: [10, 5256000]"],
                            ["compact_interval 199999999999m", "valid range: [10, 5256000]"],
                            ["compact_interval 9999999999999m", "Invalid option compact_interval: 9999999999999, valid range: [10, 5256000]"],
                            ["compact_interval 5256001m", "Invalid option compact_interval: 5256001, valid range: [10, 5256000]"],
                            ["compact_interval 3651", "Invalid option compact_interval: 5257440, valid range: [10, 5256000]"],
                            ["compact_interval 86400s", "Invalid option compact_interval unit: s, only m, h, d allowed"],
                            ["compact_interval -1", "syntax error near"],
                            ["compact_time_offset -1", "syntax error near"],
                            ["compact_time_offset 3600s", "Invalid option compact_time_offset unit: s, only h allowed"],
                            ["compact_time_offset 1d", "Invalid option compact_time_offset unit: d, only h allowed"],
                            ["compact_time_offset 24", "Invalid option compact_time_offset: 24, valid range: [0, 23]"],
                            ["compact_time_offset 24h", "Invalid option compact_time_offset: 24, valid range: [0, 23]"],
                            ["compact_time_offset 9999999999999", "valid range: [0, 23]"],
                            ["compact_time_offset 199999999999", "valid range: [0, 23]"],
                            ["compact_time_offset 1d", "Invalid option compact_time_offset unit: d, only h allowed"],
                            ["compact_interval 10m compact_time_range -120d,-60 compact_time_offset 1d", "Invalid option compact_time_offset unit: d, only h allowed"],
                            ]
        tdSql.execute('create database if not exists db')
        for item in compact_err_list:
            tdSql.error(f"create database db {item[0]}", expectErrInfo=item[1], fullMatched=False)
            tdSql.error(f"alter database db {item[0]}", expectErrInfo=item[1], fullMatched=False)
        tdSql.execute('drop database db')
        
    def do_compact_auto(self):
        self.create_db_compact()
        self.alter_db_compact()
        self.compact_error()

        print("do compact auto ....................... [passed]")

    #
    # ------------------- test_compact_vgroups.py ----------------
    #
    # Test cases ======================
    def run_compact_vgroups_error(self):
        # invalid sql
        sql = "compact vgroups;"
        tdLog.info(f"expect error SQL: {sql}")
        tdSql.error(sql)

        # invalid sql
        sql = "compact vgroups in"
        tdLog.info(f"expect error SQL: {sql}")
        tdSql.error(sql)

        # invalid sql
        sql = "compact vgroups in ()"
        tdLog.info(f"expect error SQL: {sql}")
        tdSql.error(sql)

        # error without using database
        sql = "compact vgroups in (2)"
        tdLog.info(f"expect error SQL: {sql}")
        tdSql.error(sql)

        # error with duplicate vgroup
        sql = "compact db1.vgroups in (2, 2)"
        tdLog.info(f"expect error SQL: {sql}")
        tdSql.error(sql)

        # error with invalid vgroup id
        sql = "compact db1.vgroups in (0, -1)"
        tdLog.info(f"expect error SQL: {sql}")
        tdSql.error(sql)

        # error to compact vgroups not in the same dat
        sql = "compact db1.vgroups in (7, 8)"
        tdLog.info(f"expect error SQL: {sql}")
        tdSql.error(sql)

        # error to compact vgroups not in the same database
        sql = "compact db1.vgroups in (2, 5, 8)"
        tdLog.info(f"expect error SQL: {sql}")
        tdSql.error(sql)
    
    def waitCompactFinish(self):
        while True:
            sql = 'show compacts'
            rows = tdSql.query(sql)
            if rows == 0:
                break
            time.sleep(1)

    def run_compact_vgroups_sql(self):
        # make sure there is no compacts
        sql = 'show compacts'
        rows = tdSql.query(sql)
        tdSql.checkEqual(rows, 0)

        # use db1 and compact with db name should be ok
        sql = 'use db1'
        tdLog.info(f'expect success SQL: {sql}')
        tdSql.execute(sql)
        
        sql = 'compact vgroups in (2)'
        tdLog.info(f'expect success SQL: {sql}')
        tdSql.execute(sql)

        # check there should be one row in compacts
        sql = 'show compacts'
        rows = tdSql.query(sql)
        tdSql.checkEqual(rows, 1)

        compactId = tdSql.getData(0, 0)

        # query the compact status
        sql = f'show compact {compactId}'
        tdLog.info(f'expect success SQL: {sql}')
        rows = tdSql.query(sql)
        tdSql.checkEqual(rows, 1)
        tdSql.checkEqual(tdSql.getData(0, 0), compactId) # compact_id
        tdSql.checkEqual(tdSql.getData(0, 1), 2) # vgroup_id

        # wait for compact finish
        self.waitCompactFinish()

        # start a new compact
        sql = 'compact db2.vgroups in (7, 10)'
        tdLog.info(f'expect success SQL: {sql}')
        tdSql.execute(sql)

        sql = 'show compacts'
        rows = tdSql.query(sql)
        tdSql.checkEqual(rows, 1)
        
        compactId = tdSql.getData(0, 0)
        sql = f'show compact {compactId}'
        tdLog.info(f'expect success SQL: {sql}')
        rows = tdSql.query(sql)
        tdSql.checkEqual(rows, 2)
        tdSql.checkEqual(tdSql.getData(0, 1) in (7, 10), True)
        tdSql.checkEqual(tdSql.getData(1, 1) in (7, 10), True)
        tdSql.checkEqual(tdSql.getData(0, 1) != tdSql.getData(1, 1), True)

        # wait for compact finish
        self.waitCompactFinish()

    def do_compact_vgroups(self):
        # create database db1
        sql = "create database db1 vgroups 5" 
        tdLog.info(sql)
        tdSql.execute(sql)

        # create database db2
        sql = "create database db2 vgroups 5" 
        tdLog.info(sql)
        tdSql.execute(sql)

        # error test
        self.run_compact_vgroups_error()

        # success to compact vgroups 
        self.run_compact_vgroups_sql()
        print("do compact vgroups .................... [passed]")

    #
    # ------------------- main ----------------
    #
    def do_compact_col(self):
        tdSql.execute("drop database if exists tbname_vgroup")
        tdSql.execute("create database if not exists tbname_vgroup")
        tdSql.execute('use tbname_vgroup')
        tdSql.execute('drop database if exists dbvg')
        tdSql.execute('create database dbvg vgroups 8;')

        tdSql.execute('use dbvg;')

        tdSql.execute('create table st(ts timestamp, f int) tags (t int);')
        
        tables = []
        tables.append("ct1 using st tags(1) values('2021-04-19 00:00:01', 1)")
        tables.append("ct2 using st tags(2) values('2021-04-19 00:00:02', 2)")
        tables.append("ct3 using st tags(3) values('2021-04-19 00:00:03', 3)")
        tables.append("ct4 using st tags(4) values('2021-04-19 00:00:04', 4)")
        
        sql = "insert into " + " ".join(tables)
        tdSql.execute(sql)

        tdSql.execute('create table st2(ts timestamp, f int) tags (t int);')

        tables = []
        tables.append("ct21 using st2 tags(1) values('2021-04-19 00:00:01', 1)")
        tables.append("ct22 using st2 tags(2) values('2021-04-19 00:00:02', 2)")
        tables.append("ct23 using st2 tags(3) values('2021-04-19 00:00:03', 3)")
        tables.append("ct24 using st2 tags(4) values('2021-04-19 00:00:04', 4)")

        sql = "insert into " + " ".join(tables)
        tdSql.execute(sql)

        col_names = tdSql.getColNameList("compact database tbname_vgroup")
        if col_names[0] != "result":
           raise Exception("first column name of compact result shall be result")

        col_names = tdSql.getColNameList("show variables")
        if col_names[0] != "name":
           raise Exception("first column name of compact result shall be name")

        tdSql.execute('drop database tbname_vgroup')

        print("do compact cols ....................... [passed]")

    #
    # ------------------- main ----------------
    #
    def test_db_compact(self):
        """Database compact
        
        1. Compact a empty database and verify no error occurs
        2. Show compacts and get compact id
        3. Show compact with the compact id and verify the result
        4. Kill the compact operation and verify it is killed successfully
        5. Create databases with different compact options
        6. Verify the compact options are set correctly
        7. Alter the compact options and verify the options are changed correctly
        8. Verify error handling for invalid compact options
        9. Create databases with vgroups
        10. Compact specific vgroups and verify the operation is successful
        11. Verify "compact database" command can return column names correctly
        
        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-11-03 Alex Duan Migrated from uncatalog/system-test/0-others/test_compact.py
            - 2025-11-03 Alex Duan Migrated from uncatalog/system-test/0-others/test_compact_auto.py
            - 2025-11-03 Alex Duan Migrated from uncatalog/system-test/0-others/test_compact_vgroups.py
            - 2025-11-04 Alex Duan Migrated from uncatalog/system-test/2-query/test_compact_col.py
 
        """
        self.do_compact_vgroups()
        self.do_compact()
        self.do_compact_auto()
        self.do_compact_col()
