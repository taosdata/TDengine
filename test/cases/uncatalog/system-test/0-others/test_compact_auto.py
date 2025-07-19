from new_test_framework.utils import tdLog, tdSql
import taos
import sys
import time
import socket
import os
import threading
import platform


class TestCompactAuto:
    def setup_class(cls):
        tdLog.debug("start to execute %s" % __file__)
        cls.default_compact_options = [ "0d", "0d,0d", "0h"]
        cls.compact_options = [["db00", "0m", "-0d,0", "0", "0d", "0d,0d", "0h"],
                                ["db01", "0m", "-2d,-1", "0", "0d", "-2d,-1d", "0h"],
                                ["db02", "2880m", "-61d,-1", "0", "2d", "-61d,-1d", "0h"],
                                ["db03", "48h", "-87840m,-60", "1h", "2d", "-61d,-60d", "1h"],
                                ["db04", "2d", "-87840m,-1440h", "12", "2d", "-61d,-60d", "12h"],
                                ["db05", "2", "-61,-1440h", "23h", "2d", "-61d,-60d", "23h"],
                                ]

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
        
    def test_compact_auto(self):
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
        self.create_db_compact()
        self.alter_db_compact()
        self.compact_error()

        tdLog.success(f"{__file__} successfully executed")


