import time
from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck, clusterComCheck


class TestDatabaseCreate:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_database_create(self):
        """DB Name: basic

        1. Case sensitivity
        2. Illegal names
        3. Chinese names

        Catalog:
            - Database:Create

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-5-12 Simon Guan Migrated from tsim/parser/create_db.sim
        """

        tdLog.info(f"======================== dnode1 start")

        dbPrefix = "fi_in_db"
        tbPrefix = "fi_in_tb"
        mtPrefix = "fi_in_mt"
        tbNum = 10
        rowNum = 20
        totalNum = 200

        tdLog.info(f"excuting test script create_db.sim")
        tdLog.info(f"=============== set up")
        i = 0
        db = dbPrefix + str(i)
        mt = mtPrefix + str(i)

        tdSql.error(f"createdatabase {db}")
        tdSql.execute(f"create database {db}")
        tdSql.execute(f"use {db}")
        tdSql.query(f"select * from information_schema.ins_databases")

        tdSql.checkRows(3)
        tdSql.checkData(2, 0, db)
        tdSql.execute(f"drop database {db}")

        # case1: case_insensitivity test
        tdLog.info(f"=========== create_db.sim case1: case insensitivity test")
        tdSql.error(f"CREATEDATABASE {db}")
        tdSql.execute(f"CREATE DATABASE {db}")
        tdSql.execute(f"use {db}")
        tdSql.query(f"select * from information_schema.ins_databases")

        tdSql.checkRows(3)
        tdSql.checkData(2, 0, db)

        tdSql.execute(f"drop database {db}")
        tdLog.info(f"case_insensitivity test passed")

        # case2: illegal_db_name test
        tdLog.info(f"=========== create_db.sim case2: illegal_db_name test")
        illegal_db1 = "1db"
        illegal_db2 = "d@b"

        tdSql.error(f"create database {illegal_db1}")
        tdSql.error(f"create database {illegal_db2}")
        tdLog.info(f"illegal_db_name test passed")

        # case3: chinese_char_in_db_name test
        tdLog.info(f"========== create_db.sim case3: chinese_char_in_db_name test")
        CN_db1 = "数据库"
        CN_db2 = "数据库1"
        CN_db3 = "db数据库1"
        tdSql.error(f"create database {CN_db1}")
        tdSql.error(f"create database {CN_db2}")
        tdSql.error(f"create database {CN_db3}")
        # sql select * from information_schema.ins_databases
        # if $rows != 3 then
        #  return -1
        # endi
        # if $tdSql.getData(0,0) != $CN_db1 then
        #  return -1
        # endi
        # if $tdSql.getData(1,0) != $CN_db2 then
        #  return -1
        # endi
        # if $tdSql.getData(2,0) != $CN_db3 then
        #  return -1
        # endi
        # sql drop database $CN_db1
        # sql drop database $CN_db2
        # sql drop database $CN_db3
        tdLog.info(f"case_chinese_char_in_db_name test passed")

        # case4: db_already_exists
        tdLog.info(f"create_db.sim case4: db_already_exists")
        tdSql.execute(f"create database db0")
        tdSql.error(f"create database db0")
        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkRows(3)

        tdSql.execute(f"drop database db0")
        tdLog.info(f"db_already_exists test passed")

        # case5: db_meta_data
        tdLog.info(f"create_db.sim case5: db_meta_data test")
        # cfg params
        replica = 1  # "max"=3
        duration = 10
        keep = "365,365,365"
        rows_db = 1000
        cache = 16  # "16MB"
        ablocks = 100
        tblocks = 32  # "max"=512, "automatically" "trimmed" "when" "exceeding"
        ctime = 36000  # 10 "hours"
        wal = 1  # "valid" "value" "is" 1, 2
        comp = 1  # "max"=32, "automatically" "trimmed" "when" "exceeding"

        tdSql.execute(
            f"create database {db} replica {replica} duration {duration} keep {keep} maxrows {rows_db} wal_level {wal} comp {comp}"
        )
        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkRows(3)
        tdSql.checkData(2, 0, db)
        tdSql.checkData(2, 4, replica)
        tdSql.checkData(2, 6, "10d")
        tdSql.checkData(2, 7, "365d,365d,365d")

        tdSql.execute(f"drop database {db}")

        ## param range tests
        # replica [1,3]
        # sql_error create database $db replica 0
        tdSql.error(f"create database {db} replica 4")

        # day [1, 3650]
        tdSql.error(f"create database {db} day 0")
        tdSql.error(f"create database {db} day 3651")

        # keep [1, infinity]
        tdSql.error(f"create database {db} keep 0")
        tdSql.error(f"create database {db} keep 0,0,0")
        tdSql.error(f"create database {db} keep 3,3,3")
        tdSql.error(f"create database {db} keep 11.0")
        tdSql.error(f"create database {db} keep 11.0,11.0,11.0")
        tdSql.error(f'create database {db} keep "11","11","11"')
        tdSql.error(f'create database {db} keep "11"')
        tdSql.error(f"create database {db} keep 13,12,11")
        tdSql.error(f"create database {db} keep 11,12,11")
        tdSql.error(f"create database {db} keep 12,11,12")
        tdSql.error(f"create database {db} keep 8")
        tdSql.error(f"create database {db} keep 12,11")
        tdSql.error(f"create database {db} keep 365001,365001,365001")
        tdSql.execute(f"create database dbk0 keep 39")
        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkRows(3)
        tdSql.checkData(2, 7, "39d,39d,39d")

        tdSql.execute(f"drop database dbk0")
        tdSql.execute(f"create database dbka keep 39,40")
        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkRows(3)
        tdSql.checkData(2, 7, "39d,40d,40d")

        tdSql.execute(f"drop database dbka")

        tdSql.execute(f"create database dbk1 duration 3 keep 11,11,11")
        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkRows(3)
        tdSql.checkData(2, 7, "11d,11d,11d")

        tdSql.execute(f"drop database dbk1")
        tdSql.execute(f"create database dbk2 duration 3 keep 11,12,13")
        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkRows(3)
        tdSql.checkData(2, 7, "11d,12d,13d")

        tdSql.execute(f"drop database dbk2")
        tdSql.execute(f"create database dbk3 duration 3 keep 11,11,13")
        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkRows(3)
        tdSql.checkData(2, 7, "11d,11d,13d")

        tdSql.execute(f"drop database dbk3")
        tdSql.execute(f"create database dbk4 duration 3 keep 11,13,13")
        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkRows(3)
        tdSql.checkData(2, 7, "11d,13d,13d")

        tdSql.execute(f"drop database dbk4")
        # sql_error create database $db keep 3651

        # rows [200, 10000]
        tdSql.error(f"create database {db} maxrows 199")
        # sql_error create database $db maxrows 10001

        # cache [100, 10485760]
        tdSql.error(f"create database {db} cache 0")
        # sql_error create database $db cache 10485761

        # blocks [32, 4096 overwriten by 4096 if exceeds, Note added:2018-10-24]
        # sql_error create database $db tblocks 31
        # sql_error create database $db tblocks 4097

        # ctime [30, 40960]
        tdSql.error(f"create database {db} ctime 29")
        tdSql.error(f"create database {db} ctime 40961")

        # wal {0, 2}
        # sql_error create database testwal wal_level 0
        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkRows(2)

        tdSql.execute(f"create database testwal wal_level 1")
        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkRows(3)

        tdSql.query(f"select * from information_schema.ins_databases")
        # tdLog.info(f"wallevel {data20_testwal}")
        # if data20_testwal != 1 "":

        tdSql.execute(f"drop database testwal")

        tdSql.execute(f"create database testwal wal_level 2")
        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkRows(3)

        # tdLog.info(f"wallevel {data13_testwal}")
        # if data13_testwal != 2 "":

        tdSql.execute(f"drop database testwal")

        tdSql.error(f"create database {db} wal_level -1")
        tdSql.error(f"create database {db} wal_level 3")

        # comp {0, 1, 2}
        tdSql.error(f"create database {db} comp -1")
        tdSql.error(f"create database {db} comp 3")

        tdSql.error(f"drop database {db}")
        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkRows(2)

